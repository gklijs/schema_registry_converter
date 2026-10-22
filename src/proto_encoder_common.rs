use prost_reflect::prost::Message;
use prost_reflect::{DynamicMessage, FileDescriptor, MessageDescriptor, ReflectMessage};
use serde_json::Value;

use crate::error::SRCError;
use crate::proto_decoder_common::{ReferencedSchemas, to_file_descriptor};
use crate::proto_resolver::{IndexResolver, to_index_bytes};
use crate::schema_registry_common::{
    RegisteredSchema, SchemaIdHeader, SchemaType, get_payload, schema_id_header_for,
};

/// The compiled schema a subject resolved to, to encode messages with.
#[derive(Debug)]
pub(crate) struct EncodeContext {
    id: u32,
    guid: Option<String>,
    file_descriptor: FileDescriptor,
    indexes: IndexResolver,
}

pub(crate) fn to_encode_context(
    registered_schema: RegisteredSchema,
    referenced_schemas: ReferencedSchemas,
) -> Result<EncodeContext, SRCError> {
    if registered_schema.schema_type != SchemaType::Protobuf {
        return Err(SRCError::non_retryable_without_cause(&format!(
            "schema with id {} is not a protobuf schema but {:?}",
            registered_schema.id, registered_schema.schema_type
        )));
    }
    let file_descriptor = to_file_descriptor(&registered_schema.schema, referenced_schemas)?;
    let indexes = IndexResolver::from_file(file_descriptor.file_descriptor_proto());
    Ok(EncodeContext {
        id: registered_schema.id,
        guid: registered_schema.guid,
        file_descriptor,
        indexes,
    })
}

impl EncodeContext {
    /// The descriptor of a message declared in the schema, to create a [`DynamicMessage`] with.
    pub(crate) fn message_descriptor(
        &self,
        full_name: &str,
    ) -> Result<MessageDescriptor, SRCError> {
        self.index(full_name)?;
        self.file_descriptor
            .parent_pool()
            .get_message_by_name(full_name)
            .ok_or_else(|| not_in_schema(full_name))
    }

    /// The message in the canonical protobuf JSON mapping, as the message `full_name`.
    pub(crate) fn message_from_json(
        &self,
        value: &Value,
        full_name: &str,
    ) -> Result<DynamicMessage, SRCError> {
        let descriptor = self.message_descriptor(full_name)?;
        DynamicMessage::deserialize(descriptor, value).map_err(|e| {
            SRCError::non_retryable_with_cause(
                e,
                &format!("could not create protobuf message {} from json", full_name),
            )
        })
    }

    /// The message prefixed with the schema id and its message index, the confluent wire format.
    pub(crate) fn encode(&self, message: &DynamicMessage) -> Result<Vec<u8>, SRCError> {
        let mut payload = self.index(message.descriptor().full_name())?;
        message.encode(&mut payload).map_err(|e| {
            SRCError::non_retryable_with_cause(e, "could not encode protobuf message")
        })?;
        Ok(get_payload(self.id, payload))
    }

    /// The message without prefix, with a header carrying the schema guid and message index.
    pub(crate) fn encode_with_header_id(
        &self,
        message: &DynamicMessage,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let index = self.index(message.descriptor().full_name())?;
        let header = schema_id_header_for(self.guid.as_deref(), is_key, &index)?;
        Ok((message.encode_to_vec(), header))
    }

    /// The message is found by its full name, so a message created with a descriptor from
    /// another source than [`EncodeContext::message_descriptor`] can be encoded too, as long as
    /// the schema declares a message with that name.
    fn index(&self, full_name: &str) -> Result<Vec<u8>, SRCError> {
        match self.indexes.find_index(full_name) {
            Some(index) => Ok(to_index_bytes(&index)),
            None => Err(not_in_schema(full_name)),
        }
    }
}

fn not_in_schema(full_name: &str) -> SRCError {
    SRCError::non_retryable_without_cause(&format!(
        "could not find message {} in the protobuf schema",
        full_name
    ))
}

#[cfg(test)]
mod tests {
    use crate::proto_decoder_common::ReferencedSchemas;
    use crate::proto_encoder_common::to_encode_context;
    use crate::schema_registry_common::{RegisteredSchema, SchemaType};
    use prost_reflect::{DynamicMessage, ReflectMessage, Value};
    use serde_json::json;

    fn registered(schema: &str, schema_type: SchemaType) -> RegisteredSchema {
        RegisteredSchema {
            id: 7,
            schema_type,
            schema: schema.to_owned(),
            references: Vec::new(),
            properties: None,
            tags: None,
            subject: None,
            version: None,
            guid: Some(String::from("cc0e0e0e-53c1-4a1a-8f1a-000000000001")),
        }
    }

    const SCHEMA: &str = "syntax = \"proto3\"; package a; message A { uint64 beat = 1; } message B { message C { map<string, int64> counts = 1; } }";

    #[test]
    fn test_encode_first_and_nested_message() {
        let context = to_encode_context(
            registered(SCHEMA, SchemaType::Protobuf),
            ReferencedSchemas::new(),
        )
        .unwrap();
        let mut a = DynamicMessage::new(context.message_descriptor("a.A").unwrap());
        a.set_field_by_name("beat", Value::U64(101));
        assert_eq!(context.encode(&a).unwrap(), vec![0, 0, 0, 0, 7, 0, 8, 101]);

        // index [1, 0]: count 2, then 1 and 0, all zigzag varints
        let c = context
            .message_from_json(&json!({"counts": {"x": "3"}}), "a.B.C")
            .unwrap();
        let encoded = context.encode(&c).unwrap();
        assert_eq!(&encoded[..8], &[0, 0, 0, 0, 7, 4, 2, 0]);
        let decoded = DynamicMessage::decode(c.descriptor(), &encoded[8..]).unwrap();
        assert_eq!(decoded, c);
    }

    #[test]
    fn test_encode_with_header_id() {
        let context = to_encode_context(
            registered(SCHEMA, SchemaType::Protobuf),
            ReferencedSchemas::new(),
        )
        .unwrap();
        let a = context
            .message_from_json(&json!({"beat": 101}), "a.A")
            .unwrap();
        let (bytes, header) = context.encode_with_header_id(&a, false).unwrap();
        assert_eq!(bytes, vec![8, 101]);
        assert_eq!(header.name, "__value_schema_id");
        assert_eq!(
            header.value,
            vec![
                0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01, 0x00
            ]
        );
    }

    #[test]
    fn test_message_not_in_schema_gives_error() {
        let context = to_encode_context(
            registered(SCHEMA, SchemaType::Protobuf),
            ReferencedSchemas::new(),
        )
        .unwrap();
        let err = context.message_descriptor("a.Missing").unwrap_err();
        assert_eq!(
            err.error,
            "could not find message a.Missing in the protobuf schema"
        );
        // a message from another schema with a name this schema doesn't declare
        let other = to_encode_context(
            registered(
                "syntax = \"proto3\"; message Other { int32 x = 1; }",
                SchemaType::Protobuf,
            ),
            ReferencedSchemas::new(),
        )
        .unwrap();
        let message = DynamicMessage::new(other.message_descriptor("Other").unwrap());
        assert!(context.encode(&message).is_err());
    }

    #[test]
    fn test_invalid_json_gives_error() {
        let context = to_encode_context(
            registered(SCHEMA, SchemaType::Protobuf),
            ReferencedSchemas::new(),
        )
        .unwrap();
        let err = context
            .message_from_json(&json!({"beat": "not a number"}), "a.A")
            .unwrap_err();
        assert_eq!(err.error, "could not create protobuf message a.A from json");
    }

    #[test]
    fn test_non_protobuf_schema_gives_error() {
        let err = to_encode_context(
            registered("{\"type\":\"string\"}", SchemaType::Avro),
            ReferencedSchemas::new(),
        )
        .unwrap_err();
        assert_eq!(
            err.error,
            "schema with id 7 is not a protobuf schema but Avro"
        );
    }
}
