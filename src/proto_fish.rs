use crate::proto::MessageResolver;
use crate::schema_registry::{
    get_bytes_result, get_referenced_schema, get_schema_by_id_and_type, BytesResult,
    RegisteredSchema, SRCError, SchemaType,
};
use bytes::Bytes;
use protofish::{Context, MessageValue, Value};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;

#[derive(Debug)]
struct ResolveContext {
    resolver: MessageResolver,
    context: Context,
}

#[derive(Debug)]
pub struct ProtoDecoder {
    schema_registry_url: String,
    cache: &'static mut HashMap<u32, Result<ResolveContext, SRCError>, RandomState>,
}

impl ProtoDecoder {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. It's possible for recoverable errors to stay in the cash, when a result
    /// comes back as an error you can use remove_errors_from_cache to clean the cache, keeping the
    /// correctly fetched schema's
    pub fn new(schema_registry_url: String) -> ProtoDecoder {
        let new_cache = Box::new(HashMap::new());
        ProtoDecoder {
            schema_registry_url,
            cache: Box::leak(new_cache),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a mut
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(Value::Bytes(Bytes::new())),
            BytesResult::Valid(id, bytes) => {
                Ok(Value::Message(Box::from(self.deserialize(id, &bytes)?)))
            }
            BytesResult::Invalid(i) => Ok(Value::Bytes(Bytes::from(i))),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&mut self, id: u32, bytes: &[u8]) -> Result<MessageValue, SRCError> {
        match self.get_context(id) {
            Ok(s) => {
                let (index, data) = to_index_and_data(bytes);
                let full_name = match s.resolver.find_name(index) {
                    Some(n) => n,
                    None => {
                        return Err(SRCError::non_retryable_without_cause(&*format!(
                            "Could not retrieve name for index: {:?}",
                            index
                        )))
                    }
                };
                let message_info = s.context.get_message(full_name).unwrap();
                Ok(message_info.decode(data, &s.context))
            }
            Err(e) => Err(Clone::clone(e)),
        }
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn get_context(&mut self, id: u32) -> &mut Result<ResolveContext, SRCError> {
        let schema_registry_url = &self.schema_registry_url;
        self.cache.entry(id).or_insert_with(|| {
            match get_schema_by_id_and_type(id, schema_registry_url, SchemaType::Protobuf) {
                Ok(registered_schema) => to_resolve_context(schema_registry_url, registered_schema),
                Err(e) => Err(e.into_cache()),
            }
        })
    }
}

fn add_files(
    schema_registry_url: &str,
    registered_schema: RegisteredSchema,
    files: &mut Vec<String>,
) -> Result<(), SRCError> {
    for r in registered_schema.references {
        let child_schema = match get_referenced_schema(schema_registry_url, &r) {
            Ok(v) => v,
            Err(e) => {
                return Err(SRCError::retryable_with_cause(
                    e,
                    "Could not get child schema",
                ))
            }
        };
        add_files(schema_registry_url, child_schema, files)?;
    }
    files.push(registered_schema.schema);
    Ok(())
}

fn to_resolve_context(
    schema_registry_url: &str,
    registered_schema: RegisteredSchema,
) -> Result<ResolveContext, SRCError> {
    let resolver = MessageResolver::new(&registered_schema.schema);
    let mut files = Vec::new();
    add_files(schema_registry_url, registered_schema, &mut files)?;
    match Context::parse(&files) {
        Ok(context) => Ok(ResolveContext { resolver, context }),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "Error creating proto context",
        )),
    }
}

fn to_index_and_data(bytes: &[u8]) -> (&[u8], &[u8]) {
    match bytes[0] {
        0 => (&[0], &bytes[1..]),
        i => {
            let count = i as usize;
            (&bytes[1..count], &bytes[count + 1..])
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::proto_fish::ProtoDecoder;
    use mockito::{mock, server_address};
    use protofish::Value;

    fn get_proto_hb_schema() -> &'static str {
        r#"syntax = \"proto3\";package nl.openweb.data;message Heartbeat {uint64 beat = 1;}"#
    }

    fn get_proto_complex() -> &'static str {
        r#"syntax = \"proto3\"; import \"result.proto\"; message A {bytes id = 1;} message B {bytes id = 1;} message C {bytes id = 1; D d = 2; message D {int64 counter = 1;}} package org.schema_registry_test_app.proto; message ProtoTest {bytes id = 1; enum Language {Java = 0;Rust = 1;} Language by = 2;int64 counter = 3;string input = 4;repeated A results = 5;}"#
    }

    fn get_proto_hb_101() -> &'static [u8] {
        &[0, 0, 0, 0, 7, 0, 8, 101]
    }

    fn get_proto_body(schema: &str) -> String {
        format!("{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\"}}", schema)
    }

    #[test]
    fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/7")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema()))
            .create();

        let mut decoder = ProtoDecoder::new(format!("http://{}", server_address()));
        let heartbeat = decoder.decode(Some(get_proto_hb_101()));

        let message = match heartbeat {
            Ok(Value::Message(x)) => *x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }
}
