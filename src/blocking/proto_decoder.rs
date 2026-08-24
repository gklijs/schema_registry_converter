use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::blocking::schema_registry::{
    get_referenced_schema, get_schema_by_guid_and_type, get_schema_by_id_and_type, SrSettings,
};
use crate::error::SRCError;
use crate::proto_common_types::add_common_files;
use crate::proto_resolver::{resolve_name, to_index_and_data, MessageResolver};
use crate::schema_registry_common::{
    get_bytes_result, parse_schema_id_header, BytesResult, HeaderSchemaId, RegisteredSchema,
    SchemaType,
};
use protofish::context::Context;
use protofish::decode::{MessageValue, Value};

#[derive(Debug)]
pub struct ProtoDecoder {
    sr_settings: SrSettings,
    cache: DashMap<u32, Result<Arc<DecodeContext>, SRCError>>,
    /// Cache for schemas looked up by guid rather than id, used by
    /// [`ProtoDecoder::decode_with_header_id`]. Kept separate from `cache` (keyed by id) since
    /// a guid and an id are different keyspaces.
    guid_cache: DashMap<String, Result<Arc<DecodeContext>, SRCError>>,
}

impl ProtoDecoder {
    /// Creates a new decoder which will use the supplied url used in creating the sr settings to
    /// fetch the schema's since the schema needed is encoded in the binary, independent of the
    /// SubjectNameStrategy we don't need any additional data. Retriable errors (e.g. a transient
    /// network/HTTP failure) are never cached, so those are simply retried on the next call.
    /// Non-retriable errors (e.g. a schema that doesn't exist or can't be parsed) are cached to
    /// avoid repeatedly retrying a lookup that isn't going to succeed; if the underlying problem
    /// gets fixed you can use remove_errors_from_cache to clean those out.
    pub fn new(sr_settings: SrSettings) -> ProtoDecoder {
        ProtoDecoder {
            sr_settings,
            cache: DashMap::new(),
            guid_cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache. You might need/want to run this when the
    /// underlying problem has been fixed (e.g. a schema was missing and has since been
    /// registered) and want the next call to try again immediately. Retriable errors aren't
    /// stored in the cache in the first place, so there's nothing to remove for those.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
        self.guid_cache.retain(|_, v| v.is_ok());
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub fn decode(&self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(Value::Bytes(Bytes::new())),
            BytesResult::Valid(id, bytes) => {
                Ok(Value::Message(Box::from(self.deserialize(id, bytes)?)))
            }
            BytesResult::Invalid(i) => Ok(Value::Bytes(Bytes::copy_from_slice(i))),
        }
    }
    /// Like [`ProtoDecoder::decode`], but for a message that may carry its schema id/guid and
    /// message index in a `__key_schema_id`/`__value_schema_id` header instead of (or in
    /// addition to) the payload prefix, mirroring Confluent's `DualSchemaIdDeserializer`:
    /// `header_value` present -> resolve the schema and message index from it and treat `bytes`
    /// as the raw (unprefixed) payload; `header_value` absent -> falls straight through to
    /// [`ProtoDecoder::decode`], so the only overhead for a caller who always passes `None` is
    /// this one check. See https://github.com/gklijs/schema_registry_converter/issues/139.
    pub fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Value, SRCError> {
        let payload = match bytes {
            Some(v) => v,
            None => return Ok(Value::Bytes(Bytes::new())),
        };
        match header_value {
            None => self.decode(Some(payload)),
            Some(header) => {
                let (schema_id, index_bytes) = parse_schema_id_header(header)?;
                let context = match schema_id {
                    HeaderSchemaId::Id(id) => self.context(id)?,
                    HeaderSchemaId::Guid(guid) => self.guid_context(guid)?,
                };
                let (index, _empty) = to_index_and_data(index_bytes)?;
                let full_name = resolve_name(&context.resolver, &index)?;
                let message_info = context.context.get_message(&full_name).unwrap();
                Ok(Value::Message(Box::from(
                    message_info.decode(payload, &context.context),
                )))
            }
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<MessageValue, SRCError> {
        match self.context(id) {
            Ok(s) => {
                let (index, data) = to_index_and_data(bytes)?;
                let full_name = resolve_name(&s.resolver, &index)?;
                let message_info = s.context.get_message(&full_name).unwrap();
                Ok(message_info.decode(&data, &s.context))
            }
            Err(e) => Err(e),
        }
    }
    /// Decodes bytes into a decode result.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub fn decode_with_context(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResultWithContext>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => match self.deserialize_with_context(id, bytes) {
                Ok(v) => Ok(Some(v)),
                Err(e) => Err(e),
            },
            BytesResult::Invalid(_) => {
                Err(SRCError::new("no protobuf compatible bytes", None, false))
            }
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize_with_context(
        &self,
        id: u32,
        bytes: &[u8],
    ) -> Result<DecodeResultWithContext, SRCError> {
        match self.context(id) {
            Ok(s) => {
                let (index, data_bytes) = to_index_and_data(bytes)?;
                let full_name = resolve_name(&s.resolver, &index)?;
                let message_info = s.context.get_message(&full_name).unwrap();
                let value = message_info.decode(&data_bytes, &s.context);
                Ok(DecodeResultWithContext {
                    value,
                    context: s.clone(),
                    full_name,
                    data_bytes,
                })
            }
            Err(e) => Err(e),
        }
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn context(&self, id: u32) -> Result<Arc<DecodeContext>, SRCError> {
        match self.cache.entry(id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let result =
                    match get_schema_by_id_and_type(id, &self.sr_settings, SchemaType::Protobuf) {
                        Ok(v) => to_resolve_context(&self.sr_settings, v),
                        Err(e) => Err(e),
                    };
                match result {
                    // Retriable errors (transient network/HTTP issues) don't make sense to
                    // cache, so the entry is left vacant and the next call issues a fresh
                    // request rather than replaying a stale failure. Non-retriable errors,
                    // e.g. a parse failure, are cached permanently. Unlike the async decoder,
                    // this caches the fully-parsed Context rather than re-parsing on every
                    // decode call — see https://github.com/gklijs/schema_registry_converter/issues/175.
                    Err(e) if e.retriable => Err(e),
                    result => entry
                        .insert(result.map_err(SRCError::into_cache))
                        .value()
                        .clone(),
                }
            }
        }
    }

    /// Like [`ProtoDecoder::context`], but looks the schema up by guid instead of id -- used by
    /// [`ProtoDecoder::decode_with_header_id`] when the header carries a guid rather than an id.
    fn guid_context(&self, guid: String) -> Result<Arc<DecodeContext>, SRCError> {
        match self.guid_cache.entry(guid.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let result = match get_schema_by_guid_and_type(
                    &guid,
                    &self.sr_settings,
                    SchemaType::Protobuf,
                ) {
                    Ok(v) => to_resolve_context(&self.sr_settings, v),
                    Err(e) => Err(e),
                };
                match result {
                    Err(e) if e.retriable => Err(e),
                    result => entry
                        .insert(result.map_err(SRCError::into_cache))
                        .value()
                        .clone(),
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct DecodeResultWithContext {
    pub value: MessageValue,
    pub context: Arc<DecodeContext>,
    pub full_name: Arc<String>,
    pub data_bytes: Vec<u8>,
}

fn add_files(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
    files: &mut Vec<String>,
) -> Result<(), SRCError> {
    for r in registered_schema.references {
        let child_schema = get_referenced_schema(sr_settings, &r)?;
        add_files(sr_settings, child_schema, files)?;
    }
    files.push(registered_schema.schema);
    Ok(())
}

#[derive(Debug)]
pub struct DecodeContext {
    pub resolver: MessageResolver,
    pub context: Context,
    pub registered_schema: RegisteredSchema,
}

fn to_resolve_context(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
) -> Result<Arc<DecodeContext>, SRCError> {
    let mut vec_of_schemas = Vec::new();
    add_files(sr_settings, registered_schema.clone(), &mut vec_of_schemas)?;
    // The root schema is always pushed last by add_files, so pop it off rather than
    // re-parsing it below with the other, dependent schemas.
    let root_schema = vec_of_schemas.pop().unwrap();
    let resolver = MessageResolver::new(&root_schema);
    let mut files: HashSet<String> = HashSet::new();
    add_common_files(resolver.imports(), &mut files);
    for s in vec_of_schemas {
        let dependent_resolver = MessageResolver::new(&s);
        add_common_files(dependent_resolver.imports(), &mut files);
        files.insert(s);
    }
    files.insert(root_schema);
    match Context::parse(&files) {
        Ok(context) => Ok(Arc::new(DecodeContext {
            resolver,
            context,
            registered_schema,
        })),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "Error creating proto context",
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::blocking::proto_decoder::ProtoDecoder;
    use crate::blocking::schema_registry::SrSettings;
    use protofish::decode::Value;
    use test_utils::{
        get_proto_body, get_proto_body_with_reference, get_proto_complex,
        get_proto_complex_proto_test_message, get_proto_complex_references, get_proto_hb_101,
        get_proto_hb_101_empty_payload, get_proto_hb_schema, get_proto_money_result,
        get_proto_result,
    };

    #[test]
    fn test_decoder_default() {
        let mut server = mockito::Server::new();

        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101()));

        let message = match heartbeat {
            Ok(Value::Message(x)) => *x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }

    #[test]
    fn test_decode_with_header_id() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/schemas/guids/cc0e0e0e-53c1-4a1a-8f1a-000000000001")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        // magic byte 0x01 + 16-byte guid + single-message index byte 0x00
        let header_value = [
            0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x00,
        ];
        let payload = &get_proto_hb_101()[6..]; // data only, no prefix/index

        let heartbeat = decoder
            .decode_with_header_id(Some(&header_value), Some(payload))
            .unwrap();
        let message = match heartbeat {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(Value::UInt64(101u64), message.fields[0].value);

        // header absent -> falls straight through to decode(), which expects the prefix
        let _m2 = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();
        let heartbeat = decoder
            .decode_with_header_id(None, Some(get_proto_hb_101()))
            .unwrap();
        let message = match heartbeat {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(Value::UInt64(101u64), message.fields[0].value);
    }

    #[test]
    fn test_decoder_five_byte_message_gives_error_instead_of_panic() {
        let mut server = mockito::Server::new();

        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let result = decoder.decode(Some(get_proto_hb_101_empty_payload()));

        assert!(result.is_err())
    }

    #[test]
    fn test_decode_with_contxt_default() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode_with_context(Some(get_proto_hb_101()))
            .unwrap();

        assert!(heartbeat.is_some());

        let message = heartbeat.unwrap().value;

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }

    #[test]
    fn test_decoder_cache() {
        let mut server = mockito::Server::new();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let error = decoder.decode(Some(get_proto_hb_101())).unwrap_err();

        assert!(error.cached);

        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let error = decoder.decode(Some(get_proto_hb_101())).unwrap_err();

        assert!(error.cached);

        decoder.remove_errors_from_cache();

        let message = match decoder.decode(Some(get_proto_hb_101())).unwrap() {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value);
    }

    #[test]
    fn test_decoder_complex() {
        let mut server = mockito::Server::new();

        let _m1 = server
            .mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m2 = server
            .mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let proto_test = decoder.decode(Some(get_proto_complex_proto_test_message()));

        let message = match proto_test {
            Ok(Value::Message(x)) => *x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(message.fields[1].value, Value::Int64(1))
    }

    #[test]
    fn test_decoder_complex_with_common_type_import_on_reference() {
        // Regression test for https://github.com/gklijs/schema_registry_converter/issues/178:
        // the *referenced* schema (not the top-level one) imports a well-known/common type.
        let mut server = mockito::Server::new();

        let _m1 = server
            .mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m2 = server
            .mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_money_result(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let proto_test = decoder.decode(Some(get_proto_complex_proto_test_message()));

        let message = match proto_test {
            Ok(Value::Message(x)) => *x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(message.fields[1].value, Value::Int64(1))
    }

    #[test]
    fn display_decoder() {
        let sr_settings = SrSettings::new("http://127.0.0.1:1234".to_string());
        let decoder = ProtoDecoder::new(sr_settings);
        assert_eq!(
            "ProtoDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {}, guid_cache: {} }"
                .to_owned(),
            format!("{:?}", decoder)
        )
    }
}
