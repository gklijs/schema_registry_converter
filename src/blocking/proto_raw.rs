use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::Arc;

use crate::blocking::schema_registry::{
    get_schema_by_guid_and_type, get_schema_by_id_and_type, get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::proto_raw_common::{
    index_bytes, index_bytes_single_message, to_bytes, to_bytes_single_message, to_decode_context,
    DecodeContext, EncodeContext,
};
use crate::proto_resolver::{resolve_name, to_index_and_data, IndexResolver};
use crate::schema_registry_common::{
    build_schema_id_header, get_bytes_result, invalid_bytes_error, parse_schema_id_header,
    BytesResult, HeaderSchemaId, RegisteredSchema, SchemaIdHeader, SchemaType, SubjectNameStrategy,
    KEY_SCHEMA_ID_HEADER, VALUE_SCHEMA_ID_HEADER,
};

/// Encoder that works by prepending the correct bytes in order to make it valid schema registry
/// bytes. Ideally you want to make sure the bytes are based on the exact schema used for encoding
/// but you need a protobuf struct that has introspection to make that work, and both protobuf and
/// prost don't support that at the moment.
#[derive(Debug)]
pub struct ProtoRawEncoder {
    sr_settings: SrSettings,
    cache: DashMap<String, Result<Arc<EncodeContext>, SRCError>>,
}

impl ProtoRawEncoder {
    /// Creates a new encoder
    pub fn new(sr_settings: SrSettings) -> ProtoRawEncoder {
        ProtoRawEncoder {
            sr_settings,
            cache: DashMap::new(),
        }
    }
    /// Removes errors from the cache, can be useful to retry failed encodings.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Encodes the bytes by adding a few bytes to the message with additional information. The full
    /// names is the optional package followed with the message name, and optionally inner messages.
    pub fn encode(
        &self,
        bytes: &[u8],
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        match self.encoding_context(key, subject_name_strategy) {
            Ok(encode_context) => to_bytes(&encode_context, bytes, full_name),
            Err(e) => Err(e),
        }
    }

    /// Like [`ProtoRawEncoder::encode`], but instead of prefixing the payload with the schema id
    /// (the default confluent wire format), returns the raw protobuf bytes alongside a
    /// [`SchemaIdHeader`] carrying the schema's guid and the message index. Attach the header to
    /// the Kafka record using whatever Kafka client you're using -- this crate has no dependency
    /// on one. `is_key` picks between the `__key_schema_id`/`__value_schema_id` header names,
    /// matching Confluent's `HeaderSchemaIdSerializer`. See
    /// https://github.com/gklijs/schema_registry_converter/issues/139.
    ///
    /// Requires a schema registry that returns a `guid` (Confluent Schema Registry 8.0+); a
    /// registry that doesn't will make this return an error.
    pub fn encode_with_header_id(
        &self,
        bytes: &[u8],
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let key = subject_name_strategy.get_subject()?;
        let encode_context = self.encoding_context(key, subject_name_strategy)?;
        let index = index_bytes(&encode_context, full_name)?;
        let header = schema_id_header_for(&encode_context, &index, is_key)?;
        Ok((bytes.to_vec(), header))
    }

    pub fn encode_single_message(
        &self,
        bytes: &[u8],
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        match self.encoding_context(key, subject_name_strategy) {
            Ok(encode_context) => to_bytes_single_message(&encode_context, bytes),
            Err(e) => Err(e),
        }
    }

    /// Like [`ProtoRawEncoder::encode_single_message`], but returns the schema id/guid as a
    /// [`SchemaIdHeader`] instead of prefixing the payload with it. See
    /// [`ProtoRawEncoder::encode_with_header_id`].
    pub fn encode_single_message_with_header_id(
        &self,
        bytes: &[u8],
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let key = subject_name_strategy.get_subject()?;
        let encode_context = self.encoding_context(key, subject_name_strategy)?;
        let index = index_bytes_single_message(&encode_context)?;
        let header = schema_id_header_for(&encode_context, &index, is_key)?;
        Ok((bytes.to_vec(), header))
    }

    fn encoding_context(
        &self,
        key: String,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Arc<EncodeContext>, SRCError> {
        match self.cache.entry(key) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_subject(&self.sr_settings, subject_name_strategy) {
                    Ok(registered_schema) => Ok(Arc::new(EncodeContext {
                        id: registered_schema.id,
                        guid: registered_schema.guid,
                        resolver: IndexResolver::new(&registered_schema.schema),
                    })),
                    Err(e) => Err(e.into_cache()),
                };
                e.insert(v).value().clone()
            }
        }
    }
}

/// Builds the [`SchemaIdHeader`] for `encode_context`, requiring it to carry a guid (populated
/// when the schema registry response included one, i.e. Confluent Schema Registry 8.0+),
/// appending `index` (the message index, unlike Avro/JSON which have none) after the guid.
fn schema_id_header_for(
    encode_context: &EncodeContext,
    index: &[u8],
    is_key: bool,
) -> Result<SchemaIdHeader, SRCError> {
    let guid = encode_context.guid.as_deref().ok_or_else(|| {
        SRCError::non_retryable_without_cause(
            "Schema registry response did not include a guid; encoding the schema id in a \
             header requires Confluent Schema Registry 8.0+",
        )
    })?;
    let name = if is_key {
        KEY_SCHEMA_ID_HEADER
    } else {
        VALUE_SCHEMA_ID_HEADER
    };
    build_schema_id_header(name, guid, index)
}

#[derive(Debug)]
pub struct ProtoRawDecoder {
    sr_settings: SrSettings,
    cache: DashMap<u32, Result<Arc<DecodeContext>, SRCError>>,
    /// Cache for schemas looked up by guid rather than id, used by
    /// [`ProtoRawDecoder::decode_with_header_id`]. Kept separate from `cache` (keyed by id)
    /// since a guid and an id are different keyspaces.
    guid_cache: DashMap<String, Result<Arc<DecodeContext>, SRCError>>,
}

impl ProtoRawDecoder {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. It's possible for recoverable errors to stay in the cache, when a result
    /// comes back as an error you can use remove_errors_from_cache to clean the cache, keeping the
    /// correctly fetched schema's
    pub fn new(sr_settings: SrSettings) -> ProtoRawDecoder {
        ProtoRawDecoder {
            sr_settings,
            cache: DashMap::new(),
            guid_cache: DashMap::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
        self.guid_cache.retain(|_, v| v.is_ok());
    }
    /// Reads the bytes to get the name, and gives back the data bytes.
    pub fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<RawDecodeResult>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => Ok(Some(self.deserialize(id, bytes)?)),
            BytesResult::Invalid(i) => Err(SRCError::non_retryable_without_cause(
                &invalid_bytes_error(i),
            )),
        }
    }
    /// Like [`ProtoRawDecoder::decode`], but for a message that may carry its schema id/guid and
    /// message index in a `__key_schema_id`/`__value_schema_id` header instead of (or in
    /// addition to) the payload prefix, mirroring Confluent's `DualSchemaIdDeserializer`:
    /// `header_value` present -> resolve the schema and message index from it and treat `bytes`
    /// as the raw (unprefixed) payload; `header_value` absent -> falls straight through to
    /// [`ProtoRawDecoder::decode`], so the only overhead for a caller who always passes `None`
    /// is this one check. See https://github.com/gklijs/schema_registry_converter/issues/139.
    pub fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Option<RawDecodeResult>, SRCError> {
        let payload = match bytes {
            Some(v) => v,
            None => return Ok(None),
        };
        match header_value {
            None => self.decode(Some(payload)),
            Some(header) => {
                let (schema_id, index_bytes) = parse_schema_id_header(header)?;
                let context = match schema_id {
                    HeaderSchemaId::Id(id) => self.context(id)?,
                    HeaderSchemaId::Guid(guid) => self.context_by_guid(guid)?,
                };
                let (index, _empty) = to_index_and_data(index_bytes)?;
                let full_name = resolve_name(&context.resolver, &index)?;
                Ok(Some(RawDecodeResult {
                    schema: context.schema.clone(),
                    full_name,
                    bytes: payload.to_vec(),
                }))
            }
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<RawDecodeResult, SRCError> {
        match self.context(id) {
            Ok(s) => {
                let schema = &s.schema;
                let (index, data) = to_index_and_data(bytes)?;
                let full_name = resolve_name(&s.resolver, &index)?;
                Ok(RawDecodeResult {
                    schema: schema.clone(),
                    full_name,
                    bytes: data,
                })
            }
            Err(e) => Err(e),
        }
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn context(&self, id: u32) -> Result<Arc<DecodeContext>, SRCError> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_id_and_type(id, &self.sr_settings, SchemaType::Protobuf)
                {
                    Ok(r) => Ok(Arc::new(to_decode_context(r))),
                    Err(e) => Err(e.into_cache()),
                };
                e.insert(v).value().clone()
            }
        }
    }

    /// Like [`ProtoRawDecoder::context`], but looks the schema up by guid instead of id -- used
    /// by [`ProtoRawDecoder::decode_with_header_id`] when the header carries a guid rather than
    /// an id.
    fn context_by_guid(&self, guid: String) -> Result<Arc<DecodeContext>, SRCError> {
        match self.guid_cache.entry(guid.clone()) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_guid_and_type(
                    &guid,
                    &self.sr_settings,
                    SchemaType::Protobuf,
                ) {
                    Ok(r) => Ok(Arc::new(to_decode_context(r))),
                    Err(e) => Err(e.into_cache()),
                };
                e.insert(v).value().clone()
            }
        }
    }
}

#[derive(Debug)]
pub struct RawDecodeResult {
    pub schema: RegisteredSchema,
    pub full_name: Arc<String>,
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::blocking::proto_raw::{ProtoRawDecoder, ProtoRawEncoder};
    use crate::blocking::schema_registry::SrSettings;
    use crate::schema_registry_common::{
        SchemaType, SubjectNameStrategy, SuppliedReference, SuppliedSchema, VALUE_SCHEMA_ID_HEADER,
    };
    use test_utils::{
        get_proto_body, get_proto_body_with_reference, get_proto_complex,
        get_proto_complex_only_data, get_proto_complex_proto_test_message,
        get_proto_complex_references, get_proto_hb_101, get_proto_hb_101_empty_payload,
        get_proto_hb_101_only_data, get_proto_hb_schema, get_proto_result,
    };

    #[test]
    fn test_encode_and_decode_with_header_id() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(format!(
                "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":7, \"guid\":\"cc0e0e0e-53c1-4a1a-8f1a-000000000001\"}}",
                get_proto_hb_schema()
            ))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoRawEncoder::new(sr_settings.clone());
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let (bytes, header) = encoder
            .encode_with_header_id(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                &strategy,
                false,
            )
            .unwrap();

        assert_eq!(bytes, get_proto_hb_101_only_data());
        assert_eq!(header.name, VALUE_SCHEMA_ID_HEADER);
        assert_eq!(
            header.value,
            vec![
                0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01, 0x00,
            ]
        );

        let _m2 = server
            .mock("GET", "/schemas/guids/cc0e0e0e-53c1-4a1a-8f1a-000000000001")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let decoder = ProtoRawDecoder::new(sr_settings);
        let decoded = decoder
            .decode_with_header_id(Some(&header.value), Some(&bytes))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.bytes, get_proto_hb_101_only_data());
        assert_eq!(*decoded.full_name, "nl.openweb.data.Heartbeat");

        // header absent -> falls straight through to decode(), which expects the prefix
        let _m3 = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();
        let decoded = decoder
            .decode_with_header_id(None, Some(get_proto_hb_101()))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.bytes, get_proto_hb_101_only_data());
    }

    #[test]
    fn test_encode_default() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let encoded_data = encoder
            .encode(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .unwrap();

        assert_eq!(encoded_data, get_proto_hb_101())
    }

    #[test]
    fn test_encode_single_message() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let encoded_data = encoder
            .encode_single_message(get_proto_hb_101_only_data(), &strategy)
            .unwrap();

        assert_eq!(encoded_data, get_proto_hb_101())
    }

    #[test]
    fn test_encode_single_message_multiple_in_schema() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_complex(), 7))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        assert!(encoder
            .encode_single_message(get_proto_complex_only_data(), &strategy,)
            .is_err())
    }

    #[test]
    fn test_encode_cache() {
        let mut server = mockito::Server::new();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
        let error = encoder
            .encode(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .unwrap_err();
        assert!(error.cached);

        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let error = encoder
            .encode(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .unwrap_err();
        assert!(error.cached);
        encoder.remove_errors_from_cache();

        let encoded_data = encoder
            .encode(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .unwrap();

        assert_eq!(encoded_data, get_proto_hb_101())
    }

    #[test]
    fn test_encode_complex() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("POST", "/subjects/result.proto/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_result(), 5))
            .create();

        let _m = server
            .mock("POST", "/subjects/result.proto?deleted=false")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body("{\"version\":1}")
            .create();

        let _m = server
            .mock("POST", "/subjects/test.proto/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_result(), 6))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoRawEncoder::new(sr_settings);
        let result_reference = SuppliedReference {
            name: String::from("result.proto"),
            subject: String::from("result.proto"),
            schema: String::from(get_proto_result()),
            references: vec![],
            properties: None,
            tags: None,
        };
        let supplied_schema = SuppliedSchema {
            name: Some(String::from("test.proto")),
            schema_type: SchemaType::Protobuf,
            schema: String::from(get_proto_complex()),
            references: vec![result_reference],
            properties: None,
            tags: None,
        };
        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(supplied_schema);

        let encoded_data = encoder
            .encode(
                get_proto_complex_only_data(),
                "org.schema_registry_test_app.proto.ProtoTest",
                &strategy,
            )
            .unwrap();

        assert_eq!(encoded_data, get_proto_complex_proto_test_message())
    }

    #[test]
    fn display_rew_decoder() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = ProtoRawEncoder::new(sr_settings);
        assert_eq!(
            "ProtoRawEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {} }"
                .to_owned(),
            format!("{:?}", decoder)
        )
    }

    #[test]
    fn test_decoder_default() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoRawDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101()));

        let raw_result = match heartbeat {
            Ok(Some(v)) => v,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(raw_result.bytes, get_proto_hb_101_only_data());
        assert_eq!(*raw_result.full_name, "nl.openweb.data.Heartbeat")
    }

    #[test]
    fn test_decoder_five_byte_message_gives_error_instead_of_panic() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoRawDecoder::new(sr_settings);
        let result = decoder.decode(Some(get_proto_hb_101_empty_payload()));

        assert!(result.is_err())
    }

    #[test]
    fn test_decoder_cache() {
        let mut server = mockito::Server::new();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoRawDecoder::new(sr_settings);

        let error = decoder.decode(Some(get_proto_hb_101())).unwrap_err();
        assert!(error.cached);

        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let error = decoder.decode(Some(get_proto_hb_101())).unwrap_err();
        assert!(error.cached);

        decoder.remove_errors_from_cache();

        let raw_result = decoder.decode(Some(get_proto_hb_101())).unwrap().unwrap();

        assert_eq!(raw_result.bytes, get_proto_hb_101_only_data());
        assert_eq!(*raw_result.full_name, "nl.openweb.data.Heartbeat")
    }

    #[test]
    fn test_decoder_complex() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m = server
            .mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoRawDecoder::new(sr_settings);
        let proto_test = decoder.decode(Some(get_proto_complex_proto_test_message()));

        let raw_result = match proto_test {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(raw_result.bytes, get_proto_complex_only_data())
    }

    #[test]
    fn display_decoder() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = ProtoRawDecoder::new(sr_settings);
        assert_eq!(
            "ProtoRawDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {}, guid_cache: {} }"
                .to_owned(),
            format!("{:?}", decoder)
        )
    }
}
