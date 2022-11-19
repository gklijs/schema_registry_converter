use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::Arc;

use crate::blocking::schema_registry::{
    get_schema_by_id_and_type, get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::proto_raw_common::{
    to_bytes, to_bytes_single_message, to_decode_context, DecodeContext, EncodeContext,
};
use crate::proto_resolver::{resolve_name, to_index_and_data, IndexResolver};
use crate::schema_registry_common::{
    get_bytes_result, get_subject, BytesResult, RegisteredSchema, SchemaType, SubjectNameStrategy,
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
        let key = get_subject(subject_name_strategy)?;
        match self.encoding_context(key, subject_name_strategy) {
            Ok(encode_context) => to_bytes(&encode_context, bytes, full_name),
            Err(e) => Err(e),
        }
    }

    pub fn encode_single_message(
        &self,
        bytes: &[u8],
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = get_subject(subject_name_strategy)?;
        match self.encoding_context(key, subject_name_strategy) {
            Ok(encode_context) => to_bytes_single_message(&encode_context, bytes),
            Err(e) => Err(e),
        }
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
                        resolver: IndexResolver::new(&*registered_schema.schema),
                    })),
                    Err(e) => Err(e.into_cache()),
                };
                e.insert(v).value().clone()
            }
        }
    }
}

#[derive(Debug)]
pub struct ProtoRawDecoder {
    sr_settings: SrSettings,
    cache: DashMap<u32, Result<Arc<DecodeContext>, SRCError>>,
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
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Reads the bytes to get the name, and gives back the data bytes.
    pub fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<RawDecodeResult>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => Ok(Some(self.deserialize(id, &bytes)?)),
            BytesResult::Invalid(i) => Err(SRCError::non_retryable_without_cause(&*format!(
                "Invalid bytes {:?}",
                i
            ))),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<RawDecodeResult, SRCError> {
        match self.context(id) {
            Ok(s) => {
                let schema = &s.schema;
                let (index, data) = to_index_and_data(bytes);
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
}

#[derive(Debug)]
pub struct RawDecodeResult {
    pub schema: RegisteredSchema,
    pub full_name: Arc<String>,
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use mockito::{mock, server_address};

    use crate::blocking::proto_raw::{ProtoRawDecoder, ProtoRawEncoder};
    use crate::blocking::schema_registry::SrSettings;
    use crate::schema_registry_common::{
        SchemaType, SubjectNameStrategy, SuppliedReference, SuppliedSchema,
    };
    use test_utils::{
        get_proto_body, get_proto_body_with_reference, get_proto_complex,
        get_proto_complex_only_data, get_proto_complex_proto_test_message,
        get_proto_complex_references, get_proto_hb_101, get_proto_hb_101_only_data,
        get_proto_hb_schema, get_proto_result,
    };

    #[test]
    fn test_encode_default() {
        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
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
        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
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
        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_complex(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let encoder = ProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        assert!(encoder
            .encode_single_message(get_proto_complex_only_data(), &strategy,)
            .is_err())
    }

    #[test]
    fn test_encode_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
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

        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
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
        let _m = mock("POST", "/subjects/result.proto/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_result(), 5))
            .create();

        let _m = mock("POST", "/subjects/result.proto?deleted=false")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body("{\"version\":1}")
            .create();

        let _m = mock("POST", "/subjects/test.proto/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_result(), 6))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let encoder = ProtoRawEncoder::new(sr_settings);
        let result_reference = SuppliedReference {
            name: String::from("result.proto"),
            subject: String::from("result.proto"),
            schema: String::from(get_proto_result()),
            references: vec![],
        };
        let supplied_schema = SuppliedSchema {
            name: Some(String::from("test.proto")),
            schema_type: SchemaType::Protobuf,
            schema: String::from(get_proto_complex()),
            references: vec![result_reference],
        };
        let strategy =
            SubjectNameStrategy::RecordNameStrategyWithSchema(Box::from(supplied_schema));

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
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoRawEncoder::new(sr_settings);
        assert_eq!(
            "ProtoRawEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {} }"
                .to_owned(),
            format!("{:?}", decoder)
        )
    }

    #[test]
    fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
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
    fn test_decoder_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoRawDecoder::new(sr_settings);

        let error = decoder.decode(Some(get_proto_hb_101())).unwrap_err();
        assert!(error.cached);

        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
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
        let _m = mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m = mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
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
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoRawDecoder::new(sr_settings);
        assert_eq!(
            "ProtoRawDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {} }"
                .to_owned(),
            format!("{:?}", decoder)
        )
    }
}
