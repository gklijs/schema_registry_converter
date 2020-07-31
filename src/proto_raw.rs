use crate::proto_resolver::{to_index_and_data, IndexResolver, MessageResolver};
use crate::schema_registry::{
    get_bytes_result, get_payload, get_schema_by_id_and_type, get_schema_by_subject, get_subject,
    BytesResult, RegisteredSchema, SRCError, SchemaType, SrSettings, SubjectNameStrategy,
};
use integer_encoding::VarInt;
use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;

/// Encoder that works by prepending the correct bytes in order to make it valid schema registry
/// bytes. Ideally you want to make sure the bytes are based on the exact schema used for encoding
/// but you need a protobuf struct that has introspection to make that work, and both protobuf and
/// prost don't support that at the moment.
#[derive(Debug)]
pub struct ProtoRawEncoder {
    sr_settings: SrSettings,
    cache: HashMap<String, Result<EncodeContext, SRCError>, RandomState>,
}

impl ProtoRawEncoder {
    /// Creates a new encoder
    pub fn new(sr_settings: SrSettings) -> ProtoRawEncoder {
        ProtoRawEncoder {
            sr_settings,
            cache: HashMap::new(),
        }
    }
    /// Removes errors from the cache, can be usefull to retry failed encodings.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Encodes the bytes by adding a few bytes to the message with additional information. The full
    /// names is the optional package followed with the message name, and optionally inner messages.
    pub fn encode(
        &mut self,
        bytes: &[u8],
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = get_subject(subject_name_strategy)?;
        match self.get_encoding_context(key, subject_name_strategy) {
            Ok(encode_context) => to_bytes(encode_context, bytes, full_name),
            Err(e) => Err(Clone::clone(e)),
        }
    }

    fn get_encoding_context(
        &mut self,
        key: String,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> &mut Result<EncodeContext, SRCError> {
        match self.cache.entry(key) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_subject(&self.sr_settings, &subject_name_strategy) {
                    Ok(registered_schema) => Ok(EncodeContext {
                        id: registered_schema.id,
                        resolver: IndexResolver::new(&*registered_schema.schema),
                    }),
                    Err(e) => Err(e.into_cache()),
                };
                e.insert(v)
            }
        }
    }
}

#[derive(Debug)]
pub struct ProtoRawDecoder {
    sr_settings: SrSettings,
    cache: HashMap<u32, Result<DecodeContext, SRCError>, RandomState>,
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
            cache: HashMap::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Reads the bytes to get the name, and gives back the data bytes.
    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Option<RawDecodeResult>, SRCError> {
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
    fn deserialize(&mut self, id: u32, bytes: &[u8]) -> Result<RawDecodeResult, SRCError> {
        match self.get_context(id) {
            Ok(s) => {
                let (index, data) = to_index_and_data(bytes);
                let full_name = match s.resolver.find_name(&index) {
                    Some(n) => n,
                    None => {
                        return Err(SRCError::non_retryable_without_cause(&*format!(
                            "Could not retrieve name for index: {:?}",
                            index
                        )))
                    }
                };
                Ok(RawDecodeResult {
                    schema: &s.schema,
                    full_name,
                    bytes: data,
                })
            }
            Err(e) => Err(Clone::clone(e)),
        }
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn get_context(&mut self, id: u32) -> &Result<DecodeContext, SRCError> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => &*e.into_mut(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_id_and_type(id, &self.sr_settings, SchemaType::Protobuf)
                {
                    Ok(r) => {
                        let schema = String::from(&r.schema);
                        Ok(DecodeContext {
                            schema: r,
                            resolver: MessageResolver::new(&*schema),
                        })
                    }
                    Err(e) => Err(e.into_cache()),
                };
                &*e.insert(v)
            }
        }
    }
}

fn to_bytes(
    encode_context: &EncodeContext,
    bytes: &[u8],
    full_name: &str,
) -> Result<Vec<u8>, SRCError> {
    let mut index_bytes = match encode_context.resolver.find_index(full_name) {
        Some(v) if v.len() == 1 && v[0] == 0i32 => vec![0u8],
        Some(v) => {
            let mut result = (v.len() as i32).encode_var_vec();
            for i in v {
                result.append(&mut i.encode_var_vec())
            }
            result
        }
        None => {
            return Err(SRCError::non_retryable_without_cause(&*format!(
                "could not find name {} with resolver",
                full_name
            )))
        }
    };
    index_bytes.extend(bytes);
    Ok(get_payload(encode_context.id, index_bytes))
}

#[derive(Debug)]
struct EncodeContext {
    id: u32,
    resolver: IndexResolver,
}

#[derive(Debug)]
struct DecodeContext {
    schema: RegisteredSchema,
    resolver: MessageResolver,
}

#[derive(Debug)]
pub struct RawDecodeResult<'a> {
    pub schema: &'a RegisteredSchema,
    pub full_name: &'a str,
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::proto_raw::{ProtoRawDecoder, ProtoRawEncoder};
    use crate::schema_registry::{
        SchemaType, SrSettings, SubjectNameStrategy, SuppliedReference, SuppliedSchema,
    };
    use mockito::{mock, server_address};

    fn get_proto_hb_schema() -> &'static str {
        r#"syntax = \"proto3\";package nl.openweb.data;message Heartbeat {uint64 beat = 1;}"#
    }

    fn get_proto_result() -> &'static str {
        r#"syntax = \"proto3\"; package org.schema_registry_test_app.proto; message Result { string up = 1; string down = 2; } "#
    }

    fn get_proto_complex() -> &'static str {
        r#"syntax = \"proto3\"; import \"result.proto\"; message A {bytes id = 1;} message B {bytes id = 1;} message C {bytes id = 1; D d = 2; message D {int64 counter = 1;}} package org.schema_registry_test_app.proto; message ProtoTest {bytes id = 1; enum Language {Java = 0;Rust = 1;} Language by = 2;int64 counter = 3;string input = 4;repeated A results = 5;}"#
    }

    fn get_proto_hb_101_only_data() -> &'static [u8] {
        &get_proto_hb_101()[6..]
    }

    fn get_proto_hb_101() -> &'static [u8] {
        &[0, 0, 0, 0, 7, 0, 8, 101]
    }

    fn get_proto_complex_proto_test_message_data_only() -> &'static [u8] {
        &get_proto_complex_proto_test_message()[7..]
    }

    fn get_proto_complex_proto_test_message() -> &'static [u8] {
        &[
            0, 0, 0, 0, 6, 2, 6, 10, 16, 11, 134, 69, 48, 212, 168, 77, 40, 147, 167, 30, 246, 208,
            32, 252, 79, 24, 1, 34, 6, 83, 116, 114, 105, 110, 103, 42, 16, 10, 6, 83, 84, 82, 73,
            78, 71, 18, 6, 115, 116, 114, 105, 110, 103,
        ]
    }

    fn get_proto_body(schema: &str, id: u32) -> String {
        format!(
            "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":{}}}",
            schema, id
        )
    }

    fn get_proto_body_with_reference(schema: &str, id: u32, reference: &str) -> String {
        format!(
            "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":{}, \"references\":[{}]}}",
            schema, id, reference
        )
    }

    fn get_complex_references() -> &'static str {
        r#"{"name": "result.proto", "subject": "result.proto", "version": 1}"#
    }

    #[test]
    fn test_encode_default() {
        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = ProtoRawEncoder::new(sr_settings);
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
    fn test_encode_complex() {
        let _m = mock("POST", "/subjects/result.proto/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_result(), 5))
            .create();

        let _m = mock("POST", "/subjects/result.proto")
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
        let mut encoder = ProtoRawEncoder::new(sr_settings);
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
                get_proto_complex_proto_test_message_data_only(),
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
        let _m = mock("GET", "/schemas/ids/7")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = ProtoRawDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101()));

        let raw_result = match heartbeat {
            Ok(Some(v)) => v,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(raw_result.bytes, get_proto_hb_101_only_data());
        assert_eq!(raw_result.full_name, "nl.openweb.data.Heartbeat")
    }

    #[test]
    fn test_decoder_complex() {
        let _m = mock("GET", "/schemas/ids/6")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_complex_references(),
            ))
            .create();

        let _m = mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = ProtoRawDecoder::new(sr_settings);
        let proto_test = decoder.decode(Some(get_proto_complex_proto_test_message()));

        let raw_result = match proto_test {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(
            raw_result.bytes,
            get_proto_complex_proto_test_message_data_only()
        )
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
