use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;

use crate::async_impl::schema_registry::{
    get_schema_by_id_and_type, get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::proto_raw_common::{to_bytes, to_decode_context, DecodeContext, EncodeContext};
use crate::proto_resolver::{to_index_and_data, IndexResolver};
use crate::schema_registry_common::{
    get_bytes_result, get_subject, BytesResult, RegisteredSchema, SchemaType, SubjectNameStrategy,
};
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;

/// Encoder that works by prepending the correct bytes in order to make it valid schema registry
/// bytes. Ideally you want to make sure the bytes are based on the exact schema used for encoding
/// but you need a protobuf struct that has introspection to make that work, and both protobuf and
/// prost don't support that at the moment.
#[derive(Debug)]
pub struct ProtoRawEncoder<'a> {
    sr_settings: SrSettings,
    cache: HashMap<String, Shared<BoxFuture<'a, Result<EncodeContext, SRCError>>>, RandomState>,
}

impl<'a> ProtoRawEncoder<'a> {
    /// Creates a new encoder
    pub fn new(sr_settings: SrSettings) -> ProtoRawEncoder<'a> {
        ProtoRawEncoder {
            sr_settings,
            cache: HashMap::new(),
        }
    }
    /// Removes errors from the cache, can be useful to retry failed encodings.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
    }
    /// Encodes the bytes by adding a few bytes to the message with additional information. The full
    /// names is the optional package followed with the message name, and optionally inner messages.
    pub async fn encode(
        &mut self,
        bytes: &[u8],
        full_name: &str,
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = get_subject(&subject_name_strategy)?;
        let encode_context = self
            .get_encoding_context(key, subject_name_strategy)
            .clone()
            .await?;
        to_bytes(&encode_context, bytes, full_name)
    }

    fn get_encoding_context(
        &mut self,
        key: String,
        subject_name_strategy: SubjectNameStrategy,
    ) -> &Shared<BoxFuture<'a, Result<EncodeContext, SRCError>>> {
        match self.cache.entry(key) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let v = async move {
                    match get_schema_by_subject(&sr_settings, &subject_name_strategy).await {
                        Ok(registered_schema) => Ok(EncodeContext {
                            id: registered_schema.id,
                            resolver: IndexResolver::new(&*registered_schema.schema),
                        }),
                        Err(e) => Err(e.into_cache()),
                    }
                }
                .boxed()
                .shared();
                e.insert(v)
            }
        }
    }
}

#[derive(Debug)]
pub struct ProtoRawDecoder<'a> {
    sr_settings: SrSettings,
    cache: HashMap<u32, Shared<BoxFuture<'a, Result<DecodeContext, SRCError>>>, RandomState>,
}

impl<'a> ProtoRawDecoder<'a> {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. It's possible for recoverable errors to stay in the cache, when a result
    /// comes back as an error you can use remove_errors_from_cache to clean the cache, keeping the
    /// correctly fetched schema's
    pub fn new(sr_settings: SrSettings) -> ProtoRawDecoder<'a> {
        ProtoRawDecoder {
            sr_settings,
            cache: HashMap::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
    }
    /// Reads the bytes to get the name, and gives back the data bytes.
    pub async fn decode(
        &mut self,
        bytes: Option<&[u8]>,
    ) -> Result<Option<RawDecodeResult>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => Ok(Some(self.deserialize(id, &bytes).await?)),
            BytesResult::Invalid(i) => Err(SRCError::non_retryable_without_cause(&*format!(
                "Invalid bytes {:?}",
                i
            ))),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    async fn deserialize(&mut self, id: u32, bytes: &[u8]) -> Result<RawDecodeResult, SRCError> {
        let context = self.get_context(id).clone().await?;
        let (index, data) = to_index_and_data(bytes);
        let full_name = match context.resolver.find_name(&index) {
            Some(n) => n.clone(),
            None => {
                return Err(SRCError::non_retryable_without_cause(&*format!(
                    "Could not retrieve name for index: {:?}",
                    index
                )))
            }
        };
        let schema = context.schema;
        Ok(RawDecodeResult {
            schema,
            full_name,
            bytes: data,
        })
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn get_context(&mut self, id: u32) -> &Shared<BoxFuture<'a, Result<DecodeContext, SRCError>>> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => &*e.into_mut(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let v = async move {
                    match get_schema_by_id_and_type(id, &sr_settings, SchemaType::Protobuf).await {
                        Ok(r) => Ok(to_decode_context(r)),
                        Err(e) => Err(e.into_cache()),
                    }
                }
                .boxed()
                .shared();
                &*e.insert(v)
            }
        }
    }
}

#[derive(Debug)]
pub struct RawDecodeResult {
    pub schema: RegisteredSchema,
    pub full_name: String,
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use mockito::{mock, server_address};

    use crate::async_impl::proto_raw::{ProtoRawDecoder, ProtoRawEncoder};
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::{
        SchemaType, SubjectNameStrategy, SuppliedReference, SuppliedSchema,
    };

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

    #[tokio::test]
    async fn test_encode_default() {
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
                strategy,
            )
            .await
            .unwrap();

        assert_eq!(encoded_data, get_proto_hb_101())
    }

    #[tokio::test]
    async fn test_encode_complex() {
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
                strategy,
            )
            .await
            .unwrap();

        assert_eq!(encoded_data, get_proto_complex_proto_test_message())
    }

    #[test]
    fn display_rew_decoder() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoRawEncoder::new(sr_settings);
        assert_eq!(
            true
                .to_owned(),
            format!("{:?}", decoder).starts_with("ProtoRawEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client")
        )
    }

    #[tokio::test]
    async fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/7")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = ProtoRawDecoder::new(sr_settings);
        let raw_result = decoder
            .decode(Some(get_proto_hb_101()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(raw_result.bytes, get_proto_hb_101_only_data());
        assert_eq!(raw_result.full_name, "nl.openweb.data.Heartbeat")
    }

    #[tokio::test]
    async fn test_decoder_complex() {
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
        let raw_result = decoder
            .decode(Some(get_proto_complex_proto_test_message()))
            .await
            .unwrap()
            .unwrap();

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
            true
                .to_owned(),
            format!("{:?}", decoder).starts_with("ProtoRawDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client")
        )
    }
}
