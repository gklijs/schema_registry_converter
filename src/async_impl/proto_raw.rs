use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;

use crate::async_impl::schema_registry::{
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
    /// Removes errors from the cache, might be useful to retry failed encodings.
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

    /// Encodes the bytes by adding a few bytes to the message with additional information.
    /// This should only be used when the schema only had one message
    pub async fn encode_single_message(
        &mut self,
        bytes: &[u8],
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = get_subject(&subject_name_strategy)?;
        let encode_context = self
            .get_encoding_context(key, subject_name_strategy)
            .clone()
            .await?;
        to_bytes_single_message(&encode_context, bytes)
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
        let full_name = resolve_name(&context.resolver, &index)?.clone();
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
    use test_utils::{
        get_proto_body, get_proto_body_with_reference, get_proto_complex,
        get_proto_complex_only_data, get_proto_complex_proto_test_message,
        get_proto_complex_references, get_proto_hb_101, get_proto_hb_101_only_data,
        get_proto_hb_schema, get_proto_result,
    };

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
    async fn test_encode_single_message() {
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
            .encode_single_message(get_proto_hb_101_only_data(), strategy)
            .await
            .unwrap();

        assert_eq!(encoded_data, get_proto_hb_101())
    }

    #[tokio::test]
    async fn test_encode_single_message_multiple_in_schema() {
        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_complex(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = ProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        assert_eq!(
            encoder
                .encode_single_message(get_proto_complex_only_data(), strategy,)
                .await
                .is_err(),
            true
        )
    }

    #[tokio::test]
    async fn test_encode_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = ProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
        let error = encoder
            .encode(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                strategy.clone(),
            )
            .await
            .unwrap_err();
        assert_eq!(true, error.cached);

        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let error = encoder
            .encode(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                strategy.clone(),
            )
            .await
            .unwrap_err();
        assert_eq!(true, error.cached);

        encoder.remove_errors_from_cache();

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
                get_proto_complex_only_data(),
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
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
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
    async fn test_decoder_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = ProtoRawDecoder::new(sr_settings);

        let error = decoder.decode(Some(get_proto_hb_101())).await.unwrap_err();
        assert_eq!(true, error.cached);

        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let error = decoder.decode(Some(get_proto_hb_101())).await.unwrap_err();
        assert_eq!(true, error.cached);

        decoder.remove_errors_from_cache();

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
        let mut decoder = ProtoRawDecoder::new(sr_settings);
        let raw_result = decoder
            .decode(Some(get_proto_complex_proto_test_message()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(raw_result.bytes, get_proto_complex_only_data())
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
