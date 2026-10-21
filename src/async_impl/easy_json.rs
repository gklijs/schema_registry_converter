//! [`Arc`] wrapped implementations for [`JsonEncoder`] and [`JsonDecoder`]
//!
//! Implementations for [`EasyJsonDecoder`] and [`EasyJsonEncoder`]. These Newtype
//! structs wrap the [`JsonDecoder`] and [`JsonEncoder`] structs in an [`Arc`] to make it
//! _easy_.
//!
//! **NOTE**: This module requires `easy` and `json` features to be enabled.
use crate::async_impl::json::{DecodeResult, JsonDecoder, JsonEncoder, JsonSchema};
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use crate::schema_registry_common::{SchemaIdHeader, SubjectNameStrategy};
use serde_json::Value;
use std::sync::Arc;

/// A decoder used to transform bytes to a [DecodeResult], its much like [JsonDecoder] but wrapped with an arc to make it easier.
#[derive(Clone, Debug)]
pub struct EasyJsonDecoder {
    decoder: Arc<JsonDecoder>,
}

impl EasyJsonDecoder {
    pub fn new(sr_settings: SrSettings) -> EasyJsonDecoder {
        let decoder = Arc::new(JsonDecoder::new(sr_settings));
        EasyJsonDecoder { decoder }
    }
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<DecodeResult>, SRCError> {
        self.decoder.decode(bytes).await
    }
    /// See [`JsonDecoder::decode_with_header_id`].
    pub async fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResult>, SRCError> {
        self.decoder
            .decode_with_header_id(header_value, bytes)
            .await
    }
    /// See [`JsonDecoder::validate`].
    pub fn validate(&self, schema: &JsonSchema, value: &Value) -> Result<(), SRCError> {
        self.decoder.validate(schema, value)
    }
}

/// An encoder used to transform a [Value] to bytes, its much like [JsonEncoder] but wrapped with an arc to make it easier.
#[derive(Clone, Debug)]
pub struct EasyJsonEncoder {
    encoder: Arc<JsonEncoder>,
}

impl EasyJsonEncoder {
    pub fn new(sr_settings: SrSettings) -> EasyJsonEncoder {
        let encoder = Arc::new(JsonEncoder::new(sr_settings));
        EasyJsonEncoder { encoder }
    }
    pub async fn encode(
        &self,
        value: &Value,
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder.encode(value, subject_name_strategy).await
    }
    /// See [`JsonEncoder::encode_with_header_id`].
    pub async fn encode_with_header_id(
        &self,
        value: &Value,
        subject_name_strategy: SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        self.encoder
            .encode_with_header_id(value, subject_name_strategy, is_key)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_json::{EasyJsonDecoder, EasyJsonEncoder};
    use crate::async_impl::json::validate;
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::{get_payload, SubjectNameStrategy};

    use mockito::Server;
    use serde_json::Value;
    use std::fs::{read_to_string, File};
    use test_utils::{get_json_body, json_result_java_bytes, json_result_schema};

    #[tokio::test]
    async fn test_decoder_default() {
        let result_value: String = read_to_string("tests/schema/result-example.json")
            .unwrap()
            .parse()
            .unwrap();
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 7))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = EasyJsonDecoder::new(sr_settings);
        let message = decoder
            .decode(Some(&*get_payload(7, result_value.into_bytes())))
            .await
            .unwrap()
            .unwrap();
        validate(&message.schema, &message.value).unwrap();
        assert_eq!(
            "string",
            message
                .value
                .as_object()
                .unwrap()
                .get("down")
                .unwrap()
                .as_str()
                .unwrap()
        );
        assert_eq!(
            "STRING",
            message
                .value
                .as_object()
                .unwrap()
                .get("up")
                .unwrap()
                .as_str()
                .unwrap()
        )
    }

    #[tokio::test]
    async fn test_encode_default() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = EasyJsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, strategy).await.unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }

    const GUID: &str = "cc0e0e0e-53c1-4a1a-8f1a-000000000001";
    const GUID_HEADER: [u8; 17] = [
        0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01,
    ];

    #[tokio::test]
    async fn test_header_id_round_trip() {
        let mut server = Server::new_async().await;
        let body = format!(
            "{{\"schema\":\"{}\", \"schemaType\":\"JSON\", \"id\":10, \"guid\":\"{}\"}}",
            json_result_schema(),
            GUID
        );
        let _latest = server
            .mock("GET", "/subjects/testresult-key/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&body)
            .create();
        let _guid = server
            .mock(
                "GET",
                format!("/schemas/guids/{GUID}?deleted=true").as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&body)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = EasyJsonEncoder::new(sr_settings.clone());
        let decoder = EasyJsonDecoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), true);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let (bytes, header) = encoder
            .encode_with_header_id(&result_example, strategy, true)
            .await
            .unwrap();
        assert_eq!(bytes, &json_result_java_bytes()[5..]);
        assert_eq!(header.name, "__key_schema_id");
        assert_eq!(header.value, GUID_HEADER);

        let message = decoder
            .decode_with_header_id(Some(&header.value), Some(&bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.value, result_example);
        decoder.validate(&message.schema, &message.value).unwrap();
        assert!(decoder
            .validate(&message.schema, &serde_json::json!({"up": 1}))
            .is_err());
    }
}
