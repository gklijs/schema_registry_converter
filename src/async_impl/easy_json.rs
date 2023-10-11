use crate::async_impl::json::{DecodeResult, JsonDecoder, JsonEncoder};
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use crate::schema_registry_common::SubjectNameStrategy;
use serde_json::Value;
use std::sync::Arc;

/// A decoder used to transform bytes to a [DecodeResult], its much like [JsonDecoder] but wrapped with an arc to make it easier.
pub struct EasyJsonDecoder {
    decoder: Arc<JsonDecoder<'static>>,
}

impl EasyJsonDecoder {
    pub fn new(sr_settings: SrSettings) -> EasyJsonDecoder {
        let decoder = Arc::new(JsonDecoder::new(sr_settings));
        EasyJsonDecoder { decoder }
    }
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<DecodeResult>, SRCError> {
        self.decoder.decode(bytes).await
    }
}

/// An encoder used to transform a [Value] to bytes, its much like [JsonEncoder] but wrapped with an arc to make it easier.
pub struct EasyJsonEncoder {
    encoder: Arc<JsonEncoder<'static>>,
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
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_json::{EasyJsonDecoder, EasyJsonEncoder};
    use crate::async_impl::json::validate;
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::{get_payload, SubjectNameStrategy};
    use mockito::{mock, server_address};
    use serde_json::Value;
    use std::fs::{read_to_string, File};
    use test_utils::{get_json_body, json_result_java_bytes, json_result_schema};

    #[tokio::test]
    async fn test_decoder_default() {
        let result_value: String = read_to_string("tests/schema/result-example.json")
            .unwrap()
            .parse()
            .unwrap();
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = EasyJsonDecoder::new(sr_settings);
        let message = decoder
            .decode(Some(&*get_payload(7, result_value.into_bytes())))
            .await
            .unwrap()
            .unwrap();
        validate(message.schema, &message.value).unwrap();
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
        let _m = mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let encoder = EasyJsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, strategy).await.unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }
}
