use crate::async_impl::schema_registry::SrSettings;
use crate::avro_common::{DecodeResult, DecodeResultWithSchema};
use crate::error::SRCError;
use crate::schema_registry_common::SubjectNameStrategy;
use crate::{
    async_impl::avro::{AvroDecoder, AvroEncoder},
    avro_common::AvroSchema,
};
use apache_avro::types::Value;
use serde::Serialize;
use std::sync::Arc;

/// A decoder used to transform bytes to a [DecodeResult], its much like [AvroDecoder] but wrapped with an arc to make it easier.
pub struct EasyAvroDecoder {
    decoder: Arc<AvroDecoder<'static>>,
}

impl EasyAvroDecoder {
    pub fn new(sr_settings: SrSettings) -> EasyAvroDecoder {
        let decoder = Arc::new(AvroDecoder::new(sr_settings));
        EasyAvroDecoder { decoder }
    }
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<DecodeResult, SRCError> {
        self.decoder.decode(bytes).await
    }
    pub async fn decode_with_schema(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResultWithSchema>, SRCError> {
        self.decoder.decode_with_schema(bytes).await
    }
}

/// An encoder used to transform a [Value] to bytes, its much like [AvroEncoder] but wrapped with an arc to make it easier.
pub struct EasyAvroEncoder {
    encoder: Arc<AvroEncoder<'static>>,
}

impl EasyAvroEncoder {
    pub fn new(sr_settings: SrSettings) -> EasyAvroEncoder {
        let encoder = Arc::new(AvroEncoder::new(sr_settings));
        EasyAvroEncoder { encoder }
    }
    pub async fn encode(
        &self,
        values: Vec<(&str, Value)>,
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder.encode(values, subject_name_strategy).await
    }
    pub async fn encode_struct(
        &self,
        item: impl Serialize,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder
            .encode_struct(item, subject_name_strategy)
            .await
    }

    pub async fn encode_value(
        &self,
        item: Value,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder.encode_value(item, subject_name_strategy).await
    }

    pub async fn get_schema_and_id(
        &self,
        key: &str,
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Arc<AvroSchema>, SRCError> {
        self.encoder
            .get_schema_and_id(key, subject_name_strategy)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_avro::{EasyAvroDecoder, EasyAvroEncoder};
    use crate::async_impl::schema_registry::SrSettings;
    use crate::avro_common::get_supplied_schema;
    use crate::schema_registry_common::SubjectNameStrategy;
    use apache_avro::types::Value;
    use apache_avro::{from_value, Schema};
    use mockito::Server;

    use test_utils::Heartbeat;

    #[tokio::test]
    async fn test_decoder_default() {
        let mut server = Server::new_async().await;
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(server.url());
        let decoder = EasyAvroDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode(Some(&[0, 0, 0, 0, 1, 6]))
            .await
            .unwrap()
            .value;

        assert_eq!(
            heartbeat,
            Value::Record(vec![("beat".to_string(), Value::Long(3))])
        );

        let item = match from_value::<Heartbeat>(&heartbeat) {
            Ok(h) => h,
            Err(_) => unreachable!(),
        };
        assert_eq!(item.beat, 3i64);
    }

    #[tokio::test]
    async fn test_decode_with_schema_default() {
        let mut server = Server::new_async().await;
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(server.url());
        let decoder = EasyAvroDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode_with_schema(Some(&[0, 0, 0, 0, 1, 6]))
            .await
            .unwrap()
            .unwrap()
            .value;

        assert_eq!(
            heartbeat,
            Value::Record(vec![("beat".to_string(), Value::Long(3))])
        );

        let item = match from_value::<Heartbeat>(&heartbeat) {
            Ok(h) => h,
            Err(_) => unreachable!(),
        };
        assert_eq!(item.beat, 3i64);
    }

    #[tokio::test]
    async fn test_encode_value() {
        let mut server = Server::new_async().await;
        let _m = server.mock("GET", "/subjects/heartbeat-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(server.url());
        let encoder = EasyAvroEncoder::new(sr_settings);

        let value_strategy =
            SubjectNameStrategy::TopicNameStrategy(String::from("heartbeat"), false);
        let bytes = encoder
            .encode(vec![("beat", Value::Long(3))], value_strategy)
            .await
            .unwrap();

        assert_eq!(bytes, vec![0, 0, 0, 0, 3, 6])
    }

    #[tokio::test]
    async fn test_primitive_schema() {
        let mut server = Server::new_async().await;
        let sr_settings = SrSettings::new(server.url());
        let encoder = EasyAvroEncoder::new(sr_settings);

        let _m = server
            .mock("POST", "/subjects/heartbeat-key/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":4}"#)
            .create();

        let primitive_schema_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            String::from("heartbeat"),
            true,
            get_supplied_schema(&Schema::String),
        );
        let bytes = encoder
            .encode_struct("key-value", &primitive_schema_strategy)
            .await;

        assert_eq!(
            bytes,
            Ok(vec![
                0, 0, 0, 0, 4, 18, 107, 101, 121, 45, 118, 97, 108, 117, 101
            ])
        );
    }
}
