use crate::async_impl::proto_raw::{ProtoRawDecoder, ProtoRawEncoder, RawDecodeResult};
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use crate::schema_registry_common::SubjectNameStrategy;
use std::sync::Arc;

/// A decoder used to transform bytes to a [RawDecodeResult], its much like [ProtoRawDecoder] but wrapped with an arc to make it easier.
/// You can use the bytes from the result to create a proto object.
pub struct EasyProtoRawDecoder {
    decoder: Arc<ProtoRawDecoder<'static>>,
}

impl EasyProtoRawDecoder {
    pub fn new(sr_settings: SrSettings) -> EasyProtoRawDecoder {
        let decoder = Arc::new(ProtoRawDecoder::new(sr_settings));
        EasyProtoRawDecoder { decoder }
    }
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<RawDecodeResult>, SRCError> {
        self.decoder.decode(bytes).await
    }
}

/// An encoder used to transform the proto bytes to bytes compatible with confluent schema registry, its much like [ProtoRawEncoder] but wrapped with an arc to make it easier.
/// This wil just add the magic byte, schema reference, and message reference. The bytes should already be valid proto bytes for the schema used.
/// When a schema with multiple messages is used the full_name needs to be supplied to properly encode the message reference.
pub struct EasyProtoRawEncoder {
    encoder: Arc<ProtoRawEncoder<'static>>,
}

impl EasyProtoRawEncoder {
    pub fn new(sr_settings: SrSettings) -> EasyProtoRawEncoder {
        let encoder = Arc::new(ProtoRawEncoder::new(sr_settings));
        EasyProtoRawEncoder { encoder }
    }
    pub async fn encode(
        &self,
        bytes: &[u8],
        full_name: &str,
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder
            .encode(bytes, full_name, subject_name_strategy)
            .await
    }
    pub async fn encode_single_message(
        &self,
        bytes: &[u8],
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder
            .encode_single_message(bytes, subject_name_strategy)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_proto_raw::{EasyProtoRawDecoder, EasyProtoRawEncoder};
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::SubjectNameStrategy;
    use mockito::{mock, server_address};
    use test_utils::{
        get_proto_body, get_proto_hb_101, get_proto_hb_101_only_data, get_proto_hb_schema,
    };

    #[tokio::test]
    async fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = EasyProtoRawDecoder::new(sr_settings);
        let raw_result = decoder
            .decode(Some(get_proto_hb_101()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(raw_result.bytes, get_proto_hb_101_only_data());
        assert_eq!(*raw_result.full_name, "nl.openweb.data.Heartbeat")
    }

    #[tokio::test]
    async fn test_encode_default() {
        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let encoder = EasyProtoRawEncoder::new(sr_settings);
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
        let encoder = EasyProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let encoded_data = encoder
            .encode_single_message(get_proto_hb_101_only_data(), strategy)
            .await
            .unwrap();

        assert_eq!(encoded_data, get_proto_hb_101())
    }
}
