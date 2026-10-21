use crate::async_impl::proto_raw::{ProtoRawDecoder, ProtoRawEncoder, RawDecodeResult};
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use crate::schema_registry_common::{SchemaIdHeader, SubjectNameStrategy};
use std::sync::Arc;

/// A decoder used to transform bytes to a [RawDecodeResult], its much like [ProtoRawDecoder] but wrapped with an arc to make it easier.
/// You can use the bytes from the result to create a proto object.
#[derive(Clone, Debug)]
pub struct EasyProtoRawDecoder {
    decoder: Arc<ProtoRawDecoder>,
}

impl EasyProtoRawDecoder {
    pub fn new(sr_settings: SrSettings) -> EasyProtoRawDecoder {
        let decoder = Arc::new(ProtoRawDecoder::new(sr_settings));
        EasyProtoRawDecoder { decoder }
    }
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<RawDecodeResult>, SRCError> {
        self.decoder.decode(bytes).await
    }
    /// See [`ProtoRawDecoder::decode_with_header_id`].
    pub async fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Option<RawDecodeResult>, SRCError> {
        self.decoder
            .decode_with_header_id(header_value, bytes)
            .await
    }
}

/// An encoder used to transform the proto bytes to bytes compatible with confluent schema registry, its much like [ProtoRawEncoder] but wrapped with an arc to make it easier.
/// This wil just add the magic byte, schema reference, and message reference. The bytes should already be valid proto bytes for the schema used.
/// When a schema with multiple messages is used the full_name needs to be supplied to properly encode the message reference.
#[derive(Clone, Debug)]
pub struct EasyProtoRawEncoder {
    encoder: Arc<ProtoRawEncoder>,
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
    /// See [`ProtoRawEncoder::encode_with_header_id`].
    pub async fn encode_with_header_id(
        &self,
        bytes: &[u8],
        full_name: &str,
        subject_name_strategy: SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        self.encoder
            .encode_with_header_id(bytes, full_name, subject_name_strategy, is_key)
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
    /// See [`ProtoRawEncoder::encode_single_message_with_header_id`].
    pub async fn encode_single_message_with_header_id(
        &self,
        bytes: &[u8],
        subject_name_strategy: SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        self.encoder
            .encode_single_message_with_header_id(bytes, subject_name_strategy, is_key)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_proto_raw::{EasyProtoRawDecoder, EasyProtoRawEncoder};
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::SubjectNameStrategy;
    use mockito::Server;

    use test_utils::{
        get_proto_body, get_proto_hb_101, get_proto_hb_101_only_data, get_proto_hb_schema,
    };

    #[tokio::test]
    async fn test_decoder_default() {
        let mut server = Server::new_async().await;
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
        let mut server = Server::new_async().await;
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
        let mut server = Server::new_async().await;
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
        let encoder = EasyProtoRawEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let encoded_data = encoder
            .encode_single_message(get_proto_hb_101_only_data(), strategy)
            .await
            .unwrap();

        assert_eq!(encoded_data, get_proto_hb_101())
    }

    const GUID: &str = "cc0e0e0e-53c1-4a1a-8f1a-000000000001";
    const GUID_HEADER: [u8; 18] = [
        0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x00,
    ];

    #[tokio::test]
    async fn test_header_id_round_trip() {
        let mut server = Server::new_async().await;
        let body = format!(
            "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":7, \"guid\":\"{}\"}}",
            get_proto_hb_schema(),
            GUID
        );
        let _latest = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
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
        let encoder = EasyProtoRawEncoder::new(sr_settings.clone());
        let decoder = EasyProtoRawDecoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let (bytes, header) = encoder
            .encode_with_header_id(
                get_proto_hb_101_only_data(),
                "nl.openweb.data.Heartbeat",
                strategy.clone(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(bytes, get_proto_hb_101_only_data());
        assert_eq!(header.name, "__value_schema_id");
        assert_eq!(header.value, GUID_HEADER);

        let (bytes, header) = encoder
            .encode_single_message_with_header_id(get_proto_hb_101_only_data(), strategy, true)
            .await
            .unwrap();
        assert_eq!(bytes, get_proto_hb_101_only_data());
        assert_eq!(header.name, "__key_schema_id");
        assert_eq!(header.value, GUID_HEADER);

        let raw_result = decoder
            .decode_with_header_id(Some(&header.value), Some(&bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw_result.bytes, get_proto_hb_101_only_data());
        assert_eq!(*raw_result.full_name, "nl.openweb.data.Heartbeat");
    }
}
