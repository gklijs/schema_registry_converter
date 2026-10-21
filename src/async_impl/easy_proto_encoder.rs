use crate::async_impl::proto_encoder::ProtoEncoder;
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use crate::schema_registry_common::{SchemaIdHeader, SubjectNameStrategy};
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde_json::Value;
use std::sync::Arc;

/// An encoder used to transform a [DynamicMessage], or JSON, to bytes, its much like
/// [ProtoEncoder] but wrapped with an arc to make it easier.
#[derive(Clone, Debug)]
pub struct EasyProtoEncoder {
    encoder: Arc<ProtoEncoder>,
}

impl EasyProtoEncoder {
    pub fn new(sr_settings: SrSettings) -> EasyProtoEncoder {
        let encoder = Arc::new(ProtoEncoder::new(sr_settings));
        EasyProtoEncoder { encoder }
    }
    /// See [`ProtoEncoder::encode`].
    pub async fn encode(
        &self,
        message: &DynamicMessage,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder.encode(message, subject_name_strategy).await
    }
    /// See [`ProtoEncoder::encode_with_header_id`].
    pub async fn encode_with_header_id(
        &self,
        message: &DynamicMessage,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        self.encoder
            .encode_with_header_id(message, subject_name_strategy, is_key)
            .await
    }
    /// See [`ProtoEncoder::encode_json`].
    pub async fn encode_json(
        &self,
        value: &Value,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encoder
            .encode_json(value, full_name, subject_name_strategy)
            .await
    }
    /// See [`ProtoEncoder::encode_json_with_header_id`].
    pub async fn encode_json_with_header_id(
        &self,
        value: &Value,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        self.encoder
            .encode_json_with_header_id(value, full_name, subject_name_strategy, is_key)
            .await
    }
    /// See [`ProtoEncoder::get_message_descriptor`].
    pub async fn get_message_descriptor(
        &self,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<MessageDescriptor, SRCError> {
        self.encoder
            .get_message_descriptor(full_name, subject_name_strategy)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_proto_encoder::EasyProtoEncoder;
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::SubjectNameStrategy;
    use mockito::Server;
    use prost_reflect::{DynamicMessage, Value};
    use serde_json::json;
    use test_utils::{get_proto_hb_101, get_proto_hb_schema};

    const GUID: &str = "cc0e0e0e-53c1-4a1a-8f1a-000000000001";
    const GUID_BYTES: [u8; 16] = [
        0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01,
    ];

    #[tokio::test]
    async fn test_easy_encoder() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(format!(
                "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":7, \"guid\":\"{}\"}}",
                get_proto_hb_schema(),
                GUID
            ))
            .expect(1)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = EasyProtoEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let descriptor = encoder
            .get_message_descriptor("nl.openweb.data.Heartbeat", &strategy)
            .await
            .unwrap();
        let mut message = DynamicMessage::new(descriptor);
        message.set_field_by_name("beat", Value::U64(101));
        assert_eq!(
            encoder.encode(&message, &strategy).await.unwrap(),
            get_proto_hb_101()
        );
        assert_eq!(
            encoder
                .encode_json(
                    &json!({"beat": 101}),
                    "nl.openweb.data.Heartbeat",
                    &strategy
                )
                .await
                .unwrap(),
            get_proto_hb_101()
        );

        let (bytes, header) = encoder
            .encode_with_header_id(&message, &strategy, false)
            .await
            .unwrap();
        assert_eq!(bytes, &get_proto_hb_101()[6..]);
        assert_eq!(header.name, "__value_schema_id");
        assert_eq!(header.value[0], 0x01);
        assert_eq!(&header.value[1..17], &GUID_BYTES);

        let (bytes, header) = encoder
            .encode_json_with_header_id(
                &json!({"beat": 101}),
                "nl.openweb.data.Heartbeat",
                &strategy,
                true,
            )
            .await
            .unwrap();
        assert_eq!(bytes, &get_proto_hb_101()[6..]);
        assert_eq!(header.name, "__key_schema_id");
        assert_eq!(&header.value[1..17], &GUID_BYTES);
        _m.assert_async().await;
    }
}
