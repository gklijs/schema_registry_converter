use crate::async_impl::proto_decoder::{DecodeResultWithContext, ProtoDecoder};
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use prost_reflect::Value;
use std::sync::Arc;

/// A decoder used to transform bytes to a [Value], its much like [ProtoDecoder] but wrapped with an arc to make it easier.
/// The mean use of this way of decoding is if you don't know the format at compile time.
/// If you do know the format it's better to use the ProtoRawDecoder, and use a different library to deserialize just the proto bytes.
#[derive(Clone, Debug)]
pub struct EasyProtoDecoder {
    decoder: Arc<ProtoDecoder>,
}

impl EasyProtoDecoder {
    pub fn new(sr_settings: SrSettings) -> EasyProtoDecoder {
        let decoder = Arc::new(ProtoDecoder::new(sr_settings));
        EasyProtoDecoder { decoder }
    }
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        self.decoder.decode(bytes).await
    }
    pub async fn decode_with_context(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResultWithContext>, SRCError> {
        self.decoder.decode_with_context(bytes).await
    }
    /// See [`ProtoDecoder::decode_with_header_id`].
    pub async fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Value, SRCError> {
        self.decoder
            .decode_with_header_id(header_value, bytes)
            .await
    }
    /// See [`ProtoDecoder::decode_with_context_and_header_id`].
    pub async fn decode_with_context_and_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResultWithContext>, SRCError> {
        self.decoder
            .decode_with_context_and_header_id(header_value, bytes)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_proto_decoder::EasyProtoDecoder;
    use crate::async_impl::schema_registry::SrSettings;
    use mockito::Server;

    use prost_reflect::{DynamicMessage, Value};
    use test_utils::{get_proto_body, get_proto_hb_101, get_proto_hb_schema};

    fn field(message: &DynamicMessage, name: &str) -> Value {
        message.get_field_by_name(name).unwrap().into_owned()
    }

    #[tokio::test]
    async fn test_decoder_default() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = EasyProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::U64(101u64), field(&message, "beat"))
    }

    #[tokio::test]
    async fn test_decode_with_context_default() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = EasyProtoDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode_with_context(Some(get_proto_hb_101()))
            .await
            .unwrap();

        assert!(heartbeat.is_some());
        let message = heartbeat.unwrap().value;

        assert_eq!(Value::U64(101u64), field(&message, "beat"))
    }

    #[tokio::test]
    async fn test_decode_with_context_and_header_id() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock(
                "GET",
                "/schemas/guids/cc0e0e0e-53c1-4a1a-8f1a-000000000001?deleted=true",
            )
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = EasyProtoDecoder::new(sr_settings);
        // magic byte 0x01 + 16-byte guid + single-message index byte 0x00
        let header_value = [
            0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x00,
        ];
        let payload = &get_proto_hb_101()[6..];
        let result = decoder
            .decode_with_context_and_header_id(Some(&header_value), Some(payload))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Value::U64(101u64), field(&result.value, "beat"));
        let value = decoder
            .decode_with_header_id(Some(&header_value), Some(payload))
            .await
            .unwrap();
        match value {
            Value::Message(message) => assert_eq!(Value::U64(101u64), field(&message, "beat")),
            v => panic!("Other value: {:?} than expected Message", v),
        }
    }
}
