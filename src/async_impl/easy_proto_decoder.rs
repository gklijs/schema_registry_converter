use crate::async_impl::proto_decoder::{DecodeResultWithContext, ProtoDecoder};
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use protofish::decode::Value;
use std::sync::Arc;

/// A decoder used to transform bytes to a [Value], its much like [ProtoDecoder] but wrapped with an arc to make it easier.
/// The mean use of this way of decoding is if you don't know the format at compile time.
/// If you do know the format it's better to use the ProtoRawDecoder, and use a different library to deserialize just the proto bytes.
pub struct EasyProtoDecoder {
    decoder: Arc<ProtoDecoder<'static>>,
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
}

#[cfg(test)]
mod tests {
    use crate::async_impl::easy_proto_decoder::EasyProtoDecoder;
    use crate::async_impl::schema_registry::SrSettings;
    use mockito::{mock, server_address};
    use protofish::decode::Value;
    use test_utils::{get_proto_body, get_proto_hb_101, get_proto_hb_schema};

    #[tokio::test]
    async fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = EasyProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }

    #[tokio::test]
    async fn test_decode_with_context_default() {
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = EasyProtoDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode_with_context(Some(get_proto_hb_101()))
            .await
            .unwrap();

        assert!(heartbeat.is_some());
        let message = heartbeat.unwrap().value;

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }
}
