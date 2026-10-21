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
