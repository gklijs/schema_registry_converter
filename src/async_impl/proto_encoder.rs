//! Encoder for protobuf messages that are only known at runtime, as a [`DynamicMessage`], or as
//! JSON in the canonical protobuf JSON mapping.
//!
//! **NOTE**: Requires the `proto_encoder` feature
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde_json::Value;
use std::sync::Arc;

use crate::async_impl::schema_registry::{
    SrSettings, get_referenced_schemas, get_schema_by_subject,
};
use crate::error::SRCError;
use crate::proto_encoder_common::{EncodeContext, to_encode_context};
use crate::schema_registry_common::{SchemaIdHeader, SubjectNameStrategy};

type SharedFutureEncodeContext = Shared<BoxFuture<'static, Result<Arc<EncodeContext>, SRCError>>>;

/// Encodes a [`DynamicMessage`] with the schema the subject name strategy resolves to, adding the
/// schema id and message index the confluent wire format needs. Unlike the
/// [`crate::async_impl::proto_raw::ProtoRawEncoder`], the schema is compiled, with the schemas
/// it references, so a message can also be created from JSON, or with
/// [`ProtoEncoder::get_message_descriptor`].
#[derive(Debug)]
pub struct ProtoEncoder {
    sr_settings: SrSettings,
    direct_cache: DashMap<String, Arc<EncodeContext>>,
    cache: DashMap<String, SharedFutureEncodeContext>,
}

impl ProtoEncoder {
    /// Creates a new encoder. Retriable errors (e.g. a transient network/HTTP failure) are never
    /// cached, so those are simply retried on the next call. Non-retriable errors (e.g. a schema
    /// that doesn't exist or can't be compiled) are cached; use remove_errors_from_cache once
    /// the underlying problem is fixed.
    pub fn new(sr_settings: SrSettings) -> ProtoEncoder {
        ProtoEncoder {
            sr_settings,
            direct_cache: DashMap::new(),
            cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
    }
    /// Encodes the message, the message index is found by the full name of the message, so the
    /// schema needs to declare a message with that name.
    pub async fn encode(
        &self,
        message: &DynamicMessage,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encode_context(subject_name_strategy)
            .await?
            .encode(message)
    }
    /// Like [`ProtoEncoder::encode`], but instead of prefixing the payload with the schema id,
    /// returns the protobuf bytes alongside a [`SchemaIdHeader`] carrying the schema's guid and
    /// the message index. `is_key` picks between the `__key_schema_id`/`__value_schema_id`
    /// header names. Requires a schema registry that returns a `guid` (Confluent Schema
    /// Registry 8.0+).
    pub async fn encode_with_header_id(
        &self,
        message: &DynamicMessage,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        self.encode_context(subject_name_strategy)
            .await?
            .encode_with_header_id(message, is_key)
    }
    /// Encodes the JSON, in the canonical protobuf JSON mapping, as the message `full_name`.
    pub async fn encode_json(
        &self,
        value: &Value,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let context = self.encode_context(subject_name_strategy).await?;
        context.encode(&context.message_from_json(value, full_name)?)
    }
    /// Like [`ProtoEncoder::encode_json`], but with the schema id in a header, see
    /// [`ProtoEncoder::encode_with_header_id`].
    pub async fn encode_json_with_header_id(
        &self,
        value: &Value,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let context = self.encode_context(subject_name_strategy).await?;
        context.encode_with_header_id(&context.message_from_json(value, full_name)?, is_key)
    }
    /// The descriptor of the message `full_name` in the schema the subject name strategy
    /// resolves to, to create a [`DynamicMessage`] to encode with.
    pub async fn get_message_descriptor(
        &self,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<MessageDescriptor, SRCError> {
        self.encode_context(subject_name_strategy)
            .await?
            .message_descriptor(full_name)
    }

    /// Gets the encode context directly or via a shared future. The direct cache is for
    /// performance.
    async fn encode_context(
        &self,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Arc<EncodeContext>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        if let Some(context) = self.direct_cache.get(&key) {
            return Ok(context.value().clone());
        }
        match self
            .encode_context_by_shared_future(key.clone(), subject_name_strategy)
            .await
        {
            Ok(v) => {
                if !self.direct_cache.contains_key(&key) {
                    self.direct_cache.insert(key.clone(), v.clone());
                    self.cache.remove(&key);
                }
                Ok(v)
            }
            // Retriable errors (transient network/HTTP issues) don't make sense to cache, so
            // the entry is dropped and the next call issues a fresh request rather than
            // replaying a stale failure.
            Err(e) if e.retriable => {
                self.cache.remove(&key);
                Err(e)
            }
            Err(e) => Err(e.into_cache()),
        }
    }

    fn encode_context_by_shared_future(
        &self,
        key: String,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> SharedFutureEncodeContext {
        match self.cache.entry(key) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let subject_name_strategy = subject_name_strategy.clone();
                let v = async move {
                    let schema =
                        get_schema_by_subject(&sr_settings, &subject_name_strategy).await?;
                    let references = get_referenced_schemas(&sr_settings, &schema).await?;
                    to_encode_context(schema, references).map(Arc::new)
                }
                .boxed()
                .shared();
                e.insert(v).value().clone()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::async_impl::proto_encoder::ProtoEncoder;
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::SubjectNameStrategy;
    use mockito::Server;
    use prost_reflect::{DynamicMessage, Value};
    use serde_json::json;
    use test_utils::{get_proto_body, get_proto_hb_101, get_proto_hb_schema};

    fn heartbeat_strategy() -> SubjectNameStrategy {
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"))
    }

    #[tokio::test]
    async fn test_encode_dynamic_message_and_json() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .expect(1)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoEncoder::new(sr_settings);
        let strategy = heartbeat_strategy();

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
        let encoded = encoder
            .encode_json(
                &json!({"beat": 101}),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .await
            .unwrap();
        assert_eq!(encoded, get_proto_hb_101());
        _m.assert_async().await;
    }

    #[tokio::test]
    async fn test_encode_cache() {
        let mut server = Server::new_async().await;
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoEncoder::new(sr_settings);
        let strategy = heartbeat_strategy();
        let error = encoder
            .encode_json(
                &json!({"beat": 101}),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .await
            .unwrap_err();
        assert!(error.cached);

        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();
        encoder.remove_errors_from_cache();
        let encoded = encoder
            .encode_json(
                &json!({"beat": 101}),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .await
            .unwrap();
        assert_eq!(encoded, get_proto_hb_101());
    }
}
