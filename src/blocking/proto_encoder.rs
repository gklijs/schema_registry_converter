//! Encoder for protobuf messages that are only known at runtime, as a [`DynamicMessage`], or as
//! JSON in the canonical protobuf JSON mapping.
//!
//! **NOTE**: Requires the `proto_encoder` feature
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde_json::Value;
use std::sync::Arc;

use crate::blocking::schema_registry::{SrSettings, get_referenced_schemas, get_schema_by_subject};
use crate::error::SRCError;
use crate::proto_encoder_common::{EncodeContext, to_encode_context};
use crate::schema_registry_common::{SchemaIdHeader, SubjectNameStrategy};

/// Encodes a [`DynamicMessage`] with the schema the subject name strategy resolves to, adding the
/// schema id and message index the confluent wire format needs. Unlike the
/// [`crate::blocking::proto_raw::ProtoRawEncoder`], the schema is compiled, with the schemas it
/// references, so a message can also be created from JSON, or with
/// [`ProtoEncoder::get_message_descriptor`].
#[derive(Debug)]
pub struct ProtoEncoder {
    sr_settings: SrSettings,
    cache: DashMap<String, Result<Arc<EncodeContext>, SRCError>>,
}

impl ProtoEncoder {
    /// Creates a new encoder. Retriable errors (e.g. a transient network/HTTP failure) are never
    /// cached, so those are simply retried on the next call. Non-retriable errors (e.g. a schema
    /// that doesn't exist or can't be compiled) are cached; use remove_errors_from_cache once
    /// the underlying problem is fixed.
    pub fn new(sr_settings: SrSettings) -> ProtoEncoder {
        ProtoEncoder {
            sr_settings,
            cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Encodes the message, the message index is found by the full name of the message, so the
    /// schema needs to declare a message with that name.
    pub fn encode(
        &self,
        message: &DynamicMessage,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        self.encode_context(subject_name_strategy)?.encode(message)
    }
    /// Like [`ProtoEncoder::encode`], but instead of prefixing the payload with the schema id,
    /// returns the protobuf bytes alongside a [`SchemaIdHeader`] carrying the schema's guid and
    /// the message index. `is_key` picks between the `__key_schema_id`/`__value_schema_id`
    /// header names. Requires a schema registry that returns a `guid` (Confluent Schema
    /// Registry 8.0+).
    pub fn encode_with_header_id(
        &self,
        message: &DynamicMessage,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        self.encode_context(subject_name_strategy)?
            .encode_with_header_id(message, is_key)
    }
    /// Encodes the JSON, in the canonical protobuf JSON mapping, as the message `full_name`.
    pub fn encode_json(
        &self,
        value: &Value,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let context = self.encode_context(subject_name_strategy)?;
        context.encode(&context.message_from_json(value, full_name)?)
    }
    /// Like [`ProtoEncoder::encode_json`], but with the schema id in a header, see
    /// [`ProtoEncoder::encode_with_header_id`].
    pub fn encode_json_with_header_id(
        &self,
        value: &Value,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let context = self.encode_context(subject_name_strategy)?;
        context.encode_with_header_id(&context.message_from_json(value, full_name)?, is_key)
    }
    /// The descriptor of the message `full_name` in the schema the subject name strategy
    /// resolves to, to create a [`DynamicMessage`] to encode with.
    pub fn get_message_descriptor(
        &self,
        full_name: &str,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<MessageDescriptor, SRCError> {
        self.encode_context(subject_name_strategy)?
            .message_descriptor(full_name)
    }

    fn encode_context(
        &self,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Arc<EncodeContext>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        match self.cache.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let result = get_schema_by_subject(&self.sr_settings, subject_name_strategy)
                    .and_then(|schema| {
                        let references = get_referenced_schemas(&self.sr_settings, &schema)?;
                        to_encode_context(schema, references)
                    })
                    .map(Arc::new);
                match result {
                    // Retriable errors (transient network/HTTP issues) don't make sense to
                    // cache, so the entry is left vacant and the next call issues a fresh
                    // request rather than replaying a stale failure.
                    Err(e) if e.retriable => Err(e),
                    result => entry
                        .insert(result.map_err(SRCError::into_cache))
                        .value()
                        .clone(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::blocking::proto_encoder::ProtoEncoder;
    use crate::blocking::schema_registry::SrSettings;
    use crate::schema_registry_common::SubjectNameStrategy;
    use prost_reflect::{DynamicMessage, Value};
    use serde_json::json;
    use test_utils::{
        get_proto_body, get_proto_body_with_reference, get_proto_complex,
        get_proto_complex_proto_test_message, get_proto_complex_references, get_proto_hb_101,
        get_proto_hb_schema, get_proto_result,
    };

    fn heartbeat_strategy() -> SubjectNameStrategy {
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"))
    }

    #[test]
    fn test_encode_dynamic_message_and_json() {
        let mut server = mockito::Server::new();
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
            .unwrap();
        let mut message = DynamicMessage::new(descriptor);
        message.set_field_by_name("beat", Value::U64(101));
        assert_eq!(
            encoder.encode(&message, &strategy).unwrap(),
            get_proto_hb_101()
        );

        let encoded = encoder
            .encode_json(
                &json!({"beat": "101"}),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .unwrap();
        assert_eq!(encoded, get_proto_hb_101());
        _m.assert();
    }

    #[test]
    fn test_encode_with_references_and_nested_index() {
        let mut server = mockito::Server::new();
        let _m1 = server
            .mock("GET", "/subjects/test.proto/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body_with_reference(
                get_proto_complex(),
                6,
                get_proto_complex_references(),
            ))
            .create();
        let _m2 = server
            .mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = ProtoEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::RecordNameStrategy(String::from("test.proto"));
        let descriptor = encoder
            .get_message_descriptor("org.schema_registry_test_app.proto.ProtoTest", &strategy)
            .unwrap();
        // the payload after the id (6) and message index ([3])
        let expected = get_proto_complex_proto_test_message();
        let message = DynamicMessage::decode(descriptor, &expected[7..]).unwrap();
        assert_eq!(encoder.encode(&message, &strategy).unwrap(), expected);
    }

    #[test]
    fn test_encode_with_header_id_needs_guid() {
        let mut server = mockito::Server::new();
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
        let encoder = ProtoEncoder::new(sr_settings);
        let result = encoder.encode_json_with_header_id(
            &json!({"beat": 101}),
            "nl.openweb.data.Heartbeat",
            &heartbeat_strategy(),
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_cache() {
        let mut server = mockito::Server::new();
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
            .unwrap_err();
        assert!(error.cached);

        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 7))
            .create();
        assert!(
            encoder
                .encode_json(
                    &json!({"beat": 101}),
                    "nl.openweb.data.Heartbeat",
                    &strategy
                )
                .unwrap_err()
                .cached
        );

        encoder.remove_errors_from_cache();
        let encoded = encoder
            .encode_json(
                &json!({"beat": 101}),
                "nl.openweb.data.Heartbeat",
                &strategy,
            )
            .unwrap();
        assert_eq!(encoded, get_proto_hb_101());
    }

    #[test]
    fn display_encoder() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let encoder = ProtoEncoder::new(sr_settings);
        assert_eq!(
            format!("{:?}", encoder),
            "ProtoEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {} }"
        )
    }
}
