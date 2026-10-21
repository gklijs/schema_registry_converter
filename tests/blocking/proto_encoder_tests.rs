//! Round trips through a real schema registry: the [`ProtoEncoder`] registers a schema with a
//! reference, and the [`ProtoDecoder`] decodes what it encoded, both with the schema id in the
//! payload and with the schema guid in a header.

use schema_registry_converter::blocking::proto_decoder::ProtoDecoder;
use schema_registry_converter::blocking::proto_encoder::ProtoEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::prost_reflect::{DynamicMessage, ReflectMessage, Value};
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedReference, SuppliedSchema, VALUE_SCHEMA_ID_HEADER,
};
use serde_json::json;

use crate::blocking::proto_tests::get_schema_registry_url;

const RESULT_SCHEMA: &str = r#"syntax = "proto3";
package org.schema_registry_converter.encoder;

message Result {
  string up = 1;
  string down = 2;
}
"#;

const ENVELOPE_SCHEMA: &str = r#"syntax = "proto3";
package org.schema_registry_converter.encoder;

import "encoder_result.proto";

message Header {
  string source = 1;
}

message Envelope {
  string id = 1;
  repeated Result results = 2;
  map<string, int64> counts = 3;
  message Inner {
    bool flag = 1;
  }
  Inner inner = 4;
}
"#;

fn strategy() -> SubjectNameStrategy {
    let reference = SuppliedReference::new(
        String::from("encoder_result.proto"),
        String::from("encoder_result.proto"),
        String::from(RESULT_SCHEMA),
    );
    let schema = SuppliedSchema::new(
        Some(String::from(
            "org.schema_registry_converter.encoder.Envelope",
        )),
        SchemaType::Protobuf,
        String::from(ENVELOPE_SCHEMA),
    )
    .with_references(vec![reference]);
    SubjectNameStrategy::TopicNameStrategyWithSchema(String::from("protoencoder"), false, schema)
}

fn sr_settings() -> SrSettings {
    SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap()
}

fn as_json(message: &DynamicMessage) -> serde_json::Value {
    serde_json::to_value(message).unwrap()
}

fn expected_json() -> serde_json::Value {
    json!({
        "id": "some-id",
        "results": [{"up": "STRING", "down": "string"}],
        "counts": {"x": "3"},
        "inner": {"flag": true}
    })
}

#[test]
fn test_proto_encoder_round_trip() {
    let encoder = ProtoEncoder::new(sr_settings());
    let decoder = ProtoDecoder::new(sr_settings());
    let strategy = strategy();

    let bytes = encoder
        .encode_json(
            &expected_json(),
            "org.schema_registry_converter.encoder.Envelope",
            &strategy,
        )
        .unwrap();
    // magic byte, 4 byte schema id, then the message index [1]: count 1 and index 1, zigzag
    assert_eq!(bytes[0], 0);
    assert_eq!(&bytes[5..7], &[2, 2]);

    let message = match decoder.decode(Some(&bytes)).unwrap() {
        Value::Message(message) => message,
        v => panic!("Other value: {:?} than expected Message", v),
    };
    assert_eq!(
        message.descriptor().full_name(),
        "org.schema_registry_converter.encoder.Envelope"
    );
    assert_eq!(as_json(&message), expected_json());

    // a nested message, created with the descriptor from the encoder
    let descriptor = encoder
        .get_message_descriptor(
            "org.schema_registry_converter.encoder.Envelope.Inner",
            &strategy,
        )
        .unwrap();
    let mut inner = DynamicMessage::new(descriptor);
    inner.set_field_by_name("flag", Value::Bool(true));
    let bytes = encoder.encode(&inner, &strategy).unwrap();
    // message index [1, 0]: count 2, then 1 and 0
    assert_eq!(&bytes[5..8], &[4, 2, 0]);
    let result = decoder.decode_with_context(Some(&bytes)).unwrap().unwrap();
    assert_eq!(
        result.full_name,
        "org.schema_registry_converter.encoder.Envelope.Inner"
    );
    assert_eq!(as_json(&result.value), json!({"flag": true}));
    assert!(result.context.schema.guid.is_some());
}

#[test]
fn test_proto_encoder_round_trip_with_header_id() {
    let encoder = ProtoEncoder::new(sr_settings());
    let decoder = ProtoDecoder::new(sr_settings());

    let (bytes, header) = encoder
        .encode_json_with_header_id(
            &expected_json(),
            "org.schema_registry_converter.encoder.Envelope",
            &strategy(),
            false,
        )
        .unwrap();
    assert_eq!(header.name, VALUE_SCHEMA_ID_HEADER);

    let result = decoder
        .decode_with_context_and_header_id(Some(&header.value), Some(&bytes))
        .unwrap()
        .unwrap();
    assert_eq!(
        result.full_name,
        "org.schema_registry_converter.encoder.Envelope"
    );
    assert_eq!(as_json(&result.value), expected_json());
}
