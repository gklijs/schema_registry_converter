use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::Message;
use serde_json::Value;

use schema_registry_converter::blocking::json::JsonDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::VALUE_SCHEMA_ID_HEADER;

use crate::blocking::kafka_consumer::get_consumer;

#[derive(Debug)]
pub struct DeserializedJsonRecord<'a> {
    pub key: String,
    pub value: Value,
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
}

/// Consumes a message encoded with the default confluent wire format (schema id prefixing the
/// payload).
pub fn consume_json(
    brokers: &str,
    group_id: &str,
    registry: String,
    topics: &[&str],
    auto_commit: bool,
    test: Box<dyn Fn(DeserializedJsonRecord)>,
) {
    let sr_settings = SrSettings::new_builder(registry)
        .no_proxy()
        .build()
        .unwrap();
    let mut decoder = JsonDecoder::new(sr_settings);
    let consumer = get_consumer(brokers, group_id, topics, auto_commit);

    match consumer.iter().next() {
        Some(r) => match r {
            Err(e) => {
                panic!("Got error consuming message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_json_record(&m, &mut decoder);
                test(des_r);
            }
        },
        None => panic!("No record received in json consumer, while that was expected"),
    };
}

fn get_deserialized_json_record<'a>(
    m: &'a BorrowedMessage,
    decoder: &mut JsonDecoder,
) -> DeserializedJsonRecord<'a> {
    let key = deserialize_key(m);
    print!("value needed for test {:?}", m.payload());
    let value = match decoder.decode(m.payload()) {
        Ok(Some(v)) => v.value,
        Ok(None) => panic!("Expected a value, got a tombstone"),
        Err(e) => panic!("Error getting value: {}", e),
    };
    DeserializedJsonRecord {
        key,
        value,
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
    }
}

fn deserialize_key(m: &BorrowedMessage) -> String {
    match String::from_utf8(Vec::from(m.key().unwrap())) {
        Ok(s) => s,
        Err(_) => {
            println!("It was not a String.. Setting empty string");
            String::from("")
        }
    }
}

/// Like [`consume_json`], but for a message whose schema id/guid is carried in a
/// `__value_schema_id` header instead of the payload prefix, mirroring Confluent's
/// `HeaderSchemaIdSerializer`/`DualSchemaIdDeserializer`. See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub fn consume_json_with_header_id(
    brokers: &str,
    group_id: &str,
    registry: String,
    topics: &[&str],
    auto_commit: bool,
    test: Box<dyn Fn(DeserializedJsonRecord)>,
) {
    let sr_settings = SrSettings::new_builder(registry)
        .no_proxy()
        .build()
        .unwrap();
    let mut decoder = JsonDecoder::new(sr_settings);
    let consumer = get_consumer(brokers, group_id, topics, auto_commit);

    match consumer.iter().next() {
        Some(r) => match r {
            Err(e) => {
                panic!("Got error consuming message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_json_record_with_header_id(&m, &mut decoder);
                test(des_r);
            }
        },
        None => panic!("No record received in json consumer, while that was expected"),
    };
}

fn get_deserialized_json_record_with_header_id<'a>(
    m: &'a BorrowedMessage,
    decoder: &mut JsonDecoder,
) -> DeserializedJsonRecord<'a> {
    let key = deserialize_key(m);
    let header_value = m.headers().and_then(|headers| {
        headers
            .iter()
            .find(|h| h.key == VALUE_SCHEMA_ID_HEADER)
            .and_then(|h| h.value)
    });
    print!("value needed for test {:?}", m.payload());
    let value = match decoder.decode_with_header_id(header_value, m.payload()) {
        Ok(Some(v)) => v.value,
        Ok(None) => panic!("Expected a value, got a tombstone"),
        Err(e) => panic!("Error getting value: {}", e),
    };
    DeserializedJsonRecord {
        key,
        value,
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
    }
}
