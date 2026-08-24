use protofish::decode::Value;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::Message;

use schema_registry_converter::blocking::proto_decoder::ProtoDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::VALUE_SCHEMA_ID_HEADER;

use crate::blocking::kafka_consumer::get_consumer;

#[derive(Debug)]
pub struct DeserializedProtoRecord<'a> {
    pub key: String,
    pub value: Value,
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
}

pub fn consume_proto(
    brokers: &str,
    group_id: &str,
    registry: String,
    topics: &[&str],
    auto_commit: bool,
    test: Box<dyn Fn(DeserializedProtoRecord)>,
) {
    let sr_settings = SrSettings::new_builder(registry)
        .no_proxy()
        .build()
        .unwrap();
    let decoder = ProtoDecoder::new(sr_settings);
    let consumer = get_consumer(brokers, group_id, topics, auto_commit);

    match consumer.iter().next() {
        Some(r) => match r {
            Err(e) => {
                panic!("Got error producing message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_proto_record(&m, &decoder);
                test(des_r);
            }
        },
        None => panic!("No next record for proto consumer"),
    };
}

fn get_deserialized_proto_record<'a>(
    m: &'a BorrowedMessage,
    decoder: &'a ProtoDecoder,
) -> DeserializedProtoRecord<'a> {
    let key = deserialize_key(m);
    print!("value needed for test {:?}", m.payload());
    let value = match decoder.decode(m.payload()) {
        Ok(v) => v,
        Err(e) => panic!("Error getting value: {}", e),
    };
    DeserializedProtoRecord {
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

/// Like [`consume_proto`], but for a message whose schema id/guid (and message index) is
/// carried in a `__value_schema_id` header instead of the payload prefix, mirroring Confluent's
/// `HeaderSchemaIdSerializer`/`DualSchemaIdDeserializer`. See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub fn consume_proto_with_header_id(
    brokers: &str,
    group_id: &str,
    registry: String,
    topics: &[&str],
    auto_commit: bool,
    test: Box<dyn Fn(DeserializedProtoRecord)>,
) {
    let sr_settings = SrSettings::new_builder(registry)
        .no_proxy()
        .build()
        .unwrap();
    let decoder = ProtoDecoder::new(sr_settings);
    let consumer = get_consumer(brokers, group_id, topics, auto_commit);

    match consumer.iter().next() {
        Some(r) => match r {
            Err(e) => {
                panic!("Got error producing message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_proto_record_with_header_id(&m, &decoder);
                test(des_r);
            }
        },
        None => panic!("No next record for proto consumer"),
    };
}

fn get_deserialized_proto_record_with_header_id<'a>(
    m: &'a BorrowedMessage,
    decoder: &'a ProtoDecoder,
) -> DeserializedProtoRecord<'a> {
    let key = deserialize_key(m);
    let header_value = m.headers().and_then(|headers| {
        headers
            .iter()
            .find(|h| h.key == VALUE_SCHEMA_ID_HEADER)
            .and_then(|h| h.value)
    });
    let value = match decoder.decode_with_header_id(header_value, m.payload()) {
        Ok(v) => v,
        Err(e) => panic!("Error getting value: {}", e),
    };
    DeserializedProtoRecord {
        key,
        value,
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
    }
}
