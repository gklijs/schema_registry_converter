use crate::blocking::kafka_consumer::get_consumer;
use apache_avro::types::Value;
use rdkafka::Message;
use rdkafka::message::{BorrowedMessage, Headers};
use schema_registry_converter::blocking::avro::AvroDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::VALUE_SCHEMA_ID_HEADER;

#[derive(Debug)]
pub struct DeserializedAvroRecord<'a> {
    pub key: Value,
    pub value: Value,
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
}

pub fn consume_avro(
    brokers: &str,
    group_id: &str,
    registry: String,
    topics: &[&str],
    auto_commit: bool,
    test: Box<dyn Fn(DeserializedAvroRecord)>,
) {
    let sr_settings = SrSettings::new_builder(registry)
        .no_proxy()
        .build()
        .unwrap();
    let decoder = AvroDecoder::new(sr_settings);
    let consumer = get_consumer(brokers, group_id, topics, auto_commit);

    match consumer.iter().next() {
        Some(r) => match r {
            Err(e) => {
                panic!("Got error consuming message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_avro_record(&m, &decoder);
                test(des_r);
            }
        },
        None => panic!("No record received in avro consumer, while that was expected"),
    };
}

fn get_deserialized_avro_record<'a>(
    m: &'a BorrowedMessage,
    decoder: &'a AvroDecoder,
) -> DeserializedAvroRecord<'a> {
    let key = deserialize_key(m, decoder);
    print!("value needed for test {:?}", m.payload());
    let value = match decoder.decode(m.payload()) {
        Ok(v) => v.value,
        Err(e) => panic!("Error getting value: {}", e),
    };
    DeserializedAvroRecord {
        key,
        value,
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
    }
}

fn deserialize_key<'a>(m: &'a BorrowedMessage, decoder: &'a AvroDecoder) -> Value {
    match decoder.decode(m.key()) {
        Ok(v) => v.value,
        Err(e) => {
            println!(
                "encountered error, key probably was not avro encoded: {}",
                e
            );
            match String::from_utf8(Vec::from(m.key().unwrap())) {
                Ok(s) => Value::String(s),
                Err(_) => {
                    println!("It was not a String either :(");
                    Value::Bytes(Vec::from(m.key().unwrap()))
                }
            }
        }
    }
}

/// Like [`consume_avro`], but for a message whose schema id/guid is carried in a
/// `__value_schema_id` header instead of the payload prefix, mirroring Confluent's
/// `HeaderSchemaIdSerializer`/`DualSchemaIdDeserializer`. See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub fn consume_avro_with_header_id(
    brokers: &str,
    group_id: &str,
    registry: String,
    topics: &[&str],
    auto_commit: bool,
    test: Box<dyn Fn(DeserializedAvroRecord)>,
) {
    let sr_settings = SrSettings::new_builder(registry)
        .no_proxy()
        .build()
        .unwrap();
    let decoder = AvroDecoder::new(sr_settings);
    let consumer = get_consumer(brokers, group_id, topics, auto_commit);

    match consumer.iter().next() {
        Some(r) => match r {
            Err(e) => {
                panic!("Got error consuming message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_avro_record_with_header_id(&m, &decoder);
                test(des_r);
            }
        },
        None => panic!("No record received in avro consumer, while that was expected"),
    };
}

fn get_deserialized_avro_record_with_header_id<'a>(
    m: &'a BorrowedMessage,
    decoder: &'a AvroDecoder,
) -> DeserializedAvroRecord<'a> {
    let key = deserialize_key(m, decoder);
    let header_value = m.headers().and_then(|headers| {
        headers
            .iter()
            .find(|h| h.key == VALUE_SCHEMA_ID_HEADER)
            .and_then(|h| h.value)
    });
    let value = match decoder.decode_with_header_id(header_value, m.payload()) {
        Ok(v) => v.value,
        Err(e) => panic!("Error getting value: {}", e),
    };
    DeserializedAvroRecord {
        key,
        value,
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
    }
}
