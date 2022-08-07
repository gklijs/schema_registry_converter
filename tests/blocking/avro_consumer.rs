use crate::blocking::kafka_consumer::get_consumer;
use apache_avro::types::Value;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use schema_registry_converter::blocking::avro::AvroDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;

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
    let sr_settings = SrSettings::new(registry);
    let decoder = AvroDecoder::new(sr_settings);
    let consumer = get_consumer(brokers, group_id, topics, auto_commit);

    for message in consumer.iter() {
        match message {
            Err(e) => {
                assert!(false, "Got error consuming message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_avro_record(&m, &decoder);
                test(des_r);
                return;
            }
        };
    }
}

fn get_deserialized_avro_record<'a>(
    m: &'a BorrowedMessage,
    decoder: &'a AvroDecoder,
) -> DeserializedAvroRecord<'a> {
    let key = match decoder.decode(m.key()) {
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
    };
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
