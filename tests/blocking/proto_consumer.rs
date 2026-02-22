use protofish::decode::Value;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

use schema_registry_converter::blocking::proto_decoder::ProtoDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;

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
        Some(r) => {
            match r {
                Err(e) => {
                    panic!("Got error producing message: {}", e);
                }
                Ok(m) => {
                    let des_r = get_deserialized_proto_record(&m, &decoder);
                    test(des_r);
                }
            }
        }
        None => panic!("No next record for proto consumer")
    };
}

fn get_deserialized_proto_record<'a>(
    m: &'a BorrowedMessage,
    decoder: &'a ProtoDecoder,
) -> DeserializedProtoRecord<'a> {
    let key = match String::from_utf8(Vec::from(m.key().unwrap())) {
        Ok(s) => s,
        Err(_) => {
            println!("It was not a String.. Setting empty string");
            String::from("")
        }
    };
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
