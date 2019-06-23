extern crate schema_registry_converter;

mod kafka_consumer;
mod kafka_producer;

use crate::kafka_consumer::{consume, DeserializedRecord};
use crate::kafka_producer::get_producer;
use avro_rs::types::Value;
use rand::Rng;
use schema_registry_converter::schema_registry::{SubjectNameStrategy, SuppliedSchema};

fn get_schema_registry_url() -> String {
    "localhost:8081".into()
}

fn get_brokers() -> &'static str {
    "127.0.0.1:9092"
}

fn get_heartbeat_schema() -> SuppliedSchema {
    SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into())
}

fn test_beat_value(key_value: i64, value_value: i64) -> Box<Fn(DeserializedRecord) -> ()> {
    Box::new(move |rec: DeserializedRecord| {
        println!("testing record {:#?}", rec);
        let key_values = match rec.key {
            Value::Record(v) => v,
            _ => panic!("Not a record, while only only those expected"),
        };
        let beat_key = match &key_values[0] {
            (_id, Value::Long(v)) => v,
            _ => panic!("Not a long value of while that was expected"),
        };
        assert_eq!(&key_value, beat_key, "compare key values");
        let value_values = match rec.value {
            Value::Record(v) => v,
            _ => panic!("Not a record, while only only those expected"),
        };
        let beat_value = match &value_values[0] {
            (_id, Value::Long(v)) => v,
            _ => panic!("Not a long value of while that was expected"),
        };
        assert_eq!(&value_value, beat_value, "compare value values");
    })
}

fn do_test(topic: &str, key_strategy: SubjectNameStrategy, value_strategy: SubjectNameStrategy) {
    let mut rng = rand::thread_rng();
    let key_value = rng.gen::<i64>();
    let value_value = rng.gen::<i64>();
    let mut producer = get_producer(get_brokers(), get_schema_registry_url());
    let key_values = vec![("beat", Value::Long(key_value))];
    let value_values = vec![("beat", Value::Long(value_value))];
    producer.send(
        topic,
        key_values,
        value_values,
        key_strategy,
        value_strategy,
    );
    consume(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &vec![topic],
        test_beat_value(key_value, value_value),
    )
}

#[test]
#[cfg_attr(not(feature = "kafka_test"), ignore)]
fn test1_topic_name_strategy_with_schema() {
    let topic = "topicnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        topic.into(),
        true,
        get_heartbeat_schema(),
    );
    let value_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        topic.into(),
        false,
        get_heartbeat_schema(),
    );
    do_test(topic, key_strategy, value_strategy)
}

#[test]
#[cfg_attr(not(feature = "kafka_test"), ignore)]
fn test2_record_name_strategy_with_schema() {
    let topic = "recordnamestrategy";
    let key_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_heartbeat_schema());
    let value_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_heartbeat_schema());
    do_test(topic, key_strategy, value_strategy)
}

#[test]
#[cfg_attr(not(feature = "kafka_test"), ignore)]
fn test3_topic_record_name_strategy_with_schema() {
    let topic = "topicrecordnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
        topic.into(),
        get_heartbeat_schema(),
    );
    let value_strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
        topic.into(),
        get_heartbeat_schema(),
    );
    do_test(topic, key_strategy, value_strategy)
}

#[test]
#[cfg_attr(not(feature = "kafka_test"), ignore)]
fn test4_topic_name_strategy_schema_now_available() {
    let topic = "topicnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicNameStrategy(topic.into(), true);
    let value_strategy = SubjectNameStrategy::TopicNameStrategy(topic.into(), false);
    do_test(topic, key_strategy, value_strategy)
}

#[test]
#[cfg_attr(not(feature = "kafka_test"), ignore)]
fn test5_record_name_strategy_schema_now_available() {
    let topic = "recordnamestrategy";
    let key_strategy = SubjectNameStrategy::RecordNameStrategy("nl.openweb.data.Heartbeat".into());
    let value_strategy =
        SubjectNameStrategy::RecordNameStrategy("nl.openweb.data.Heartbeat".into());
    do_test(topic, key_strategy, value_strategy)
}

#[test]
#[cfg_attr(not(feature = "kafka_test"), ignore)]
fn test6_topic_record_name_strategy_schema_now_available() {
    let topic = "topicrecordnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicRecordNameStrategy(
        topic.into(),
        "nl.openweb.data.Heartbeat".into(),
    );
    let value_strategy = SubjectNameStrategy::TopicRecordNameStrategy(
        topic.into(),
        "nl.openweb.data.Heartbeat".into(),
    );
    do_test(topic, key_strategy, value_strategy)
}
