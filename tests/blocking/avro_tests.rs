extern crate schema_registry_converter;

use crate::blocking::avro_consumer::{consume_avro, DeserializedAvroRecord};
use crate::blocking::kafka_producer::get_producer;
use avro_rs::types::Value;
use rand::Rng;
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};

fn get_schema_registry_url() -> String {
    String::from("http://localhost:8081")
}

fn get_brokers() -> &'static str {
    "127.0.0.1:9092"
}

fn get_heartbeat_schema() -> Box<SuppliedSchema> {
    Box::from(SuppliedSchema {
        name: Some(String::from("nl.openweb.data.Heartbeat")),
        schema_type: SchemaType::Avro,
        schema: String::from(
            r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
        ),
        references: vec![],
    })
}

fn test_beat_value(key_value: i64, value_value: i64) -> Box<dyn Fn(DeserializedAvroRecord) -> ()> {
    Box::new(move |rec: DeserializedAvroRecord| {
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

fn do_avro_test(
    topic: &str,
    key_strategy: SubjectNameStrategy,
    value_strategy: SubjectNameStrategy,
) {
    let mut rng = rand::thread_rng();
    let key_value = rng.gen::<i64>();
    let value_value = rng.gen::<i64>();
    let mut producer = get_producer(get_brokers(), get_schema_registry_url());
    let key_values = vec![("beat", Value::Long(key_value))];
    let value_values = vec![("beat", Value::Long(value_value))];
    producer.send_avro(
        topic,
        key_values,
        value_values,
        key_strategy,
        value_strategy,
    );
    consume_avro(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        true,
        test_beat_value(key_value, value_value),
    )
}

#[test]
fn test1_topic_name_strategy_with_schema() {
    let topic = "topicnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        String::from(topic),
        true,
        get_heartbeat_schema(),
    );
    let value_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        String::from(topic),
        false,
        get_heartbeat_schema(),
    );
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test2_record_name_strategy_with_schema() {
    let topic = "recordnamestrategy";
    let key_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_heartbeat_schema());
    let value_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_heartbeat_schema());
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test3_topic_record_name_strategy_with_schema() {
    let topic = "topicrecordnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
        String::from(topic),
        get_heartbeat_schema(),
    );
    let value_strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
        String::from(topic),
        get_heartbeat_schema(),
    );
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test4_topic_name_strategy_schema_now_available() {
    let topic = "topicnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicNameStrategy(String::from(topic), true);
    let value_strategy = SubjectNameStrategy::TopicNameStrategy(String::from(topic), false);
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test5_record_name_strategy_schema_now_available() {
    let topic = "recordnamestrategy";
    let key_strategy =
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
    let value_strategy =
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test6_topic_record_name_strategy_schema_now_available() {
    let topic = "topicrecordnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicRecordNameStrategy(
        String::from(topic),
        String::from("nl.openweb.data.Heartbeat"),
    );
    let value_strategy = SubjectNameStrategy::TopicRecordNameStrategy(
        String::from(topic),
        String::from("nl.openweb.data.Heartbeat"),
    );
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test7_test_avro_from_java_test_app() {
    let topic = "testavro";
    let test = Box::new(move |rec: DeserializedAvroRecord| {
        println!("testing record {:#?}", rec);
        match rec.key {
            Value::String(s) => assert_eq!("testkey", s, "check string key"),
            _ => panic!("Keys wasn't a string"),
        };
        let value_values = match rec.value {
            Value::Record(v) => v,
            _ => panic!("Not a record, while only only those expected"),
        };
        let id_key = match &value_values[0] {
            (_id, Value::Fixed(16, _v)) => _id,
            _ => panic!("Not a fixed value of 16 bytes while that was expected"),
        };
        assert_eq!("id", id_key, "expected id key to be id");
        let enum_value = match &value_values[1] {
            (_id, Value::Enum(0, v)) => v,
            _ => panic!("Not an enum value for by while that was expected"),
        };
        assert_eq!("Java", enum_value, "expect message from Java");
        let counter_value = match &value_values[2] {
            (_id, Value::Long(v)) => v,
            _ => panic!("Not a long value for counter while that was expected"),
        };
        assert_eq!(&1i64, counter_value, "counter is 1");
        let input_value = match &value_values[3] {
            (_id, Value::Union(v)) => v,
            _ => panic!("Not an unions value for input while that was expected"),
        };
        assert_eq!(
            &Box::new(Value::String(String::from("String"))),
            input_value,
            "Optional string is string"
        );
        let results = match &value_values[4] {
            (_id, Value::Array(v)) => v,
            _ => panic!("Not an array value for results while that was expected"),
        };
        let result = match results.get(0).expect("one item to be present") {
            Value::Record(v) => v,
            _ => panic!("Not record for first of results while that was expected"),
        };
        let up_result = match &result[0] {
            (_id, Value::String(v)) => v,
            _ => panic!("First result value wasn't a string"),
        };
        assert_eq!("STRING", up_result, "expected upper case string");
        let down_result = match &result[1] {
            (_id, Value::String(v)) => v,
            _ => panic!("Second result value wasn't a string"),
        };
        assert_eq!("string", down_result, "expected upper case string");
    });
    consume_avro(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        false,
        test,
    )
}
