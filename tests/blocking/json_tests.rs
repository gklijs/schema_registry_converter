extern crate schema_registry_converter;

use serde_json::Value;

use crate::blocking::json_consumer::{
    consume_json, consume_json_with_header_id, DeserializedJsonRecord,
};

fn get_schema_registry_url() -> String {
    String::from("http://localhost:8081")
}

fn get_brokers() -> &'static str {
    "127.0.0.1:9092"
}

/// Assertions shared by `test1_test_json_from_java_test_app` and
/// `test2_test_json_header_id_from_java_test_app`: both consume the same `JsonTest` value the
/// Java test app produces (see `TestJson.testValue()`), just via different wire formats.
fn assert_java_json_test_record(rec: DeserializedJsonRecord, topic: &str) {
    println!("testing record {:#?}", rec);
    assert_eq!(String::from("testkey"), rec.key, "check string key");
    let value = match &rec.value {
        Value::Object(v) => v,
        _ => panic!("Not an object value while that was expected"),
    };
    match value.get("id") {
        Some(Value::Array(v)) => assert_eq!(16, v.len(), "expected id of 16 size"),
        _ => panic!("Not an array value for id while that was expected"),
    };
    assert_eq!(
        Some(&Value::String(String::from("Java"))),
        value.get("by"),
        "expect message from Java"
    );
    assert_eq!(Some(&Value::from(1)), value.get("counter"), "counter is 1");
    assert_eq!(
        Some(&Value::String(String::from("String"))),
        value.get("input"),
        "Optional string is string"
    );
    let results = match value.get("results") {
        Some(Value::Array(v)) => v,
        _ => panic!("Not an array value for results while that was expected"),
    };
    let result = match results.first().expect("one item to be present") {
        Value::Object(v) => v,
        _ => panic!("Not an object for first of results while that was expected"),
    };
    assert_eq!(
        Some(&Value::String(String::from("STRING"))),
        result.get("up"),
        "expected upper case string"
    );
    assert_eq!(
        Some(&Value::String(String::from("string"))),
        result.get("down"),
        "expected lower case string"
    );
    assert_eq!(
        rec.topic, topic,
        "Topic in record should match the actual topic"
    );
    assert!(
        rec.partition >= 0,
        "Partition in record should be a positive number"
    );
    assert!(
        rec.offset >= 0,
        "Offset in record should be a positive number"
    );
}

#[test]
fn test1_test_json_from_java_test_app() {
    let topic = "testjson";
    consume_json(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        false,
        Box::new(move |rec| assert_java_json_test_record(rec, topic)),
    )
}

#[test]
fn test2_test_json_header_id_from_java_test_app() {
    // Consumes what the Java test app produces to "testjsonheader" via HeaderSchemaIdSerializer:
    // the schema guid goes in a __value_schema_id header instead of the payload prefix. See
    // https://github.com/gklijs/schema_registry_converter/issues/139.
    let topic = "testjsonheader";
    consume_json_with_header_id(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        false,
        Box::new(move |rec| assert_java_json_test_record(rec, topic)),
    )
}
