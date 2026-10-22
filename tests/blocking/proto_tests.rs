extern crate schema_registry_converter;

use schema_registry_converter::prost_reflect::{DynamicMessage, ReflectMessage, Value};

use crate::blocking::proto_consumer::{
    DeserializedProtoRecord, consume_proto, consume_proto_with_header_id,
};

pub fn get_schema_registry_url() -> String {
    String::from("http://localhost:8081")
}

fn field(message: &DynamicMessage, name: &str) -> Value {
    message
        .get_field_by_name(name)
        .unwrap_or_else(|| panic!("no field {} in {}", name, message.descriptor().full_name()))
        .into_owned()
}

fn get_brokers() -> &'static str {
    "127.0.0.1:9092"
}

/// Assertions shared by `test1_test_proto_from_java_test_app` and
/// `test3_test_proto_header_id_from_java_test_app`: both consume the same `ProtoTest` value the
/// Java test app produces (see `TestProto.testValue()`), just via different wire formats.
fn assert_java_proto_test_record(rec: DeserializedProtoRecord, topic: &str) {
    println!("testing record {:#?}", rec);
    assert_eq!(String::from("testkey"), rec.key, "check string key");
    let message = match rec.value {
        Value::Message(v) => v,
        _ => panic!("Not a message value while that was expected"),
    };
    match field(&message, "id") {
        Value::Bytes(v) => assert_eq!(16, v.len(), "expected id of 16 size"),
        _ => panic!("Not a bytes value for id while that was expected"),
    };
    assert_eq!(Value::I64(1), field(&message, "counter"), "counter is 1");
    assert_eq!(
        Value::String(String::from("String")),
        field(&message, "input"),
        "Optional string is string"
    );
    // `results` is a `repeated Result`, the Java test app sets a single one.
    let results = match field(&message, "results") {
        Value::List(v) => v,
        _ => panic!("Not a list value for results while that was expected"),
    };
    assert_eq!(1, results.len(), "Expect one result");
    let results = match &results[0] {
        Value::Message(v) => v,
        _ => panic!("Not a message value for the result while that was expected"),
    };
    assert_eq!(
        Value::String(String::from("STRING")),
        field(results, "up"),
        "Optional string is STRING"
    );
    assert_eq!(
        Value::String(String::from("string")),
        field(results, "down"),
        "Optional string is string"
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
fn test1_test_proto_from_java_test_app() {
    let topic = "testproto";
    consume_proto(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        false,
        Box::new(move |rec| assert_java_proto_test_record(rec, topic)),
    )
}

#[test]
fn test2_test_google_proto_from_java_test_app() {
    let topic = "testgoogle";
    let test = Box::new(move |rec: DeserializedProtoRecord| {
        println!("testing record {:#?}", rec);
        assert_eq!(String::from("testkey"), rec.key, "check string key");
        match rec.value {
            Value::Message(v) => v,
            _ => panic!("Not a message, while only only those expected"),
        };
    });
    consume_proto(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        false,
        test,
    )
}

#[test]
fn test3_test_proto_header_id_from_java_test_app() {
    // Consumes what the Java test app produces to "testprotoheader" via HeaderSchemaIdSerializer
    // -- same ProtoTest value as test1, but with the schema guid (and message index) in a
    // __value_schema_id header instead of the payload prefix. See
    // https://github.com/gklijs/schema_registry_converter/issues/139.
    let topic = "testprotoheader";
    consume_proto_with_header_id(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        false,
        Box::new(move |rec| assert_java_proto_test_record(rec, topic)),
    )
}
