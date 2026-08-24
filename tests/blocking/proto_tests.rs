extern crate schema_registry_converter;

use protofish::decode::Value;

use crate::blocking::proto_consumer::{
    consume_proto, consume_proto_with_header_id, DeserializedProtoRecord,
};

pub fn get_schema_registry_url() -> String {
    String::from("http://localhost:8081")
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
    let value_values = match rec.value {
        Value::Message(v) => v.fields,
        _ => panic!("Not a message value while that was expected"),
    };
    match &value_values[0].value {
        Value::Bytes(v) => assert_eq!(16, v.len(), "expected id of 16 size"),
        _ => panic!("Not a bytes value for counter while that was expected"),
    };
    let counter_value = match &value_values[1].value {
        Value::Int64(v) => v,
        _ => panic!("Not a long value for counter while that was expected"),
    };
    assert_eq!(&1, counter_value, "counter is 1");
    let input_value = match &value_values[2].value {
        Value::String(v) => v,
        _ => panic!("Not a string value for input while that was expected"),
    };
    assert_eq!(
        &String::from("String"),
        input_value,
        "Optional string is string"
    );
    let results = match &value_values[3].value {
        Value::Message(v) => v.fields.clone(),
        _ => panic!("Not an array value for results while that was expected"),
    };
    assert_eq!(2, results.len(), "Expect two result");
    let input_value = match &results[0].value {
        Value::String(v) => v,
        _ => panic!("Not a string value for input while that was expected"),
    };
    assert_eq!(
        &String::from("STRING"),
        input_value,
        "Optional string is STRING"
    );
    let input_value = match &results[1].value {
        Value::String(v) => v,
        _ => panic!("Not a string value for input while that was expected"),
    };
    assert_eq!(
        &String::from("string"),
        input_value,
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
            Value::Message(v) => v.fields,
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
