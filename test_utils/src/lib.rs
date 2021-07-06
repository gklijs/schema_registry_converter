use serde::{Deserialize, Serialize};

pub fn get_json_body(schema: &str, id: u32) -> String {
    format!(
        "{{\"schema\":\"{}\", \"schemaType\":\"JSON\", \"id\":{}}}",
        schema, id
    )
}

pub fn get_json_body_with_reference(schema: &str, id: u32, reference: &str) -> String {
    format!(
        "{{\"schema\":\"{}\", \"schemaType\":\"JSON\", \"id\":{}, \"references\":[{}]}}",
        schema, id, reference
    )
}

pub fn get_proto_hb_schema() -> &'static str {
    r#"syntax = \"proto3\";package nl.openweb.data;message Heartbeat {uint64 beat = 1;}"#
}

pub fn get_proto_result() -> &'static str {
    r#"syntax = \"proto3\"; package org.schema_registry_test_app.proto; message Result { string up = 1; string down = 2; } "#
}

pub fn get_proto_complex() -> &'static str {
    r#"syntax = \"proto3\"; import \"result.proto\"; message A {bytes id = 1;} message B {bytes id = 1;} message C {bytes id = 1; D d = 2; message D {int64 counter = 1;}} package org.schema_registry_test_app.proto; message ProtoTest {bytes id = 1; enum Language {Java = 0;Rust = 1;} Language by = 2;int64 counter = 3;string input = 4;repeated A results = 5;}"#
}

pub fn get_proto_hb_101_only_data() -> &'static [u8] {
    &get_proto_hb_101()[6..]
}

pub fn get_proto_hb_101() -> &'static [u8] {
    &[0, 0, 0, 0, 7, 0, 8, 101]
}

pub fn get_proto_complex_only_data() -> &'static [u8] {
    &get_proto_complex_proto_test_message()[7..]
}

pub fn get_proto_complex_proto_test_message() -> &'static [u8] {
    &[
        0, 0, 0, 0, 6, 2, 6, 10, 16, 11, 134, 69, 48, 212, 168, 77, 40, 147, 167, 30, 246, 208, 32,
        252, 79, 24, 1, 34, 6, 83, 116, 114, 105, 110, 103, 42, 16, 10, 6, 83, 84, 82, 73, 78, 71,
        18, 6, 115, 116, 114, 105, 110, 103,
    ]
}

pub fn get_proto_body(schema: &str, id: u32) -> String {
    format!(
        "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":{}}}",
        schema, id
    )
}

pub fn get_proto_body_with_reference(schema: &str, id: u32, reference: &str) -> String {
    format!(
        "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":{}, \"references\":[{}]}}",
        schema, id, reference
    )
}

pub fn get_proto_complex_references() -> &'static str {
    r#"{"name": "result.proto", "subject": "result.proto", "version": 1}"#
}

pub fn json_result_schema() -> &'static str {
    r#"{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Json Test\",\"type\":\"object\",\"additionalProperties\":false,\"javaType\":\"org.schema_registry_test_app.json.Result\",\"properties\":{\"up\":{\"type\":\"string\"},\"down\":{\"type\":\"string\"}},\"required\":[\"up\",\"down\"]}"#
}

pub fn json_result_schema_with_id() -> &'static str {
    r#"{\"$id\":\"http://www.example.com/result.json\",\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Json Test\",\"type\":\"object\",\"additionalProperties\":false,\"javaType\":\"org.schema_registry_test_app.json.Result\",\"properties\":{\"up\":{\"type\":\"string\"},\"down\":{\"type\":\"string\"}},\"required\":[\"up\",\"down\"]}"#
}

pub fn json_test_ref_schema() -> &'static str {
    r#"{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Json Test\",\"type\":\"object\",\"additionalProperties\":false,\"javaType\":\"org.schema_registry_test_app.json.JsonTest\",\"properties\":{\"id\":{\"type\":\"array\",\"items\":{\"type\":\"integer\"}},\"by\":{\"type\":\"string\",\"enum\":[\"Java\",\"Rust\",\"Js\",\"Python\",\"Go\",\"C\"]},\"counter\":{\"type\":\"integer\"},\"input\":{\"type\":\"string\"},\"results\":{\"type\":\"array\",\"items\":{\"$ref\":\"http://www.example.com/result.json\"}}},\"required\":[\"id\",\"by\",\"counter\",\"results\"]}"#
}

pub fn json_get_result_references() -> &'static str {
    r#"{"name": "http://www.example.com/result.json", "subject": "result.json", "version": 1}"#
}

pub fn json_result_java_bytes() -> &'static [u8] {
    &[
        0, 0, 0, 0, 10, 123, 34, 100, 111, 119, 110, 34, 58, 34, 115, 116, 114, 105, 110, 103, 34,
        44, 34, 117, 112, 34, 58, 34, 83, 84, 82, 73, 78, 71, 34, 125,
    ]
}

pub fn json_incorrect_bytes() -> &'static [u8] {
    &[
        0, 0, 0, 0, 10, 0, 34, 100, 111, 119, 110, 34, 58, 34, 115, 116, 114, 105, 110, 103, 34,
        44, 34, 117, 112, 34, 58, 34, 83, 84, 82, 73, 78, 71, 34, 125,
    ]
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Heartbeat {
    pub beat: i64,
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub enum Atype {
    #[serde(rename = "AUTO")]
    Auto,
    #[serde(rename = "MANUAL")]
    Manual,
}

impl Default for Atype {
    fn default() -> Self {
        Atype::Auto
    }
}

pub type Uuid = [u8; 16];

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConfirmAccountCreation {
    pub id: Uuid,
    pub a_type: Atype,
}

impl Default for ConfirmAccountCreation {
    fn default() -> ConfirmAccountCreation {
        ConfirmAccountCreation {
            id: Uuid::default(),
            a_type: Atype::Auto,
        }
    }
}
