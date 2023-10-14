use schema_registry_converter::proto_resolver::{IndexResolver, MessageResolver};
use std::sync::Arc;

fn get_proto_complex() -> &'static str {
    r#"syntax = "proto3"; import "result.proto"; message A {bytes id = 1;} message B {bytes id = 1;} message C {bytes id = 1; D d = 2; message D {int64 counter = 1;}} package org.schema_registry_test_app.proto; message ProtoTest {bytes id = 1; enum Language {Java = 0;Rust = 1;} Language by = 2;int64 counter = 3;string input = 4;repeated A results = 5;}"#
}

fn get_three_complex() -> &'static str {
    r#"syntax = "proto3";
package org.schema_registry_test_app.proto;

import "google/type/color.proto";
import "google/type/datetime.proto";
import "google/type/money.proto";

message GoogleTest {
  google.type.Color color = 1;
  google.type.DateTime dateTime = 2;
  google.type.Money money = 3;
  Language by = 4;
  int64 counter = 5;

  enum Language {
    Java = 0;
    Rust = 1;
    Js = 2;
    Python = 3;
    Go = 4;
    C = 5;
  }
}"#
}

#[test]
fn test_schema_with_three_imports() {
    let resolver = MessageResolver::new(get_three_complex());
    assert_eq!(resolver.imports.len(), 3);
    assert_eq!(resolver.imports[0], String::from("google/type/color.proto"));
    assert_eq!(
        resolver.imports[1],
        String::from("google/type/datetime.proto")
    );
    assert_eq!(resolver.imports[2], String::from("google/type/money.proto"));
}

#[test]
fn test_complex_schema_index_resolver() {
    let resolver = IndexResolver::new(get_proto_complex());

    assert_eq!(
        resolver.find_index("org.schema_registry_test_app.proto.A"),
        Some(Arc::new(vec![0]))
    );
    assert_eq!(
        resolver.find_index("org.schema_registry_test_app.proto.C.D"),
        Some(Arc::new(vec![2, 0]))
    );
    assert_eq!(
        resolver.find_index("org.schema_registry_test_app.proto.ProtoTest"),
        Some(Arc::new(vec![3]))
    );
}
