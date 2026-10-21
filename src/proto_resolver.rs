use std::collections::HashMap;
use std::io::BufReader;
use std::sync::Arc;

use crate::error::SRCError;
use integer_encoding::{VarInt, VarIntReader};
use prost_types::field_descriptor_proto::Type;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};

/// Resolves a confluent message index, the path to a message in the schema, to the full name of
/// that message.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "proto_raw"), allow(dead_code))]
pub(crate) struct MessageResolver {
    map: HashMap<Vec<i32>, Arc<String>>,
}

/// Resolves the full name of a message to its confluent message index.
#[derive(Debug, Clone)]
#[cfg_attr(
    not(any(feature = "proto_encoder", feature = "proto_raw")),
    allow(dead_code)
)]
pub(crate) struct IndexResolver {
    map: HashMap<String, Arc<Vec<i32>>>,
}

#[cfg_attr(not(feature = "proto_raw"), allow(dead_code))]
impl MessageResolver {
    /// Parses the schema text, without resolving its imports.
    pub(crate) fn new(schema: &str) -> Result<MessageResolver, SRCError> {
        Ok(MessageResolver::from_file(&parse_schema(schema)?))
    }

    pub(crate) fn from_file(file: &FileDescriptorProto) -> MessageResolver {
        let map = message_indexes(file)
            .into_iter()
            .map(|(index, name)| (index, Arc::new(name)))
            .collect();
        MessageResolver { map }
    }

    pub(crate) fn find_name(&self, index: &[i32]) -> Option<Arc<String>> {
        self.map.get(index).cloned()
    }
}

impl IndexResolver {
    /// Parses the schema text, without resolving its imports.
    #[cfg_attr(not(feature = "proto_raw"), allow(dead_code))]
    pub(crate) fn new(schema: &str) -> Result<IndexResolver, SRCError> {
        Ok(IndexResolver::from_file(&parse_schema(schema)?))
    }

    #[cfg_attr(
        not(any(feature = "proto_encoder", feature = "proto_raw")),
        allow(dead_code)
    )]
    pub(crate) fn from_file(file: &FileDescriptorProto) -> IndexResolver {
        let map = message_indexes(file)
            .into_iter()
            .map(|(index, name)| (name, Arc::new(index)))
            .collect();
        IndexResolver { map }
    }

    #[cfg_attr(
        not(any(feature = "proto_encoder", feature = "proto_raw")),
        allow(dead_code)
    )]
    pub(crate) fn find_index(&self, name: &str) -> Option<Arc<Vec<i32>>> {
        self.map.get(name).cloned()
    }

    #[cfg_attr(not(feature = "proto_raw"), allow(dead_code))]
    pub(crate) fn is_single_message(&self) -> bool {
        self.map.len() == 1
    }
}

#[cfg_attr(not(feature = "proto_raw"), allow(dead_code))]
fn parse_schema(schema: &str) -> Result<FileDescriptorProto, SRCError> {
    protox_parse::parse("schema.proto", schema)
        .map_err(|e| SRCError::non_retryable_with_cause(e, "Could not parse protobuf schema"))
}

/// The index and full name of every message declared in the schema. An index is the position
/// among the messages declared at that level of nesting, so the messages protobuf generates for
/// `map` and `group` fields don't count, like confluent doesn't count them. Works on both a
/// parsed schema and a compiled one.
pub(crate) fn message_indexes(file: &FileDescriptorProto) -> Vec<(Vec<i32>, String)> {
    let mut result = Vec::new();
    add_declared_messages(
        &file.message_type,
        &file.extension,
        &[],
        file.package(),
        &mut Vec::new(),
        &mut result,
    );
    result
}

fn add_declared_messages(
    messages: &[DescriptorProto],
    extensions: &[FieldDescriptorProto],
    fields: &[FieldDescriptorProto],
    prefix: &str,
    index: &mut Vec<i32>,
    result: &mut Vec<(Vec<i32>, String)>,
) {
    let declared = messages
        .iter()
        .filter(|m| !is_generated(m, fields.iter().chain(extensions)));
    for (i, message) in declared.enumerate() {
        let full_name = if prefix.is_empty() {
            message.name().to_owned()
        } else {
            format!("{}.{}", prefix, message.name())
        };
        index.push(i as i32);
        result.push((index.clone(), full_name.clone()));
        add_declared_messages(
            &message.nested_type,
            &message.extension,
            &message.field,
            &full_name,
            index,
            result,
        );
        index.pop();
    }
}

/// Whether the message is generated for a `map` field, or for one of the `group` fields (a type
/// name that's either relative, when only parsed, or fully qualified, when compiled).
fn is_generated<'a>(
    message: &DescriptorProto,
    mut fields: impl Iterator<Item = &'a FieldDescriptorProto>,
) -> bool {
    message.options.as_ref().is_some_and(|o| o.map_entry())
        || fields.any(|f| {
            f.r#type() == Type::Group && f.type_name().rsplit('.').next() == Some(message.name())
        })
}

/// The confluent wire format bytes of a message index: the number of indexes followed by the
/// indexes, as zigzag varints, or just `0` for the common case of the first message.
#[cfg_attr(
    not(any(feature = "proto_encoder", feature = "proto_raw")),
    allow(dead_code)
)]
pub(crate) fn to_index_bytes(index: &[i32]) -> Vec<u8> {
    if index == [0] {
        return vec![0u8];
    }
    let mut result = (index.len() as i32).encode_var_vec();
    for i in index {
        result.append(&mut i.encode_var_vec())
    }
    result
}

#[cfg_attr(
    not(any(feature = "proto_decoder", feature = "proto_raw")),
    allow(dead_code)
)]
pub(crate) fn to_index_and_data(bytes: &[u8]) -> Result<(Vec<i32>, Vec<u8>), SRCError> {
    if bytes.is_empty() {
        return Err(SRCError::non_retryable_without_cause(
            "Empty bytes given to decode the message index from",
        ));
    }
    if bytes[0] == 0 {
        Ok((vec![0], bytes[1..].to_vec()))
    } else {
        let mut reader = BufReader::with_capacity(bytes.len(), bytes);
        let count: i32 = reader.read_varint().map_err(|e| {
            SRCError::non_retryable_with_cause(e, "Could not read the message index count")
        })?;
        let mut index = Vec::new();
        for _ in 0..count {
            let i = reader.read_varint().map_err(|e| {
                SRCError::non_retryable_with_cause(e, "Could not read a message index value")
            })?;
            index.push(i)
        }
        Ok((index, reader.buffer().to_vec()))
    }
}

#[cfg_attr(not(feature = "proto_raw"), allow(dead_code))]
pub(crate) fn resolve_name(
    resolver: &MessageResolver,
    index: &[i32],
) -> Result<Arc<String>, SRCError> {
    match resolver.find_name(index) {
        Some(n) => Ok(n),
        None => Err(SRCError::non_retryable_without_cause(&format!(
            "Could not retrieve name for index: {:?}",
            index
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::proto_resolver::{to_index_and_data, IndexResolver, MessageResolver};
    use std::sync::Arc;

    fn get_proto_simple() -> &'static str {
        r#"syntax = "proto3";package nl.openweb.data; message Heartbeat{uint64 beat = 1;}"#
    }

    fn get_proto_complex() -> &'static str {
        r#"syntax = "proto3"; import "result.proto"; message A {bytes id = 1;} message B {bytes id = 1;} message C {bytes id = 1; D d = 2; message D {int64 counter = 1;}} package org.schema_registry_test_app.proto; message ProtoTest {bytes id = 1; enum Language {Java = 0;Rust = 1;} Language by = 2;int64 counter = 3;string input = 4;repeated A results = 5;}"#
    }

    fn get_including_optional() -> &'static str {
        r#"syntax = "proto3";

package in.abc.event_entities;

option java_outer_classname = "EventSourceProto";

message EventSource {
  message Actor {
    enum UserEntity {
      USER_ENTITY_UNSPECIFIED = 0;
      USER_ENTITY_ADMIN = 1;
    }

    UserEntity entity = 1; // the user type
    uint32 id = 2; // unique identifier of the user entity
  }
  string system = 1; // the system from where the order event originated from
  optional Actor initiator = 2; // the user that initiated the change to the order
  optional Actor proxy = 3; // the user that executed the change on behalf of another user
  optional string reason = 4; // reason for change to the order
}"#
    }
    fn get_with_comment_line() -> &'static str {
        r#"// Main message
syntax = "proto3";
message Receipts {
  string company_id = 1;
  string created = 2;
  string key = 3;
  string partition = 4;
  string updated = 5;
}"#
    }
    fn get_with_two_block_comments() -> &'static str {
        r#"/* some
        block
        comment */
syntax = "proto3";
message Receipts {
  string company_id = 1;
  string created = 2;
  string key = 3;
  string partition = 4;
/* some other block comment */
  string updated = 5;
}"#
    }

    #[test]
    fn test_simple_schema_message_resolver() {
        let resolver = MessageResolver::new(get_proto_simple()).unwrap();

        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("nl.openweb.data.Heartbeat")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
    }

    #[test]
    fn test_simple_schema_index_resolver() {
        let resolver = IndexResolver::new(get_proto_simple()).unwrap();

        assert_eq!(
            resolver.find_index("nl.openweb.data.Heartbeat"),
            Some(Arc::new(vec![0]))
        );
        assert_eq!(resolver.find_index("nl.openweb.data.Foo"), None);
    }

    #[test]
    fn test_complex_schema_message_resolver() {
        let resolver = MessageResolver::new(get_proto_complex()).unwrap();

        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from(
                "org.schema_registry_test_app.proto.A"
            )))
        );
        assert_eq!(
            resolver.find_name(&[2, 0]),
            Some(Arc::new(String::from(
                "org.schema_registry_test_app.proto.C.D"
            )))
        );
        assert_eq!(
            resolver.find_name(&[3]),
            Some(Arc::new(String::from(
                "org.schema_registry_test_app.proto.ProtoTest"
            )))
        );
    }

    #[test]
    fn test_schema_with_optional_fields() {
        let resolver = MessageResolver::new(get_including_optional()).unwrap();
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("in.abc.event_entities.EventSource")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
    }

    #[test]
    fn test_complex_schema_index_resolver() {
        let resolver = IndexResolver::new(get_proto_complex()).unwrap();

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

    #[test]
    fn test_schema_with_comment_line() {
        let resolver = MessageResolver::new(get_with_comment_line()).unwrap();
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("Receipts")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
    }

    #[test]
    fn test_schema_with_two_block_comments() {
        let resolver = MessageResolver::new(get_with_two_block_comments()).unwrap();
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("Receipts")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
    }

    #[test]
    fn test_schema_with_brace_in_string_default() {
        // A brace character inside a string literal (e.g. a default/option value) used to be
        // mistaken for message nesting, desyncing the index bookkeeping for anything declared
        // after it.
        let schema = r#"syntax = "proto2"; message Outer { optional string weird = 1 [default = "{"]; } message Inner { optional int32 x = 1; }"#;
        let resolver = MessageResolver::new(schema).unwrap();
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("Outer")))
        );
        assert_eq!(
            resolver.find_name(&[1]),
            Some(Arc::new(String::from("Inner")))
        );
    }

    #[test]
    fn test_schema_with_brace_in_single_quoted_string() {
        let schema = r#"syntax = "proto2"; message Outer { optional string weird = 1 [default = '}']; } message Inner { optional int32 x = 1; }"#;
        let resolver = MessageResolver::new(schema).unwrap();
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("Outer")))
        );
        assert_eq!(
            resolver.find_name(&[1]),
            Some(Arc::new(String::from("Inner")))
        );
    }

    #[test]
    fn test_schema_with_escaped_quote_in_string() {
        // an escaped quote inside the string shouldn't be mistaken for the closing quote, which
        // would otherwise leave the rest of the literal (including its brace) unprotected.
        let schema = r#"syntax = "proto2"; message Outer { optional string weird = 1 [default = "a\"{"]; } message Inner { optional int32 x = 1; }"#;
        let resolver = MessageResolver::new(schema).unwrap();
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("Outer")))
        );
        assert_eq!(
            resolver.find_name(&[1]),
            Some(Arc::new(String::from("Inner")))
        );
    }

    #[test]
    fn test_schema_with_braces_in_comments() {
        // A brace in a comment used to be mistaken for message nesting.
        let schema = "syntax = \"proto3\"; message A { int32 x = 1; } // a {b}\n/* c } */ message B { int32 y = 1; }";
        let resolver = MessageResolver::new(schema).unwrap();
        assert_eq!(resolver.find_name(&[0]), Some(Arc::new(String::from("A"))));
        assert_eq!(resolver.find_name(&[1]), Some(Arc::new(String::from("B"))));
    }

    #[test]
    fn test_map_and_group_messages_are_skipped() {
        // `counts` and `Grp` make protobuf generate the nested `CountsEntry` and `Grp`
        // messages, which confluent doesn't count, since they aren't declared as a `message`.
        let schema = "syntax = \"proto2\"; package p; message A { map<string, int64> counts = 1; optional group Grp = 2 { optional int32 g = 3; } message B { optional int32 x = 1; } }";
        let resolver = MessageResolver::new(schema).unwrap();
        assert_eq!(
            resolver.find_name(&[0, 0]),
            Some(Arc::new(String::from("p.A.B")))
        );
        assert_eq!(resolver.find_name(&[0, 1]), None);
        let resolver = IndexResolver::new(schema).unwrap();
        assert_eq!(resolver.find_index("p.A.B"), Some(Arc::new(vec![0, 0])));
        assert_eq!(resolver.find_index("p.A.CountsEntry"), None);
        assert_eq!(resolver.find_index("p.A.Grp"), None);
    }

    #[test]
    fn test_invalid_schema_gives_error() {
        let err = MessageResolver::new("message A { int32 x = 1; }").unwrap_err();
        assert_eq!(err.error, "Could not parse protobuf schema");
        assert!(!err.retriable);
        assert!(IndexResolver::new("message A {").is_err());
    }

    #[test]
    fn to_index_and_data_empty_bytes_gives_error() {
        // this is what a 5-byte schema-registry-framed message (magic byte + 4-byte id +
        // zero-length payload) turns into once get_bytes_result strips the header: an empty
        // slice. This used to panic on `bytes[0]` instead of returning an error.
        let result = to_index_and_data(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn to_index_and_data_truncated_varint_count_gives_error() {
        // a non-zero first byte signals a varint-encoded index count should follow, but there's
        // nothing after it; 0x80 has its continuation bit set, so a byte must follow. This used
        // to panic via `.unwrap()` on the `read_varint()` result.
        let result = to_index_and_data(&[0x80]);
        assert!(result.is_err());
    }

    #[test]
    fn to_index_and_data_truncated_varint_index_gives_error() {
        // count (zig-zag decoded) says two index values follow, but only one more byte remains.
        let result = to_index_and_data(&[4, 5]);
        assert!(result.is_err());
    }

    #[test]
    fn to_index_and_data_single_index_with_data() {
        let result = to_index_and_data(&[0, 42, 43]);
        assert_eq!(result, Ok((vec![0], vec![42, 43])));
    }
}
