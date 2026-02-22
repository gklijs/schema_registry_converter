use std::io::BufReader;
use std::sync::Arc;

use crate::error::SRCError;
use dashmap::DashMap;
use integer_encoding::VarIntReader;
use logos::Logos;

#[derive(Debug, Clone)]
pub struct MessageResolver {
    pub map: DashMap<Vec<i32>, Arc<String>>,
    pub imports: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct IndexResolver {
    map: DashMap<String, Arc<Vec<i32>>>,
}

impl MessageResolver {
    pub fn new(s: &str) -> MessageResolver {
        let helper = ResolverHelper::new(s);
        let map = DashMap::new();
        for i in &helper.indexes {
            map.insert(i.clone(), Arc::new(find_name(i, &helper)));
        }
        MessageResolver {
            map,
            imports: helper.imports,
        }
    }

    pub fn find_name(&self, index: &[i32]) -> Option<Arc<String>> {
        self.map.get(index).map(|e| e.value().clone())
    }
    pub fn imports(&self) -> &Vec<String> {
        &self.imports
    }
}

impl IndexResolver {
    pub fn new(s: &str) -> IndexResolver {
        let helper = ResolverHelper::new(s);
        let map = DashMap::new();
        for i in &helper.indexes {
            map.insert(find_name(i, &helper), Arc::new(i.clone()));
        }
        IndexResolver { map }
    }

    pub fn find_index(&self, name: &str) -> Option<Arc<Vec<i32>>> {
        self.map.get(name).map(|e| e.value().clone())
    }

    pub fn is_single_message(&self) -> bool {
        self.map.len() == 1
    }
}

pub struct ResolverHelper {
    package: Option<String>,
    indexes: Vec<Vec<i32>>,
    names: Vec<String>,
    imports: Vec<String>,
}

#[derive(Logos, Debug, PartialEq)]
enum Token {
    #[regex(r"/\*[^/]+\*/", logos::skip, priority = 20)]
    BlockComment,

    #[regex(r"//[\w\s]+\n", logos::skip, priority = 20)]
    LineComment,

    #[regex(r"package\s+[a-zA-z0-9\\.\\_]+;", priority = 10)]
    Package,

    #[regex(r"message\s+[a-zA-z0-9\\_]+", priority = 10)]
    Message,

    #[regex(r#"import\s+"[a-zA-z0-9\\.\\_/]+";"#, priority = 10)]
    Import,

    #[token("{", priority = 10)]
    Open,

    #[token("}", priority = 10)]
    Close,

    #[regex(r"\S", logos::skip, priority = 1)]
    #[regex(r"[\s]+", logos::skip, priority = 1)]
    Ignorable,
}

impl ResolverHelper {
    pub fn new(s: &str) -> ResolverHelper {
        let mut index: Vec<i32> = vec![0];
        let mut package: Option<String> = None;
        let mut indexes: Vec<Vec<i32>> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut imports: Vec<String> = Vec::new();

        let mut lex = Token::lexer(s);
        let mut next: Option<Result<Token, _>> = lex.next();

        while let Some(token) = next {
            match token {
                Ok(Token::Package) => {
                    let slice = lex.slice();
                    package = Some(String::from(slice[8..slice.len() - 1].trim()));
                }
                Ok(Token::Message) => {
                    let slice = lex.slice();
                    let message = String::from(slice[8..slice.len()].trim());
                    for i in &indexes {
                        if same_vec(i, &index) {
                            *index.last_mut().unwrap() += 1;
                        }
                    }
                    indexes.push(index.clone());
                    names.push(message);
                }
                Ok(Token::Import) => {
                    let slice = lex.slice();
                    let import = String::from(slice[8..slice.len() - 2].trim());
                    imports.push(import);
                }
                Ok(Token::Open) => {
                    index.push(0);
                }
                Ok(Token::Close) => {
                    index.pop();
                }
                Err(_)
                | Ok(Token::Ignorable)
                | Ok(Token::BlockComment)
                | Ok(Token::LineComment) => (),
            };
            next = lex.next()
        }

        ResolverHelper {
            package,
            indexes,
            names,
            imports,
        }
    }
}

fn find_part<'a>(index: &'a [i32], helper: &'a ResolverHelper) -> &'a str {
    for i in 0..helper.indexes.len() {
        if same_vec(index, &helper.indexes[i]) {
            return &helper.names[i];
        }
    }
    unreachable!()
}

fn find_name(index: &[i32], helper: &ResolverHelper) -> String {
    let mut result = match &helper.package {
        None => String::new(),
        Some(v) => String::from(v),
    };
    for i in 1..index.len() + 1 {
        let part = find_part(&index[..i], helper);
        if !result.is_empty() {
            result.push('.')
        }
        result.push_str(part)
    }
    result
}

fn same_vec(first: &[i32], second: &[i32]) -> bool {
    if first.len() != second.len() {
        return false;
    };
    for i in 0..first.len() {
        if first[i] != second[i] {
            return false;
        }
    }
    true
}

pub fn to_index_and_data(bytes: &[u8]) -> (Vec<i32>, Vec<u8>) {
    if bytes[0] == 0 {
        (vec![0], bytes[1..].to_vec())
    } else {
        let mut reader = BufReader::with_capacity(bytes.len(), bytes);
        let count: i32 = reader.read_varint().unwrap();
        let mut index = Vec::new();
        for _ in 0..count {
            index.push(reader.read_varint().unwrap())
        }
        (index, reader.buffer().to_vec())
    }
}

pub fn resolve_name(resolver: &MessageResolver, index: &[i32]) -> Result<Arc<String>, SRCError> {
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
    use crate::proto_resolver::{IndexResolver, MessageResolver};
    use std::sync::Arc;

    fn get_proto_simple() -> &'static str {
        r#"syntax = "proto3";package nl.openweb.data; message Heartbeat{uint64 beat = 1;}"#
    }

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
        let resolver = MessageResolver::new(get_proto_simple());

        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("nl.openweb.data.Heartbeat")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
        assert_eq!(resolver.imports.len(), 0)
    }

    #[test]
    fn test_simple_schema_index_resolver() {
        let resolver = IndexResolver::new(get_proto_simple());

        assert_eq!(
            resolver.find_index("nl.openweb.data.Heartbeat"),
            Some(Arc::new(vec![0]))
        );
        assert_eq!(resolver.find_index("nl.openweb.data.Foo"), None);
    }

    #[test]
    fn test_complex_schema_message_resolver() {
        let resolver = MessageResolver::new(get_proto_complex());

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
        assert_eq!(resolver.imports.len(), 1);
        assert_eq!(resolver.imports[0], String::from("result.proto"))
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
    fn test_schema_with_optional_fields() {
        let resolver = MessageResolver::new(get_including_optional());
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("in.abc.event_entities.EventSource")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
        assert_eq!(resolver.imports.len(), 0)
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

    #[test]
    fn test_schema_with_comment_line() {
        let resolver = MessageResolver::new(get_with_comment_line());
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("Receipts")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
        assert_eq!(resolver.imports.len(), 0)
    }

    #[test]
    fn test_schema_with_two_block_comments() {
        let resolver = MessageResolver::new(get_with_two_block_comments());
        assert_eq!(
            resolver.find_name(&[0]),
            Some(Arc::new(String::from("Receipts")))
        );
        assert_eq!(resolver.find_name(&[1]), None);
        assert_eq!(resolver.imports.len(), 0)
    }
}
