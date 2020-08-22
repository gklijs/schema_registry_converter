use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::io::BufReader;

use crate::error::SRCError;
use integer_encoding::VarIntReader;
use logos::Logos;

#[derive(Debug, Clone)]
pub(crate) struct MessageResolver {
    map: HashMap<Vec<i32>, String, RandomState>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexResolver {
    map: HashMap<String, Vec<i32>, RandomState>,
}

impl MessageResolver {
    pub(crate) fn new(s: &str) -> MessageResolver {
        let helper = ResolverHelper::new(s);
        let mut map = HashMap::new();
        for i in &helper.indexes {
            map.insert(i.clone(), find_name(&*i, &helper));
        }
        MessageResolver { map }
    }

    pub(crate) fn find_name(&self, index: &[i32]) -> Option<&String> {
        self.map.get(index)
    }
}

impl IndexResolver {
    pub(crate) fn new(s: &str) -> IndexResolver {
        let helper = ResolverHelper::new(s);
        let mut map = HashMap::new();
        for i in &helper.indexes {
            map.insert(find_name(&*i, &helper), i.clone());
        }
        IndexResolver { map }
    }

    pub(crate) fn find_index(&self, name: &str) -> Option<&Vec<i32>> {
        self.map.get(name)
    }
}

struct ResolverHelper {
    package: Option<String>,
    indexes: Vec<Vec<i32>>,
    names: Vec<String>,
}

#[derive(Logos, Debug, PartialEq)]
enum Token {
    #[regex(r"package\s+[a-zA-z0-9\\.\\_]+;")]
    Package,

    #[regex(r"message\s+[a-zA-z0-9\\_]+")]
    Message,

    #[token("{")]
    Open,

    #[token("}")]
    Close,

    #[regex(r"\S")]
    Ignorable,

    #[error]
    #[regex(r"[\s]+", logos::skip)]
    Error,
}

impl ResolverHelper {
    fn new(s: &str) -> ResolverHelper {
        let mut index: Vec<i32> = vec![0];
        let mut package: Option<String> = None;
        let mut indexes: Vec<Vec<i32>> = Vec::new();
        let mut names: Vec<String> = Vec::new();

        let mut lex = Token::lexer(s);
        let mut next: Option<Token> = lex.next();

        while next != None {
            match next.unwrap() {
                Token::Package => {
                    let slice = lex.slice();
                    package = Some(String::from(slice[8..slice.len() - 1].trim()));
                }
                Token::Message => {
                    let slice = lex.slice();
                    let message = String::from(slice[8..slice.len()].trim());
                    for i in &indexes {
                        if same_vec(i, &*index) {
                            *index.last_mut().unwrap() += 1;
                        }
                    }
                    indexes.push(index.clone());
                    names.push(message);
                }
                Token::Open => {
                    index.push(0);
                }
                Token::Close => {
                    index.pop();
                }
                _ => (),
            };
            next = lex.next()
        }

        ResolverHelper {
            package,
            indexes,
            names,
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

pub(crate) fn to_index_and_data(bytes: &[u8]) -> (Vec<i32>, Vec<u8>) {
    if bytes[0] == 0 {
        (vec![0], bytes[1..].to_vec())
    } else {
        let mut reader = BufReader::new(bytes);
        let count: i32 = reader.read_varint().unwrap();
        let mut index = Vec::new();
        for _ in 0..count {
            index.push(reader.read_varint().unwrap())
        }
        (index, reader.buffer().to_vec())
    }
}

pub(crate) fn resolve_name<'a>(
    resolver: &'a MessageResolver,
    index: &[i32],
) -> Result<&'a String, SRCError> {
    match resolver.find_name(&index) {
        Some(n) => Ok(n),
        None => Err(SRCError::non_retryable_without_cause(&*format!(
            "Could not retrieve name for index: {:?}",
            index
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::proto_resolver::{IndexResolver, MessageResolver};

    fn get_proto_simple() -> &'static str {
        r#"syntax = "proto3";package nl.openweb.data; message Heartbeat{uint64 beat = 1;}"#
    }

    fn get_proto_complex() -> &'static str {
        r#"syntax = "proto3"; import "result.proto"; message A {bytes id = 1;} message B {bytes id = 1;} message C {bytes id = 1; D d = 2; message D {int64 counter = 1;}} package org.schema_registry_test_app.proto; message ProtoTest {bytes id = 1; enum Language {Java = 0;Rust = 1;} Language by = 2;int64 counter = 3;string input = 4;repeated A results = 5;}"#
    }

    #[test]
    fn test_simple_schema_message_resolver() {
        let resolver = MessageResolver::new(get_proto_simple());

        assert_eq!(
            resolver.find_name(&[0]),
            Some(&String::from("nl.openweb.data.Heartbeat"))
        );
        assert_eq!(resolver.find_name(&[1]), None)
    }

    #[test]
    fn test_simple_schema_index_resolver() {
        let resolver = IndexResolver::new(get_proto_simple());

        assert_eq!(
            resolver.find_index("nl.openweb.data.Heartbeat"),
            Some(&vec![0])
        );
        assert_eq!(resolver.find_index("nl.openweb.data.Foo"), None)
    }

    #[test]
    fn test_complex_schema_message_resolver() {
        let resolver = MessageResolver::new(get_proto_complex());

        assert_eq!(
            resolver.find_name(&[0]),
            Some(&String::from("org.schema_registry_test_app.proto.A"))
        );
        assert_eq!(
            resolver.find_name(&[2, 0]),
            Some(&String::from("org.schema_registry_test_app.proto.C.D"))
        );
        assert_eq!(
            resolver.find_name(&[3]),
            Some(&String::from(
                "org.schema_registry_test_app.proto.ProtoTest"
            ))
        );
    }

    #[test]
    fn test_complex_schema_index_resolver() {
        let resolver = IndexResolver::new(get_proto_complex());

        assert_eq!(
            resolver.find_index("org.schema_registry_test_app.proto.A"),
            Some(&vec![0])
        );
        assert_eq!(
            resolver.find_index("org.schema_registry_test_app.proto.C.D"),
            Some(&vec![2, 0])
        );
        assert_eq!(
            resolver.find_index("org.schema_registry_test_app.proto.ProtoTest"),
            Some(&vec![3])
        );
    }
}
