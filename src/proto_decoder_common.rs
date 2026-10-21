use std::collections::HashMap;

use prost_reflect::{DynamicMessage, FileDescriptor, MessageDescriptor};
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

use crate::error::SRCError;
use crate::proto_common_types::CommonTypesResolver;

/// The file name the root schema is compiled under. The schema registry only names the schemas
/// a schema references (by the path they're imported with), not the root schema itself, so it
/// gets a name no import is going to use.
const ROOT_FILE_NAME: &str = "__schema_registry_converter_root__.proto";

/// The (transitively) referenced schemas of a root schema, keyed by the name they're imported by.
pub(crate) type ReferencedSchemas = HashMap<String, String>;

struct ReferencedSchemasResolver(ReferencedSchemas);

impl FileResolver for ReferencedSchemasResolver {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.0.get(name) {
            Some(schema) => File::from_source(name, schema),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

/// Compiles the root schema together with everything it imports. Imports are resolved against
/// the referenced schemas from the registry first, then the well-known `google/protobuf/*`
/// types protox ships with, and lastly the `google/type/*` common types.
pub(crate) fn to_file_descriptor(
    root_schema: &str,
    mut referenced_schemas: ReferencedSchemas,
) -> Result<FileDescriptor, SRCError> {
    referenced_schemas.insert(ROOT_FILE_NAME.to_owned(), root_schema.to_owned());
    let mut resolver = ChainFileResolver::new();
    resolver.add(ReferencedSchemasResolver(referenced_schemas));
    resolver.add(GoogleFileResolver::new());
    resolver.add(CommonTypesResolver);
    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler.include_imports(true);
    compiler
        .open_file(ROOT_FILE_NAME)
        .map_err(|e| SRCError::non_retryable_with_cause(e, "Error creating proto context"))?;
    compiler
        .descriptor_pool()
        .get_file_by_name(ROOT_FILE_NAME)
        .ok_or_else(|| {
            SRCError::non_retryable_without_cause("Compiled proto context misses the root schema")
        })
}

/// Finds the message the confluent message index points at. Each index is the position among
/// the messages declared in the schema text at that level, so the messages protobuf generates
/// for `map` fields and `group` fields don't count, as they aren't declared as a `message`.
/// The index comes from the payload or a header a producer controls, so one that doesn't match
/// the schema is an error rather than a panic.
pub(crate) fn find_message(
    file: &FileDescriptor,
    index: &[i32],
) -> Result<MessageDescriptor, SRCError> {
    let not_found = || {
        SRCError::non_retryable_without_cause(&format!(
            "could not find a message with index {:?} in the resolved protobuf schema",
            index
        ))
    };
    let (first, rest) = index.split_first().ok_or_else(not_found)?;
    let mut message = nth_declared(
        file.messages(),
        |m| {
            file.extensions()
                .any(|e| e.is_group() && e.kind().as_message() == Some(m))
        },
        *first,
    )
    .ok_or_else(not_found)?;
    for i in rest {
        let parent = message;
        message = nth_declared(
            parent.child_messages(),
            |m| {
                parent
                    .fields()
                    .any(|f| f.is_group() && f.kind().as_message() == Some(m))
                    || parent
                        .child_extensions()
                        .any(|e| e.is_group() && e.kind().as_message() == Some(m))
            },
            *i,
        )
        .ok_or_else(not_found)?;
    }
    Ok(message)
}

fn nth_declared(
    messages: impl Iterator<Item = MessageDescriptor>,
    is_group_type: impl Fn(&MessageDescriptor) -> bool,
    index: i32,
) -> Option<MessageDescriptor> {
    let index = usize::try_from(index).ok()?;
    messages
        .filter(|m| !m.is_map_entry() && !is_group_type(m))
        .nth(index)
}

/// Decodes the data bytes, the payload without the id and message index, as the given message.
pub(crate) fn decode_message(
    descriptor: MessageDescriptor,
    data: &[u8],
) -> Result<DynamicMessage, SRCError> {
    DynamicMessage::decode(descriptor.clone(), data).map_err(|e| {
        SRCError::non_retryable_with_cause(
            e,
            &format!(
                "could not decode bytes as protobuf message {}",
                descriptor.full_name()
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::proto_decoder_common::{
        decode_message, find_message, to_file_descriptor, ReferencedSchemas,
    };
    use prost_reflect::{MapKey, Value};

    fn compile(schema: &str) -> prost_reflect::FileDescriptor {
        to_file_descriptor(schema, ReferencedSchemas::new()).unwrap()
    }

    #[test]
    fn test_find_nested_message() {
        let file = compile(
            "syntax = \"proto3\"; package a.b; message A { int32 x = 1; } message B { message C { int32 y = 1; } message D { message E { int32 z = 1; } } }",
        );
        assert_eq!(find_message(&file, &[0]).unwrap().full_name(), "a.b.A");
        assert_eq!(find_message(&file, &[1, 0]).unwrap().full_name(), "a.b.B.C");
        assert_eq!(
            find_message(&file, &[1, 1, 0]).unwrap().full_name(),
            "a.b.B.D.E"
        );
    }

    #[test]
    fn test_find_message_skips_map_entries_and_groups() {
        // `counts` and `Grp` make protobuf add the nested `CountsEntry` and `Grp` messages,
        // which a confluent message index skips, since they aren't declared as a `message`.
        let file = compile(
            "syntax = \"proto2\"; message A { map<string, int64> counts = 1; optional group Grp = 2 { optional int32 g = 3; } message B { optional int32 x = 1; } }",
        );
        assert_eq!(find_message(&file, &[0, 0]).unwrap().full_name(), "A.B");
        assert!(find_message(&file, &[0, 1]).is_err());
    }

    #[test]
    fn test_find_message_with_invalid_index_gives_error() {
        let file = compile("syntax = \"proto3\"; message A { int32 x = 1; }");
        let err = find_message(&file, &[3]).unwrap_err();
        assert_eq!(
            err.error,
            "could not find a message with index [3] in the resolved protobuf schema"
        );
        assert!(find_message(&file, &[-1]).is_err());
        assert!(find_message(&file, &[]).is_err());
    }

    #[test]
    fn test_decode_map_field() {
        let file = compile("syntax = \"proto3\"; message A { map<string, int64> counts = 1; }");
        let descriptor = find_message(&file, &[0]).unwrap();
        // counts = {"a": 3}: field 1, length delimited entry with key (1) "a" and value (2) 3
        let data = [0x0a, 0x05, 0x0a, 0x01, b'a', 0x10, 0x03];
        let message = decode_message(descriptor, &data).unwrap();
        let counts = message.get_field_by_name("counts").unwrap();
        assert_eq!(
            counts
                .as_map()
                .unwrap()
                .get(&MapKey::String(String::from("a"))),
            Some(&Value::I64(3))
        );
    }

    #[test]
    fn test_decode_invalid_bytes_gives_error() {
        let file = compile("syntax = \"proto3\"; package a; message A { int32 x = 1; }");
        let descriptor = find_message(&file, &[0]).unwrap();
        let err = decode_message(descriptor, &[0xff, 0xff]).unwrap_err();
        assert_eq!(err.error, "could not decode bytes as protobuf message a.A");
        assert!(!err.retriable);
    }

    #[test]
    fn test_references_and_common_types_are_resolved() {
        let mut references = ReferencedSchemas::new();
        references.insert(
            String::from("a/b/c/metadata.proto"),
            String::from("syntax = \"proto3\"; package a.b.c; import \"google/protobuf/timestamp.proto\"; message Metadata { .google.protobuf.Timestamp at = 1; }"),
        );
        let file = to_file_descriptor(
            "syntax = \"proto3\"; package a.b.c.d; import \"a/b/c/metadata.proto\"; import \"google/type/money.proto\"; message Top { .a.b.c.Metadata metadata = 1; google.type.Money price = 2; }",
            references,
        )
        .unwrap();
        assert_eq!(
            find_message(&file, &[0]).unwrap().full_name(),
            "a.b.c.d.Top"
        );
    }

    #[test]
    fn test_missing_reference_gives_error() {
        let err = to_file_descriptor(
            "syntax = \"proto3\"; import \"missing.proto\"; message A { int32 x = 1; }",
            ReferencedSchemas::new(),
        )
        .unwrap_err();
        assert_eq!(err.error, "Error creating proto context");
        assert!(!err.retriable);
    }
}
