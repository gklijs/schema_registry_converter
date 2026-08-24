//! Contains structs, enums' and functions common to async and blocking implementation of schema
//! registry. So stuff dealing with the responses from schema registry, determining the subject, etc.
use crate::error::SRCError;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Clone)]
pub(crate) enum SrAuthorization {
    None,
    Token(String),
    Basic(String, Option<String>),
}

impl fmt::Debug for SrAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SrAuthorization::None => write!(f, "None"),
            SrAuthorization::Token(_) => write!(f, "Token"),
            SrAuthorization::Basic(_, _) => write!(f, "Basic"),
        }
    }
}

/// By default, the schema registry supports three types. It's possible there will be more in the future
/// or to add your own. Therefore, the other is one of the schema types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
    Other(String),
}

/// The schema registry supports sub schema's they will be stored separately in the schema registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SuppliedReference {
    pub name: String,
    pub subject: String,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
    pub properties: Option<HashMap<String, String>>,
    pub tags: Option<HashMap<String, Vec<String>>>,
}

/// Errors as returned by the schema registry API
#[derive(Clone, Debug, Deserialize)]
pub struct RawError {
    pub error_code: u32,
    pub message: String,
}

impl Display for RawError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "error_code: {}, message: {}",
            self.error_code, self.message
        )
    }
}

/// Schema as it might be provided to create messages, they will be added to the schema registry if
/// not already present
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SuppliedSchema {
    pub name: Option<String>,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
    pub properties: Option<HashMap<String, String>>,
    pub tags: Option<HashMap<String, Vec<String>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RegisteredReference {
    pub name: String,
    pub subject: String,
    pub version: u32,
    pub properties: Option<HashMap<String, String>>,
    pub tags: Option<HashMap<String, Vec<String>>>,
}

/// Schema as retrieved from the schema registry. It's close to the json received and doesn't do
/// type specific transformations.
///
/// `subject` and `version` are populated from the registry response when available. The Confluent
/// Schema Registry includes them in `GET /schemas/ids/{id}` responses; an id can be registered
/// against multiple subjects, so the value here is "the subject the registry returned" rather than
/// an authoritative single answer. Use `GET /schemas/ids/{id}/subjects` if you need the full set.
/// `guid` is the schema's UUID, as returned by Confluent Schema Registry 8.0+ alongside the int
/// `id`. It's `None` against older registries. See [`SchemaIdHeader`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RegisteredSchema {
    pub id: u32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<RegisteredReference>,
    pub properties: Option<HashMap<String, String>>,
    pub tags: Option<HashMap<String, Vec<String>>>,
    pub subject: Option<String>,
    pub version: Option<u32>,
    pub guid: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RawRegisteredSchema {
    pub subject: Option<String>,
    pub version: Option<u32>,
    pub id: Option<u32>,
    pub guid: Option<String>,
    pub schema_type: Option<String>,
    pub references: Option<Vec<RegisteredReference>>,
    pub schema: Option<String>,
    pub metadata: Option<Metadata>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub tags: Option<HashMap<String, Vec<String>>>,
    pub properties: Option<HashMap<String, String>>,
}

/// Intermediate result to just handle the byte transformation. When used in a decoder just the
/// id might me enough because the resolved schema is cashed already.
///
/// Borrows from the payload passed to [`get_bytes_result`] rather than copying it -- decoding is
/// one of the hottest paths in this crate (every message goes through it), and the payload is
/// already available as a borrow for at least the duration of the call in every caller. See
/// https://github.com/gklijs/schema_registry_converter/issues/190.
#[derive(Debug, PartialEq)]
pub enum BytesResult<'a> {
    Null,
    Invalid(&'a [u8]),
    Valid(u32, &'a [u8]),
}

/// Strategy similar to the one in the Java client. By default, schema's needs to be backwards
/// compatible. Historically the only available strategy was the TopicNameStrategy. This meant in
/// practice that a topic could only have one type, or the restriction on backwards compatibility
/// was to be abandoned. Using either of the two other strategies allows multiple types of schema
/// on topic, while still being able to keep the restriction on schema's being backwards
/// compatible.
/// Depending on the strategy, either the topic, whether the value is used as key, the fully
/// qualified name (only for RecordNameStrategy), or the schema needs to be provided.
#[derive(Clone, Debug)]
pub enum SubjectNameStrategy {
    RecordNameStrategy(String),
    TopicNameStrategy(String, bool),
    TopicRecordNameStrategy(String, String),
    RecordNameStrategyWithSchema(SuppliedSchema),
    TopicNameStrategyWithSchema(String, bool, SuppliedSchema),
    TopicRecordNameStrategyWithSchema(String, SuppliedSchema),
}

impl SubjectNameStrategy {
    /// Helper function to get the schema from the strategy.
    pub(crate) fn get_schema(&self) -> Option<&SuppliedSchema> {
        match self {
            SubjectNameStrategy::RecordNameStrategy(_) => None,
            SubjectNameStrategy::TopicNameStrategy(_, _) => None,
            SubjectNameStrategy::TopicRecordNameStrategy(_, _) => None,
            SubjectNameStrategy::RecordNameStrategyWithSchema(s) => Some(s),
            SubjectNameStrategy::TopicNameStrategyWithSchema(_, _, s) => Some(s),
            SubjectNameStrategy::TopicRecordNameStrategyWithSchema(_, s) => Some(s),
        }
    }

    /// Gets the subject part which is also used as key to cache the results. It's constructed so that
    /// it's compatible with the Java client.
    pub fn get_subject(&self) -> Result<String, SRCError> {
        match self {
        SubjectNameStrategy::RecordNameStrategy(rn) => Ok(rn.clone()),
        SubjectNameStrategy::TopicNameStrategy(t, is_key) => {
            if *is_key {
                Ok(format!("{}-key", t))
            } else {
                Ok(format!("{}-value", t))
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategy(t, rn) => Ok(format!("{}-{}", t, rn)),
        SubjectNameStrategy::RecordNameStrategyWithSchema(s) => match &s.name {
            None => Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema",
            )),
            Some(n) => Ok(n.clone()),
        },
        SubjectNameStrategy::TopicNameStrategyWithSchema(t, is_key, _) => {
            if *is_key {
                Ok(format!("{}-key", t))
            } else {
                Ok(format!("{}-value", t))
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategyWithSchema(t, s) => match &s.name {
            None => Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema",
            )),
            Some(n) => Ok(format!("{}-{}", t, n)),
        },
    }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SrCall<'a> {
    GetById(u32),
    GetByGuid(&'a str),
    GetLatest(&'a str),
    GetBySubjectAndVersion(&'a str, u32),
    PostNew(&'a str, &'a str),
    PostForVersion(&'a str, &'a str),
}

pub(crate) fn url_for_call(call: &SrCall<'_>, base_url: &str) -> String {
    match call {
        SrCall::GetById(id) => format!("{}/schemas/ids/{}?deleted=true", base_url, id),
        // `deleted=true` matters here just like it does for GetById: a consumer decoding an old
        // Kafka message whose header carries a guid must still be able to resolve it even if the
        // subject/version it was registered under has since been soft-deleted.
        SrCall::GetByGuid(guid) => format!("{}/schemas/guids/{}?deleted=true", base_url, guid),
        SrCall::GetLatest(subject) => {
            // Use escape sequences instead of slashes in the subject
            format!(
                "{}/subjects/{}/versions/latest",
                base_url,
                subject.replace("/", "%2F")
            )
        }
        SrCall::GetBySubjectAndVersion(subject, version) => {
            // Use escape sequences instead of slashes in the subject
            format!(
                "{}/subjects/{}/versions/{}",
                base_url,
                subject.replace("/", "%2F"),
                version
            )
        }
        SrCall::PostNew(subject, _) => {
            // Use escape sequences instead of slashes in the subject
            format!(
                "{}/subjects/{}/versions",
                base_url,
                subject.replace("/", "%2F")
            )
        }
        SrCall::PostForVersion(subject, _) => {
            // Use escape sequences instead of slashes in the subject
            format!(
                "{}/subjects/{}?deleted=false",
                base_url,
                subject.replace("/", "%2F")
            )
        }
    }
}

/// Creates payload that can be included as a key or value on a kafka record
pub fn get_payload(id: u32, encoded_bytes: Vec<u8>) -> Vec<u8> {
    let mut payload = vec![0u8];
    let mut buf = [0u8; 4];
    BigEndian::write_u32(&mut buf, id);
    payload.extend_from_slice(&buf);
    payload.extend_from_slice(encoded_bytes.as_slice());
    payload
}

/// Just analyses the bytes which are contained in the key or value of an kafka record. When valid
/// it will return the id and the data bytes. The way schema registry messages are encoded is
/// starting with a zero, with the next 4 bytes having the id. The other bytes are the encoded
/// message.
pub fn get_bytes_result(bytes: Option<&[u8]>) -> BytesResult<'_> {
    match bytes {
        None => BytesResult::Null,
        Some(p) if p.len() > 4 && p[0] == 0 => {
            let mut buf = &p[1..5];
            let id = buf.read_u32::<BigEndian>().unwrap();
            BytesResult::Valid(id, &p[5..])
        }
        Some(p) => BytesResult::Invalid(p),
    }
}

/// Returns a descriptive error message for invalid bytes, including a hint about the magic byte
/// when the first byte is not 0x00.
pub fn invalid_bytes_error(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        "Invalid bytes: empty payload, expected confluent schema registry wire format".to_string()
    } else if bytes.len() <= 4 {
        format!(
            "Invalid bytes: payload too short ({} bytes), expected at least 5 bytes for confluent schema registry wire format",
            bytes.len()
        )
    } else {
        format!(
            "Invalid bytes: first byte is {:#04x}, expected magic byte 0x00 for confluent schema registry wire format. The message may not be encoded with the schema registry serializer.",
            bytes[0]
        )
    }
}

/// Header name used for the schema id/guid of a Kafka record's key, matching Confluent's
/// `SchemaId.KEY_SCHEMA_ID_HEADER`. See [`SchemaIdHeader`].
pub const KEY_SCHEMA_ID_HEADER: &str = "__key_schema_id";
/// Header name used for the schema id/guid of a Kafka record's value, matching Confluent's
/// `SchemaId.VALUE_SCHEMA_ID_HEADER`. See [`SchemaIdHeader`].
pub const VALUE_SCHEMA_ID_HEADER: &str = "__value_schema_id";

/// A schema id/guid to attach to a Kafka record as a header, as an alternative to prefixing the
/// payload (see [`get_payload`]). Produced by the `_with_header_id` encode methods, mirroring
/// Confluent's `HeaderSchemaIdSerializer`.
///
/// This crate has no dependency on any particular Kafka client, so this is plain data: attach
/// `value` under `name` using whatever `Headers`/`OwnedHeaders`/etc. type your Kafka client uses.
/// See https://github.com/gklijs/schema_registry_converter/issues/139.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaIdHeader {
    pub name: &'static str,
    pub value: Vec<u8>,
}

/// What a [`SchemaIdHeader`]'s bytes resolved to, produced by [`parse_schema_id_header`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HeaderSchemaId {
    Id(u32),
    Guid(String),
}

/// Builds the bytes for a [`SchemaIdHeader`]: magic byte `0x01` (Confluent Schema Registry 8.0's
/// GUID wire format) followed by the 16 raw bytes of `guid`, followed by `message_indexes`
/// verbatim (used by the protobuf decoders to carry the message index that would otherwise be a
/// payload-prefix; empty for Avro/JSON).
pub(crate) fn build_schema_id_header(
    name: &'static str,
    guid: &str,
    message_indexes: &[u8],
) -> Result<SchemaIdHeader, SRCError> {
    let raw = guid_to_bytes(guid)?;
    let mut value = Vec::with_capacity(1 + raw.len() + message_indexes.len());
    value.push(0x01);
    value.extend_from_slice(&raw);
    value.extend_from_slice(message_indexes);
    Ok(SchemaIdHeader { name, value })
}

/// Builds the [`SchemaIdHeader`] for an `encode_with_header_id` call: picks the
/// `__key_schema_id`/`__value_schema_id` header name based on `is_key`, and errors out if the
/// schema has no `guid` (a registry older than Confluent Schema Registry 8.0, which doesn't
/// return one). `message_indexes` is the protobuf message index that would otherwise be a
/// payload-prefix (empty for Avro/JSON). Shared by every encoder (Avro, JSON, protobuf; blocking
/// and async) so the guid-required error message and header-name selection can't drift between
/// them.
pub(crate) fn schema_id_header_for(
    guid: Option<&str>,
    is_key: bool,
    message_indexes: &[u8],
) -> Result<SchemaIdHeader, SRCError> {
    let guid = guid.ok_or_else(|| {
        SRCError::non_retryable_without_cause(
            "Schema registry response did not include a guid; encoding the schema id in a \
             header requires Confluent Schema Registry 8.0+",
        )
    })?;
    let name = if is_key {
        KEY_SCHEMA_ID_HEADER
    } else {
        VALUE_SCHEMA_ID_HEADER
    };
    build_schema_id_header(name, guid, message_indexes)
}

/// Parses the bytes of a `__key_schema_id`/`__value_schema_id` header, mirroring Confluent's
/// `SchemaId.fromBytes`: magic byte `0x00` followed by a 4-byte id, or magic byte `0x01` followed
/// by a 16-byte guid. Returns the id/guid plus whatever bytes follow it (the protobuf message
/// index, when present; empty otherwise).
pub(crate) fn parse_schema_id_header(bytes: &[u8]) -> Result<(HeaderSchemaId, &[u8]), SRCError> {
    match bytes.first() {
        Some(0x00) if bytes.len() >= 5 => {
            let id = BigEndian::read_u32(&bytes[1..5]);
            Ok((HeaderSchemaId::Id(id), &bytes[5..]))
        }
        Some(0x00) => Err(SRCError::non_retryable_without_cause(&format!(
            "Invalid schema id header: {} bytes, expected at least 5 (magic byte 0x00 + 4-byte id)",
            bytes.len()
        ))),
        Some(0x01) if bytes.len() >= 17 => {
            Ok((HeaderSchemaId::Guid(bytes_to_guid(&bytes[1..17])), &bytes[17..]))
        }
        Some(0x01) => Err(SRCError::non_retryable_without_cause(&format!(
            "Invalid schema id header: {} bytes, expected at least 17 (magic byte 0x01 + 16-byte guid)",
            bytes.len()
        ))),
        Some(b) => Err(SRCError::non_retryable_without_cause(&format!(
            "Invalid schema id header: first byte is {:#04x}, expected magic byte 0x00 (id) or 0x01 (guid)",
            b
        ))),
        None => Err(SRCError::non_retryable_without_cause(
            "Invalid schema id header: empty",
        )),
    }
}

/// Parses a hyphenated or bare-hex UUID string into its 16 raw bytes.
fn guid_to_bytes(guid: &str) -> Result<[u8; 16], SRCError> {
    let hex: String = guid.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(SRCError::non_retryable_without_cause(&format!(
            "Invalid schema guid '{}': expected 32 hex characters (with or without dashes)",
            guid
        )));
    }
    let mut bytes = [0u8; 16];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|e| {
            SRCError::non_retryable_with_cause(e, &format!("Invalid schema guid '{}'", guid))
        })?;
    }
    Ok(bytes)
}

/// Formats 16 raw bytes as a standard hyphenated UUID string.
fn bytes_to_guid(bytes: &[u8]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

#[cfg(test)]
mod test {
    use crate::error::SRCError;
    use crate::schema_registry_common::{
        build_schema_id_header, get_bytes_result, invalid_bytes_error, parse_schema_id_header,
        BytesResult, HeaderSchemaId, RegisteredSchema, SchemaType, SrAuthorization,
        SubjectNameStrategy, SuppliedSchema, KEY_SCHEMA_ID_HEADER,
    };

    #[test]
    fn display_record_name_strategy() {
        let sns = SubjectNameStrategy::RecordNameStrategy(String::from("bla"));
        assert_eq!(
            "RecordNameStrategy(\"bla\")".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_topic_name_strategy() {
        let sns = SubjectNameStrategy::TopicNameStrategy(String::from("bla"), true);
        assert_eq!(
            "TopicNameStrategy(\"bla\", true)".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_topic_record_name_strategy() {
        let sns =
            SubjectNameStrategy::TopicRecordNameStrategy(String::from("bla"), String::from("foo"));
        assert_eq!(
            "TopicRecordNameStrategy(\"bla\", \"foo\")".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_authorization_token() {
        let authorization =
            SrAuthorization::Token(String::from("some token that should not be displayed"));
        assert_eq!("Token", format!("{:?}", authorization))
    }

    #[test]
    fn display_authorization_basic() {
        let authorization = SrAuthorization::Basic(
            String::from("some username that should not be displayed"),
            None,
        );
        assert_eq!("Basic", format!("{:?}", authorization))
    }

    #[test]
    fn display_schema_type_other() {
        let schema_type = SchemaType::Other(String::from("flatbuffers"));
        assert_eq!(r#"Other("flatbuffers")"#, format!("{:?}", schema_type))
    }

    #[test]
    fn registered_schema_get_fields() {
        let registered_schema = RegisteredSchema {
            id: 0,
            schema_type: SchemaType::Avro,
            schema: String::from("some schema"),
            references: vec![],
            properties: None,
            tags: None,
            subject: None,
            version: None,
            guid: None,
        };
        assert_eq!(0, registered_schema.id);
        assert_eq!(SchemaType::Avro, registered_schema.schema_type);
        assert_eq!("some schema", registered_schema.schema);
        assert!(registered_schema.references.is_empty());
        assert!(registered_schema.properties.is_none());
        assert!(registered_schema.tags.is_none());
        assert!(registered_schema.subject.is_none());
        assert!(registered_schema.version.is_none());
        assert!(registered_schema.guid.is_none());
        assert_eq!(
            r#"RegisteredSchema { id: 0, schema_type: Avro, schema: "some schema", references: [], properties: None, tags: None, subject: None, version: None, guid: None }"#,
            format!("{:?}", registered_schema)
        )
    }

    #[test]
    fn display_byte_result_invalid() {
        let byte_result = BytesResult::Invalid(&[0, 0]);
        assert_eq!(r#"Invalid([0, 0])"#, format!("{:?}", byte_result))
    }

    #[test]
    fn display_byte_result_valid() {
        let byte_result = BytesResult::Valid(6, &[101, 33]);
        assert_eq!(r#"Valid(6, [101, 33])"#, format!("{:?}", byte_result))
    }

    #[test]
    fn error_when_name_mandatory() {
        let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
            String::from("someTopic"),
            SuppliedSchema {
                name: None,
                schema_type: SchemaType::Other(String::from("foo")),
                schema: "".to_string(),
                references: vec![],
                properties: None,
                tags: None,
            },
        );

        let result = strategy.get_subject();

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema"
            ))
        );
    }

    #[test]
    fn get_bytes_result_null() {
        let result = get_bytes_result(None);
        assert_eq!(BytesResult::Null, result)
    }

    #[test]
    fn get_bytes_result_valid() {
        let result = get_bytes_result(Some(&[0, 0, 0, 0, 7, 101, 99]));
        assert_eq!(BytesResult::Valid(7, &[101, 99]), result)
    }

    #[test]
    fn get_bytes_result_invalid() {
        let result = get_bytes_result(Some(&[0, 0, 0, 0]));
        assert_eq!(BytesResult::Invalid(&[0, 0, 0, 0]), result)
    }

    #[test]
    fn build_and_parse_schema_id_header_round_trips_guid() {
        let header = build_schema_id_header(
            KEY_SCHEMA_ID_HEADER,
            "cc0e0e0e-53c1-4a1a-8f1a-000000000001",
            &[],
        )
        .unwrap();
        assert_eq!(KEY_SCHEMA_ID_HEADER, header.name);
        assert_eq!(
            &[
                0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01,
            ],
            header.value.as_slice()
        );

        let (schema_id, rest) = parse_schema_id_header(&header.value).unwrap();
        assert_eq!(
            HeaderSchemaId::Guid("cc0e0e0e-53c1-4a1a-8f1a-000000000001".to_string()),
            schema_id
        );
        assert!(rest.is_empty());
    }

    #[test]
    fn build_schema_id_header_keeps_trailing_message_indexes() {
        let header = build_schema_id_header(
            KEY_SCHEMA_ID_HEADER,
            "00000000-0000-0000-0000-000000000000",
            &[2, 4, 0],
        )
        .unwrap();
        let (_, rest) = parse_schema_id_header(&header.value).unwrap();
        assert_eq!(&[2, 4, 0], rest);
    }

    #[test]
    fn build_schema_id_header_invalid_guid() {
        let err = build_schema_id_header(KEY_SCHEMA_ID_HEADER, "not-a-guid", &[]).unwrap_err();
        assert!(err.error.starts_with("Invalid schema guid 'not-a-guid'"));
    }

    #[test]
    fn parse_schema_id_header_id_variant() {
        let (schema_id, rest) = parse_schema_id_header(&[0x00, 0, 0, 0, 7]).unwrap();
        assert_eq!(HeaderSchemaId::Id(7), schema_id);
        assert!(rest.is_empty());
    }

    #[test]
    fn parse_schema_id_header_unknown_magic_byte() {
        let err = parse_schema_id_header(&[0x02, 0, 0, 0, 0]).unwrap_err();
        assert!(err
            .error
            .starts_with("Invalid schema id header: first byte is 0x02"));
    }

    #[test]
    fn parse_schema_id_header_empty() {
        let err = parse_schema_id_header(&[]).unwrap_err();
        assert_eq!("Invalid schema id header: empty", err.error);
    }

    #[test]
    fn parse_schema_id_header_truncated_id() {
        // A correctly-prefixed but truncated header must be reported as truncated, not
        // misattributed to a wrong magic byte (the byte at index 0 *is* the valid 0x00 magic
        // byte -- there just aren't enough bytes after it).
        let err = parse_schema_id_header(&[0x00, 0, 0]).unwrap_err();
        assert_eq!(
            "Invalid schema id header: 3 bytes, expected at least 5 (magic byte 0x00 + 4-byte id)",
            err.error
        );
    }

    #[test]
    fn parse_schema_id_header_truncated_guid() {
        let err = parse_schema_id_header(&[0x01, 0, 0]).unwrap_err();
        assert_eq!(
            "Invalid schema id header: 3 bytes, expected at least 17 (magic byte 0x01 + 16-byte guid)",
            err.error
        );
    }

    #[test]
    fn invalid_bytes_error_empty() {
        let result = invalid_bytes_error(&[]);
        assert_eq!(
            "Invalid bytes: empty payload, expected confluent schema registry wire format",
            result
        )
    }

    #[test]
    fn invalid_bytes_error_too_short() {
        let result = invalid_bytes_error(&[0, 0, 0, 0]);
        assert_eq!(
            "Invalid bytes: payload too short (4 bytes), expected at least 5 bytes for confluent schema registry wire format",
            result
        )
    }

    #[test]
    fn invalid_bytes_error_wrong_magic_byte() {
        let result = invalid_bytes_error(&[1, 0, 0, 0, 1, 6]);
        assert_eq!(
            "Invalid bytes: first byte is 0x01, expected magic byte 0x00 for confluent schema registry wire format. The message may not be encoded with the schema registry serializer.",
            result
        )
    }
}
