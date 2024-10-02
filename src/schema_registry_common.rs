//! Contains structs, enums' and functions common to async and blocking implementation of schema
//! registry. So stuff dealing with the responses from schema registry, determining the subject, etc.
use core::fmt;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::error::SRCError;

#[derive(Clone)]
pub(crate) enum SrAuthorization {
    None,
    Token(String),
    Basic(String, Option<String>),
}

impl fmt::Debug for SrAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SrAuthorization::None => write!(f, "None"),
            SrAuthorization::Token(_) => write!(f, "Token"),
            SrAuthorization::Basic(_, _) => write!(f, "Basic"),
        }
    }
}

/// By default the schema registry supports three types. It's possible there will be more in the future
/// or to add your own. Therefore the other is one of the schema types.
#[derive(Clone, Debug, PartialEq)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
    Other(String),
}

/// The schema registry supports sub schema's they will be stored separately in the schema registry
#[derive(Clone, Debug)]
pub struct SuppliedReference {
    pub name: String,
    pub subject: String,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
}

/// Schema as it might be provided to create messages, they will be added to th schema registry if
/// not already present
#[derive(Clone, Debug)]
pub struct SuppliedSchema {
    pub name: Option<String>,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RegisteredReference {
    pub name: String,
    pub subject: String,
    pub version: u32,
}

/// Schema as retrieved from the schema registry. It's close to the json received and doesn't do
/// type specific transformations.
#[derive(Clone, Debug)]
pub struct RegisteredSchema {
    pub id: u32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<RegisteredReference>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawRegisteredSchema {
    pub subject: Option<String>,
    pub version: Option<u32>,
    pub id: Option<u32>,
    pub schema_type: Option<String>,
    pub references: Option<Vec<RegisteredReference>>,
    pub schema: Option<String>,
}

/// Intermediate result to just handle the byte transformation. When used in a decoder just the
/// id might me enough because the resolved schema is cashed already.
#[derive(Debug, PartialEq)]
pub enum BytesResult {
    Null,
    Invalid(Vec<u8>),
    Valid(u32, Vec<u8>),
}

/// Strategy similar to the one in the Java client. By default schema's needs to be backwards
/// compatible. Historically the only available strategy was the TopicNameStrategy. This meant in
/// practice that a topic could only have one type, or the restriction on backwards compatibility
/// was to be abandoned. Using either of the two other strategies allows multiple types of schema
/// on on topic, while still being able to keep the restriction on schema's being backwards
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
    GetLatest(&'a str),
    GetBySubjectAndVersion(&'a str, u32),
    PostNew(&'a str, &'a str),
    PostForVersion(&'a str, &'a str),
}

pub(crate) fn url_for_call(call: &SrCall, base_url: &str) -> String {
    match call {
        SrCall::GetById(id) => format!("{}/schemas/ids/{}?deleted=true", base_url, id),
        SrCall::GetLatest(subject) => {
            // Use escape sequences instead of slashes in the subject
            format!("{}/subjects/{}/versions/latest", base_url, subject.replace("/", "%2F"))
        }
        SrCall::GetBySubjectAndVersion(subject, version) => {
            // Use escape sequences instead of slashes in the subject
            format!("{}/subjects/{}/versions/{}", base_url, subject.replace("/", "%2F"), version)
        }
        SrCall::PostNew(subject, _) => {
            // Use escape sequences instead of slashes in the subject
            format!("{}/subjects/{}/versions", base_url, subject.replace("/", "%2F"))
        }
        SrCall::PostForVersion(subject, _) => {
            // Use escape sequences instead of slashes in the subject
            format!("{}/subjects/{}?deleted=false", base_url, subject.replace("/", "%2F"))
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
pub fn get_bytes_result(bytes: Option<&[u8]>) -> BytesResult {
    match bytes {
        None => BytesResult::Null,
        Some(p) if p.len() > 4 && p[0] == 0 => {
            let mut buf = &p[1..5];
            let id = buf.read_u32::<BigEndian>().unwrap();
            BytesResult::Valid(id, p[5..].to_owned())
        }
        Some(p) => BytesResult::Invalid(p[..].to_owned()),
    }
}

#[cfg(test)]
mod test {
    use crate::error::SRCError;
    use crate::schema_registry_common::{
        get_bytes_result, BytesResult, RegisteredSchema, SchemaType, SrAuthorization,
        SubjectNameStrategy, SuppliedSchema,
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
        };
        assert_eq!(0, registered_schema.id);
        assert_eq!(SchemaType::Avro, registered_schema.schema_type);
        assert_eq!("some schema", registered_schema.schema);
        assert!(registered_schema.references.is_empty());
        assert_eq!(
            r#"RegisteredSchema { id: 0, schema_type: Avro, schema: "some schema", references: [] }"#,
            format!("{:?}", registered_schema)
        )
    }

    #[test]
    fn display_byte_result_invalid() {
        let byte_result = BytesResult::Invalid(vec![0, 0]);
        assert_eq!(r#"Invalid([0, 0])"#, format!("{:?}", byte_result))
    }

    #[test]
    fn display_byte_result_valid() {
        let byte_result = BytesResult::Valid(6, vec![101, 33]);
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
        assert_eq!(BytesResult::Valid(7, vec![101, 99]), result)
    }

    #[test]
    fn get_bytes_result_invalid() {
        let result = get_bytes_result(Some(&[0, 0, 0, 0]));
        assert_eq!(BytesResult::Invalid(vec![0, 0, 0, 0]), result)
    }
}
