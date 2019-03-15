//! This module contains the code specific for the schema registry.

extern crate avro_rs;
extern crate curl;
extern crate serde_json;

use self::avro_rs::Schema;
use self::curl::easy::{Easy2, Handler, List, WriteError};
use self::serde_json::{Value as JsonValue, Map as JsonMap};
use core::fmt;
use std::error::Error;
use std::ops::Deref;
use std::str;

/// Because we need both the resulting schema, as have a way of posting the schema as json, we use
/// this struct so we keep them both together.
#[derive(Clone, Debug)]
pub struct SuppliedSchema {
    raw: String,
    parsed: Schema,
}

/// Will panic when the input is invalid.
impl SuppliedSchema {
    /// Creates a new Supplied Schema
    pub fn new(raw: String) -> SuppliedSchema {
        let parsed = match Schema::parse_str(raw.as_str()) {
            Ok(v) => v,
            Err(e) => panic!(
                "Supplied raw value {} cant be turned into a Schema, error: {}",
                raw, e
            ),
        };
        SuppliedSchema { raw, parsed }
    }
}

/// Strategy similar to the one in the Java client. By default schema's needs to be backwards
/// compatible. Historically the only available strategy was the TopicNameStrategy. This meant in
/// practice that a topic could only have one type, or the restriction on backwards compatibility
/// was to be abandoned. Using either of the two other strategies allows multiple types of schema
/// on on topic, while still being able to keep the restriction on schema's being backwards
/// compatible.
///
/// ```
/// # extern crate mockito;
/// # extern crate schema_registry_converter;
/// # extern crate avro_rs;
/// # use mockito::{mock, SERVER_ADDRESS};
/// # use schema_registry_converter::Encoder;
/// # use schema_registry_converter::schema_registry::{SRCError, SubjectNameStrategy, SuppliedSchema};
/// # use avro_rs::types::Value;
///
/// # let _n = mock("POST", "/subjects/hb-nl.openweb.data.Heartbeat/versions")
/// #    .with_status(200)
/// #    .with_header("content-type", "application/vnd.schemaregistry.v1+json")
/// #    .with_body(r#"{"id":23}"#)
/// #    .create();
///
/// let mut encoder = Encoder::new(SERVER_ADDRESS);
///
/// let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#);
/// let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema("hb".into(), heartbeat_schema);
/// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
/// assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 23, 6]))
/// ```
#[derive(Clone, Debug)]
pub enum SubjectNameStrategy {
    RecordNameStrategy(String),
    TopicNameStrategy(String, bool),
    TopicRecordNameStrategy(String, String),
    RecordNameStrategyWithSchema(SuppliedSchema),
    TopicNameStrategyWithSchema(String, bool, SuppliedSchema),
    TopicRecordNameStrategyWithSchema(String, SuppliedSchema),
}

/// Gets a schema by an id. This is used to get the correct schema te deserialize bytes, when the
/// id is encoded in the bytes.
pub fn get_schema_by_id(id: u32, schema_registry_url: &str) -> Result<Schema, SRCError> {
    let url = schema_registry_url.to_owned() + "/schemas/ids/" + &id.to_string();
    schema_from_url(&url, Option::from(id)).and_then(|t| Ok(t.0))
}

/// Gets the schema and the id by supplying a SubjectNameStrategy. This is used to correctly
/// transform a vector to bytes.
pub fn get_schema_by_subject(
    schema_registry_url: &str,
    subject_name_strategy: &SubjectNameStrategy,
) -> Result<(Schema, u32), SRCError> {
    let schema = get_schema(subject_name_strategy);
    match schema {
        None => {
            let url = format!(
                "{}/subjects/{}/versions/latest",
                schema_registry_url,
                get_subject(subject_name_strategy)
            );
            schema_from_url(&url, None)
        }
        Some(v) => {
            let url = format!(
                "{}/subjects/{}/versions",
                schema_registry_url,
                get_subject(subject_name_strategy)
            );
            post_schema(&url, v)
        }
    }
}

/// Helper function to get the schema from the strategy.
fn get_schema(subject_name_strategy: &SubjectNameStrategy) -> Option<SuppliedSchema> {
    match subject_name_strategy {
        SubjectNameStrategy::RecordNameStrategy(_) => None,
        SubjectNameStrategy::TopicNameStrategy(_, _) => None,
        SubjectNameStrategy::TopicRecordNameStrategy(_, _) => None,
        SubjectNameStrategy::RecordNameStrategyWithSchema(s) => Some(s.clone()),
        SubjectNameStrategy::TopicNameStrategyWithSchema(_, _, s) => Some(s.clone()),
        SubjectNameStrategy::TopicRecordNameStrategyWithSchema(_, s) => Some(s.clone()),
    }
}

/// Gets the subject part which is also used as key to cache the results. It's constructed so that
/// it's compatible with the Java client.
pub fn get_subject(subject_name_strategy: &SubjectNameStrategy) -> String {
    match subject_name_strategy {
        SubjectNameStrategy::RecordNameStrategy(rn) => rn.clone(),
        SubjectNameStrategy::TopicNameStrategy(t, is_key) => {
            if *is_key {
                format!("{}-key", t)
            } else {
                format!("{}-value", t)
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategy(t, rn) => format!("{}-{}", t, rn),
        SubjectNameStrategy::RecordNameStrategyWithSchema(s) => match s.parsed {
            Schema::Record { ref name, .. } => name.fullname(None),
            _ => panic!("Supplied schema is not a record"),
        },
        SubjectNameStrategy::TopicNameStrategyWithSchema(t, is_key, _) => {
            if *is_key {
                format!("{}-key", t)
            } else {
                format!("{}-value", t)
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategyWithSchema(t, s) => match s.parsed {
            Schema::Record { ref name, .. } => format!("{}-{}", t, name.fullname(None)),
            _ => panic!("Supplied schema is not a record"),
        },
    }
}

/// Handles the work of doing an http call and transforming it to a schema while handling
/// possible errors. When there is an error it might be useful to retry.
fn schema_from_url(url: &str, id: Option<u32>) -> Result<(Schema, u32), SRCError> {
    let easy = match perform_get(url) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::new(
                "error performing get to schema registry",
                Some(e.description()),
                true,
            ))
        }
    };
    let json: JsonValue = match to_json(easy) {
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    let raw_schema = match json["schema"].as_str() {
        Some(v) => v,
        None => {
            return Err(SRCError::new(
                "Could not get raw schema from response",
                None,
                false,
            ))
        }
    };
    let schema = match Schema::parse_str(raw_schema) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::new(
                "Could not parse schema",
                Some(&e.to_string()),
                false,
            ))
        }
    };
    let id = match id {
        Some(v) => v,
        None => {
            let id_from_response = match json["id"].as_u64() {
                Some(v) => v,
                None => return Err(SRCError::new("Could not get id from response", None, false)),
            };
            id_from_response as u32
        }
    };
    Ok((schema, id))
}

/// Handles posting the schema, and getting back the id. When the schema is already in the schema
/// registry, the matching id is returned. When it's not it depends on the settings of the schema
/// registry. The default config will check if the schema is backwards compatible. One of the ways
/// to do this is to add a default value for new fields.
pub fn post_schema(url: &str, schema: SuppliedSchema) -> Result<(Schema, u32), SRCError> {
    let mut root_element = JsonMap::new();
    root_element.insert("schema".into(), JsonValue::String(schema.raw.clone()));
    let schema_element = JsonValue::Object(root_element);
    let schema_str = schema_element.to_string();

    let easy = match perform_post(url, schema_str.as_str()) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::new(
                "error performing post to schema registry",
                Some(e.description()),
                true,
            ))
        }
    };
    let json: JsonValue = match to_json(easy) {
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    let id = match json["id"].as_i64() {
        Some(v) => v,
        None => return Err(SRCError::new("Could not get id from response", None, false)),
    };
    Ok((schema.parsed, id as u32))
}

/// Does the get, doing it like this makes for more compact code.
fn perform_get(url: &str) -> Result<Easy2<Collector>, curl::Error> {
    let mut easy = Easy2::new(Collector(Vec::new()));
    easy.get(true)?;
    easy.url(url)?;
    easy.perform()?;
    Ok(easy)
}

/// Does the post, setting the headers correctly
fn perform_post(url: &str, schema_raw: &str) -> Result<Easy2<Collector>, curl::Error> {
    let mut easy = Easy2::new(Collector(Vec::new()));
    easy.post(true)?;
    easy.url(url)?;
    easy.post_fields_copy(schema_raw.as_bytes())?;
    let mut list = List::new();
    list.append("Content-Type: application/vnd.schemaregistry.v1+json")?;
    list.append("Accept: application/vnd.schemaregistry.v1+json")?;
    easy.http_headers(list)?;
    easy.perform()?;
    Ok(easy)
}

/// If the response code was 200, tries to format the payload as json
fn to_json(mut easy: Easy2<Collector>) -> Result<JsonValue, SRCError> {
    match easy.response_code() {
        Ok(200) => (),
        Ok(v) => {
            return Err(SRCError::new(
                format!("Did not get a 200 response code but {} instead", v).as_str(),
                None,
                false,
            ))
        }

        Err(e) => {
            return Err(SRCError::new(
                format!("Encountered error getting http response: {}", e).as_str(),
                Some(e.description()),
                true,
            ))
        }
    }
    let mut data = Vec::new();
    match easy.get_ref() {
        Collector(b) => data.extend_from_slice(b),
    }
    let body = match str::from_utf8(data.as_ref()) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::new(
                "Invalid UTF-8 sequence",
                Some(e.description()),
                false,
            ))
        }
    };
    match serde_json::from_str(body) {
        Ok(v) => Ok(v),
        Err(e) => Err(SRCError::new(
            "Invalid json string",
            Some(e.description()),
            false,
        )),
    }
}

/// Struct to store the payload in
struct Collector(Vec<u8>);

/// Used to easily get the payload from a http call.
impl Handler for Collector {
    fn write(&mut self, data: &[u8]) -> Result<usize, WriteError> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
}

/// Error struct which makes it easy to know if the resulting error is also preserved in the cache
/// or not. And whether trying it again might not cause an error.
#[derive(Debug, PartialEq)]
pub struct SRCError {
    error: String,
    side: Option<String>,
    retriable: bool,
    cached: bool,
}

/// Implements clone so when an error is returned from the cache, a copy can be returned
impl Clone for SRCError {
    fn clone(&self) -> SRCError {
        let side = match &self.side {
            Some(v) => Some(String::from(v.deref())),
            None => None,
        };
        SRCError {
            error: String::from(self.error.deref()),
            side,
            retriable: self.retriable,
            cached: self.cached,
        }
    }
}

/// Gives the information from the error in a readable format.
impl fmt::Display for SRCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.side {
            Some(cause) => write!(
                f,
                "Error: {}, was cause by {}, it's retriable: {}, it's cached: {}",
                self.error, &cause, self.retriable, self.cached
            ),
            None => write!(
                f,
                "Error: {} had no other cause, it's retriable: {}, it's cached: {}",
                self.error, self.retriable, self.cached
            ),
        }
    }
}

/// Specific error from which can be determined whether retrying might not lead to an error and
/// whether the error is cashed, it's turned into the cashed variant when it's put into the cache.
impl SRCError {
    pub fn new(error: &str, cause: Option<&str>, retriable: bool) -> SRCError {
        let side = match cause {
            Some(v) => Some(v.to_owned()),
            None => None,
        };
        SRCError {
            error: error.to_owned(),
            side,
            retriable,
            cached: false,
        }
    }
    /// Should be called before putting the error in the cache
    pub fn into_cache(self) -> SRCError {
        SRCError {
            error: self.error,
            side: self.side,
            retriable: self.retriable,
            cached: true,
        }
    }
}

#[test]
#[should_panic]
fn panic_when_invalid_schema() {
    SuppliedSchema::new(r#"{"type":"record","name":"Name"}"#.into());
}

#[test]
fn display_record_name_strategy() {
    let sns = SubjectNameStrategy::RecordNameStrategy("bla".into());
    assert_eq!(
        "RecordNameStrategy(\"bla\")".to_owned(),
        format!("{:?}", sns)
    )
}

#[test]
fn display_topic_name_strategy() {
    let sns = SubjectNameStrategy::TopicNameStrategy("bla".into(), true);
    assert_eq!(
        "TopicNameStrategy(\"bla\", true)".to_owned(),
        format!("{:?}", sns)
    )
}

#[test]
fn display_topic_record_name_strategy() {
    let sns = SubjectNameStrategy::TopicRecordNameStrategy("bla".into(), "foo".into());
    assert_eq!(
        "TopicRecordNameStrategy(\"bla\", \"foo\")".to_owned(),
        format!("{:?}", sns)
    )
}

#[test]
fn display_record_name_strategy_with_schema() {
    let ss = SuppliedSchema::new(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#.into());
    let sns = SubjectNameStrategy::RecordNameStrategyWithSchema(ss);
    assert_eq!("RecordNameStrategyWithSchema(SuppliedSchema { raw: \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Name\\\",\\\"namespace\\\":\\\"nl.openweb.data\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"avro.java.string\\\":\\\"String\\\"}]}\", parsed: Record { name: Name { name: \"Name\", namespace: Some(\"nl.openweb.data\"), aliases: None }, doc: None, fields: [RecordField { name: \"name\", doc: None, default: None, schema: String, order: Ascending, position: 0 }], lookup: {\"name\": 0} } })".to_owned(), format!("{:?}", sns))
}

#[test]
fn display_topic_name_strategy_with_schema() {
    let ss = SuppliedSchema::new(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#.into());
    let sns = SubjectNameStrategy::TopicNameStrategyWithSchema("bla".into(), true, ss);
    assert_eq!("TopicNameStrategyWithSchema(\"bla\", true, SuppliedSchema { raw: \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Name\\\",\\\"namespace\\\":\\\"nl.openweb.data\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"avro.java.string\\\":\\\"String\\\"}]}\", parsed: Record { name: Name { name: \"Name\", namespace: Some(\"nl.openweb.data\"), aliases: None }, doc: None, fields: [RecordField { name: \"name\", doc: None, default: None, schema: String, order: Ascending, position: 0 }], lookup: {\"name\": 0} } })".to_owned(), format!("{:?}", sns))
}

#[test]
fn display_topic_record_name_strategy_with_schema() {
    let ss = SuppliedSchema::new(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#.into());
    let sns = SubjectNameStrategy::TopicRecordNameStrategyWithSchema("bla".into(), ss);
    assert_eq!("TopicRecordNameStrategyWithSchema(\"bla\", SuppliedSchema { raw: \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Name\\\",\\\"namespace\\\":\\\"nl.openweb.data\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"avro.java.string\\\":\\\"String\\\"}]}\", parsed: Record { name: Name { name: \"Name\", namespace: Some(\"nl.openweb.data\"), aliases: None }, doc: None, fields: [RecordField { name: \"name\", doc: None, default: None, schema: String, order: Ascending, position: 0 }], lookup: {\"name\": 0} } })".to_owned(), format!("{:?}", sns))
}

#[test]
#[should_panic]
fn panic_when_schema_is_not_a_record() {
    let ss = SuppliedSchema::new(r#"{"type":"boolean","value":"true"}"#.into());
    let sns = SubjectNameStrategy::RecordNameStrategyWithSchema(ss);
    get_subject(&sns);
}

#[test]
#[should_panic]
fn panic_when_schema_is_not_a_record_2() {
    let ss = SuppliedSchema::new(r#"{"type":"boolean","value":"true"}"#.into());
    let sns = SubjectNameStrategy::TopicRecordNameStrategyWithSchema("bla".into(), ss);
    get_subject(&sns);
}

#[test]
fn handling_http_error() {
    let easy = Easy2::new(Collector(Vec::new()));
    let result = to_json(easy);
    assert_eq!(
        result,
        Err(SRCError::new(
            "Did not get a 200 response code but 0 instead",
            None,
            false
        ))
    )
}

#[test]
fn display_erro_no_cause() {
    let err = SRCError::new("Could not get id from response", None, false);
    assert_eq!(format!("{}", err), "Error: Could not get id from response had no other cause, it\'s retriable: false, it\'s cached: false".to_owned())
}

#[test]
fn display_erro_with_cause() {
    let err = SRCError::new(
        "Could not get id from response",
        Some("error in response"),
        false,
    );
    assert_eq!(format!("{}", err), "Error: Could not get id from response, was cause by error in response, it\'s retriable: false, it\'s cached: false".to_owned())
}
