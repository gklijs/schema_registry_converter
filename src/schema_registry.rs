//! This module contains the code specific for the schema registry.

extern crate avro_rs;
extern crate curl;
extern crate serde_json;

use self::avro_rs::Schema;
use self::curl::easy::Easy2;
use self::curl::easy::Handler;
use self::curl::easy::WriteError;
use self::serde_json::Value as JsonValue;
use std::error::Error;
use std::ops::Deref;
use std::str;
use core::fmt;
use std::error;

/// Strategy similar to the one in the Java client. By default schema's needs to be backwards
/// compatible. Historically the only available strategy was the TopicNameStrategy. This meant in
/// practice that a topic could only have one type, or the restriction on backwards compatibility
/// was to be abandoned. Using either of the two other strategies allows multiple types of schema
/// on on topic, while still being able to keep the restriction on schema's being backwards
/// compatible.
#[derive(Debug)]
pub enum SubjectNameStrategy<'a> {
    RecordNameStrategy(&'a str),
    TopicNameStrategy(&'a str, bool),
    TopicRecordNameStrategy(&'a str, &'a str),
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
    let url = schema_registry_url.to_owned()
        + "/subjects/"
        + &get_subject(subject_name_strategy)
        + "/versions/latest";
    schema_from_url(&url, None)
}

/// Gets the subject part which is also used as key to cache the results. It's constructed so that
/// it's compatible with the Java client.
pub fn get_subject(subject_name_strategy: &SubjectNameStrategy) -> String {
    match subject_name_strategy {
        SubjectNameStrategy::RecordNameStrategy(rn) => String::from(*rn),
        SubjectNameStrategy::TopicNameStrategy(t, is_key) => {
            if *is_key {
                String::from(*t) + "-key"
            } else {
                String::from(*t) + "-value"
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategy(t, rn) => String::from(*t) + "-" + rn,
    }
}

struct Collector(Vec<u8>);

impl Handler for Collector {
    fn write(&mut self, data: &[u8]) -> Result<usize, WriteError> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
}

/// Handles the work of doing an http call and transforming it to a schema while hopefully handling
/// all possible errors. For now there is now distinction between recoverable and unrecoverable
/// errors.
fn schema_from_url(url: &str, id: Option<u32>) -> Result<(Schema, u32), SRCError> {
    let mut data = Vec::new();
    let mut easy = Easy2::new(Collector(Vec::new()));
    if let Err(e) = easy.get(true) {
        return Err(SRCError::new("error setting get on easy", Some(e.description()),true));
    }
    if let Err(e) = easy.url(url) {
        return Err(SRCError::new("error setting url on easy", Some(e.description()),true));
    }
    if let Err(e) = easy.perform() {
        return Err(SRCError::new("error performing easy", Some(e.description()),true));
    }
    match easy.response_code() {
        Ok(200) => (),
        Ok(v) =>
            return Err(SRCError::new(format!(
                "Did not get a 200 response code from {} but {} instead",
                url, v
            ).as_str(), None, false)),

        Err(e) =>
            return Err(SRCError::new(format!(
                "Encountered error getting http response from {}: {}",
                url, e
            ).as_str(), Some(e.description()), true)),

    }
    match easy.get_ref() {
        Collector(b) => data.extend_from_slice(b),
    }
    let body = match str::from_utf8(data.as_ref()) {
        Ok(v) => v,
        Err(e) => return Err(SRCError::new("Invalid UTF-8 sequence", Some(e.description()), false)),
    };
    let json: JsonValue = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return Err(SRCError::new("Invalid json string", Some(e.description()), false)),
    };
    let raw_schema = match json["schema"].as_str() {
        Some(v) => v,
        None => return Err(SRCError::new("Could not get raw schema from response", None, false)),
    };
    let schema = match Schema::parse_str(raw_schema) {
        Ok(v) => v,
        Err(e) => return Err(SRCError::new("Could not parse schema", Some(&e.to_string()), false)),
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

/// Error struct which makes it easy to know if the resulting error is also preserved in the cache
/// or not. And whether trying it again might not cause an error.
#[derive(Debug, PartialEq)]
pub struct SRCError {
    error: String,
    side: Option<String>,
    retriable: bool,
    cached: bool,
}

impl Clone for SRCError {
    fn clone(&self) -> SRCError {
        let side = match &self.side{
            Some(v) => Some(String::from(v.deref())),
            None => None,
        };
        SRCError{
            error: String::from(self.error.deref()),
            side,
            retriable: self.retriable,
            cached: self.cached,
        }
    }
}

impl fmt::Display for SRCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.side {
            Some(cause) => write!(f, "Error: {}, was cause by {}, it's retriable: {}, it's cached: {}", self.error, &cause, self.retriable, self.cached),
            None => write!(f, "Error: {} had no other cause, it's retriable: {}, it's cached: {}", self.error, self.retriable, self.cached),
        }
    }
}

impl error::Error for SRCError{
    fn description(&self) -> &str {
        &self.error
    }
}

impl SRCError{
    pub fn new(error: &str, cause: Option<&str>, retriable: bool) -> SRCError {
        let side = match cause {
            Some(v) => Some(v.to_owned()),
            None => None,
        };
        SRCError{
            error: error.to_owned(),
            side,
            retriable,
            cached: false,
        }
    }
    pub fn into_cache(self) -> SRCError {
        SRCError{
            error: self.error,
            side: self.side,
            retriable: self.retriable,
            cached: true,
        }
    }
}
