extern crate avro_rs;
extern crate curl;
extern crate serde_json;

use self::avro_rs::Schema;
use self::curl::easy::Easy2;
use self::curl::easy::Handler;
use self::curl::easy::WriteError;
use self::serde_json::Value as JsonValue;
use std::error::Error;
use std::str;

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
pub fn get_schema_by_id(id: u32, schema_registry_url: &str) -> Result<Schema, String> {
    let url = schema_registry_url.to_owned() + "/schemas/ids/" + &id.to_string();
    schema_from_url(&url, Option::from(id)).and_then(|t| Ok(t.0))
}

/// Gets the schema and the id by supplying a SubjectNameStrategy. This is used to correctly
/// transform a vector to bytes.
pub fn get_schema_by_subject(
    schema_registry_url: &str,
    subject_name_strategy: &SubjectNameStrategy,
) -> Result<(Schema, u32), String> {
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
fn schema_from_url(url: &str, id: Option<u32>) -> Result<(Schema, u32), String> {
    let mut data = Vec::new();
    let mut easy = Easy2::new(Collector(Vec::new()));
    if let Err(e) = easy.get(true) {
        return Err(e.description().to_owned());
    }
    if let Err(e) = easy.url(url) {
        return Err(e.description().to_owned());
    }
    if let Err(e) = easy.perform() {
        return Err(e.description().to_owned());
    }
    match easy.response_code() {
        Ok(200) => (),
        Ok(v) => {
            return Err(format!(
                "Did not get a 200 response code from {} but {} instead",
                url, v
            ))
        }
        Err(e) => {
            return Err(format!(
                "Encountered error getting http response from {}: {}",
                url, e
            ))
        }
    }
    match easy.get_ref() {
        Collector(b) => data.extend_from_slice(b),
    }
    let body = match str::from_utf8(data.as_ref()) {
        Ok(v) => v,
        Err(e) => return Err(format!("Invalid UTF-8 sequence: {}", e)),
    };
    let json: JsonValue = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return Err(format!("Invalid json string: {}", e)),
    };
    let raw_schema = match json["schema"].as_str() {
        Some(v) => v,
        None => return Err("Could not get raw schema from response".to_owned()),
    };
    let schema = match Schema::parse_str(raw_schema) {
        Ok(v) => v,
        Err(e) => return Err(format!("Could not parse schema: {}", e)),
    };
    let id = match id {
        Some(v) => v,
        None => {
            let id_from_response = match json["id"].as_u64() {
                Some(v) => v,
                None => return Err("Could not get id from response".to_owned()),
            };
            id_from_response as u32
        }
    };
    Ok((schema, id))
}
