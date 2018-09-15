extern crate avro_rs;
extern crate curl;
extern crate serde_json;

use self::avro_rs::Schema;
use self::curl::easy::Easy;
use self::serde_json::Value as JsonValue;
use std::error::Error;
use std::str;

pub fn get_schema(id: u32, registry: &str) -> Result<Schema, String> {
    let url = registry.to_owned() + "/schemas/ids/" + &id.to_string();
    schema_from_url(&url, Option::from(id)).and_then(|t| Ok(t.0))
}

pub fn get_latest_schema(topic: &str, registry: &str) -> Result<(Schema, u32), String> {
    let url = registry.to_owned() + "/subjects/" + topic + "-value/versions/latest";
    schema_from_url(&url, None)
}

fn schema_from_url(url: &str, id: Option<u32>) -> Result<(Schema, u32), String> {
    let mut data = Vec::new();
    let mut handle = Easy::new();
    if let Err(e) = handle.url(url) {
        return Err(e.description().to_owned());
    }
    {
        let mut transfer = handle.transfer();
        if let Err(e) = transfer.write_function(|new_data| {
            data.extend_from_slice(new_data);
            Ok(new_data.len())
        }) {
            return Err(e.description().to_owned());
        }
        if let Err(e) = transfer.perform() {
            return Err(e.description().to_owned());
        }
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
