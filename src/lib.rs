extern crate avro_rs;
extern crate byteorder;
extern crate core;
extern crate rdkafka;

mod schema_registry;

use avro_rs::types::{Record, Value};
use avro_rs::{from_avro_datum, to_avro_datum, Schema};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use rdkafka::message::{BorrowedMessage, Message};
use schema_registry::{get_schema_by_id, get_schema_by_subject, get_subject};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Debug)]
pub struct Decoder {
    schema_registry_url: String,
    cache: &'static mut HashMap<u32, Result<Schema, String>, RandomState>,
}

impl Decoder {
    pub fn new(schema_registry_url: &str) -> Decoder {
        let new_cache = Box::new(HashMap::new());
        Decoder {
            schema_registry_url: String::from(schema_registry_url),
            cache: Box::leak(new_cache),
        }
    }
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v {
            Ok(_) => true,
            Err(_) => false,
        });
    }
    pub fn decode_key<'a>(&'a mut self, msg: &BorrowedMessage) -> Result<Value, String> {
        match msg.key() {
            None => Ok(Value::Null),
            Some(p) if p[0] == 0 => self.deserialize(p),
            Some(p) => Ok(Value::Bytes(p.to_vec())),
        }
    }
    pub fn decode_value<'a>(&'a mut self, msg: &BorrowedMessage) -> Result<Value, String> {
        match msg.payload() {
            None => Ok(Value::Null),
            Some(p) if p[0] == 0 => self.deserialize(p),
            Some(p) => Ok(Value::Bytes(p.to_vec())),
        }
    }
    pub fn deserialize<'a>(&'a mut self, bytes: &'a [u8]) -> Result<Value, String> {
        let mut buf = &bytes[1..5];
        let id = match buf.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => return Err(format!("Could not get id from bytes: {}", e)),
        };
        let mut reader = Cursor::new(&bytes[5..]);
        let sr = &self.schema_registry_url;
        let schema = self
            .cache
            .entry(id)
            .or_insert_with(|| get_schema_by_id(id, sr));
        match schema {
            Ok(v) => match from_avro_datum(v, &mut reader, None) {
                Ok(v) => Ok(v),
                Err(e) => Err(format!(
                    "Could not transform bytes using schema, error: {}",
                    e
                )),
            },
            Err(e) => Err(e.to_owned()),
        }
    }
}

#[derive(Debug)]
pub struct Encoder {
    schema_registry_url: String,
    cache: &'static mut HashMap<String, Result<(Schema, u32), String>, RandomState>,
}

impl Encoder {
    pub fn new(schema_registry_url: &str) -> Encoder {
        let new_cache = Box::new(HashMap::new());
        Encoder {
            schema_registry_url: String::from(schema_registry_url),
            cache: Box::leak(new_cache),
        }
    }
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v {
            Ok(_) => true,
            Err(_) => false,
        });
    }
    pub fn encode(
        &mut self,
        values: Vec<(&'static str, Value)>,
        topic: Option<&str>,
        record_name: Option<&str>,
        is_key: bool,
    ) -> Result<Vec<u8>, String> {
        let subject = match get_subject(topic, record_name, is_key) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let schema = &self.schema_registry_url;
        let schema_and_id = match self
            .cache
            .entry(subject)
            .or_insert_with(|| get_schema_by_subject(schema, topic, record_name, is_key))
        {
            Ok(v) => v,
            Err(e) => return Err(e.to_owned()),
        };
        to_bytes(&schema_and_id.0, schema_and_id.1, values)
    }
}

fn to_bytes(
    schema: &Schema,
    id: u32,
    values: Vec<(&'static str, Value)>,
) -> Result<Vec<u8>, String> {
    let mut record = match Record::new(schema) {
        Some(v) => v,
        None => return Err("Could not create record from schema".to_owned()),
    };
    for value in values {
        record.put(value.0, value.1)
    }
    let mut payload = vec![0u8];
    {
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, id);
        payload.extend_from_slice(&buf);
    }
    match to_avro_datum(schema, record) {
        Ok(v) => payload.extend_from_slice(v.as_slice()),
        Err(e) => return Err(format!("Could not get avro bytes: {}", e)),
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
