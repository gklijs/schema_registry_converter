extern crate avro_rs;
extern crate byteorder;
extern crate core;
extern crate rdkafka;

mod schema_registry;

use avro_rs::from_avro_datum;
use avro_rs::types::Value;
use avro_rs::Schema;
use byteorder::{BigEndian, ReadBytesExt};
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Message;
use schema_registry::get_latest_schema;
use schema_registry::get_schema;
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
    pub fn empty_errors<'a>(&mut self) {
        self.cache.retain(|_, v| match v {
            Ok(_) => true,
            Err(_) => false,
        });
    }
    pub fn deserialize<'a>(&'a mut self, bytes: &'a [u8]) -> Result<Value, String> {
        let mut buf = &bytes[1..5];
        let id = match buf.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => return Err(format!("Could not get id from bytes: {}", e)),
        };
        let mut reader = Cursor::new(&bytes[5..]);
        let sr = &self.schema_registry_url;
        let schema = self.cache.entry(id).or_insert_with(|| get_schema(id, sr));
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
