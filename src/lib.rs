extern crate avro_rs;
extern crate byteorder;
extern crate core;

pub mod schema_registry;

use avro_rs::types::{Record, Value};
use avro_rs::{from_avro_datum, to_avro_datum, Schema};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use schema_registry::{get_schema_by_id, get_schema_by_subject, get_subject, SubjectNameStrategy};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::io::Cursor;

/// A decoder used to transform bytes to a Value object
///
/// The main purpose of having this struct is to be able to cache the schema's. Because the need to
/// be retrieved over http from the schema registry, and we can already see from the bytes which
/// schema we should use, this can save a lot of unnecessary calls.
/// Errors are also stored to the cache, because they may not be recoverable. A function is
/// available to remove the errors from the cache. To get the value avro_rs is used.
///
/// For both the key and the payload/key it's possible to use the schema registry, this struct supports
/// both. But only using the SubjectNameStrategy::TopicNameStrategy it has to be made explicit
/// whether it's actual used as key or value.
///
/// ```
///  # extern crate mockito;
///  # extern crate schema_registry_converter;
///  # extern crate avro_rs;
///  # use mockito::{mock, SERVER_ADDRESS};
///  # use schema_registry_converter::Decoder;
///  # use avro_rs::types::Value;
///
/// let _m = mock("GET", "/schemas/ids/7")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
///     .create();
///
/// let _m = mock("GET", "/schemas/ids/101")
///     .with_status(404)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
///     .create();
///
/// let mut decoder = Decoder::new(SERVER_ADDRESS);
/// let heartbeat = decoder.decode(Some(&[0,0,0,0,7,6]));
///
///  assert_eq!(heartbeat, Ok(Value::Record(vec!(("beat".to_string(), Value::Long(3))))))
/// ```
#[derive(Debug)]
pub struct Decoder {
    schema_registry_url: String,
    cache: &'static mut HashMap<u32, Result<Schema, String>, RandomState>,
}

impl Decoder {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. It's possible for recoverable errors to stay in the cash, when a result
    /// comes back as an error you can use remove_errors_from_cache to clean the cache, keeping the
    /// correctly fetched schema's
    pub fn new(schema_registry_url: &str) -> Decoder {
        let new_cache = Box::new(HashMap::new());
        Decoder {
            schema_registry_url: String::from(schema_registry_url),
            cache: Box::leak(new_cache),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v {
            Ok(_) => true,
            Err(_) => false,
        });
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a mut
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    ///
    /// ```no_run
    /// # extern crate rdkafka;
    /// # extern crate schema_registry_converter;
    /// # extern crate avro_rs;
    /// # use rdkafka::message::{Message, BorrowedMessage};
    /// # use schema_registry_converter::Decoder;
    /// # use avro_rs::types::Value;
    /// fn get_value<'a>(
    ///     msg: &'a BorrowedMessage,
    ///     decoder: &'a mut Decoder,
    /// ) -> Value{
    ///     match decoder.decode(msg.payload()){
    ///         Ok(v) => v,
    ///         Err(e) => panic!("Error getting value: {}", e),
    ///     }
    /// }
    /// ```
    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Value, String> {
        match bytes {
            None => Ok(Value::Null),
            Some(p) if p[0] == 0 => self.deserialize(p),
            Some(p) => Ok(Value::Bytes(p.to_vec())),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize<'a>(&'a mut self, bytes: &'a [u8]) -> Result<Value, String> {
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

/// A decoder used to transform bytes to a Value object
///
/// The main purpose of having this struct is to be able to cache the schema's. Because the need to
/// be retrieved over http from the schema registry, and we can already see from the bytes which
/// schema we should use, this can save a lot of unnecessary calls.
/// Errors are also stored to the cache, because they may not be recoverable. A function is
/// available to remove the errors from the cache. To get the value avro_rs is used.
///
/// For both the key and the payload/key it's possible to use the schema registry, this struct supports
/// both. But only using the SubjectNameStrategy::TopicNameStrategy it has to be made explicit
/// whether it's actual used as key or value.
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
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v {
            Ok(_) => true,
            Err(_) => false,
        });
    }
    pub fn encode(
        &mut self,
        values: Vec<(&'static str, Value)>,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, String> {
        let schema_registry_url = &self.schema_registry_url;
        let schema_and_id = match self
            .cache
            .entry(get_subject(subject_name_strategy))
            .or_insert_with(|| get_schema_by_subject(schema_registry_url, &subject_name_strategy))
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
