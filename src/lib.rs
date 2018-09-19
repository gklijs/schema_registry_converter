//! Rust encoder and decoder in order to work with the Confluent schema registry.
//!
//! This crate contains ways to handle encoding and decoding of messages making use of the
//! [confluent schema-registry]. This happens in a way which is compatible to the
//! [confluent java serde].
//!
//! [confluent schema-registry]: https://docs.confluent.io/current/schema-registry/docs/index.html
//! [confluent java serde]: https://github.com/confluentinc/schema-registry/tree/master/avro-serde/src/main/java/io/confluent/kafka/streams/serdes/avro
//!
//! Both the Decoder and the Encoder have a cache to allow re-use of the Schema objects used for
//! the avro transitions. For testing I only used a very basic schema, at [avro-rs] more complex
//! schema can be found.
//!
//! For Encoding data it's assumed the schema is already available in the schema registry, and the
//! latest version will be used. In the Java serde you can supply a schema, this is not supported
//! now. For decoding it works the same as the Java part, using the id encoded in the bytes, the
//! correct schema will be fetched and used to decode the message to a avro_rs::types::Value.
//!
//! [avro-rs]: https://crates.io/crates/avro-rs

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
use schema_registry::SRCError;

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
/// let _m = mock("GET", "/schemas/ids/1")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
///     .create();
///
/// let mut decoder = Decoder::new(SERVER_ADDRESS);
/// let heartbeat = decoder.decode(Some(&[0,0,0,0,1,6]));
///
/// assert_eq!(heartbeat, Ok(Value::Record(vec!(("beat".to_string(), Value::Long(3))))))
/// ```
#[derive(Debug)]
pub struct Decoder {
    schema_registry_url: String,
    cache: &'static mut HashMap<u32, Result<Schema, SRCError>, RandomState>,
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
    ///
    /// ```
    ///  # extern crate mockito;
    ///  # extern crate schema_registry_converter;
    ///  # extern crate avro_rs;
    ///  # use mockito::{mock, SERVER_ADDRESS};
    ///  # use schema_registry_converter::Decoder;
    ///  # use schema_registry_converter::schema_registry::SRCError;
    ///  # use avro_rs::types::Value;
    ///
    /// let mut decoder = Decoder::new(SERVER_ADDRESS);
    /// let bytes = [0,0,0,0,2,6];
    ///
    /// let _m = mock("GET", "/schemas/ids/2")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    /// let heartbeat = decoder.decode(Some(&bytes));
    /// assert_eq!(heartbeat, Err(SRCError::new("Did not get a 200 response code from 127.0.0.1:1234/schemas/ids/2 but 404 instead", None, false).into_cache()));
    /// let _m = mock("GET", "/schemas/ids/2")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes));
    /// assert_eq!(heartbeat, Err(SRCError::new("Did not get a 200 response code from 127.0.0.1:1234/schemas/ids/2 but 404 instead", None, false).into_cache()));
    ///
    /// decoder.remove_errors_from_cache();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes));
    /// assert_eq!(heartbeat, Ok(Value::Record(vec!(("beat".to_string(), Value::Long(3))))))
    /// ```
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
    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        match bytes {
            None => Ok(Value::Null),
            Some(p) if p[0] == 0 => self.deserialize(p),
            Some(p) => Ok(Value::Bytes(p.to_vec())),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize<'a>(&'a mut self, bytes: &'a [u8]) -> Result<Value, SRCError> {
        let mut buf = &bytes[1..5];
        let id = match buf.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => return Err(SRCError::new("Could not get id from bytes", Some(&e.to_string()), false)),
        };
        let mut reader = Cursor::new(&bytes[5..]);
        let sr = &self.schema_registry_url;
        let schema = self
            .cache
            .entry(id)
            .or_insert_with(|| match get_schema_by_id(id, sr){
                Ok(v) => Ok(v),
                Err(e) => Err(e.into_cache()),
            });
        match schema {
            Ok(v) => match from_avro_datum(&v, &mut reader, None) {
                Ok(v) => Ok(v),
                Err(e) => Err(SRCError::new("Could not transform bytes using schema", Some(&e.to_string()), false)),
            },
            Err(e) => Err(e.clone()),
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
///
/// ```
///  # extern crate mockito;
///  # extern crate schema_registry_converter;
///  # extern crate avro_rs;
///  # use mockito::{mock, SERVER_ADDRESS};
///  # use schema_registry_converter::Encoder;
///  # use schema_registry_converter::schema_registry::SubjectNameStrategy;
///  # use avro_rs::types::Value;
///
/// let _m = mock("GET", "/subjects/heartbeat-value/versions/latest")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
///     .create();
///
/// let _m = mock("GET", "/subjects/heartbeat-key/versions/latest")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Name\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"avro.java.string\":\"String\"}]}"}"#)
///     .create();
///
/// let mut encoder = Encoder::new(SERVER_ADDRESS);
///
/// let key_strategy = SubjectNameStrategy::TopicNameStrategy("heartbeat", true);
/// let bytes = encoder.encode(vec!(("name", Value::String("Some name".to_owned()))), &key_strategy);
///
/// assert_eq!(bytes, Ok(vec!(0, 0, 0, 0, 4, 18, 83, 111, 109, 101, 32, 110, 97, 109, 101)));
///
/// let value_strategy = SubjectNameStrategy::TopicNameStrategy("heartbeat", false);
/// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &value_strategy);
///
/// assert_eq!(bytes, Ok(vec!(0,0,0,0,3,6)))
/// ```
#[derive(Debug)]
pub struct Encoder {
    schema_registry_url: String,
    cache: &'static mut HashMap<String, Result<(Schema, u32), SRCError>, RandomState>,
}

impl Encoder {
    /// Creates a new encoder which will use the supplied url to fetch the schema's. The schema's
    /// need to be retrieved together with the id, in order for a consumer to decode the bytes.
    /// For the encoding several strategies are available in the java client, all three of them are
    /// supported. The schema's does have to be present in the schema registry already. This is
    /// unlike the Java client with wich it's possible to update/upload the schema when it's not
    /// present yet. While it may be added to this library, it's also not hard to do it separately.
    /// New schema's can set by doing a post at /subjects/{subject}/versions.
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
    ///
    /// ```
    ///  # extern crate mockito;
    ///  # extern crate schema_registry_converter;
    ///  # extern crate avro_rs;
    ///  # use mockito::{mock, SERVER_ADDRESS};
    ///  # use schema_registry_converter::Encoder;
    ///  # use schema_registry_converter::schema_registry::SubjectNameStrategy;
    ///  # use schema_registry_converter::schema_registry::SRCError;
    ///  # use avro_rs::types::Value;
    ///
    /// let mut encoder = Encoder::new(SERVER_ADDRESS);
    /// let strategy = SubjectNameStrategy::RecordNameStrategy("nl.openweb.data.Heartbeat");
    ///
    /// let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    ///
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    /// assert_eq!(bytes, Err(SRCError::new("Did not get a 200 response code from 127.0.0.1:1234/subjects/nl.openweb.data.Heartbeat/versions/latest but 404 instead", None, false).into_cache()));
    ///
    /// let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    /// assert_eq!(bytes, Err(SRCError::new("Did not get a 200 response code from 127.0.0.1:1234/subjects/nl.openweb.data.Heartbeat/versions/latest but 404 instead", None, false).into_cache()));
    ///
    /// encoder.remove_errors_from_cache();
    ///
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    /// assert_eq!(bytes, Ok(vec!(0,0,0,0,4,6)))
    /// ```
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v {
            Ok(_) => true,
            Err(_) => false,
        });
    }
    /// Encodes a vector of values to bytes. The corrects values of the 'keys' depend on the schema
    /// being fetched at runtime. For example you might agree on a schema with a consuming party and
    /// /or upload a schema to the schema registry before starting the program. In the future an
    /// 'encode with schema' might be added which makes it easier to make sure the schema will
    /// become available in the correct way.
    ///
    /// ```
    ///  # extern crate mockito;
    ///  # extern crate schema_registry_converter;
    ///  # extern crate avro_rs;
    ///  # use mockito::{mock, SERVER_ADDRESS};
    ///  # use schema_registry_converter::Encoder;
    ///  # use schema_registry_converter::schema_registry::SubjectNameStrategy;
    ///  # use avro_rs::types::Value;
    ///
    /// let _m = mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let mut encoder = Encoder::new(SERVER_ADDRESS);
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategy("heartbeat", "nl.openweb.data.Heartbeat");
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    ///
    /// assert_eq!(bytes, Ok(vec!(0,0,0,0,3,6)))
    /// ```
    pub fn encode<'a>(
        &mut self,
        values: Vec<(&'static str, Value)>,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let schema_registry_url = &self.schema_registry_url;
        let schema_and_id = self
            .cache
            .entry(get_subject(subject_name_strategy))
            .or_insert_with(|| match get_schema_by_subject(schema_registry_url, &subject_name_strategy){
                Ok(v) => Ok(v),
                Err(e) => Err(e.into_cache())
            });
        match schema_and_id{
            Ok((schema, id)) => to_bytes(&schema, *id, values),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Using the schema with a vector of values the values will be correctly deserialized according to
/// the avro specification.
fn to_bytes(
    schema: &Schema,
    id: u32,
    values: Vec<(&'static str, Value)>,
) -> Result<Vec<u8>, SRCError> {
    let mut record = match Record::new(schema) {
        Some(v) => v,
        None => return Err(SRCError::new("Could not create record from schema", None, false)),
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
        Err(e) => return Err(SRCError::new("Could not get avro bytes", Some(&e.to_string()), false)),
    }
    Ok(payload)
}