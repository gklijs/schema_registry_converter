//! Rust encoder and decoder in order to work with the Confluent schema registry.
//!
//! This crate contains ways to handle encoding and decoding of messages making use of the
//! [confluent schema-registry]. This happens in a way that is compatible to the
//! [confluent java serde]. As a result it becomes easy to work with the same data in both the jvm
//! and rust.
//!
//! [confluent schema-registry]: https://docs.confluent.io/current/schema-registry/docs/index.html
//! [confluent java serde]: https://github.com/confluentinc/schema-registry/tree/master/avro-serde/src/main/java/io/confluent/kafka/streams/serdes/avro
//!
//! Both the Decoder and the Encoder have a cache to allow re-use of the Schema objects used for
//! the avro transitions.
//!
//! For Encoding data it's possible to supply a schema else the latest available schema will be used.
//! For Decoding it works the same as the Java part, using the id encoded in the bytes, the
//! correct schema will be fetched and used to decode the message to a avro_rs::types::Value.
//!
//! Resulting errors are SRCError, besides the error they also contain a .cached which tells whether
//! the error is cached or not. Another property added to the error is retriable, in some cases, like
//! when the network fails it might be worth to retry the same function. The library itself doesn't
//! automatically does retries.
//!
//! [avro-rs]: https://crates.io/crates/avro-rs

pub mod schema_registry;

use avro_rs::to_value;
use avro_rs::types::{Record, Value};
use avro_rs::{from_avro_datum, to_avro_datum, Schema};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use schema_registry::SRCError;
use schema_registry::{get_schema_by_id, get_schema_by_subject, get_subject, SubjectNameStrategy};
use serde::ser::Serialize;
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
///  # use mockito::{mock, server_address};
///  # use schema_registry_converter::Decoder;
///  # use avro_rs::types::Value;
///
/// let _m = mock("GET", "/schemas/ids/1")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
///     .create();
///
/// let mut decoder = Decoder::new(server_address().to_string());
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
    pub fn new(schema_registry_url: String) -> Decoder {
        let new_cache = Box::new(HashMap::new());
        Decoder {
            schema_registry_url,
            cache: Box::leak(new_cache),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    ///
    /// ```
    ///  # use mockito::{mock, server_address};
    ///  # use schema_registry_converter::Decoder;
    ///  # use schema_registry_converter::schema_registry::SRCError;
    ///  # use avro_rs::types::Value;
    ///
    /// let mut decoder = Decoder::new(server_address().to_string());
    /// let bytes = [0,0,0,0,2,6];
    ///
    /// let _m = mock("GET", "/schemas/ids/2")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    /// let heartbeat = decoder.decode(Some(&bytes));
    /// assert_eq!(heartbeat, Err(SRCError::new("Did not get a 200 response code but 404 instead", None, false).into_cache()));
    /// let _m = mock("GET", "/schemas/ids/2")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes));
    /// assert_eq!(heartbeat, Err(SRCError::new("Did not get a 200 response code but 404 instead", None, false).into_cache()));
    ///
    /// decoder.remove_errors_from_cache();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes));
    /// assert_eq!(heartbeat, Ok(Value::Record(vec!(("beat".to_string(), Value::Long(3))))))
    /// ```
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a mut
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    ///
    /// ```no_run
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
            Some(p) if p.len() > 4 && p[0] == 0 => self.deserialize(p),
            Some(p) => Ok(Value::Bytes(p.to_vec())),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize<'a>(&'a mut self, bytes: &'a [u8]) -> Result<Value, SRCError> {
        let mut buf = &bytes[1..5];
        let id = buf.read_u32::<BigEndian>().unwrap();
        let mut reader = Cursor::new(&bytes[5..]);
        let sr = &self.schema_registry_url;
        let schema = self
            .cache
            .entry(id)
            .or_insert_with(|| match get_schema_by_id(id, sr) {
                Ok(v) => Ok(v),
                Err(e) => Err(e.into_cache()),
            });
        match schema {
            Ok(v) => match from_avro_datum(&v, &mut reader, None) {
                Ok(v) => Ok(v),
                Err(e) => Err(SRCError::new(
                    "Could not transform bytes using schema",
                    Some(&e.to_string()),
                    false,
                )),
            },
            Err(e) => Err(e.clone()),
        }
    }
}

/// An encoder used to transform a Value object to bytes
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
///  # use mockito::{mock, server_address};
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
/// let mut encoder = Encoder::new(server_address().to_string());
///
/// let key_strategy = SubjectNameStrategy::TopicNameStrategy("heartbeat".into(), true);
/// let bytes = encoder.encode(vec!(("name", Value::String("Some name".to_owned()))), &key_strategy);
///
/// assert_eq!(bytes, Ok(vec!(0, 0, 0, 0, 4, 18, 83, 111, 109, 101, 32, 110, 97, 109, 101)));
///
/// let value_strategy = SubjectNameStrategy::TopicNameStrategy("heartbeat".into(), false);
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
    pub fn new(schema_registry_url: String) -> Encoder {
        let new_cache = Box::new(HashMap::new());
        Encoder {
            schema_registry_url,
            cache: Box::leak(new_cache),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    ///
    /// ```
    ///  # use mockito::{mock, server_address};
    ///  # use schema_registry_converter::Encoder;
    ///  # use schema_registry_converter::schema_registry::SubjectNameStrategy;
    ///  # use schema_registry_converter::schema_registry::SRCError;
    ///  # use avro_rs::types::Value;
    ///
    /// let mut encoder = Encoder::new(server_address().to_string());
    /// let strategy = SubjectNameStrategy::RecordNameStrategy("nl.openweb.data.Heartbeat".into());
    ///
    /// let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    ///
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    /// assert_eq!(bytes, Err(SRCError::new("Did not get a 200 response code but 404 instead", None, false).into_cache()));
    ///
    /// let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    /// assert_eq!(bytes, Err(SRCError::new("Did not get a 200 response code but 404 instead", None, false).into_cache()));
    ///
    /// encoder.remove_errors_from_cache();
    ///
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    /// assert_eq!(bytes, Ok(vec!(0,0,0,0,4,6)))
    /// ```
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Encodes a vector of values to bytes. The corrects values of the 'keys' depend on the schema
    /// being fetched at runtime. For example you might agree on a schema with a consuming party and
    /// /or upload a schema to the schema registry before starting the program. In the future an
    /// 'encode with schema' might be added which makes it easier to make sure the schema will
    /// become available in the correct way.
    ///
    /// ```
    ///  # use mockito::{mock, server_address};
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
    /// let mut encoder = Encoder::new(server_address().to_string());
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategy("heartbeat".into(), "nl.openweb.data.Heartbeat".into());
    /// let bytes = encoder.encode(vec!(("beat", Value::Long(3))), &strategy);
    ///
    /// assert_eq!(bytes, Ok(vec!(0,0,0,0,3,6)))
    /// ```
    pub fn encode(
        &mut self,
        values: Vec<(&'static str, Value)>,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let schema_and_id = self.get_schema_and_id(subject_name_strategy);
        match schema_and_id {
            Ok((schema, id)) => to_bytes(&schema, *id, values),
            Err(e) => Err(e.clone()),
        }
    }

    /// Encodes struct to bytes. The corrects values of the 'keys' depend on the schema being
    /// fetched at runtime. For example you might agree on a schema with a consuming party and
    /// /or upload a schema to the schema registry before starting the program. In the future an
    /// 'encode with schema' might be added which makes it easier to make sure the schema will
    /// become available in the correct way.
    ///
    /// ```
    ///  # use mockito::{mock, server_address};
    ///  # use schema_registry_converter::Encoder;
    ///  # use schema_registry_converter::schema_registry::SubjectNameStrategy;
    ///  # use serde::Serialize;
    ///  # use avro_rs::types::Value;
    ///
    /// let _m = mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    ///  #[derive(Serialize)]
    ///    struct Heartbeat {
    ///        beat: i64,
    ///    }
    ///
    /// let mut encoder = Encoder::new(server_address().to_string());
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategy("heartbeat".into(), "nl.openweb.data.Heartbeat".into());
    /// let bytes = encoder.encode_struct(Heartbeat{beat: 3}, &strategy);
    ///
    /// assert_eq!(bytes, Ok(vec!(0,0,0,0,3,6)))
    /// ```
    pub fn encode_struct(
        &mut self,
        item: impl Serialize,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let schema_and_id = self.get_schema_and_id(subject_name_strategy);
        match schema_and_id {
            Ok((schema, id)) => item_to_bytes(&schema, *id, item),
            Err(e) => Err(e.clone()),
        }
    }

    fn get_schema_and_id(
        &mut self,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> &mut Result<(Schema, u32), SRCError> {
        let schema_registry_url = &self.schema_registry_url;
        self.cache
            .entry(get_subject(subject_name_strategy))
            .or_insert_with(|| {
                match get_schema_by_subject(schema_registry_url, &subject_name_strategy) {
                    Ok(v) => Ok(v),
                    Err(e) => Err(e.into_cache()),
                }
            })
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
        None => {
            return Err(SRCError::new(
                "Could not create record from schema",
                None,
                false,
            ));
        }
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
        Err(e) => {
            return Err(SRCError::new(
                "Could not get avro bytes",
                Some(&e.to_string()),
                false,
            ));
        }
    }
    Ok(payload)
}

/// Using the schema with an item implementing serialize the item will be correctly deserialized
/// according to the avro specification.
fn item_to_bytes(schema: &Schema, id: u32, item: impl Serialize) -> Result<Vec<u8>, SRCError> {
    let record = match to_value(item) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::new(
                "Could not get avro record from struct",
                Some(&e.to_string()),
                false,
            ));
        }
    };
    let mut payload = vec![0u8];
    {
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, id);
        payload.extend_from_slice(&buf);
    }
    match to_avro_datum(schema, record) {
        Ok(v) => payload.extend_from_slice(v.as_slice()),
        Err(e) => {
            return Err(SRCError::new(
                "Could not get avro bytes",
                Some(&e.to_string()),
                false,
            ));
        }
    }
    Ok(payload)
}

#[test]
fn to_bytes_no_record() {
    let schema = Schema::Boolean;
    let result = to_bytes(&schema, 5, vec![("beat", Value::Long(3))]);
    assert_eq!(
        result,
        Err(SRCError::new(
            "Could not create record from schema",
            None,
            false,
        ))
    )
}

#[test]
fn to_bytes_no_tranfer_wrong() {
    let schema = Schema::parse_str(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#).unwrap();
    let result = to_bytes(&schema, 5, vec![("beat", Value::Long(3))]);
    assert_eq!(
        result,
        Err(SRCError::new(
            "Could not get avro bytes",
            Some("Decoding error: value does not match schema"),
            false,
        ))
    )
}

#[cfg(test)]
mod tests {
    use crate::schema_registry::{SRCError, SubjectNameStrategy, SuppliedSchema};
    use crate::Decoder;
    use crate::Encoder;
    use avro_rs::types::Value;
    use mockito::{mock, server_address};

    #[test]
    fn display_decoder() {
        let decoder = Decoder::new(server_address().to_string());
        assert_eq!(
            "Decoder { schema_registry_url: \"127.0.0.1:1234\", cache: {} }".to_owned(),
            format!("{:?}", decoder)
        )
    }

    #[test]
    fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let mut decoder = Decoder::new(server_address().to_string());
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Ok(Value::Record(vec![("beat".to_string(), Value::Long(3))]))
        )
    }

    #[test]
    fn test_decoder_magic_byte_not_present() {
        let mut decoder = Decoder::new(server_address().to_string());
        let heartbeat = decoder.decode(Some(&[1, 0, 0, 0, 1, 6]));

        assert_eq!(heartbeat, Ok(Value::Bytes(vec![1, 0, 0, 0, 1, 6])))
    }

    #[test]
    fn test_decoder_not_enough_bytes() {
        let mut decoder = Decoder::new(server_address().to_string());
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0]));

        assert_eq!(heartbeat, Ok(Value::Bytes(vec![0, 0, 0, 0])))
    }

    #[test]
    fn test_decoder_wrong_data() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let mut decoder = Decoder::new(server_address().to_string());
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "Could not transform bytes using schema",
                Some("failed to fill whole buffer"),
                false,
            ))
        )
    }

    #[test]
    fn test_decoder_no_json_response() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let mut decoder = Decoder::new(server_address().to_string());
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new("Invalid json string", Some("JSON error"), false).into_cache())
        )
    }

    #[test]
    fn test_decoder_schema_registry_unavailable() {
        let mut decoder = Decoder::new("bogus".to_string());
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 10, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "error performing get to schema registry",
                Some("Couldn\'t resolve host name"),
                true,
            )
            .into_cache())
        )
    }

    #[test]
    fn test_decoder_default_no_schema_in_response() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"no-schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let mut decoder = Decoder::new(server_address().to_string());
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new("Could not get raw schema from response", None, false).into_cache())
        )
    }

    #[test]
    fn test_decoder_default_wrong_schema_in_response() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\"}"}"#)
            .create();

        let mut decoder = Decoder::new(server_address().to_string());
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "Could not parse schema",
                Some("Failed to parse schema: No `fields` in record"),
                false,
            )
            .into_cache())
        )
    }

    #[test]
    fn test_decoder_cache() {
        let mut decoder = Decoder::new(server_address().to_string());
        let bytes = [0, 0, 0, 0, 2, 6];

        let _m = mock("GET", "/schemas/ids/2")
            .with_status(404)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
            .create();
        let heartbeat = decoder.decode(Some(&bytes));
        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "Did not get a 200 response code but 404 instead",
                None,
                false,
            )
            .into_cache())
        );
        let _m = mock("GET", "/schemas/ids/2")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let heartbeat = decoder.decode(Some(&bytes));
        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "Did not get a 200 response code but 404 instead",
                None,
                false,
            )
            .into_cache())
        );

        decoder.remove_errors_from_cache();

        let heartbeat = decoder.decode(Some(&bytes));
        assert_eq!(
            heartbeat,
            Ok(Value::Record(vec![("beat".to_string(), Value::Long(3))]))
        )
    }

    #[test]
    fn display_encode() {
        let decoder = Encoder::new(server_address().to_string());
        assert_eq!(
            "Encoder { schema_registry_url: \"127.0.0.1:1234\", cache: {} }".to_owned(),
            format!("{:?}", decoder)
        )
    }

    #[test]
    fn test_encode_key_and_value() {
        let _m = mock("GET", "/subjects/heartbeat-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let _n = mock("GET", "/subjects/heartbeat-key/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Name\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"avro.java.string\":\"String\"}]}"}"#)
            .create();

        let mut encoder = Encoder::new(server_address().to_string());

        let key_strategy = SubjectNameStrategy::TopicNameStrategy("heartbeat".into(), true);
        let bytes = encoder.encode(
            vec![("name", Value::String("Some name".to_owned()))],
            &key_strategy,
        );

        assert_eq!(
            bytes,
            Ok(vec![
                0, 0, 0, 0, 4, 18, 83, 111, 109, 101, 32, 110, 97, 109, 101,
            ])
        );

        let value_strategy = SubjectNameStrategy::TopicNameStrategy("heartbeat".into(), false);
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &value_strategy);

        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 3, 6]))
    }

    #[test]
    fn test_using_record_name() {
        let _m = mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let mut encoder = Encoder::new(server_address().to_string());
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            "heartbeat".into(),
            "nl.openweb.data.Heartbeat".into(),
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 3, 6]))
    }

    #[test]
    fn test_encoder_no_id_in_response() {
        let _m = mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"no-id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let mut encoder = Encoder::new(server_address().to_string());
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            "heartbeat".into(),
            "nl.openweb.data.Heartbeat".into(),
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        assert_eq!(
            bytes,
            Err(SRCError::new("Could not get id from response", None, false).into_cache())
        )
    }

    #[test]
    fn test_encoder_schema_registry_unavailable() {
        let mut encoder = Encoder::new("bogus".into());
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            "heartbeat".into(),
            "nl.openweb.data.Balance".into(),
        );
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        assert_eq!(
            result,
            Err(SRCError::new(
                "error performing get to schema registry",
                Some("Couldn\'t resolve host name"),
                true,
            )
            .into_cache())
        )
    }

    #[test]
    fn test_encoder_schema_registry_unavailable_with_record() {
        let mut encoder = Encoder::new("bogus".into());
        let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Balance","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into());
        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(heartbeat_schema);
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        assert_eq!(
            result,
            Err(SRCError::new(
                "error performing post to schema registry",
                Some("Couldn\'t resolve host name"),
                true,
            )
            .into_cache())
        )
    }

    #[test]
    fn test_encode_cache() {
        let mut encoder = Encoder::new(server_address().to_string());
        let strategy = SubjectNameStrategy::RecordNameStrategy("nl.openweb.data.Heartbeat".into());

        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(404)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
            .create();

        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            bytes,
            Err(SRCError::new(
                "Did not get a 200 response code but 404 instead",
                None,
                false,
            )
            .into_cache())
        );

        let _n = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            bytes,
            Err(SRCError::new(
                "Did not get a 200 response code but 404 instead",
                None,
                false,
            )
            .into_cache())
        );

        encoder.remove_errors_from_cache();

        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 4, 6]))
    }

    #[test]
    fn test_encode_key_and_value_supplied_record() {
        let _n = mock("POST", "/subjects/heartbeat-key/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":3}"#)
            .create();

        let _m = mock("POST", "/subjects/heartbeat-value/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":4}"#)
            .create();

        let mut encoder = Encoder::new(server_address().to_string());

        let name_schema = SuppliedSchema::new(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#.into());
        let key_strategy =
            SubjectNameStrategy::TopicNameStrategyWithSchema("heartbeat".into(), true, name_schema);
        let bytes = encoder.encode(
            vec![("name", Value::String("Some name".to_owned()))],
            &key_strategy,
        );
        assert_eq!(
            bytes,
            Ok(vec![
                0, 0, 0, 0, 3, 18, 83, 111, 109, 101, 32, 110, 97, 109, 101,
            ])
        );
        let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into());
        let value_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            "heartbeat".into(),
            false,
            heartbeat_schema,
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &value_strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 4, 6]))
    }

    #[test]
    fn test_encode_record_name_strategy_supplied_record() {
        let _n = mock("POST", "/subjects/nl.openweb.data.Heartbeat/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":11}"#)
            .create();

        let mut encoder = Encoder::new(server_address().to_string());

        let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into());
        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(heartbeat_schema);
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 11, 6]))
    }

    #[test]
    fn test_encode_record_name_strategy_supplied_record_wrong_response() {
        let _n = mock("POST", "/subjects/nl.openweb.data.Heartbeat/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"no-id":11}"#)
            .create();

        let mut encoder = Encoder::new(server_address().to_string());

        let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into());
        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(heartbeat_schema);
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            bytes,
            Err(SRCError::new("Could not get id from response", None, false).into_cache())
        )
    }

    #[test]
    fn test_encode_topic_record_name_strategy_supplied_record() {
        let _n = mock("POST", "/subjects/hb-nl.openweb.data.Heartbeat/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":23}"#)
            .create();

        let mut encoder = Encoder::new(server_address().to_string());

        let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into());
        let strategy =
            SubjectNameStrategy::TopicRecordNameStrategyWithSchema("hb".into(), heartbeat_schema);
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 23, 6]))
    }

    #[test]
    fn test_encode_topic_record_name_strategy_schema_registry_not_available() {
        let mut encoder = Encoder::new(server_address().to_string());

        let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into());
        let strategy =
            SubjectNameStrategy::TopicRecordNameStrategyWithSchema("hb".into(), heartbeat_schema);
        let error = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            error,
            Err(SRCError::new(
                "Did not get a 200 response code but 501 instead",
                None,
                false,
            )
            .into_cache())
        )
    }
}
