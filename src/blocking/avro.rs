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

use crate::blocking::schema_registry::{
    get_bytes_result, get_payload, get_referenced_schema, get_schema_by_id_and_type,
    get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::schema_registry_common::{
    get_subject, BytesResult, RegisteredReference, RegisteredSchema, SchemaType,
    SubjectNameStrategy, SuppliedSchema,
};
use avro_rs::schema::Name;
use avro_rs::to_value;
use avro_rs::types::{Record, ToAvro, Value};
use avro_rs::{from_avro_datum, to_avro_datum, Schema};
use serde::ser::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::hash_map::{Entry, RandomState};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;

/// Because we need both the resulting schema, as have a way of posting the schema as json, we use
/// this struct so we keep them both together.
#[derive(Clone, Debug)]
struct AvroSchema {
    id: u32,
    raw: String,
    parsed: Schema,
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
/// use mockito::{mock, server_address};
/// use avro_rs::types::Value;
/// use schema_registry_converter::blocking::schema_registry::SrSettings;
/// use schema_registry_converter::blocking::avro::AvroDecoder;
///
/// let _m = mock("GET", "/schemas/ids/1")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
///     .create();
///
/// let sr_settings = SrSettings::new(format!("http://{}", server_address()));
/// let mut decoder = AvroDecoder::new(sr_settings);
/// let heartbeat = decoder.decode(Some(&[0,0,0,0,1,6])).unwrap().value;
///
/// assert_eq!(heartbeat, Value::Record(vec![("beat".to_string(), Value::Long(3))]))
/// ```
#[derive(Debug)]
pub struct AvroDecoder {
    sr_settings: SrSettings,
    cache: HashMap<u32, Result<AvroSchema, SRCError>, RandomState>,
}

impl AvroDecoder {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. It's possible for recoverable errors to stay in the cash, when a result
    /// comes back as an error you can use remove_errors_from_cache to clean the cache, keeping the
    /// correctly fetched schema's
    pub fn new(sr_settings: SrSettings) -> AvroDecoder {
        AvroDecoder {
            sr_settings,
            cache: HashMap::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    ///
    /// ```
    /// use mockito::{mock, server_address};
    /// use avro_rs::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroDecoder;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    /// use schema_registry_converter::error::SRCError;
    ///
    /// let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    /// let mut decoder = AvroDecoder::new(sr_settings);
    /// let bytes = [0,0,0,0,2,6];
    ///
    /// let _m = mock("GET", "/schemas/ids/2")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes));
    ///
    /// assert_eq!(heartbeat, Err(SRCError::new("Could not get raw schema from response", None, false).into_cache()));
    ///
    /// let _m = mock("GET", "/schemas/ids/2")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes));
    /// assert_eq!(heartbeat, Err(SRCError::new("Could not get raw schema from response", None, false).into_cache()));
    ///
    /// decoder.remove_errors_from_cache();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes)).unwrap().value;
    /// assert_eq!(heartbeat, Value::Record(vec![("beat".to_string(), Value::Long(3))]))
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
    /// use rdkafka::message::{Message, BorrowedMessage};
    /// use avro_rs::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroDecoder;
    /// fn get_value<'a>(
    ///     msg: &'a BorrowedMessage,
    ///     decoder: &'a mut AvroDecoder,
    /// ) -> Value{
    ///     match decoder.decode(msg.payload()){
    ///         Ok(r) => r.value,
    ///         Err(e) => panic!("Error getting value: {}", e),
    ///     }
    /// }
    /// ```
    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<DecodeResult, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(DecodeResult {
                name: None,
                value: Value::Null,
            }),
            BytesResult::Valid(id, bytes) => self.deserialize(id, &bytes),
            BytesResult::Invalid(bytes) => Err(SRCError::non_retryable_without_cause(&*format!(
                "Invalid bytes {:?}",
                bytes
            ))),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&mut self, id: u32, bytes: &[u8]) -> Result<DecodeResult, SRCError> {
        let schema = self.get_schema(id);
        let mut reader = Cursor::new(bytes);
        match schema {
            Ok(s) => match from_avro_datum(&s.parsed, &mut reader, None) {
                Ok(v) => Ok(DecodeResult {
                    name: get_name(&s.parsed),
                    value: v,
                }),
                Err(e) => Err(SRCError::non_retryable_with_cause(
                    e,
                    "Could not transform bytes using schema",
                )),
            },
            Err(e) => Err(Clone::clone(e)),
        }
    }

    fn get_schema(&mut self, id: u32) -> &Result<AvroSchema, SRCError> {
        let sr_settings = &self.sr_settings;
        match self.cache.entry(id) {
            Entry::Occupied(e) => &*e.into_mut(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_id_and_type(id, sr_settings, SchemaType::Avro) {
                    Ok(registered_schema) => to_avro_schema(sr_settings, registered_schema),
                    Err(e) => Err(e.into_cache()),
                };
                &*e.insert(v)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DecodeResult {
    pub name: Option<Name>,
    pub value: Value,
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
/// use mockito::{mock, server_address};
/// use avro_rs::types::Value;
/// use schema_registry_converter::blocking::avro::AvroEncoder;
/// use schema_registry_converter::blocking::schema_registry::SrSettings;
/// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
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
/// let sr_settings = SrSettings::new(format!("http://{}", server_address()));
/// let mut encoder = AvroEncoder::new(sr_settings);
///
/// let key_strategy = SubjectNameStrategy::TopicNameStrategy(String::from("heartbeat"), true);
/// let bytes = encoder.encode(vec![("name", Value::String("Some name".to_owned()))], &key_strategy);
///
/// assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 4, 18, 83, 111, 109, 101, 32, 110, 97, 109, 101]));
///
/// let value_strategy = SubjectNameStrategy::TopicNameStrategy(String::from("heartbeat"), false);
/// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &value_strategy);
///
/// assert_eq!(bytes, Ok(vec![0,0,0,0,3,6]))
/// ```
#[derive(Debug)]
pub struct AvroEncoder {
    sr_settings: SrSettings,
    cache: HashMap<String, Result<AvroSchema, SRCError>, RandomState>,
}

impl AvroEncoder {
    /// Creates a new encoder which will use the supplied url to fetch the schema's. The schema's
    /// need to be retrieved together with the id, in order for a consumer to decode the bytes.
    /// For the encoding several strategies are available in the java client, all three of them are
    /// supported. The schema's does have to be present in the schema registry already. This is
    /// unlike the Java client with wich it's possible to update/upload the schema when it's not
    /// present yet. While it may be added to this library, it's also not hard to do it separately.
    /// New schema's can set by doing a post at /subjects/{subject}/versions.
    ///
    /// ```
    /// use mockito::{mock, server_address};
    /// use avro_rs::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    /// use schema_registry_converter::schema_registry_common::{SubjectNameStrategy, SchemaType, SuppliedSchema};
    ///
    /// # let _n = mock("POST", "/subjects/hb-nl.openweb.data.Heartbeat/versions")
    /// #    .with_status(200)
    /// #    .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    /// #    .with_body(r#"{"id":23}"#)
    /// #    .create();
    ///
    /// let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    /// let mut encoder = AvroEncoder::new(sr_settings);
    ///
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(String::from("hb"), Box::from(SuppliedSchema {
    ///                 name: Some(String::from("nl.openweb.data.Heartbeat")),
    ///                 schema_type: SchemaType::Avro,
    ///                 schema: String::from(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#),
    ///                 references: vec![],
    ///             }));
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    /// assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 23, 6]))
    /// ```
    pub fn new(sr_settings: SrSettings) -> AvroEncoder {
        AvroEncoder {
            sr_settings,
            cache: HashMap::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    ///
    /// ```
    /// use mockito::{mock, server_address};
    /// use avro_rs::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    /// use schema_registry_converter::error::SRCError;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    ///
    /// let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    /// let mut encoder = AvroEncoder::new(sr_settings);
    /// let strategy = SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
    ///
    /// let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    ///
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    /// assert_eq!(bytes, Err(SRCError::new("Could not get id from response", None, false).into_cache()));
    ///
    /// let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    /// assert_eq!(bytes, Err(SRCError::new("Could not get id from response", None, false).into_cache()));
    ///
    /// encoder.remove_errors_from_cache();
    ///
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    /// assert_eq!(bytes, Ok(vec![0,0,0,0,4,6]))
    /// ```
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Encodes a vector of values to bytes. The correct values of the 'keys' depend on the schema
    /// being fetched at runtime, or the one supplied with the SubjectNameStrategy.
    ///
    /// The function get_supplied_schema might be used to easily provide the schema in the correct
    /// form.
    /// ```
    /// use mockito::{mock, server_address};
    /// use avro_rs::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    ///
    /// let _m = mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    /// let mut encoder = AvroEncoder::new(sr_settings);
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategy(String::from("heartbeat"), String::from("nl.openweb.data.Heartbeat"));
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    ///
    /// assert_eq!(bytes, Ok(vec![0,0,0,0,3,6]))
    /// ```
    pub fn encode(
        &mut self,
        values: Vec<(&'static str, Value)>,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = get_subject(subject_name_strategy)?;
        match self.get_schema_and_id(key, subject_name_strategy) {
            Ok(avro_schema) => values_to_bytes(&avro_schema, values),
            Err(e) => Err(Clone::clone(e)),
        }
    }

    /// Encodes a struct or a primitive value to bytes. The schema used for the encoding will be
    /// retrieved from the schema registry, or it will use the one supplied with the
    /// SubjectNameStrategy.
    ///
    /// The function get_supplied_schema might be used to easily provide the schema in the correct
    /// form.
    /// ```
    /// use mockito::{mock, server_address};
    /// use serde::Serialize;
    /// use avro_rs::types::Value;
    /// use avro_rs::Schema;
    /// use schema_registry_converter::blocking::avro::{AvroEncoder, get_supplied_schema};
    /// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
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
    /// let sr_settings = SrSettings::new(format!("http://{}", server_address()));
    /// let mut encoder = AvroEncoder::new(sr_settings);
    /// let existing_schema_strategy = SubjectNameStrategy::TopicRecordNameStrategy(String::from("heartbeat"), String::from("nl.openweb.data.Heartbeat"));
    /// let bytes = encoder.encode_struct(Heartbeat{beat: 3}, &existing_schema_strategy);
    ///
    /// assert_eq!(bytes, Ok(vec![0,0,0,0,3,6]));
    ///
    ///  let _n = mock("POST", "/subjects/heartbeat-key/versions")
    ///      .with_status(200)
    ///      .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///      .with_body(r#"{"id":4}"#)
    ///      .create();
    ///
    /// let primitive_schema_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(String::from("heartbeat"), true, get_supplied_schema(&Schema::String));
    /// let bytes = encoder.encode_struct("key-value", &primitive_schema_strategy);
    ///
    /// assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 4, 18, 107, 101, 121, 45, 118, 97, 108, 117, 101]));
    /// ```
    pub fn encode_struct(
        &mut self,
        item: impl Serialize,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = get_subject(subject_name_strategy)?;
        match self.get_schema_and_id(key, subject_name_strategy) {
            Ok(avro_schema) => item_to_bytes(&avro_schema, item),
            Err(e) => Err(Clone::clone(e)),
        }
    }

    fn get_schema_and_id(
        &mut self,
        key: String,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> &Result<AvroSchema, SRCError> {
        let sr_settings = &self.sr_settings;
        match self.cache.entry(key) {
            Entry::Occupied(e) => &*e.into_mut(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_subject(sr_settings, &subject_name_strategy) {
                    Ok(registered_schema) => to_avro_schema(sr_settings, registered_schema),
                    Err(e) => Err(e.into_cache()),
                };
                &*e.insert(v)
            }
        }
    }
}

fn might_replace(val: JsonValue, child: &JsonValue, replace_values: &HashSet<String>) -> JsonValue {
    match val {
        JsonValue::Object(v) => replace_in_map(v, child, replace_values),
        JsonValue::Array(v) => replace_in_array(&*v, child, replace_values),
        JsonValue::String(s) if replace_values.contains(&*s) => child.clone(),
        p => p,
    }
}

fn replace_in_array(
    parent_array: &[JsonValue],
    child: &JsonValue,
    replace_values: &HashSet<String>,
) -> JsonValue {
    JsonValue::Array(
        parent_array
            .iter()
            .map(|v| might_replace(v.clone(), child, replace_values))
            .collect(),
    )
}

fn replace_in_map(
    parent_map: JsonMap<String, JsonValue>,
    child: &JsonValue,
    replace_values: &HashSet<String>,
) -> JsonValue {
    JsonValue::Object(
        parent_map
            .iter()
            .map(|e| {
                (
                    e.0.clone(),
                    might_replace(e.1.clone(), child, replace_values),
                )
            })
            .collect(),
    )
}

fn replace_reference(parent: JsonValue, child: JsonValue) -> JsonValue {
    let (name, namespace) = match &child {
        JsonValue::Object(v) => (v["name"].as_str(), v["namespace"].as_str()),
        _ => return parent,
    };
    let mut replace_values: HashSet<String> = HashSet::new();
    match name {
        Some(v) => match namespace {
            Some(u) => {
                replace_values.insert(format!(".{}.{}", u, v));
                if parent["namespace"].as_str() == namespace {
                    replace_values.insert(String::from(v))
                } else {
                    true
                }
            }
            None => replace_values.insert(String::from(v)),
        },
        None => return parent,
    };
    match parent {
        JsonValue::Object(v) => replace_in_map(v, &child, &replace_values),
        JsonValue::Array(v) => replace_in_array(&*v, &child, &replace_values),
        p => p,
    }
}

fn add_references(
    sr_settings: &SrSettings,
    json_value: JsonValue,
    references: &[RegisteredReference],
) -> Result<JsonValue, SRCError> {
    let mut new_value = json_value;
    for r in references.iter() {
        let registered_schema = match get_referenced_schema(sr_settings, r) {
            Ok(v) => v,
            Err(e) => {
                return Err(SRCError::non_retryable_with_cause(
                    e,
                    &*format!("problem with reference {:?}", r),
                ));
            }
        };
        let child: JsonValue = match serde_json::from_str(&*registered_schema.schema) {
            Ok(v) => v,
            Err(e) => {
                return Err(SRCError::non_retryable_with_cause(
                    e,
                    &*format!("problem serializing {}", registered_schema.schema),
                ));
            }
        };
        new_value = replace_reference(new_value, child);
        new_value = match add_references(sr_settings, new_value, &registered_schema.references) {
            Ok(v) => v,
            Err(e) => return Err(e),
        }
    }
    Ok(new_value)
}

fn to_avro_schema(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
) -> Result<AvroSchema, SRCError> {
    match registered_schema.schema_type {
        SchemaType::Avro => (),
        t => {
            return Err(SRCError::non_retryable_without_cause(&*format!(
                "type {:?}, is not supported",
                t
            )));
        }
    }
    let main_schema = match serde_json::from_str(&*registered_schema.schema) {
        Ok(v) => match add_references(sr_settings, v, registered_schema.references.as_slice()) {
            Ok(u) => u,
            Err(e) => return Err(e),
        },
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                "failed to parse Avro schema",
            ));
        }
    };
    match Schema::parse(&main_schema) {
        Ok(parsed) => Ok(AvroSchema {
            id: registered_schema.id,
            raw: registered_schema.schema,
            parsed,
        }),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            &*format!(
                "Supplied raw value {:?} cant be turned into a Schema",
                registered_schema.schema
            ),
        )),
    }
}

fn to_bytes<T: ToAvro>(avro_schema: &AvroSchema, record: T) -> Result<Vec<u8>, SRCError> {
    match to_avro_datum(&avro_schema.parsed, record) {
        Ok(v) => Ok(get_payload(avro_schema.id, v)),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "Could not get Avro bytes",
        )),
    }
}

/// Using the schema with a vector of values the values will be correctly deserialized according to
/// the avro specification.
fn values_to_bytes(
    avro_schema: &AvroSchema,
    values: Vec<(&'static str, Value)>,
) -> Result<Vec<u8>, SRCError> {
    let mut record = match Record::new(&avro_schema.parsed) {
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
    to_bytes(avro_schema, record)
}

/// Using the schema with an item implementing serialize the item will be correctly deserialized
/// according to the avro specification.
fn item_to_bytes(avro_schema: &AvroSchema, item: impl Serialize) -> Result<Vec<u8>, SRCError> {
    match to_value(item)
        .map_err(|e| SRCError::non_retryable_with_cause(e, "Could not transform to avro_rs value"))
        .map(|r| r.resolve(&avro_schema.parsed))
    {
        Ok(Ok(v)) => to_bytes(avro_schema, v),
        Ok(Err(e)) => Err(SRCError::non_retryable_with_cause(e, "Failed to resolve")),
        Err(e) => Err(e),
    }
}

fn get_name(schema: &Schema) -> Option<Name> {
    match schema {
        Schema::Record { name: n, .. } => Some(n.clone()),
        _ => None,
    }
}

pub fn get_supplied_schema(schema: &Schema) -> Box<SuppliedSchema> {
    let name = match get_name(schema) {
        None => None,
        Some(n) => match n.namespace {
            None => Some(n.name),
            Some(ns) => Some(format!("{}.{}", ns, n.name)),
        },
    };
    Box::from(SuppliedSchema {
        name,
        schema_type: SchemaType::Avro,
        schema: schema.canonical_form(),
        references: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use avro_rs::from_value;
    use mockito::{mock, server_address};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    struct Heartbeat {
        beat: i64,
    }

    #[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Deserialize, Serialize)]
    pub enum Atype {
        #[serde(rename = "AUTO")]
        Auto,
        #[serde(rename = "MANUAL")]
        Manual,
    }

    impl Default for Atype {
        fn default() -> Self {
            Atype::Auto
        }
    }

    pub type Uuid = [u8; 16];

    #[serde(default)]
    #[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
    pub struct ConfirmAccountCreation {
        pub id: Uuid,
        pub a_type: Atype,
    }

    impl Default for ConfirmAccountCreation {
        fn default() -> ConfirmAccountCreation {
            ConfirmAccountCreation {
                id: Uuid::default(),
                a_type: Atype::Auto,
            }
        }
    }

    #[test]
    fn to_bytes_no_record() {
        let schema = AvroSchema {
            id: 5,
            raw: "".to_string(),
            parsed: Schema::Boolean,
        };
        let result = values_to_bytes(&schema, vec![("beat", Value::Long(3))]);
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
    fn to_bytes_no_transfer_wrong() {
        let schema = AvroSchema {
            id: 5,
            raw: String::from(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#),
            parsed: Schema::parse_str(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#).unwrap(),
        };
        let result = values_to_bytes(&schema, vec![("beat", Value::Long(3))]);
        assert_eq!(
            result,
            Err(SRCError::new(
                "Could not get Avro bytes",
                Some(String::from(
                    "Validation error: value does not match schema"
                )),
                false,
            ))
        )
    }

    #[test]
    fn display_decoder() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = AvroDecoder::new(sr_settings);
        assert_eq!(
            "AvroDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {} }"
                .to_owned(),
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6])).unwrap().value;

        assert_eq!(
            heartbeat,
            Value::Record(vec![("beat".to_string(), Value::Long(3))])
        );

        let item = match from_value::<Heartbeat>(&heartbeat) {
            Ok(h) => h,
            Err(_) => unreachable!(),
        };
        assert_eq!(item.beat, 3i64);
    }

    #[test]
    fn test_decoder_with_name() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));
        let item = match heartbeat {
            Ok(r) => {
                let name = r.name.unwrap();
                assert_eq!(name.name.as_str(), "Heartbeat");
                assert_eq!(name.namespace.unwrap().as_str(), "nl.openweb.data");
                from_value::<Heartbeat>(&r.value).unwrap()
            }
            _ => panic!(),
        };
        assert_eq!(item.beat, 3i64);
    }

    #[test]
    fn test_decoder_no_bytes() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(None).unwrap().value;

        assert_eq!(heartbeat, Value::Null)
    }

    #[test]
    fn test_decoder_with_name_no_bytes() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(None).unwrap();

        assert_eq!(
            heartbeat,
            DecodeResult {
                name: None,
                value: Value::Null
            }
        )
    }

    #[test]
    fn test_decoder_magic_byte_not_present() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[1, 0, 0, 0, 1, 6]));

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "Invalid bytes [1, 0, 0, 0, 1, 6]"
            ))
        )
    }

    #[test]
    fn test_decoder_with_name_magic_byte_not_present() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[1, 0, 0, 0, 1, 6]));

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "Invalid bytes [1, 0, 0, 0, 1, 6]"
            ))
        )
    }

    #[test]
    fn test_decoder_not_enough_bytes() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[0, 0, 0, 0]));

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "Invalid bytes [0, 0, 0, 0]"
            ))
        )
    }

    #[test]
    fn test_decoder_wrong_data() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "Could not transform bytes using schema",
                Some(String::from("failed to fill whole buffer")),
                false,
            ))
        )
    }

    #[test]
    fn test_decoder_with_name_wrong_data() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "Could not transform bytes using schema",
                Some(String::from("failed to fill whole buffer")),
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "could not parse to RawRegisteredSchema, schema might not exist on this schema registry, the http call failed, cause will give more information",
                Some(String::from(
                    "error decoding response body: expected `:` at line 1 column 130"
                )),
                false,
            )
            .into_cache())
        )
    }

    #[test]
    fn test_decoder_with_name_no_json_response() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "could not parse to RawRegisteredSchema, schema might not exist on this schema registry, the http call failed, cause will give more information",
                Some(String::from(
                    "error decoding response body: expected `:` at line 1 column 130"
                )),
                false,
            )
            .into_cache())
        )
    }

    #[test]
    fn test_decoder_schema_registry_unavailable() {
        let sr_settings = SrSettings::new(String::from("http://bogus".to_string()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[0, 0, 0, 10, 1, 6]));

        match result {
            Err(e) => assert_eq!(e.error, "http call to schema registry failed"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_decoder_default_no_schema_in_response() {
        let _m = mock("GET", "/schemas/ids/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"no-schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new(
                "Supplied raw value \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Heartbeat\\\",\\\"namespace\\\":\\\"nl.openweb.data\\\"}\" cant be turned into a Schema",
                Some(String::from(
                    "Failed to parse schema: No `fields` in record"
                )),
                false,
            ))
        )
    }

    #[test]
    fn test_decoder_fixed_with_enum() {
        let _m = mock("GET", "/schemas/ids/6")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"ConfirmAccountCreation\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"fixed\",\"name\":\"Uuid\",\"size\":16}},{\"name\":\"a_type\",\"type\":{\"type\":\"enum\",\"name\":\"Atype\",\"symbols\":[\"AUTO\",\"MANUAL\"]}}]}"}"#)
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let cac = decoder
            .decode(Some(&[
                0, 0, 0, 0, 6, 204, 240, 237, 74, 227, 188, 75, 46, 183, 163, 122, 214, 178, 72,
                118, 162, 2,
            ]))
            .unwrap()
            .value;

        assert_eq!(
            cac,
            Value::Record(vec![
                (
                    "id".to_string(),
                    Value::Fixed(
                        16,
                        vec![
                            204, 240, 237, 74, 227, 188, 75, 46, 183, 163, 122, 214, 178, 72, 118,
                            162
                        ],
                    )
                ),
                ("a_type".to_string(), Value::Enum(1, "MANUAL".to_string()))
            ])
        );
    }

    #[test]
    fn test_decoder_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let bytes = [0, 0, 0, 0, 2, 6];

        let _m = mock("GET", "/schemas/ids/2")
            .with_status(404)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
            .create();
        let heartbeat = decoder.decode(Some(&bytes));
        assert_eq!(
            heartbeat,
            Err(SRCError::new("Could not get raw schema from response", None, false).into_cache())
        );
        let _m = mock("GET", "/schemas/ids/2")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let heartbeat = decoder.decode(Some(&bytes));
        assert_eq!(
            heartbeat,
            Err(SRCError::new("Could not get raw schema from response", None, false).into_cache())
        );

        decoder.remove_errors_from_cache();

        let heartbeat = decoder.decode(Some(&bytes)).unwrap().value;
        assert_eq!(
            heartbeat,
            Value::Record(vec![("beat".to_string(), Value::Long(3))])
        )
    }

    #[test]
    fn display_encode() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let encoder = AvroEncoder::new(sr_settings);
        assert_eq!(
            "AvroEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {} }"
                .to_owned(),
            format!("{:?}", encoder)
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let key_strategy = SubjectNameStrategy::TopicNameStrategy(String::from("heartbeat"), true);
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

        let value_strategy =
            SubjectNameStrategy::TopicNameStrategy(String::from("heartbeat"), false);
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            String::from("heartbeat"),
            String::from("nl.openweb.data.Heartbeat"),
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            String::from("heartbeat"),
            String::from("nl.openweb.data.Heartbeat"),
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        assert_eq!(
            bytes,
            Err(SRCError::new("Could not get id from response", None, false).into_cache())
        )
    }

    #[test]
    fn test_encoder_schema_registry_unavailable() {
        let sr_settings = SrSettings::new(String::from("http://bogus"));
        let mut encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            String::from("heartbeat"),
            String::from("nl.openweb.data.Balance"),
        );
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        match result {
            Err(e) => assert_eq!(e.error, "http call to schema registry failed"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_encoder_unknown_protocol() {
        let sr_settings = SrSettings::new(String::from("hxxx://bogus"));
        let mut encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            String::from("heartbeat"),
            String::from("nl.openweb.data.Balance"),
        );
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        assert_eq!(
            result,
            Err(SRCError::new(
                "http call to schema registry failed",
                Some(String::from("builder error for url (hxxx://bogus/subjects/heartbeat-nl.openweb.data.Balance/versions/latest): URL scheme is not allowed")),
                true,
            )
                .into_cache())
        )
    }

    #[test]
    fn test_encoder_schema_registry_unavailable_with_record() {
        let sr_settings = SrSettings::new(String::from("http://bogus"));
        let mut encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(Box::from(
            SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Balance")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Balance","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
            },
        ));
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        match result {
            Err(e) => assert_eq!(e.error, "http call to schema registry failed"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_encode_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let _m = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(404)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
            .create();

        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            bytes,
            Err(SRCError::new("Could not get id from response", None, false).into_cache())
        );

        let _n = mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            bytes,
            Err(
                SRCError::non_retryable_without_cause("Could not get id from response")
                    .into_cache()
            )
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let key_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            String::from("heartbeat"),
            true,
            Box::from(SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Name")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#,
                ),
                references: vec![],
            }),
        );
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
        let value_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            String::from("heartbeat"),
            false,
            Box::from(SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
            }),
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(Box::from(
            SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
            },
        ));
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

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(Box::from(
            SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
            },
        ));
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            result,
            Err(
                SRCError::non_retryable_without_cause("Could not get id from response for PostNew(\"nl.openweb.data.Heartbeat\", \"{\\\"schema\\\":\\\"{\\\\\\\"type\\\\\\\":\\\\\\\"record\\\\\\\",\\\\\\\"name\\\\\\\":\\\\\\\"Heartbeat\\\\\\\",\\\\\\\"namespace\\\\\\\":\\\\\\\"nl.openweb.data\\\\\\\",\\\\\\\"fields\\\\\\\":[{\\\\\\\"name\\\\\\\":\\\\\\\"beat\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"long\\\\\\\"}]}\\\",\\\"schemaType\\\":\\\"AVRO\\\"}\")")
                    .into_cache()
            )
        )
    }

    #[test]
    fn test_encode_topic_record_name_strategy_supplied_record() {
        let _n = mock("POST", "/subjects/hb-nl.openweb.data.Heartbeat/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":23}"#)
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
            String::from("hb"),
            Box::from(SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
            }),
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 23, 6]))
    }

    #[test]
    fn test_encode_topic_record_name_strategy_schema_registry_not_available() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
            String::from("hb"),
            Box::from(SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
            }),
        );
        let error = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(
            error,
            Err(SRCError::new(
                "could not parse to RawRegisteredSchema, schema might not exist on this schema registry, the http call failed, cause will give more information",
                Some(String::from(
                    "error decoding response body: EOF while parsing a value at line 1 column 0"
                )),
                false,
            )
            .into_cache())
        )
    }

    #[test]
    fn item_to_bytes_no_tranfer_wrong() {
        let schema = AvroSchema {
            id: 5,
            raw: String::from(
                r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#,
            ),
            parsed: Schema::parse_str(
                r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#,
            ).unwrap(),
        };
        let result = crate::blocking::avro::item_to_bytes(&schema, Heartbeat { beat: 3 });
        assert_eq!(
            result,
            Err(SRCError::new(
                "Failed to resolve",
                Some(String::from(
                    "Schema resoulution error: missing field name in record"
                )),
                false,
            ))
        )
    }

    #[test]
    fn item_to_bytes_still_broken() {
        let schema = AvroSchema {
            id: 6,
            raw: String::from(
                r#"{"type":"record","name":"ConfirmAccountCreation","namespace":"nl.openweb.data","fields":[{"name":"id","type":{"type":"fixed","name":"Uuid","size":16}},{"name":"a_type","type":{"type":"enum","name":"Atype","symbols":["AUTO","MANUAL"]}}]}"#,
            ),
            parsed: Schema::parse_str(
                r#"{"type":"record","name":"ConfirmAccountCreation","namespace":"nl.openweb.data","fields":[{"name":"id","type":{"type":"fixed","name":"Uuid","size":16}},{"name":"a_type","type":{"type":"enum","name":"Atype","symbols":["AUTO","MANUAL"]}}]}"#,
            ).unwrap(),
        };
        let item = ConfirmAccountCreation {
            id: [
                204, 240, 237, 74, 227, 188, 75, 46, 183, 163, 122, 214, 178, 72, 118, 162,
            ],
            a_type: Atype::Manual,
        };
        let result = crate::blocking::avro::item_to_bytes(&schema, item);
        assert_eq!(
            result,
            Err(SRCError::new(
                "Failed to resolve",
                Some(String::from("Schema resoulution error: String expected, got Array([Int(204), Int(240), Int(237), Int(74), Int(227), Int(188), Int(75), Int(46), Int(183), Int(163), Int(122), Int(214), Int(178), Int(72), Int(118), Int(162)])")),
                false,
            ))
        )
    }

    #[test]
    fn error_when_invalid_schema() {
        let registered_schema = RegisteredSchema {
            id: 0,
            schema_type: SchemaType::Avro,
            schema: String::from(r#"{"type":"record","name":"Name"}"#),
            references: vec![],
        };
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let result = match to_avro_schema(&sr_settings, registered_schema) {
            Err(e) => e,
            _ => panic!(),
        };
        assert_eq!(
            result,
            SRCError::new(
                "Supplied raw value \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Name\\\"}\" cant be turned into a Schema",
                Some(String::from("Failed to parse schema: No `fields` in record")),
                false,
            )
        )
    }

    #[test]
    fn error_when_invalid_type() {
        let registered_schema = RegisteredSchema {
            id: 0,
            schema_type: SchemaType::Protobuf,
            schema: String::from(
                r#"syntax = "proto3"; package org.schema_registry_test_app.proto; message Result { string up = 1; string down = 2; }"#,
            ),
            references: vec![],
        };
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let result = match to_avro_schema(&sr_settings, registered_schema) {
            Err(e) => e,
            _ => panic!(),
        };
        assert_eq!(
            result,
            SRCError::new("type Protobuf, is not supported", None, false)
        )
    }

    #[test]
    fn test_primitive_schema() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let _n = mock("POST", "/subjects/heartbeat-key/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":4}"#)
            .create();

        let primitive_schema_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            String::from("heartbeat"),
            true,
            get_supplied_schema(&Schema::String),
        );
        let bytes = encoder.encode_struct("key-value", &primitive_schema_strategy);

        assert_eq!(
            bytes,
            Ok(vec![
                0, 0, 0, 0, 4, 18, 107, 101, 121, 45, 118, 97, 108, 117, 101
            ])
        );
    }

    #[test]
    fn test_primitive_schema_incompatible_strategy() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = AvroEncoder::new(sr_settings);

        let primitive_schema_strategy =
            SubjectNameStrategy::RecordNameStrategyWithSchema(get_supplied_schema(&Schema::String));
        let result = encoder.encode_struct("key-value", &primitive_schema_strategy);

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema"
            ))
        );
    }

    #[test]
    fn replace_referred_schema() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = AvroDecoder::new(sr_settings);
        let bytes = [
            0, 0, 0, 0, 5, 97, 19, 76, 118, 247, 191, 70, 148, 162, 9, 233, 76, 211, 29, 141, 180,
            0, 2, 2, 12, 83, 116, 114, 105, 110, 103, 2, 12, 83, 84, 82, 73, 78, 71, 12, 115, 116,
            114, 105, 110, 103, 0,
        ];

        let _m = mock("GET", "/schemas/ids/5")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"AvroTest\",\"namespace\":\"org.schema_registry_test_app.avro\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"fixed\",\"name\":\"Uuid\",\"size\":16}},{\"name\":\"by\",\"type\":{\"type\":\"enum\",\"name\":\"Language\",\"symbols\":[\"Java\",\"Rust\",\"Js\",\"Python\",\"Go\",\"C\"]}},{\"name\":\"counter\",\"type\":\"long\"},{\"name\":\"input\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"results\",\"type\":{\"type\":\"array\",\"items\":\"Result\"}}]}","references":[{"name":"org.schema_registry_test_app.avro.Result","subject":"avro-result","version":1}]}"#)
            .create();
        let _m = mock("GET", "/subjects/avro-result/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"avro-result","version":1,"id":2,"schema":"{\"type\":\"record\",\"name\":\"Result\",\"namespace\":\"org.schema_registry_test_app.avro\",\"fields\":[{\"name\":\"up\",\"type\":\"string\"},{\"name\":\"down\",\"type\":\"string\"}]}"}"#)
            .create();

        let result = decoder.decode(Some(&bytes));
        let value_values = match result {
            Ok(v) => match v.value {
                Value::Record(r) => r,
                _ => panic!("Not a record, while only only those expected"),
            },
            Err(e) => panic!("Some kind of error: {}", e),
        };
        let id_key = match &value_values[0] {
            (_id, Value::Fixed(16, _v)) => _id,
            _ => panic!("Not a fixed value of 16 bytes while that was expected"),
        };
        assert_eq!("id", id_key, "expected id key to be id");
        let enum_value = match &value_values[1] {
            (_id, Value::Enum(0, v)) => v,
            _ => panic!("Not an enum value for by while that was expected"),
        };
        assert_eq!("Java", enum_value, "expect message from Java");
        let counter_value = match &value_values[2] {
            (_id, Value::Long(v)) => v,
            _ => panic!("Not a long value for counter while that was expected"),
        };
        assert_eq!(&1i64, counter_value, "counter is 1");
    }
}
