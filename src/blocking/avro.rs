//! Rust encoder and decoder in order to work with the Confluent schema registry.
//!
//! This crate contains ways to handle encoding and decoding of messages making use of the
//! [confluent schema-registry]. This happens in a way that is compatible to the
//! [confluent java serde]. As a result it becomes easy to work with the same data in both the jvm
//! and rust. Since [karapace] is api compatible, it also works with this library.
//! If you want to use advanced features of the Confluent Schema Registry like client side
//! encryption [schema-registry-client] is likely better suited.
//!
//! [confluent schema-registry]: https://docs.confluent.io/current/schema-registry/docs/index.html
//! [confluent java serde]: https://github.com/confluentinc/schema-registry/tree/master/avro-serde/src/main/java/io/confluent/kafka/streams/serdes/avro
//! [karapace]: https://github.com/Aiven-Open/karapace/blob/main/README.rst
//! [schema-registry-client]: https://crates.io/crates/schema-registry-client
//!
//! Both the Decoder and the Encoder have a cache to allow re-use of the Schema objects used for
//! the avro transitions.
//!
//! For Encoding data it's possible to supply a schema else the latest available schema will be used.
//! For Decoding it works the same as the Java part, using the id encoded in the bytes, the
//! correct schema will be fetched and used to decode the message to a apache_avro::types::Value.
//!
//! Resulting errors are SRCError, besides the error they also contain a .cached which tells whether
//! the error is cached or not. Another property added to the error is retriable, in some cases, like
//! when the network fails it might be worth to retry the same function. The library itself doesn't
//! automatically does retries.
//!
//! [avro-rs]: https://crates.io/crates/avro-rs

use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::Schema;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use serde::ser::Serialize;
use serde_json::Value as JsonValue;

use crate::avro_common::{
    decode_bytes, get_name, item_to_bytes, item_to_bytes_raw, replace_reference, values_to_bytes,
    values_to_bytes_raw, AvroSchema, DecodeResult, DecodeResultWithSchema, ResolvedSchemaCache,
};
use crate::blocking::schema_registry::{
    get_referenced_schema, get_schema_by_guid_and_type, get_schema_by_id_and_type,
    get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::schema_registry_common::{
    build_schema_id_header, get_bytes_result, invalid_bytes_error, parse_schema_id_header,
    BytesResult, HeaderSchemaId, RegisteredReference, RegisteredSchema, SchemaIdHeader, SchemaType,
    SubjectNameStrategy, KEY_SCHEMA_ID_HEADER, VALUE_SCHEMA_ID_HEADER,
};

/// A decoder used to transform bytes to a Value object
///
/// The main purpose of having this struct is to be able to cache the schema's. Because the need to
/// be retrieved over http from the schema registry, and we can already see from the bytes which
/// schema we should use, this can save a lot of unnecessary calls.
/// Errors are also stored to the cache, because they may not be recoverable. A function is
/// available to remove the errors from the cache. To get the value apache_avro is used.
///
/// For both the key and the payload/key it's possible to use the schema registry, this struct supports
/// both. But only using the SubjectNameStrategy::TopicNameStrategy it has to be made explicit
/// whether it's actual used as key or value.
/// ```
/// use apache_avro::types::Value;
/// use schema_registry_converter::blocking::schema_registry::SrSettings;
/// use schema_registry_converter::blocking::avro::AvroDecoder;
///
/// let mut server = mockito::Server::new();
/// let _m = server .mock("GET", "/schemas/ids/1?deleted=true")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
///     .create();
///
/// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
/// let decoder = AvroDecoder::new(sr_settings);
/// let heartbeat = decoder.decode(Some(&[0,0,0,0,1,6])).unwrap().value;
///
/// assert_eq!(heartbeat, Value::Record(vec![("beat".to_string(), Value::Long(3))]))
/// ```
#[derive(Debug)]
pub struct AvroDecoder {
    sr_settings: SrSettings,
    cache: DashMap<u32, Result<Arc<AvroSchema>, SRCError>>,
    /// Cache for schemas looked up by guid rather than id, used by
    /// [`AvroDecoder::decode_with_header_id`]. Kept separate from `cache` (keyed by id) since a
    /// guid and an id are different keyspaces.
    guid_cache: DashMap<String, Result<Arc<AvroSchema>, SRCError>>,
    resolved_cache: ResolvedSchemaCache,
}

impl AvroDecoder {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. Retriable errors (e.g. a transient network/HTTP failure) are never cached,
    /// so those are simply retried on the next call. Non-retriable errors (e.g. a schema that
    /// doesn't exist or can't be parsed) are cached to avoid repeatedly retrying a lookup that
    /// isn't going to succeed; if the underlying problem gets fixed you can use
    /// remove_errors_from_cache to clean those out.
    pub fn new(sr_settings: SrSettings) -> AvroDecoder {
        AvroDecoder {
            sr_settings,
            cache: DashMap::new(),
            guid_cache: DashMap::new(),
            resolved_cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache. You might need/want to run this when the
    /// underlying problem has been fixed (e.g. a schema was missing and has since been
    /// registered) and want the next call to try again immediately. Retriable errors aren't
    /// stored in the cache in the first place, so there's nothing to remove for those.
    ///
    /// ```
    /// use apache_avro::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroDecoder;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    /// use schema_registry_converter::error::SRCError;
    ///
    /// let mut server = mockito::Server::new();
    /// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
    /// let decoder = AvroDecoder::new(sr_settings);
    /// let bytes = [0,0,0,0,2,6];
    ///
    /// let _m = server .mock("GET", "/schemas/ids/2?deleted=true")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    ///
    /// let error = decoder.decode(Some(&bytes)).unwrap_err();
    /// assert_eq!(error, SRCError::new("HTTP request to schema registry failed with status 404 Not Found", Some(String::from("error_code: 40403, message: Schema not found")), false).into_cache());
    ///
    /// let _m = server .mock("GET", "/schemas/ids/2?deleted=true")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let error = decoder.decode(Some(&bytes)).unwrap_err();
    /// assert_eq!(error, SRCError::new("HTTP request to schema registry failed with status 404 Not Found", Some(String::from("error_code: 40403, message: Schema not found")), false).into_cache());
    ///
    /// decoder.remove_errors_from_cache();
    ///
    /// let heartbeat = decoder.decode(Some(&bytes)).unwrap().value;
    /// assert_eq!(heartbeat, Value::Record(vec![("beat".to_string(), Value::Long(3))]))
    /// ```
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
        self.guid_cache.retain(|_, v| v.is_ok());
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    ///
    /// ```no_run
    /// use rdkafka::message::{Message, BorrowedMessage};
    /// use apache_avro::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroDecoder;
    /// fn get_value<'a>(
    ///     msg: &'a BorrowedMessage,
    ///     decoder: &'a AvroDecoder,
    /// ) -> Value{
    ///     match decoder.decode(msg.payload()){
    ///         Ok(r) => r.value,
    ///         Err(e) => panic!("Error getting value: {}", e),
    ///     }
    /// }
    /// ```
    pub fn decode(&self, bytes: Option<&[u8]>) -> Result<DecodeResult, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(DecodeResult {
                name: None,
                value: Value::Null,
            }),
            BytesResult::Valid(id, bytes) => self.deserialize(id, bytes),
            BytesResult::Invalid(bytes) => Err(SRCError::non_retryable_without_cause(
                &invalid_bytes_error(bytes),
            )),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<DecodeResult, SRCError> {
        match self.schema(id) {
            Ok(s) => match decode_bytes(&self.resolved_cache, &s, bytes) {
                Ok(v) => Ok(DecodeResult {
                    name: get_name(&s.parsed),
                    value: v,
                }),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }
    /// Like [`AvroDecoder::decode`], but for a message that may carry its schema id/guid in a
    /// `__key_schema_id`/`__value_schema_id` header instead of (or in addition to) the payload
    /// prefix, mirroring Confluent's `DualSchemaIdDeserializer`: `header_value` present ->
    /// resolve the schema from it and treat `bytes` as the raw (unprefixed) payload;
    /// `header_value` absent -> falls straight through to [`AvroDecoder::decode`], so the only
    /// overhead for a caller who always passes `None` is this one check. See
    /// https://github.com/gklijs/schema_registry_converter/issues/139.
    /// ```
    /// use apache_avro::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroDecoder;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    ///
    /// let mut server = mockito::Server::new();
    /// // GET /schemas/guids/{guid} never carries an "id" field on a real registry -- only "guid".
    /// let _m = server .mock("GET", "/schemas/guids/cc0e0e0e-53c1-4a1a-8f1a-000000000001")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
    /// let decoder = AvroDecoder::new(sr_settings);
    /// let header_value = [0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
    /// let heartbeat = decoder.decode_with_header_id(Some(&header_value), Some(&[6])).unwrap().value;
    ///
    /// assert_eq!(heartbeat, Value::Record(vec![("beat".to_string(), Value::Long(3))]))
    /// ```
    pub fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<DecodeResult, SRCError> {
        let payload = match bytes {
            Some(v) => v,
            None => {
                return Ok(DecodeResult {
                    name: None,
                    value: Value::Null,
                });
            }
        };
        match header_value {
            None => self.decode(Some(payload)),
            Some(header) => {
                let (schema_id, _rest) = parse_schema_id_header(header)?;
                let schema = match schema_id {
                    HeaderSchemaId::Id(id) => self.schema(id)?,
                    HeaderSchemaId::Guid(guid) => self.guid_schema(guid)?,
                };
                match decode_bytes(&self.resolved_cache, &schema, payload) {
                    Ok(v) => Ok(DecodeResult {
                        name: get_name(&schema.parsed),
                        value: v,
                    }),
                    Err(e) => Err(e),
                }
            }
        }
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub fn decode_with_schema(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResultWithSchema>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => match self.deserialize_with_schema(id, bytes) {
                Ok(v) => Ok(Some(v)),
                Err(e) => Err(e),
            },
            BytesResult::Invalid(bytes) => Err(SRCError::non_retryable_without_cause(
                &invalid_bytes_error(bytes),
            )),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize_with_schema(
        &self,
        id: u32,
        bytes: &[u8],
    ) -> Result<DecodeResultWithSchema, SRCError> {
        match self.schema(id) {
            Ok(schema) => match decode_bytes(&self.resolved_cache, &schema, bytes) {
                Ok(value) => Ok(DecodeResultWithSchema {
                    name: get_name(&schema.parsed),
                    value,
                    schema,
                }),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    fn schema(&self, id: u32) -> Result<Arc<AvroSchema>, SRCError> {
        let sr_settings = &self.sr_settings;
        match self.cache.entry(id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let result = match get_schema_by_id_and_type(id, sr_settings, SchemaType::Avro) {
                    Ok(registered_schema) => to_avro_schema(sr_settings, registered_schema),
                    Err(e) => Err(e),
                };
                match result {
                    // Retriable errors (transient network/HTTP issues) don't make sense to
                    // cache, so the entry is left vacant and the next call issues a fresh
                    // request rather than replaying a stale failure.
                    Err(e) if e.retriable => Err(e),
                    result => entry
                        .insert(result.map_err(SRCError::into_cache))
                        .value()
                        .clone(),
                }
            }
        }
    }

    /// Like [`AvroDecoder::schema`], but looks the schema up by guid instead of id -- used by
    /// [`AvroDecoder::decode_with_header_id`] when the header carries a guid rather than an id.
    fn guid_schema(&self, guid: String) -> Result<Arc<AvroSchema>, SRCError> {
        let sr_settings = &self.sr_settings;
        match self.guid_cache.entry(guid.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let result = match get_schema_by_guid_and_type(&guid, sr_settings, SchemaType::Avro)
                {
                    Ok(registered_schema) => to_avro_schema(sr_settings, registered_schema),
                    Err(e) => Err(e),
                };
                match result {
                    Err(e) if e.retriable => Err(e),
                    result => entry
                        .insert(result.map_err(SRCError::into_cache))
                        .value()
                        .clone(),
                }
            }
        }
    }
}

/// An encoder used to transform a Value object to bytes
///
/// The main purpose of having this struct is to be able to cache the schema's. Because the need to
/// be retrieved over http from the schema registry, and we can already see from the bytes which
/// schema we should use, this can save a lot of unnecessary calls.
/// Errors are also stored to the cache, because they may not be recoverable. A function is
/// available to remove the errors from the cache. To get the value apache_avro is used.
///
/// For both the key and the payload/key it's possible to use the schema registry, this struct supports
/// both. But only using the SubjectNameStrategy::TopicNameStrategy it has to be made explicit
/// whether it's actual used as key or value.
/// ```
/// use apache_avro::types::Value;
/// use schema_registry_converter::blocking::avro::AvroEncoder;
/// use schema_registry_converter::blocking::schema_registry::SrSettings;
/// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
///
/// let mut server = mockito::Server::new();
/// let _m = server .mock("GET", "/subjects/heartbeat-value/versions/latest")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
///     .create();
///
/// let _m = server .mock("GET", "/subjects/heartbeat-key/versions/latest")
///     .with_status(200)
///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Name\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"avro.java.string\":\"String\"}]}"}"#)
///     .create();
///
/// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
/// let encoder = AvroEncoder::new(sr_settings);
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
    cache: DashMap<String, Result<Arc<AvroSchema>, SRCError>>,
    resolved_cache: ResolvedSchemaCache,
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
    /// use apache_avro::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    /// use schema_registry_converter::schema_registry_common::{SubjectNameStrategy, SchemaType, SuppliedSchema};
    ///
    /// let mut server = mockito::Server::new();
    /// let _m = server.mock("POST", "/subjects/hb-nl.openweb.data.Heartbeat/versions")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"id":23}"#)
    ///     .create();
    ///
    /// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
    /// let encoder = AvroEncoder::new(sr_settings);
    ///
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(String::from("hb"), SuppliedSchema {
    ///                 name: Some(String::from("nl.openweb.data.Heartbeat")),
    ///                 schema_type: SchemaType::Avro,
    ///                 schema: String::from(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#),
    ///                 references: vec![],
    ///                 properties: None,
    ///                 tags: None,
    ///             });
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    /// assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 23, 6]))
    /// ```
    pub fn new(sr_settings: SrSettings) -> AvroEncoder {
        AvroEncoder {
            sr_settings,
            cache: DashMap::new(),
            resolved_cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache. You might need/want to run this when the
    /// underlying problem has been fixed (e.g. a schema was missing and has since been
    /// registered) and want the next call to try again immediately. Retriable errors aren't
    /// stored in the cache in the first place, so there's nothing to remove for those.
    ///
    /// ```
    /// use apache_avro::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    /// use schema_registry_converter::error::SRCError;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    ///
    /// let mut server = mockito::Server::new();
    /// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
    /// let encoder = AvroEncoder::new(sr_settings);
    /// let strategy = SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
    ///
    /// let _m = server .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(404)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
    ///     .create();
    ///
    /// let error = encoder.encode(vec![("beat", Value::Long(3))], &strategy).unwrap_err();
    /// assert_eq!(error, SRCError::new("HTTP request to schema registry failed with status 404 Not Found", Some(String::from("error_code: 40403, message: Schema not found")), false).into_cache());
    ///
    ///
    /// let _m = server .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let error = encoder.encode(vec![("beat", Value::Long(3))], &strategy).unwrap_err();
    /// assert_eq!(error, SRCError::new("HTTP request to schema registry failed with status 404 Not Found", Some(String::from("error_code: 40403, message: Schema not found")), false).into_cache());
    ///
    /// encoder.remove_errors_from_cache();
    ///
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    /// assert_eq!(bytes, Ok(vec![0,0,0,0,4,6]))
    /// ```
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Encodes a vector of values to bytes. The correct values of the 'keys' depend on the schema
    /// being fetched at runtime, or the one supplied with the SubjectNameStrategy.
    ///
    /// The function get_supplied_schema might be used to easily provide the schema in the correct
    /// form.
    /// ```
    /// use apache_avro::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    ///
    /// let mut server = mockito::Server::new();
    /// let _m = server .mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
    /// let encoder = AvroEncoder::new(sr_settings);
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategy(String::from("heartbeat"), String::from("nl.openweb.data.Heartbeat"));
    /// let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
    ///
    /// assert_eq!(bytes, Ok(vec![0,0,0,0,3,6]))
    /// ```
    pub fn encode(
        &self,
        values: Vec<(&str, Value)>,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        match self.get_schema_and_id(key, subject_name_strategy) {
            Ok(avro_schema) => values_to_bytes(&self.resolved_cache, &avro_schema, values),
            Err(e) => Err(e),
        }
    }

    /// Like [`AvroEncoder::encode`], but instead of prefixing the payload with the schema id
    /// (the default confluent wire format), returns the raw Avro bytes alongside a
    /// [`SchemaIdHeader`] carrying the schema's guid. Attach the header to the Kafka record
    /// using whatever Kafka client you're using -- this crate has no dependency on one. `is_key`
    /// picks between the `__key_schema_id`/`__value_schema_id` header names, matching
    /// Confluent's `HeaderSchemaIdSerializer`. See
    /// https://github.com/gklijs/schema_registry_converter/issues/139.
    ///
    /// Requires a schema registry that returns a `guid` (Confluent Schema Registry 8.0+); a
    /// registry that doesn't will make this return an error.
    /// ```
    /// use apache_avro::types::Value;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::schema_registry_common::{SubjectNameStrategy, VALUE_SCHEMA_ID_HEADER};
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    ///
    /// let mut server = mockito::Server::new();
    /// let _m = server .mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
    ///     .with_status(200)
    ///     .with_header("content-type", "application/vnd.schemaregistry.v1+json")
    ///     .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"guid":"cc0e0e0e-53c1-4a1a-8f1a-000000000001","schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
    ///     .create();
    ///
    /// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
    /// let encoder = AvroEncoder::new(sr_settings);
    /// let strategy = SubjectNameStrategy::TopicRecordNameStrategy(String::from("heartbeat"), String::from("nl.openweb.data.Heartbeat"));
    /// let (bytes, header) = encoder.encode_with_header_id(vec![("beat", Value::Long(3))], &strategy, false).unwrap();
    ///
    /// assert_eq!(bytes, vec![6]); // no prefix -- just the raw Avro-encoded value
    /// assert_eq!(header.name, VALUE_SCHEMA_ID_HEADER);
    /// assert_eq!(header.value, vec![0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
    /// ```
    pub fn encode_with_header_id(
        &self,
        values: Vec<(&str, Value)>,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let key = subject_name_strategy.get_subject()?;
        let avro_schema = self.get_schema_and_id(key, subject_name_strategy)?;
        let bytes = values_to_bytes_raw(&self.resolved_cache, &avro_schema, values)?;
        let header = schema_id_header_for(&avro_schema, is_key)?;
        Ok((bytes, header))
    }

    /// Encodes a struct or a primitive value to bytes. The schema used for the encoding will be
    /// retrieved from the schema registry, or it will use the one supplied with the
    /// SubjectNameStrategy.
    ///
    /// The function get_supplied_schema might be used to easily provide the schema in the correct
    /// form.
    /// ```
    /// use serde::Serialize;
    /// use apache_avro::types::Value;
    /// use apache_avro::Schema;
    /// use schema_registry_converter::blocking::avro::AvroEncoder;
    /// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
    /// use schema_registry_converter::blocking::schema_registry::SrSettings;
    /// use schema_registry_converter::avro_common::get_supplied_schema;
    ///
    /// let mut server = mockito::Server::new();
    /// let _m = server.mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
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
    /// let sr_settings = SrSettings::new_builder(server.url()).no_proxy().build().unwrap();
    /// let encoder = AvroEncoder::new(sr_settings);
    /// let existing_schema_strategy = SubjectNameStrategy::TopicRecordNameStrategy(String::from("heartbeat"), String::from("nl.openweb.data.Heartbeat"));
    /// let bytes = encoder.encode_struct(Heartbeat{beat: 3}, &existing_schema_strategy);
    ///
    /// assert_eq!(bytes, Ok(vec![0,0,0,0,3,6]));
    ///
    ///  let _m = server.mock("POST", "/subjects/heartbeat-key/versions")
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
        &self,
        item: impl Serialize,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        match self.get_schema_and_id(key, subject_name_strategy) {
            Ok(avro_schema) => item_to_bytes(&self.resolved_cache, &avro_schema, item),
            Err(e) => Err(e),
        }
    }

    /// Like [`AvroEncoder::encode_struct`], but returns the schema id/guid as a
    /// [`SchemaIdHeader`] instead of prefixing the payload with it. See
    /// [`AvroEncoder::encode_with_header_id`].
    pub fn encode_struct_with_header_id(
        &self,
        item: impl Serialize,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let key = subject_name_strategy.get_subject()?;
        let avro_schema = self.get_schema_and_id(key, subject_name_strategy)?;
        let bytes = item_to_bytes_raw(&self.resolved_cache, &avro_schema, item)?;
        let header = schema_id_header_for(&avro_schema, is_key)?;
        Ok((bytes, header))
    }

    fn get_schema_and_id(
        &self,
        key: String,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Arc<AvroSchema>, SRCError> {
        let sr_settings = &self.sr_settings;
        match self.cache.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let result = match get_schema_by_subject(sr_settings, subject_name_strategy) {
                    Ok(registered_schema) => to_avro_schema(sr_settings, registered_schema),
                    Err(e) => Err(e),
                };
                match result {
                    // Retriable errors (transient network/HTTP issues) don't make sense to
                    // cache, so the entry is left vacant and the next call issues a fresh
                    // request rather than replaying a stale failure.
                    Err(e) if e.retriable => Err(e),
                    result => entry
                        .insert(result.map_err(SRCError::into_cache))
                        .value()
                        .clone(),
                }
            }
        }
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
                    &format!("problem with reference {:?}", r),
                ));
            }
        };
        let child: JsonValue = match serde_json::from_str(&registered_schema.schema) {
            Ok(v) => v,
            Err(e) => {
                return Err(SRCError::non_retryable_with_cause(
                    e,
                    &format!("problem serializing {}", registered_schema.schema),
                ));
            }
        };
        new_value = replace_reference(new_value, child);
        new_value = add_references(sr_settings, new_value, &registered_schema.references)?;
    }
    Ok(new_value)
}

/// Builds the [`SchemaIdHeader`] for `schema`, requiring it to carry a guid (populated when the
/// schema registry response included one, i.e. Confluent Schema Registry 8.0+). Avro has no
/// message index, unlike protobuf, so there are no trailing bytes to append.
fn schema_id_header_for(schema: &AvroSchema, is_key: bool) -> Result<SchemaIdHeader, SRCError> {
    let guid = schema.guid.as_deref().ok_or_else(|| {
        SRCError::non_retryable_without_cause(
            "Schema registry response did not include a guid; encoding the schema id in a \
             header requires Confluent Schema Registry 8.0+",
        )
    })?;
    let name = if is_key {
        KEY_SCHEMA_ID_HEADER
    } else {
        VALUE_SCHEMA_ID_HEADER
    };
    build_schema_id_header(name, guid, &[])
}

fn to_avro_schema(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
) -> Result<Arc<AvroSchema>, SRCError> {
    match registered_schema.schema_type {
        SchemaType::Avro => (),
        t => {
            return Err(SRCError::non_retryable_without_cause(&format!(
                "type {:?}, is not supported",
                t
            )));
        }
    }
    let main_schema = match serde_json::from_str(&registered_schema.schema) {
        Ok(v) => add_references(sr_settings, v, registered_schema.references.as_slice())?,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                "failed to parse Avro schema",
            ));
        }
    };
    match Schema::parse(&main_schema) {
        Ok(parsed) => Ok(Arc::new(AvroSchema {
            id: registered_schema.id,
            raw: registered_schema.schema,
            parsed,
            subject: registered_schema.subject,
            version: registered_schema.version,
            properties: registered_schema.properties,
            tags: registered_schema.tags,
            guid: registered_schema.guid,
        })),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            &format!(
                "Supplied raw value {:?} cant be turned into a Schema",
                registered_schema.schema
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::from_value;

    use crate::avro_common::get_supplied_schema;
    use crate::schema_registry_common::SuppliedSchema;

    use super::*;
    use test_utils::Heartbeat;

    #[test]
    fn display_decoder() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = AvroDecoder::new(sr_settings);
        assert!(
            format!("{:?}", decoder).starts_with("AvroDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client")
        )
    }

    #[test]
    fn test_decoder_default() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
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
    fn test_decode_with_schema_default() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode_with_schema(Some(&[0, 0, 0, 0, 1, 6]))
            .unwrap()
            .unwrap()
            .value;

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
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));
        let item = match heartbeat {
            Ok(r) => {
                let name = r.name.unwrap();
                assert_eq!(name.name(), "Heartbeat");
                assert_eq!(name.namespace().unwrap(), "nl.openweb.data");
                from_value::<Heartbeat>(&r.value).unwrap()
            }
            _ => panic!(),
        };
        assert_eq!(item.beat, 3i64);
    }

    #[test]
    fn test_decoder_no_bytes() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(None).unwrap().value;

        assert_eq!(heartbeat, Value::Null)
    }

    #[test]
    fn test_decoder_with_name_no_bytes() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = AvroDecoder::new(sr_settings);
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
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[1, 0, 0, 0, 1, 6]));

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "Invalid bytes: first byte is 0x01, expected magic byte 0x00 for confluent schema registry wire format. The message may not be encoded with the schema registry serializer."
            ))
        )
    }

    #[test]
    fn test_decoder_with_name_magic_byte_not_present() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[1, 0, 0, 0, 1, 6]));

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "Invalid bytes: first byte is 0x01, expected magic byte 0x00 for confluent schema registry wire format. The message may not be encoded with the schema registry serializer."
            ))
        )
    }

    #[test]
    fn test_decoder_not_enough_bytes() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[0, 0, 0, 0]));

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "Invalid bytes: payload too short (4 bytes), expected at least 5 bytes for confluent schema registry wire format"
            ))
        )
    }

    #[test]
    fn test_decoder_wrong_data() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let err = decoder.decode(Some(&[0, 0, 0, 0, 1])).unwrap_err();

        assert_eq!(err.error, "Could not transform bytes using schema")
    }

    #[test]
    fn test_decoder_with_name_wrong_data() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let err = decoder.decode(Some(&[0, 0, 0, 0, 1])).unwrap_err();

        assert_eq!(err.error, "Could not transform bytes using schema")
    }

    #[test]
    fn test_decoder_no_json_response() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let error = decoder.decode(Some(&[0, 0, 0, 0, 1, 6])).unwrap_err();

        // Only assert the stable error identity; the underlying reqwest `cause` string is
        // version-dependent (newer reqwest appends "for url (...)").
        assert_eq!(error.error, "could not parse to RawRegisteredSchema");
    }

    #[test]
    fn test_decoder_with_name_no_json_response() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let error = decoder.decode(Some(&[0, 0, 0, 0, 1, 6])).unwrap_err();

        // Only assert the stable error identity; the underlying reqwest `cause` string is
        // version-dependent (newer reqwest appends "for url (...)").
        assert_eq!(error.error, "could not parse to RawRegisteredSchema");
    }

    #[test]
    fn test_decoder_schema_registry_unavailable() {
        let sr_settings = SrSettings::new_builder(String::from("http://bogus"))
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[0, 0, 0, 10, 1, 6]));

        match result {
            Err(e) => assert_eq!(e.error, "http call to schema registry failed"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_decoder_default_no_schema_in_response() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"no-schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(&[0, 0, 0, 0, 1, 6]));

        assert_eq!(
            heartbeat,
            Err(SRCError::new("Could not get raw schema from response", None, false).into_cache())
        )
    }

    #[test]
    fn test_decoder_default_wrong_schema_in_response() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\"}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let err = decoder.decode(Some(&[0, 0, 0, 0, 1, 6])).unwrap_err();

        assert_eq!(
            err.error,
           "Supplied raw value \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Heartbeat\\\",\\\"namespace\\\":\\\"nl.openweb.data\\\"}\" cant be turned into a Schema",
        )
    }

    #[test]
    fn test_decoder_fixed_with_enum() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"ConfirmAccountCreation\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"fixed\",\"name\":\"Uuid\",\"size\":16}},{\"name\":\"a_type\",\"type\":{\"type\":\"enum\",\"name\":\"Atype\",\"symbols\":[\"AUTO\",\"MANUAL\"]}}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
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
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let bytes = [0, 0, 0, 0, 2, 6];

        let _m = server
            .mock("GET", "/schemas/ids/2?deleted=true")
            .with_status(404)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
            .create();
        let error = decoder.decode(Some(&bytes)).unwrap_err();
        assert_eq!(
            error,
            SRCError::new(
                "HTTP request to schema registry failed with status 404 Not Found",
                Some(String::from("error_code: 40403, message: Schema not found")),
                false
            )
            .into_cache()
        );
        let _m = server.mock("GET", "/schemas/ids/2?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let error = decoder.decode(Some(&bytes)).unwrap_err();
        assert_eq!(
            error,
            SRCError::new(
                "HTTP request to schema registry failed with status 404 Not Found",
                Some(String::from("error_code: 40403, message: Schema not found")),
                false
            )
            .into_cache()
        );

        decoder.remove_errors_from_cache();

        let heartbeat = decoder.decode(Some(&bytes)).unwrap().value;
        assert_eq!(
            heartbeat,
            Value::Record(vec![("beat".to_string(), Value::Long(3))])
        )
    }

    #[test]
    fn test_decoder_retriable_error_is_not_cached() {
        // Regression test for https://github.com/gklijs/schema_registry_converter/issues/171:
        // a retriable error (e.g. a transient 503) must not stick around in the cache, so the
        // very next call succeeds without needing remove_errors_from_cache().
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let bytes = [0, 0, 0, 0, 9, 6];

        let _m = server
            .mock("GET", "/schemas/ids/9?deleted=true")
            .with_status(503)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":50302,"message":"Leader not known"}"#)
            .create();

        let err = decoder.decode(Some(&bytes)).unwrap_err();
        assert!(err.retriable, "expected a retriable error, got {:?}", err);
        assert!(
            !err.cached,
            "retriable error should not be cached, got {:?}",
            err
        );

        let _m = server
            .mock("GET", "/schemas/ids/9?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        // No remove_errors_from_cache() call: this must succeed purely because the retriable
        // error above was never cached in the first place.
        let heartbeat = decoder.decode(Some(&bytes)).unwrap().value;
        assert_eq!(
            heartbeat,
            Value::Record(vec![("beat".to_string(), Value::Long(3))])
        )
    }

    #[test]
    fn display_encoder() {
        let sr_settings = SrSettings::new_builder(String::from("http://127.0.0.1:1234"))
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);
        assert!(
            format!("{:?}", encoder).starts_with("AvroEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client")
        )
    }

    #[test]
    fn test_encode_key_and_value() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/subjects/heartbeat-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let _m = server.mock("GET", "/subjects/heartbeat-key/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Name\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"avro.java.string\":\"String\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

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
    fn test_encode_key_and_value_with_non_static_lifetime() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/subjects/heartbeat-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let _m = server.mock("GET", "/subjects/heartbeat-key/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Name\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"avro.java.string\":\"String\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

        let key_strategy = SubjectNameStrategy::TopicNameStrategy(String::from("heartbeat"), true);

        let field_name = String::from("name");
        let bytes = encoder.encode(
            vec![(&field_name, Value::String("Some name".to_owned()))],
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
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            String::from("heartbeat"),
            String::from("nl.openweb.data.Heartbeat"),
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 3, 6]))
    }

    #[test]
    fn test_encoder_no_id_in_response() {
        let mut server = mockito::Server::new();
        let _m = server.mock("GET", "/subjects/heartbeat-nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"no-id":3,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);
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
        let sr_settings = SrSettings::new_builder(String::from("http://bogus"))
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);
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
        let sr_settings = SrSettings::new_builder(String::from("hxxx://bogus"))
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicRecordNameStrategy(
            String::from("heartbeat"),
            String::from("nl.openweb.data.Balance"),
        );
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        // Retriable errors (transient network/HTTP issues) aren't cached — see issue #171.
        assert_eq!(
            result,
            Err(SRCError::new(
                "http call to schema registry failed",
                Some(String::from("builder error for url (hxxx://bogus/subjects/heartbeat-nl.openweb.data.Balance/versions/latest)")),
                true,
            ))
        )
    }

    #[test]
    fn test_encoder_schema_registry_unavailable_with_record() {
        let sr_settings = SrSettings::new_builder(String::from("http://bogus"))
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(SuppliedSchema {
            name: Some(String::from("nl.openweb.data.Balance")),
            schema_type: SchemaType::Avro,
            schema: String::from(
                r#"{"type":"record","name":"Balance","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
            ),
            references: vec![],
            properties: None,
            tags: None,
        });
        let result = encoder.encode(vec![("beat", Value::Long(3))], &strategy);

        match result {
            Err(e) => assert_eq!(e.error, "http call to schema registry failed"),
            _ => panic!(),
        }
    }

    #[test]
    fn test_encode_cache() {
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);
        let strategy =
            SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

        let _m = server
            .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(404)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"error_code":40403,"message":"Schema not found"}"#)
            .create();

        let error = encoder
            .encode(vec![("beat", Value::Long(3))], &strategy)
            .unwrap_err();
        assert_eq!(
            error,
            SRCError::new(
                "HTTP request to schema registry failed with status 404 Not Found",
                Some(String::from("error_code: 40403, message: Schema not found")),
                false
            )
            .into_cache()
        );

        let _m = server.mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let error = encoder
            .encode(vec![("beat", Value::Long(3))], &strategy)
            .unwrap_err();
        assert_eq!(
            error,
            SRCError::new(
                "HTTP request to schema registry failed with status 404 Not Found",
                Some(String::from("error_code: 40403, message: Schema not found")),
                false
            )
            .into_cache()
        );

        encoder.remove_errors_from_cache();

        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 4, 6]))
    }

    #[test]
    fn test_encode_key_and_value_supplied_record() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("POST", "/subjects/heartbeat-key/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":3}"#)
            .create();

        let _m = server
            .mock("POST", "/subjects/heartbeat-value/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":4}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

        let key_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            String::from("heartbeat"),
            true,
            SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Name")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#,
                ),
                references: vec![],
                properties: None,
                tags: None,
            },
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
            SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
                properties: None,
                tags: None,
            },
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &value_strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 4, 6]))
    }

    #[test]
    fn test_encode_record_name_strategy_supplied_record() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("POST", "/subjects/nl.openweb.data.Heartbeat/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":11}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(SuppliedSchema {
            name: Some(String::from("nl.openweb.data.Heartbeat")),
            schema_type: SchemaType::Avro,
            schema: String::from(
                r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
            ),
            references: vec![],
            properties: None,
            tags: None,
        });
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 11, 6]))
    }

    #[test]
    fn test_encode_record_name_strategy_supplied_record_wrong_response() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("POST", "/subjects/nl.openweb.data.Heartbeat/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"no-id":11}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(SuppliedSchema {
            name: Some(String::from("nl.openweb.data.Heartbeat")),
            schema_type: SchemaType::Avro,
            schema: String::from(
                r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
            ),
            references: vec![],
            properties: None,
            tags: None,
        });
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
        let mut server = mockito::Server::new();
        let _m = server
            .mock("POST", "/subjects/hb-nl.openweb.data.Heartbeat/versions")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":23}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
            String::from("hb"),
            SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
                properties: None,
                tags: None,
            },
        );
        let bytes = encoder.encode(vec![("beat", Value::Long(3))], &strategy);
        assert_eq!(bytes, Ok(vec![0, 0, 0, 0, 23, 6]))
    }

    #[test]
    fn test_encode_topic_record_name_strategy_schema_registry_not_available() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("POST", "/subjects/hb-nl.openweb.data.Heartbeat/versions")
            .with_status(503)
            .with_body(r#"NOT THERE!"#)
            .create();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

        let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
            String::from("hb"),
            SuppliedSchema {
                name: Some(String::from("nl.openweb.data.Heartbeat")),
                schema_type: SchemaType::Avro,
                schema: String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
                ),
                references: vec![],
                properties: None,
                tags: None,
            },
        );
        let error = encoder
            .encode(vec![("beat", Value::Long(3))], &strategy)
            .unwrap_err();
        // Retriable errors (transient network/HTTP issues) aren't cached — see issue #171.
        assert_eq!(
            error,
            SRCError::new(
                "HTTP request to schema registry failed with status 503 Service Unavailable",
                Some(String::from(
                    "error_code: 0, message: couldn't parse schema registry error json"
                )),
                true,
            )
        )
    }

    #[test]
    fn error_when_invalid_schema() {
        let registered_schema = RegisteredSchema {
            id: 0,
            schema_type: SchemaType::Avro,
            schema: String::from(r#"{"type":"record","name":"Name"}"#),
            references: vec![],
            properties: None,
            tags: None,
            subject: None,
            version: None,
            guid: None,
        };
        let sr_settings = SrSettings::new_builder(String::from("http://127.0.0.1:1234"))
            .no_proxy()
            .build()
            .unwrap();
        let err = to_avro_schema(&sr_settings, registered_schema).unwrap_err();
        assert_eq!(
            err.error,
            "Supplied raw value \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Name\\\"}\" cant be turned into a Schema"
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
            properties: None,
            tags: None,
            subject: None,
            version: None,
            guid: None,
        };
        let sr_settings = SrSettings::new_builder(String::from("http://127.0.0.1:1234"))
            .no_proxy()
            .build()
            .unwrap();
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
    fn to_avro_schema_propagates_subject_version_and_guid() {
        let registered_schema = RegisteredSchema {
            id: 7,
            schema_type: SchemaType::Avro,
            schema: String::from(r#"{"type":"record","name":"X","fields":[]}"#),
            references: vec![],
            properties: None,
            tags: None,
            subject: Some("orders-value".to_string()),
            version: Some(3),
            guid: Some("cc0e0e0e-53c1-4a1a-8f1a-000000000001".to_string()),
        };
        let sr_settings = SrSettings::new_builder(String::from("http://127.0.0.1:1234"))
            .no_proxy()
            .build()
            .unwrap();
        let avro = to_avro_schema(&sr_settings, registered_schema)
            .expect("conversion succeeds for empty references");
        assert_eq!(avro.id, 7);
        assert_eq!(avro.subject.as_deref(), Some("orders-value"));
        assert_eq!(avro.version, Some(3));
        assert_eq!(
            avro.guid.as_deref(),
            Some("cc0e0e0e-53c1-4a1a-8f1a-000000000001")
        );
    }

    #[test]
    fn test_primitive_schema() {
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = AvroEncoder::new(sr_settings);

        let _m = server
            .mock("POST", "/subjects/heartbeat-key/versions")
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
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let encoder = AvroEncoder::new(sr_settings);

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
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = AvroDecoder::new(sr_settings);
        let bytes = [
            0, 0, 0, 0, 5, 97, 19, 76, 118, 247, 191, 70, 148, 162, 9, 233, 76, 211, 29, 141, 180,
            0, 2, 2, 12, 83, 116, 114, 105, 110, 103, 2, 12, 83, 84, 82, 73, 78, 71, 12, 115, 116,
            114, 105, 110, 103, 0,
        ];

        let _m = server.mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"AvroTest\",\"namespace\":\"org.schema_registry_test_app.avro\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"fixed\",\"name\":\"Uuid\",\"size\":16}},{\"name\":\"by\",\"type\":{\"type\":\"enum\",\"name\":\"Language\",\"symbols\":[\"Java\",\"Rust\",\"Js\",\"Python\",\"Go\",\"C\"]}},{\"name\":\"counter\",\"type\":\"long\"},{\"name\":\"input\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"results\",\"type\":{\"type\":\"array\",\"items\":\"Result\"}}]}","references":[{"name":"org.schema_registry_test_app.avro.Result","subject":"avro-result","version":1}]}"#)
            .create();
        let _m = server.mock("GET", "/subjects/avro-result/versions/1")
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
