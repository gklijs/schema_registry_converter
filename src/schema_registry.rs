//! This module contains the code specific for the schema registry.

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use core::fmt;
use failure::Fail;
use reqwest::blocking::Client;
use reqwest::header;
use reqwest::header::{HeaderName, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Deref;
use std::str;
use std::time::Duration;

/// Settings used to do the calls to schema registry. For simple cases you can use `SrSettings::new`
/// or the `SrSettingsBuilder`. But you can also use it directly so you can all the available
/// settings from reqwest.
#[derive(Debug)]
pub struct SrSettings {
    urls: Vec<String>,
    client: Client,
}

/// Struct to create an SrSettings when used with multiple url's, authorization, custom headers, or
/// custom timeout.
pub struct SrSettingsBuilder {
    urls: Vec<String>,
    authorization: Option<String>,
    headers: HashMap<String, String, RandomState>,
    proxy: Option<String>,
    timeout: Duration,
}

/// Creates a new SrSettings struct that is needed to make calls to the schema registry
/// ```
/// use schema_registry_converter::schema_registry::SrSettings;
/// let sr_settings = SrSettings::new(String::from("http://localhost:8081"));
/// ```
impl SrSettings {
    /// Will create a new SrSettings with default values, the url should be fully qualified like
    /// `"http://localhost:8081"`.
    pub fn new(url: String) -> SrSettings {
        SrSettings {
            urls: vec![url],
            client: Client::new(),
        }
    }
}

/// Builder for SrSettings
/// ```
/// use schema_registry_converter::schema_registry::SrSettingsBuilder;
/// use std::time::Duration;
/// let sr_settings = SrSettingsBuilder::new(String::from("http://localhost:8081"))
///     .add_url(String::from("http://localhost:8082"))
///     .set_authorization("Bearer some_json_web_token_for_example")
///     .add_header("foo", "bar")
///     .set_proxy("http://localhost:8888")
///     .set_timeout(Duration::from_secs(5))
///     .build().unwrap();
/// ```
impl SrSettingsBuilder {
    /// Wil create a new SrSettingsBuilder with default values, the url should be fully qualified
    /// like `"http://localhost:8081"`.
    pub fn new(url: String) -> SrSettingsBuilder {
        SrSettingsBuilder {
            urls: vec![url],
            authorization: None,
            headers: HashMap::new(),
            proxy: None,
            timeout: Duration::from_secs(30),
        }
    }

    /// Adds an url. For any call urls will be tried in order. the one used to create the settings
    /// struct first. All urls should be fully qualified.
    pub fn add_url(&mut self, url: String) -> &mut SrSettingsBuilder {
        self.urls.push(url);
        self
    }

    /// Sets authorization that will be added to the header. Make sure you add the value how it
    /// should and up in the header. See if its a token, use `Bearer {token}` and not just the token.
    pub fn set_authorization(&mut self, authorization: &str) -> &mut SrSettingsBuilder {
        self.authorization = Some(String::from(authorization));
        self
    }

    /// Adds a custom header that will be added to every call.
    pub fn add_header(&mut self, key: &str, value: &str) -> &mut SrSettingsBuilder {
        self.headers.insert(String::from(key), String::from(value));
        self
    }

    /// Sets a proxy that will be used for every call.
    pub fn set_proxy(&mut self, proxy_url: &str) -> &mut SrSettingsBuilder {
        self.proxy = Some(String::from(proxy_url));
        self
    }

    /// Set a timeout, it will be used for the connect and the read.
    pub fn set_timeout(&mut self, duration: Duration) -> &mut SrSettingsBuilder {
        self.timeout = duration;
        self
    }

    pub fn build(&mut self) -> Result<SrSettings, SRCError> {
        let mut builder = Client::builder();
        let mut header_map = header::HeaderMap::new();
        for (k, v) in self.headers.iter() {
            let header_name = match HeaderName::from_bytes(k.as_bytes()) {
                Ok(h) => h,
                Err(e) => {
                    return Err(SRCError::non_retryable_with_cause(
                        e,
                        &*format!("could not create headername from {}", k),
                    ));
                }
            };
            header_map.insert(header_name, v.parse().unwrap());
        }
        if self.authorization.is_some() {
            header_map.insert(
                AUTHORIZATION,
                self.authorization.as_ref().unwrap().parse().unwrap(),
            );
        }
        builder = builder.default_headers(header_map);
        if self.proxy.is_some() {
            match reqwest::Proxy::all(self.proxy.as_ref().unwrap()) {
                Ok(v) => builder = builder.proxy(v),
                Err(e) => return Err(SRCError::non_retryable_with_cause(e, "invalid proxy value")),
            };
        }
        builder = builder.timeout(self.timeout);
        let urls = self.urls.clone();
        match builder.build() {
            Ok(client) => Ok(SrSettings { urls, client }),
            Err(e) => Err(SRCError::non_retryable_with_cause(
                e,
                "could not create new client",
            )),
        }
    }
}

/// By default the schema registry supports three types. It's possible there will be more in the future
/// or to add your own. Therefore the other is one of the schema types.
#[derive(Clone, Debug, PartialEq)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
    Other(String),
}

/// The schema registry supports sub schema's they will be stored separately in the schema registry
#[derive(Clone, Debug)]
pub struct SuppliedReference {
    pub name: String,
    pub subject: String,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
}

/// Schema as it might be provided to create messages, they will be added to th schema registry if
/// not already present
#[derive(Clone, Debug)]
pub struct SuppliedSchema {
    pub name: Option<String>,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisteredReference {
    pub name: String,
    pub subject: String,
    pub version: u32,
}

/// Schema as retrieved from the schema registry. It's close to the json received and doesn't do
/// type specific transformations.
#[derive(Clone, Debug)]
pub struct RegisteredSchema {
    pub id: u32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<RegisteredReference>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawRegisteredSchema {
    subject: Option<String>,
    version: Option<u32>,
    id: Option<u32>,
    schema_type: Option<String>,
    references: Option<Vec<RegisteredReference>>,
    schema: Option<String>,
}

/// Intermediate result to just handle the byte transformation. When used in a decoder just the
/// id might me enough because the resolved schema is cashed already.
#[derive(Debug)]
pub enum BytesResult {
    Null,
    Invalid(Vec<u8>),
    Valid(u32, Vec<u8>),
}

/// Strategy similar to the one in the Java client. By default schema's needs to be backwards
/// compatible. Historically the only available strategy was the TopicNameStrategy. This meant in
/// practice that a topic could only have one type, or the restriction on backwards compatibility
/// was to be abandoned. Using either of the two other strategies allows multiple types of schema
/// on on topic, while still being able to keep the restriction on schema's being backwards
/// compatible.
/// Depending on the strategy, either the topic, whether the value is used as key, the fully
/// qualified name (only for RecordNameStrategy), or the schema needs to be provided.
#[derive(Clone, Debug)]
pub enum SubjectNameStrategy {
    RecordNameStrategy(String),
    TopicNameStrategy(String, bool),
    TopicRecordNameStrategy(String, String),
    RecordNameStrategyWithSchema(Box<SuppliedSchema>),
    TopicNameStrategyWithSchema(String, bool, Box<SuppliedSchema>),
    TopicRecordNameStrategyWithSchema(String, Box<SuppliedSchema>),
}

/// Just analyses the bytes which are contained in the key or value of an kafka record. When valid
/// it will return the id and the data bytes. The way schema registry messages are encoded is
/// starting with a zero, with the next 4 bytes having the id. The other bytes are the encoded
/// message.
pub fn get_bytes_result(bytes: Option<&[u8]>) -> BytesResult {
    match bytes {
        None => BytesResult::Null,
        Some(p) if p.len() > 4 && p[0] == 0 => {
            let mut buf = &p[1..5];
            let id = buf.read_u32::<BigEndian>().unwrap();
            BytesResult::Valid(id, p[5..].to_owned())
        }
        Some(p) => BytesResult::Invalid(p[..].to_owned()),
    }
}

/// Creates payload that can be included as a key or value on a kafka record
pub fn get_payload(id: u32, encoded_bytes: Vec<u8>) -> Vec<u8> {
    let mut payload = vec![0u8];
    let mut buf = [0u8; 4];
    BigEndian::write_u32(&mut buf, id);
    payload.extend_from_slice(&buf);
    payload.extend_from_slice(encoded_bytes.as_slice());
    payload
}

/// Gets a schema by an id. This is used to get the correct schema te deserialize bytes, with the
/// id that is encoded in the bytes.
pub fn get_schema_by_id(id: u32, sr_settings: &SrSettings) -> Result<RegisteredSchema, SRCError> {
    let raw_schema = perform_sr_call(sr_settings, SrCall::GetById(id))?;
    raw_to_registered_schema(raw_schema, Option::from(id))
}

pub fn get_schema_by_id_and_type(
    id: u32,
    sr_settings: &SrSettings,
    schema_type: SchemaType,
) -> Result<RegisteredSchema, SRCError> {
    match get_schema_by_id(id, sr_settings) {
        Ok(v) if v.schema_type == schema_type => Ok(v),
        Ok(v) => Err(SRCError::non_retryable_without_cause(&*format!(
            "type {:?}, is not correct",
            v.schema_type
        ))),
        Err(e) => Err(e),
    }
}

/// Gets the schema and the id by supplying a SubjectNameStrategy. This is used to correctly
/// transform a vector to bytes.
pub fn get_schema_by_subject(
    sr_settings: &SrSettings,
    subject_name_strategy: &SubjectNameStrategy,
) -> Result<RegisteredSchema, SRCError> {
    let subject = get_subject(subject_name_strategy)?;
    match get_schema(subject_name_strategy) {
        None => {
            let raw_schema = perform_sr_call(sr_settings, SrCall::GetLatest(&*subject))?;
            raw_to_registered_schema(raw_schema, None)
        }
        Some(v) => post_schema(sr_settings, subject, v),
    }
}

pub fn get_referenced_schema(
    sr_settings: &SrSettings,
    registered_reference: &RegisteredReference,
) -> Result<RegisteredSchema, SRCError> {
    let raw_schema = perform_sr_call(
        sr_settings,
        SrCall::GetBySubjectAndVersion(
            &*registered_reference.subject,
            registered_reference.version,
        ),
    )?;
    raw_to_registered_schema(raw_schema, None)
}

/// Helper function to get the schema from the strategy.
fn get_schema(subject_name_strategy: &SubjectNameStrategy) -> Option<SuppliedSchema> {
    match subject_name_strategy {
        SubjectNameStrategy::RecordNameStrategy(_) => None,
        SubjectNameStrategy::TopicNameStrategy(_, _) => None,
        SubjectNameStrategy::TopicRecordNameStrategy(_, _) => None,
        SubjectNameStrategy::RecordNameStrategyWithSchema(s) => Some(*s.clone()),
        SubjectNameStrategy::TopicNameStrategyWithSchema(_, _, s) => Some(*s.clone()),
        SubjectNameStrategy::TopicRecordNameStrategyWithSchema(_, s) => Some(*s.clone()),
    }
}

/// Gets the subject part which is also used as key to cache the results. It's constructed so that
/// it's compatible with the Java client.
pub fn get_subject(subject_name_strategy: &SubjectNameStrategy) -> Result<String, SRCError> {
    match subject_name_strategy {
        SubjectNameStrategy::RecordNameStrategy(rn) => Ok(rn.clone()),
        SubjectNameStrategy::TopicNameStrategy(t, is_key) => {
            if *is_key {
                Ok(format!("{}-key", t))
            } else {
                Ok(format!("{}-value", t))
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategy(t, rn) => Ok(format!("{}-{}", t, rn)),
        SubjectNameStrategy::RecordNameStrategyWithSchema(s) => match &s.name {
            None => Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema",
            )),
            Some(n) => Ok(n.clone()),
        },
        SubjectNameStrategy::TopicNameStrategyWithSchema(t, is_key, _) => {
            if *is_key {
                Ok(format!("{}-key", t))
            } else {
                Ok(format!("{}-value", t))
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategyWithSchema(t, s) => match &s.name {
            None => Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema",
            )),
            Some(n) => Ok(format!("{}-{}", t, n)),
        },
    }
}

fn raw_to_registered_schema(
    raw_schema: RawRegisteredSchema,
    id: Option<u32>,
) -> Result<RegisteredSchema, SRCError> {
    let id = match id {
        Some(v) => v,
        None => match raw_schema.id {
            Some(v) => v,
            None => {
                return Err(SRCError::non_retryable_without_cause(
                    "Could not get id from response",
                ));
            }
        },
    };
    let schema_type = match raw_schema.schema_type {
        Some(s) if s == "AVRO" => SchemaType::Avro,
        Some(s) if s == "PROTOBUF" => SchemaType::Protobuf,
        Some(s) if s == "JSON" => SchemaType::Json,
        Some(s) => SchemaType::Other(s),
        None => SchemaType::Avro,
    };
    let schema = match raw_schema.schema {
        Some(v) => v,
        None => {
            return Err(SRCError::non_retryable_without_cause(
                "Could not get raw schema from response",
            ));
        }
    };
    let references = match raw_schema.references {
        None => Vec::new(),
        Some(v) => v,
    };
    Ok(RegisteredSchema {
        id,
        schema_type,
        schema,
        references,
    })
}

/// Handles posting the schema, and getting back the id. When the schema is already in the schema
/// registry, the matching id is returned. When it's not it depends on the settings of the schema
/// registry. The default config will check if the schema is backwards compatible. One of the ways
/// to do this is to add a default value for new fields.
pub fn post_schema(
    sr_settings: &SrSettings,
    subject: String,
    schema: SuppliedSchema,
) -> Result<RegisteredSchema, SRCError> {
    let schema_type = match &schema.schema_type {
        SchemaType::Avro => String::from("AVRO"),
        SchemaType::Protobuf => String::from("PROTOBUF"),
        SchemaType::Json => String::from("JSON"),
        SchemaType::Other(v) => v.clone(),
    };
    let references: Vec<RegisteredReference> = match schema
        .references
        .into_iter()
        .map(|r| post_reference(sr_settings, &*schema_type, r))
        .collect()
    {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                "Error posting a reference",
            ));
        }
    };
    let body = get_body(&*schema_type, &*schema.schema, &*references);
    let id = call_and_get_id(sr_settings, SrCall::PostNew(&*subject, &*body))?;
    Ok(RegisteredSchema {
        id,
        schema_type: schema.schema_type,
        schema: schema.schema,
        references,
    })
}

fn get_body(schema_type: &str, schema: &str, references: &[RegisteredReference]) -> String {
    let mut root_element = Map::new();
    root_element.insert(String::from("schema"), Value::String(String::from(schema)));
    root_element.insert(
        String::from("schemaType"),
        Value::String(String::from(schema_type)),
    );
    if !references.is_empty() {
        let values: Vec<Value> = references.iter().map(|x| json!(x)).collect();
        root_element.insert(String::from("references"), Value::Array(values));
    }
    let schema_element = Value::Object(root_element);
    schema_element.to_string()
}

fn call_and_get_id(sr_setting: &SrSettings, sr_call: SrCall) -> Result<u32, SRCError> {
    let raw_schema = perform_sr_call(sr_setting, sr_call)?;
    match raw_schema.id {
        Some(v) => Ok(v),
        None => Err(SRCError::non_retryable_without_cause(&*format!(
            "Could not get id from response for {:?}",
            sr_call
        ))),
    }
}

fn call_and_get_version(sr_setting: &SrSettings, sr_call: SrCall) -> Result<u32, SRCError> {
    let raw_schema = perform_sr_call(sr_setting, sr_call)?;
    match raw_schema.version {
        Some(v) => Ok(v),
        None => Err(SRCError::non_retryable_without_cause(&*format!(
            "Could not get version from response for {:?}",
            sr_call
        ))),
    }
}

fn post_reference(
    sr_settings: &SrSettings,
    schema_type: &str,
    reference: SuppliedReference,
) -> Result<RegisteredReference, SRCError> {
    let references: Vec<RegisteredReference> = match reference
        .references
        .into_iter()
        .map(|r| post_reference(sr_settings, &*schema_type, r))
        .collect()
    {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                "Error posting a reference",
            ));
        }
    };
    let body = get_body(schema_type, &*reference.schema, &*references);
    perform_sr_call(sr_settings, SrCall::PostNew(&*reference.subject, &*body))?;
    let version = call_and_get_version(
        sr_settings,
        SrCall::PostForVersion(&*reference.subject, &*body),
    )?;
    Ok(RegisteredReference {
        name: reference.name,
        subject: reference.subject,
        version,
    })
}

#[derive(Debug, Clone, Copy)]
enum SrCall<'a> {
    GetById(u32),
    GetLatest(&'a str),
    GetBySubjectAndVersion(&'a str, u32),
    PostNew(&'a str, &'a str),
    PostForVersion(&'a str, &'a str),
}

fn url_for_call(call: &SrCall, base_url: &str) -> String {
    match call {
        SrCall::GetById(id) => format!("{}/schemas/ids/{}", base_url, id),
        SrCall::GetLatest(subject) => format!("{}/subjects/{}/versions/latest", base_url, subject),
        SrCall::GetBySubjectAndVersion(subject, version) => {
            format!("{}/subjects/{}/versions/{}", base_url, subject, version)
        }
        SrCall::PostNew(subject, _) => format!("{}/subjects/{}/versions", base_url, subject),
        SrCall::PostForVersion(subject, _) => format!("{}/subjects/{}", base_url, subject),
    }
}

fn perform_sr_call(
    sr_settings: &SrSettings,
    sr_call: SrCall,
) -> Result<RawRegisteredSchema, SRCError> {
    let url_count = sr_settings.urls.len();
    let mut n = 0;
    loop {
        let result = perform_single_sr_call(&sr_settings.urls[n], &sr_settings.client, sr_call);
        if result.is_ok() || n + 1 == url_count {
            break result;
        }
        n += 1
    }
}

fn perform_single_sr_call(
    base_url: &str,
    client: &Client,
    sr_call: SrCall,
) -> Result<RawRegisteredSchema, SRCError> {
    let url = url_for_call(&sr_call, base_url);
    let call = match sr_call {
        SrCall::GetById(_) | SrCall::GetLatest(_) | SrCall::GetBySubjectAndVersion(_, _) => {
            client.get(&url).send()
        }
        SrCall::PostNew(_, body) | SrCall::PostForVersion(_, body) => client
            .post(&url)
            .body(String::from(body))
            .header(CONTENT_TYPE, "application/vnd.schemaregistry.v1+json")
            .header(ACCEPT, "application/vnd.schemaregistry.v1+json")
            .send(),
    };
    match call {
        Ok(v) => match v.json::<RawRegisteredSchema>() {
            Ok(r) => Ok(r),
            Err(e) => Err(SRCError::non_retryable_with_cause(
                e,
                "could not parse to RawRegisteredSchema",
            )),
        },
        Err(e) => Err(SRCError::retryable_with_cause(
            e,
            "http call to schema registry failed",
        )),
    }
}

/// Error struct which makes it easy to know if the resulting error is also preserved in the cache
/// or not. And whether trying it again might not cause an error.
#[derive(Debug, PartialEq, Fail)]
pub struct SRCError {
    pub error: String,
    pub cause: Option<String>,
    pub retriable: bool,
    pub cached: bool,
}

/// Implements clone so when an error is returned from the cache, a copy can be returned
impl Clone for SRCError {
    fn clone(&self) -> SRCError {
        let side = match &self.cause {
            Some(v) => Some(String::from(v.deref())),
            None => None,
        };
        SRCError {
            error: String::from(self.error.deref()),
            cause: side,
            retriable: self.retriable,
            cached: self.cached,
        }
    }
}

/// Gives the information from the error in a readable format.
impl fmt::Display for SRCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.cause {
            Some(cause) => write!(
                f,
                "Error: {}, was cause by {}, it's retriable: {}, it's cached: {}",
                self.error, &cause, self.retriable, self.cached
            ),
            None => write!(
                f,
                "Error: {} had no other cause, it's retriable: {}, it's cached: {}",
                self.error, self.retriable, self.cached
            ),
        }
    }
}

/// Specific error from which can be determined whether retrying might not lead to an error and
/// whether the error is cashed, it's turned into the cashed variant when it's put into the cache.
impl SRCError {
    pub fn new(error: &str, cause: Option<String>, retriable: bool) -> SRCError {
        SRCError {
            error: error.to_owned(),
            cause,
            retriable,
            cached: false,
        }
    }
    pub fn retryable_with_cause<T: Display>(cause: T, error: &str) -> SRCError {
        SRCError::new(error, Some(format!("{}", cause)), true)
    }
    pub fn non_retryable_with_cause<T: Display>(cause: T, error: &str) -> SRCError {
        SRCError::new(error, Some(format!("{}", cause)), false)
    }
    pub fn non_retryable_without_cause(error: &str) -> SRCError {
        SRCError::new(error, None, false)
    }
    /// Should be called before putting the error in the cache
    pub fn into_cache(self) -> SRCError {
        SRCError {
            error: self.error,
            cause: self.cause,
            retriable: self.retriable,
            cached: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema_registry::{get_subject, SRCError, SchemaType, SrSettingsBuilder, SubjectNameStrategy, SuppliedSchema, get_schema_by_id};
    use mockito::{mock, server_address};
    use std::time::Duration;

    #[test]
    fn display_record_name_strategy() {
        let sns = SubjectNameStrategy::RecordNameStrategy(String::from("bla"));
        assert_eq!(
            "RecordNameStrategy(\"bla\")".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_topic_name_strategy() {
        let sns = SubjectNameStrategy::TopicNameStrategy(String::from("bla"), true);
        assert_eq!(
            "TopicNameStrategy(\"bla\", true)".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_topic_record_name_strategy() {
        let sns =
            SubjectNameStrategy::TopicRecordNameStrategy(String::from("bla"), String::from("foo"));
        assert_eq!(
            "TopicRecordNameStrategy(\"bla\", \"foo\")".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_error_no_cause() {
        let err = SRCError::new("Could not get id from response", None, false);
        assert_eq!(format!("{}", err), "Error: Could not get id from response had no other cause, it\'s retriable: false, it\'s cached: false".to_owned())
    }

    #[test]
    fn display_error_with_cause() {
        let err = SRCError::new(
            "Could not get id from response",
            Some(String::from("error in response")),
            false,
        );
        assert_eq!(format!("{}", err), "Error: Could not get id from response, was cause by error in response, it\'s retriable: false, it\'s cached: false".to_owned())
    }

    #[test]
    fn error_when_name_mandatory() {
        let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
            String::from("someTopic"),
            Box::from(SuppliedSchema {
                name: None,
                schema_type: SchemaType::Other(String::from("foo")),
                schema: "".to_string(),
                references: vec![],
            }),
        );

        let result = get_subject(&strategy);

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema"
            ))
        );
    }

    #[test]
    fn put_correct_url_as_second_check_header_set() {
        let _m = mock("GET", "/schemas/ids/1")
            .match_header("foo", "bar")
            .match_header("authorization", "Bearer some_json_web_token_for_example")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettingsBuilder::new(String::from("bogus://test"))
            .add_url((&*format!("http://{}", server_address())).parse().unwrap())
            .set_authorization("Bearer some_json_web_token_for_example")
            .add_header("foo", "bar")
            .set_timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let result = get_schema_by_id(1, &sr_settings);

        match result {
            Ok(v) => assert_eq!(v.schema, String::from(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#)),
            _ => panic!()
        }
    }
}
