//! Implementations for JSON based Schemas.
//!
//! **NOTE**: Requires the `json` feature
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::stream::{self, StreamExt};
use serde_json::Value;
use url::Url;
use valico::json_schema::schema::ScopedSchema;
use valico::json_schema::Scope;

use crate::async_impl::schema_registry::{
    get_referenced_schema, get_schema_by_id_and_type, get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::json_common::{fetch_fallback, fetch_id, handle_validation, to_bytes, to_value};
use crate::schema_registry_common::{
    get_bytes_result, invalid_bytes_error, BytesResult, RegisteredReference, RegisteredSchema,
    SchemaType, SubjectNameStrategy,
};

/// Encoder that works by prepending the correct bytes in order to make it valid schema registry
/// bytes. Ideally you want to make sure the bytes are based on the exact schema used for encoding
/// but you need a protobuf struct that has introspection to make that work, and both protobuf and
/// prost don't support that at the moment.
#[derive(Debug)]
pub struct JsonEncoder<'a> {
    sr_settings: SrSettings,
    direct_cache: DashMap<String, Arc<JsonSchema>>,
    cache: DashMap<String, SharedFutureSchema<'a>>,
}

type SharedFutureSchema<'a> = Shared<BoxFuture<'a, Result<Arc<JsonSchema>, SRCError>>>;

impl<'a> JsonEncoder<'a> {
    /// Creates a new json encoder
    pub fn new(sr_settings: SrSettings) -> JsonEncoder<'a> {
        JsonEncoder {
            sr_settings,
            direct_cache: DashMap::new(),
            cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache, useful to retry failed encodings once
    /// the underlying problem has been fixed. Retriable errors aren't cached in the first place.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
    }
    /// Encodes the bytes by adding a few bytes to the message with additional information. The full
    /// names is the optional package followed with the message name, and optionally inner messages.
    pub async fn encode(
        &self,
        value: &Value,
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        let schema = &*self.get_schema(key, subject_name_strategy).await?;
        let id = schema.id;
        validate(schema.clone(), value)?;
        to_bytes(id, value)
    }

    async fn get_schema(
        &self,
        key: String,
        subject_name_strategy: SubjectNameStrategy,
    ) -> Result<Arc<JsonSchema>, SRCError> {
        match self.direct_cache.get(&key) {
            None => {
                let result = self
                    .get_schema_by_shared_future(key.clone(), subject_name_strategy)
                    .await;
                match result {
                    Ok(v) => {
                        if !self.direct_cache.contains_key(&key) {
                            self.direct_cache.insert(key.clone(), v.clone());
                            self.cache.remove(&key);
                        }
                        Ok(v)
                    }
                    // Retriable errors (transient network/HTTP issues) don't make sense to
                    // cache, so the entry is dropped and the next call issues a fresh request
                    // rather than replaying a stale failure.
                    Err(e) if e.retriable => {
                        self.cache.remove(&key);
                        Err(e)
                    }
                    Err(e) => Err(e.into_cache()),
                }
            }
            Some(result) => Ok(result.value().clone()),
        }
    }

    fn get_schema_by_shared_future(
        &self,
        key: String,
        subject_name_strategy: SubjectNameStrategy,
    ) -> SharedFutureSchema<'a> {
        match self.cache.entry(key) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let v = async move {
                    match get_schema_by_subject(&sr_settings, &subject_name_strategy).await {
                        Ok(schema) => match to_json_schema(&sr_settings, None, schema).await {
                            Ok(s) => Ok(Arc::new(s)),
                            Err(e) => Err(e),
                        },
                        Err(e) => Err(e),
                    }
                }
                .boxed()
                .shared();
                e.insert(v).value().clone()
            }
        }
    }
}

fn add_refs_to_scope(scope: &mut Scope, schema: JsonSchema) -> Result<ScopedSchema<'_>, SRCError> {
    for reference in schema.references.into_iter() {
        add_refs_to_scope(scope, reference)?;
    }
    match scope.compile_and_return_with_id(&schema.url, schema.schema, false) {
        Ok(v) => Ok(v),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "error compiling schema",
        )),
    }
}

pub fn validate(schema: JsonSchema, value: &Value) -> Result<(), SRCError> {
    let mut scope = Scope::new();
    let schema = add_refs_to_scope(&mut scope, schema)?;
    let validation = schema.validate(value);
    handle_validation(validation, value)
}

/// Schema as retrieved from the schema registry. It's close to the json received and doesn't do
/// type specific transformations.
///
/// `subject` and `version` are populated when the registry response includes them. See
/// [`crate::schema_registry_common::RegisteredSchema`] for the caveats — a schema id can be
/// registered against multiple subjects.
///
/// `properties` and `tags` carry the Confluent `metadata` block verbatim as returned by the
/// registry: `tags` is keyed by schema path (e.g. `io.confluent.field.<name>`) with the list of
/// tags on that path. Each is populated when the registry response includes it.
#[derive(Clone, Debug)]
pub struct JsonSchema {
    pub id: u32,
    pub url: Url,
    pub schema: Value,
    pub references: Vec<JsonSchema>,
    pub subject: Option<String>,
    pub version: Option<u32>,
    pub properties: Option<HashMap<String, String>>,
    pub tags: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug)]
pub struct JsonDecoder<'a> {
    sr_settings: SrSettings,
    direct_cache: DashMap<u32, Arc<JsonSchema>>,
    cache: DashMap<u32, SharedFutureSchema<'a>>,
}

impl<'a> JsonDecoder<'a> {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. Retriable errors (e.g. a transient network/HTTP failure) are never cached,
    /// so those are simply retried on the next call. Non-retriable errors (e.g. a schema that
    /// doesn't exist or can't be parsed) are cached to avoid repeatedly retrying a lookup that
    /// isn't going to succeed; if the underlying problem gets fixed you can use
    /// remove_errors_from_cache to clean those out.
    pub fn new(sr_settings: SrSettings) -> JsonDecoder<'a> {
        JsonDecoder {
            sr_settings,
            direct_cache: DashMap::new(),
            cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache. You might need/want to run this when the
    /// underlying problem has been fixed (e.g. a schema was missing and has since been
    /// registered) and want the next call to try again immediately. Retriable errors aren't
    /// stored in the cache in the first place, so there's nothing to remove for those.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
    }
    /// Reads the bytes to get the name, and gives back the data bytes.
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<DecodeResult>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => Ok(Some(self.deserialize(id, &bytes).await?)),
            BytesResult::Invalid(i) => Err(SRCError::non_retryable_without_cause(
                &invalid_bytes_error(&i),
            )),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    async fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<DecodeResult, SRCError> {
        let schema = &*self.get_schema(id).await?;
        match serde_json::from_slice(bytes) {
            Ok(value) => Ok(DecodeResult {
                schema: schema.clone(),
                value,
            }),
            Err(e) => Err(SRCError::non_retryable_with_cause(
                e,
                "could not create value from bytes",
            )),
        }
    }

    async fn get_schema(&self, id: u32) -> Result<Arc<JsonSchema>, SRCError> {
        match self.direct_cache.get(&id) {
            None => {
                let result = self.get_schema_by_shared_future(id).await;
                match result {
                    Ok(v) => {
                        if !self.direct_cache.contains_key(&id) {
                            self.direct_cache.insert(id, v.clone());
                            self.cache.remove(&id);
                        }
                        Ok(v)
                    }
                    // Retriable errors (transient network/HTTP issues) don't make sense to
                    // cache, so the entry is dropped and the next call issues a fresh request
                    // rather than replaying a stale failure.
                    Err(e) if e.retriable => {
                        self.cache.remove(&id);
                        Err(e)
                    }
                    Err(e) => Err(e.into_cache()),
                }
            }
            Some(result) => Ok(result.value().clone()),
        }
    }

    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn get_schema_by_shared_future(&self, id: u32) -> SharedFutureSchema<'a> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let v = async move {
                    match get_schema_by_id_and_type(id, &sr_settings, SchemaType::Json).await {
                        Ok(schema) => match to_json_schema(&sr_settings, None, schema).await {
                            Ok(v) => Ok(Arc::new(v)),
                            Err(e) => Err(e),
                        },
                        Err(e) => Err(e),
                    }
                }
                .boxed()
                .shared();
                e.insert(v).value().clone()
            }
        }
    }
}

fn reference_url(rr: &RegisteredReference) -> Result<Url, SRCError> {
    match Url::from_str(&rr.name) {
        Ok(v) => Ok(v),
        Err(e) => Err(SRCError::non_retryable_with_cause(e, &format!("reference schema with subject {} and version {} has invalid id {}, it has to be a fully qualified url", rr.subject, rr.version, rr.name)))
    }
}

fn main_url(schema: &Value, sr_settings: &SrSettings, id: u32) -> Url {
    match fetch_id(schema) {
        Some(url) => url,
        None => fetch_fallback(sr_settings.url(), id),
    }
}

fn to_json_schema(
    sr_settings: &SrSettings,
    optional_url: Option<Url>,
    registered_schema: RegisteredSchema,
) -> BoxFuture<Result<JsonSchema, SRCError>> {
    async move {
        let refs: Result<Vec<JsonSchema>, SRCError> = stream::iter(registered_schema.references)
            .then(|rr| async move {
                let url = reference_url(&rr)?;
                let rs = get_referenced_schema(sr_settings, &rr).await?;
                to_json_schema(sr_settings, Some(url), rs).await
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect();
        let references = refs?;
        let schema: Value = to_value(&registered_schema.schema)?;
        let url = match optional_url {
            Some(v) => v,
            None => main_url(&schema, sr_settings, registered_schema.id),
        };
        Ok(JsonSchema {
            id: registered_schema.id,
            url,
            schema,
            references,
            subject: registered_schema.subject,
            version: registered_schema.version,
            properties: registered_schema.properties,
            tags: registered_schema.tags,
        })
    }
    .boxed()
}

/// This decode result is not validated yet, if you want to validate you need to call the validate
/// function.
#[derive(Debug)]
pub struct DecodeResult {
    pub schema: JsonSchema,
    pub value: Value,
}

#[cfg(test)]
mod tests {
    use mockito::Server;
    use std::fs::{read_to_string, File};

    use serde_json::Value;

    use crate::async_impl::json::{to_json_schema, validate, JsonDecoder, JsonEncoder};
    use crate::async_impl::schema_registry::SrSettings;
    use crate::schema_registry_common::{
        get_payload, RegisteredSchema, SchemaType, SubjectNameStrategy,
    };
    use test_utils::{
        get_json_body, get_json_body_with_reference, json_get_result_references,
        json_incorrect_bytes, json_result_java_bytes, json_result_schema,
        json_result_schema_with_id, json_test_ref_schema,
    };

    #[tokio::test]
    async fn to_json_schema_propagates_subject_and_version() {
        let registered_schema = RegisteredSchema {
            id: 7,
            schema_type: SchemaType::Json,
            schema: r#"{"type":"object"}"#.to_string(),
            references: vec![],
            properties: None,
            tags: None,
            subject: Some("orders-value".to_string()),
            version: Some(3),
        };
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let json_schema = to_json_schema(&sr_settings, None, registered_schema)
            .await
            .expect("conversion succeeds for empty references");
        assert_eq!(json_schema.id, 7);
        assert_eq!(json_schema.subject.as_deref(), Some("orders-value"));
        assert_eq!(json_schema.version, Some(3));
    }

    #[tokio::test]
    async fn decode_exposes_registry_metadata() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(
                r#"{"schemaType":"JSON","schema":"{\"$id\":\"https://example.com/customer.json\",\"type\":\"object\",\"properties\":{\"ssn\":{\"type\":\"string\"}}}","metadata":{"properties":{"owner":"identity-team"},"tags":{"io.confluent.field.ssn":["PII"]}}}"#,
            )
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        // wire bytes: magic 0, schema id 1, JSON value payload
        let payload = get_payload(1, br#"{"ssn":"123-45-6789"}"#.to_vec());
        let schema = decoder
            .decode(Some(&*payload))
            .await
            .unwrap()
            .unwrap()
            .schema;

        assert_eq!(
            schema.properties.as_ref().unwrap()["owner"],
            "identity-team"
        );
        assert_eq!(
            schema.tags.as_ref().unwrap()["io.confluent.field.ssn"],
            vec!["PII".to_string()]
        );
    }

    #[tokio::test]
    async fn test_encode_java_compatibility() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, strategy).await.unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }

    #[tokio::test]
    async fn test_encode_schema_with_id() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema_with_id(), 10))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, strategy).await.unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }

    #[tokio::test]
    async fn test_encode_clean_cache() {
        let mut server = Server::new_async().await;

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, strategy.clone()).await;
        assert!(encoded_data.is_err());

        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let encoded_data = encoder.encode(&result_example, strategy.clone()).await;
        assert!(encoded_data.is_err());

        encoder.remove_errors_from_cache();

        let encoded_data = encoder.encode(&result_example, strategy).await;
        assert!(encoded_data.is_ok());
    }

    #[tokio::test]
    async fn test_decoder_default() {
        let mut server = Server::new_async().await;
        let result_value: String = read_to_string("tests/schema/result-example.json")
            .unwrap()
            .parse()
            .unwrap();
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 7))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        let message = decoder
            .decode(Some(&*get_payload(7, result_value.into_bytes())))
            .await
            .unwrap()
            .unwrap();
        validate(message.schema, &message.value).unwrap();
        assert_eq!(
            "string",
            message
                .value
                .as_object()
                .unwrap()
                .get("down")
                .unwrap()
                .as_str()
                .unwrap()
        );
        assert_eq!(
            "STRING",
            message
                .value
                .as_object()
                .unwrap()
                .get("up")
                .unwrap()
                .as_str()
                .unwrap()
        )
    }

    #[tokio::test]
    async fn test_decoder_clean_cache() {
        let mut server = Server::new_async().await;
        let result_value: String = read_to_string("tests/schema/result-example.json")
            .unwrap()
            .parse()
            .unwrap();

        let bytes = result_value.into_bytes();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        let error = decoder
            .decode(Some(&*get_payload(7, bytes.clone())))
            .await
            .unwrap_err();
        assert!(error.cached);

        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 7))
            .create();

        let error = decoder
            .decode(Some(&*get_payload(7, bytes.clone())))
            .await
            .unwrap_err();
        assert!(error.cached);

        decoder.remove_errors_from_cache();

        let message = decoder
            .decode(Some(&*get_payload(7, bytes)))
            .await
            .unwrap()
            .unwrap();
        assert!(message.value.is_object());
        validate(message.schema, &message.value).unwrap();
    }

    #[tokio::test]
    async fn test_decoder_java_compatibility() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/10?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        let message = decoder
            .decode(Some(json_result_java_bytes()))
            .await
            .unwrap()
            .unwrap();
        validate(message.schema, &message.value).unwrap();
        assert_eq!(
            "string",
            message
                .value
                .as_object()
                .unwrap()
                .get("down")
                .unwrap()
                .as_str()
                .unwrap()
        );
        assert_eq!(
            "STRING",
            message
                .value
                .as_object()
                .unwrap()
                .get("up")
                .unwrap()
                .as_str()
                .unwrap()
        )
    }

    #[tokio::test]
    async fn test_decoder_value_can_not_be_read() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/10?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        let result = decoder
            .decode(Some(json_incorrect_bytes()))
            .await
            .unwrap_err();
        assert_eq!(
            String::from("could not create value from bytes"),
            result.error
        )
    }

    #[tokio::test]
    async fn add_referred_schema() {
        let mut server = Server::new_async().await;
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        let bytes = [
            0, 0, 0, 0, 5, 123, 34, 105, 100, 34, 58, 91, 52, 53, 44, 45, 55, 57, 44, 57, 51, 44,
            49, 49, 50, 44, 52, 54, 44, 56, 55, 44, 55, 57, 44, 45, 50, 44, 45, 49, 48, 55, 44, 45,
            54, 57, 44, 55, 48, 44, 45, 56, 44, 49, 48, 48, 44, 49, 50, 54, 44, 54, 56, 44, 45, 50,
            49, 93, 44, 34, 98, 121, 34, 58, 34, 74, 97, 118, 97, 34, 44, 34, 99, 111, 117, 110,
            116, 101, 114, 34, 58, 49, 44, 34, 105, 110, 112, 117, 116, 34, 58, 34, 83, 116, 114,
            105, 110, 103, 34, 44, 34, 114, 101, 115, 117, 108, 116, 115, 34, 58, 91, 123, 34, 117,
            112, 34, 58, 34, 83, 84, 82, 73, 78, 71, 34, 44, 34, 100, 111, 119, 110, 34, 58, 34,
            115, 116, 114, 105, 110, 103, 34, 125, 93, 125,
        ];

        let _m = server
            .mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(
                json_test_ref_schema(),
                5,
                json_get_result_references(),
            ))
            .create();
        let _m = server
            .mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 4))
            .create();

        let value = decoder.decode(Some(&bytes)).await.unwrap().unwrap().value;
        match value {
            Value::Object(v) => {
                assert_eq!(&Value::String(String::from("Java")), v.get("by").unwrap())
            }
            _ => panic!("Not an Object that was expected"),
        }
    }

    #[tokio::test]
    async fn error_in_referred_schema() {
        let mut server = Server::new_async().await;
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        let bytes = [
            0, 0, 0, 0, 5, 123, 34, 105, 100, 34, 58, 91, 52, 53, 44, 45, 55, 57, 44, 57, 51, 44,
            49, 49, 50, 44, 52, 54, 44, 56, 55, 44, 55, 57, 44, 45, 50, 44, 45, 49, 48, 55, 44, 45,
            54, 57, 44, 55, 48, 44, 45, 56, 44, 49, 48, 48, 44, 49, 50, 54, 44, 54, 56, 44, 45, 50,
            49, 93, 44, 34, 98, 121, 34, 58, 34, 74, 97, 118, 97, 34, 44, 34, 99, 111, 117, 110,
            116, 101, 114, 34, 58, 49, 44, 34, 105, 110, 112, 117, 116, 34, 58, 34, 83, 116, 114,
            105, 110, 103, 34, 44, 34, 114, 101, 115, 117, 108, 116, 115, 34, 58, 91, 123, 34, 117,
            112, 34, 58, 34, 83, 84, 82, 73, 78, 71, 34, 44, 34, 100, 111, 119, 110, 34, 58, 34,
            115, 116, 114, 105, 110, 103, 34, 125, 93, 125,
        ];

        let _m = server
            .mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(
                json_test_ref_schema(),
                5,
                json_get_result_references(),
            ))
            .create();
        let _m = server
            .mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(&json_result_schema()[0..30], 4))
            .create();

        let error = decoder.decode(Some(&bytes)).await.unwrap_err();
        assert!(error.error.starts_with("could not parse schema {"))
    }

    #[tokio::test]
    async fn encounter_same_reference_again() {
        let mut server = Server::new_async().await;
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);
        let bytes_id_5 = [
            0, 0, 0, 0, 5, 123, 34, 105, 100, 34, 58, 91, 52, 53, 44, 45, 55, 57, 44, 57, 51, 44,
            49, 49, 50, 44, 52, 54, 44, 56, 55, 44, 55, 57, 44, 45, 50, 44, 45, 49, 48, 55, 44, 45,
            54, 57, 44, 55, 48, 44, 45, 56, 44, 49, 48, 48, 44, 49, 50, 54, 44, 54, 56, 44, 45, 50,
            49, 93, 44, 34, 98, 121, 34, 58, 34, 74, 97, 118, 97, 34, 44, 34, 99, 111, 117, 110,
            116, 101, 114, 34, 58, 49, 44, 34, 105, 110, 112, 117, 116, 34, 58, 34, 83, 116, 114,
            105, 110, 103, 34, 44, 34, 114, 101, 115, 117, 108, 116, 115, 34, 58, 91, 123, 34, 117,
            112, 34, 58, 34, 83, 84, 82, 73, 78, 71, 34, 44, 34, 100, 111, 119, 110, 34, 58, 34,
            115, 116, 114, 105, 110, 103, 34, 125, 93, 125,
        ];
        let bytes_id_7 = [
            0, 0, 0, 0, 7, 123, 34, 105, 100, 34, 58, 91, 52, 53, 44, 45, 55, 57, 44, 57, 51, 44,
            49, 49, 50, 44, 52, 54, 44, 56, 55, 44, 55, 57, 44, 45, 50, 44, 45, 49, 48, 55, 44, 45,
            54, 57, 44, 55, 48, 44, 45, 56, 44, 49, 48, 48, 44, 49, 50, 54, 44, 54, 56, 44, 45, 50,
            49, 93, 44, 34, 98, 121, 34, 58, 34, 74, 97, 118, 97, 34, 44, 34, 99, 111, 117, 110,
            116, 101, 114, 34, 58, 49, 44, 34, 105, 110, 112, 117, 116, 34, 58, 34, 83, 116, 114,
            105, 110, 103, 34, 44, 34, 114, 101, 115, 117, 108, 116, 115, 34, 58, 91, 123, 34, 117,
            112, 34, 58, 34, 83, 84, 82, 73, 78, 71, 34, 44, 34, 100, 111, 119, 110, 34, 58, 34,
            115, 116, 114, 105, 110, 103, 34, 125, 93, 125,
        ];

        let _m = server
            .mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(
                json_test_ref_schema(),
                5,
                json_get_result_references(),
            ))
            .create();
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(
                json_test_ref_schema(),
                7,
                json_get_result_references(),
            ))
            .create();
        let _m = server
            .mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 4))
            .create();

        let value = decoder
            .decode(Some(&bytes_id_5))
            .await
            .unwrap()
            .unwrap()
            .value;
        match value {
            Value::Object(v) => {
                assert_eq!(&Value::String(String::from("Java")), v.get("by").unwrap())
            }
            _ => panic!("Not an Object that was expected"),
        }
        let value = decoder
            .decode(Some(&bytes_id_7))
            .await
            .unwrap()
            .unwrap()
            .value;
        match value {
            Value::Object(v) => {
                assert_eq!(&Value::String(String::from("Java")), v.get("by").unwrap())
            }
            _ => panic!("Not an Object that was expected"),
        }
    }

    #[test]
    fn display_encode() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let encoder = JsonEncoder::new(sr_settings);
        println!("{:?}", encoder);
        assert!(format!("{:?}", encoder).starts_with("JsonEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client { accepts: Accepts"))
    }

    #[test]
    fn display_decode() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = JsonDecoder::new(sr_settings);
        println!("{:?}", decoder);
        assert!(
                   format!("{:?}", decoder).starts_with("JsonDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client { accepts: Accepts")
        )
    }

    #[tokio::test]
    async fn test_encode_not_valid() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value = Value::String(String::from("Foo"));

        let error = encoder.encode(&result_example, strategy).await.unwrap_err();

        assert_eq!(
            error.error,
            String::from(
                r#"Value "Foo" was not valid according to the schema because [WrongType { path: "", detail: "The value must be object" }]"#
            )
        )
    }

    #[tokio::test]
    async fn test_encode_missing_ref() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_test_ref_schema(), 10))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/jsontest-example.json").unwrap())
                .unwrap();

        let error = encoder.encode(&result_example, strategy).await.unwrap_err();

        assert!(error
            .error
            .ends_with("was not valid because of missing references"))
    }

    #[tokio::test]
    async fn decode_invalid_bytes() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = JsonDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[1, 0])).await.unwrap_err();
        assert_eq!(String::from("Invalid bytes: payload too short (2 bytes), expected at least 5 bytes for confluent schema registry wire format"), result.error)
    }
}
