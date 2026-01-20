//! This module contains the code specific for the schema registry.

use std::collections::HashMap;
use std::str;
use std::time::Duration;

use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{self, StreamExt};
use reqwest::header::{HeaderName, ACCEPT, CONTENT_TYPE};
use reqwest::{header, RequestBuilder, Response};
use reqwest::{Client, ClientBuilder};
use serde_json::{json, Map, Value};

use crate::error::SRCError;
use crate::schema_registry_common::{
    url_for_call, RawError, RawRegisteredSchema, RegisteredReference, RegisteredSchema, SchemaType,
    SrAuthorization, SrCall, SubjectNameStrategy, SuppliedReference, SuppliedSchema,
};

/// Settings used to do the calls to schema registry. For simple cases you can use `SrSettings::new`
/// or the `SrSettingsBuilder`. But you can also use it directly so you can all the available
/// settings from reqwest.
#[derive(Debug, Clone)]
pub struct SrSettings {
    urls: Vec<String>,
    client: Client,
    authorization: SrAuthorization,
}

/// Struct to create an SrSettings when used with multiple url's, authorization, custom headers, or
/// custom timeout.
pub struct SrSettingsBuilder {
    urls: Vec<String>,
    authorization: SrAuthorization,
    headers: DashMap<String, String>,
    proxy: Option<String>,
    no_proxy: bool,
    timeout: Duration,
}

/// Creates a new SrSettings struct that is needed to make calls to the schema registry
/// ```
/// use schema_registry_converter::async_impl::schema_registry::SrSettings;
/// let sr_settings = SrSettings::new(String::from("http://localhost:8081"));
/// ```
impl SrSettings {
    /// Will create a new SrSettings with default values, the url should be fully qualified like
    /// `"http://localhost:8081"`.
    pub fn new(url: String) -> SrSettings {
        SrSettings {
            urls: vec![url],
            client: Client::new(),
            authorization: SrAuthorization::None,
        }
    }

    /// Will create a new SrSettingsBuilder with default values, the url should be fully qualified
    /// like `"http://localhost:8081"`.
    pub fn new_builder(url: String) -> SrSettingsBuilder {
        SrSettingsBuilder {
            urls: vec![url],
            authorization: SrAuthorization::None,
            headers: DashMap::new(),
            proxy: None,
            no_proxy: false,
            timeout: Duration::from_secs(30),
        }
    }

    pub(crate) fn url(&self) -> &str {
        &self.urls[0]
    }
}

/// Builder for SrSettings
/// ```
/// use schema_registry_converter::async_impl::schema_registry::{SrSettings};
/// use std::time::Duration;
/// let sr_settings = SrSettings::new_builder(String::from("http://localhost:8081"))
///     .add_url(String::from("http://localhost:8082"))
///     .set_token_authorization("some_json_web_token_for_example")
///     .add_header("foo", "bar")
///     .set_proxy("http://localhost:8888")
///     .set_timeout(Duration::from_secs(5))
///     .build().unwrap();
/// ```
impl SrSettingsBuilder {
    /// Adds an url. For any call urls will be tried in order. the one used to create the settings
    /// struct first. All urls should be fully qualified.
    pub fn add_url(&mut self, url: String) -> &mut SrSettingsBuilder {
        self.urls.push(url);
        self
    }

    /// Sets the token that needs to be used to authenticate
    pub fn set_token_authorization(&mut self, token: &str) -> &mut SrSettingsBuilder {
        self.authorization = SrAuthorization::Token(String::from(token));
        self
    }

    /// Sets basic authentication, for confluent cloud, the username is the API Key and the password
    /// is the API Secret.
    pub fn set_basic_authorization(
        &mut self,
        username: &str,
        password: Option<&str>,
    ) -> &mut SrSettingsBuilder {
        self.authorization = match password {
            None => SrAuthorization::Basic(String::from(username), None),
            Some(p) => SrAuthorization::Basic(String::from(username), Some(String::from(p))),
        };
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

    /// prevents using any proxy to every call.
    pub fn no_proxy(&mut self) -> &mut SrSettingsBuilder {
        self.no_proxy = true;
        self
    }

    /// Set a timeout, it will be used for the connect and the read.
    pub fn set_timeout(&mut self, duration: Duration) -> &mut SrSettingsBuilder {
        self.timeout = duration;
        self
    }

    /// Build the settings with your own HTTP client.
    ///
    /// This method allows you to bring your own TLS client and configuration.
    ///
    /// NOTE: The other values (headers, proxy, etc.) will still be merged in
    /// and they all have higher precedence than your own builder's configuration.
    /// This means that if you set a proxy both with this builde rand your
    /// client's builder, this builder will overwrite the client's builder.
    pub fn build_with(&mut self, builder: ClientBuilder) -> Result<SrSettings, SRCError> {
        let client = self.build_client(builder)?;
        let urls = self.urls.clone();
        let authorization = self.authorization.clone();
        Ok(SrSettings {
            urls,
            client,
            authorization,
        })
    }

    /// Build the settings.
    ///
    /// If you need your own client, see `build_with`.
    pub fn build(&mut self) -> Result<SrSettings, SRCError> {
        self.build_with(Client::builder())
    }

    fn build_client(&mut self, mut builder: ClientBuilder) -> Result<Client, SRCError> {
        if !self.headers.is_empty() {
            let mut header_map = header::HeaderMap::new();
            for ref_multi in self.headers.iter() {
                let header_name = match HeaderName::from_bytes(ref_multi.key().as_bytes()) {
                    Ok(h) => h,
                    Err(e) => {
                        return Err(SRCError::non_retryable_with_cause(
                            e,
                            &format!("could not create HeaderName from {}", ref_multi.key()),
                        ));
                    }
                };
                header_map.insert(header_name, ref_multi.value().parse().unwrap());
            }
            builder = builder.default_headers(header_map);
        }
        if self.proxy.is_some() {
            match reqwest::Proxy::all(self.proxy.as_ref().unwrap()) {
                Ok(v) => builder = builder.proxy(v),
                Err(e) => return Err(SRCError::non_retryable_with_cause(e, "invalid proxy value")),
            };
        }
        if self.no_proxy {
            builder = builder.no_proxy()
        }
        builder = builder.timeout(self.timeout);
        match builder.build() {
            Ok(client) => Ok(client),
            Err(e) => Err(SRCError::non_retryable_with_cause(
                e,
                "could not create new client",
            )),
        }
    }
}

/// Gets a schema by an id. This is used to get the correct schema te deserialize bytes, with the
/// id that is encoded in the bytes.
pub async fn get_schema_by_id(
    id: u32,
    sr_settings: &SrSettings,
) -> Result<RegisteredSchema, SRCError> {
    let raw_schema = perform_sr_call(sr_settings, SrCall::GetById(id)).await?;
    raw_to_registered_schema(raw_schema, Option::from(id)).await
}

pub async fn get_schema_by_id_and_type(
    id: u32,
    sr_settings: &SrSettings,
    schema_type: SchemaType,
) -> Result<RegisteredSchema, SRCError> {
    match get_schema_by_id(id, sr_settings).await {
        Ok(v) if v.schema_type == schema_type => Ok(v),
        Ok(v) => Err(SRCError::non_retryable_without_cause(&format!(
            "type {:?}, is not correct",
            v.schema_type
        ))),
        Err(e) => Err(e),
    }
}

/// Gets the registered schema by supplying a SubjectNameStrategy. This is used to as part of the
/// encoding so we get the correct schema and id, and possible references.
pub async fn get_schema_by_subject(
    sr_settings: &SrSettings,
    subject_name_strategy: &SubjectNameStrategy,
) -> Result<RegisteredSchema, SRCError> {
    let subject = subject_name_strategy.get_subject()?;
    match subject_name_strategy.get_schema() {
        None => {
            let raw_schema = perform_sr_call(sr_settings, SrCall::GetLatest(&subject)).await?;
            raw_to_registered_schema(raw_schema, None).await
        }
        Some(v) => post_schema(sr_settings, subject, v.clone()).await,
    }
}

pub async fn get_referenced_schema(
    sr_settings: &SrSettings,
    registered_reference: &RegisteredReference,
) -> Result<RegisteredSchema, SRCError> {
    let raw_schema = perform_sr_call(
        sr_settings,
        SrCall::GetBySubjectAndVersion(&registered_reference.subject, registered_reference.version),
    )
    .await?;
    raw_to_registered_schema(raw_schema, None).await
}

async fn raw_to_registered_schema(
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
    let references = raw_schema.references.unwrap_or_default();
    let (properties, tags) = match raw_schema.metadata {
        Some(m) => (m.properties, m.tags),
        None => (None, None),
    };

    Ok(RegisteredSchema {
        id,
        schema_type,
        schema,
        references,
        properties,
        tags,
    })
}

/// Handles posting the schema, and getting back the id. When the schema is already in the schema
/// registry, the matching id is returned. When it's not it depends on the settings of the schema
/// registry. The default config will check if the schema is backwards compatible. One of the ways
/// to do this is to add a default value for new fields.
pub async fn post_schema(
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
    let references: Vec<RegisteredReference> = match stream::iter(schema.references)
        .then(|r| post_reference(sr_settings, &schema_type, r))
        .collect::<Vec<_>>()
        .await
        .into_iter()
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
    let body = get_body(
        &schema_type,
        &schema.schema,
        &references,
        schema.properties.as_ref(),
        schema.tags.as_ref(),
    )
    .await;
    let id = call_and_get_id(sr_settings, SrCall::PostNew(&subject, &body)).await?;
    Ok(RegisteredSchema {
        id,
        schema_type: schema.schema_type,
        schema: schema.schema,
        references,
        properties: schema.properties,
        tags: schema.tags,
    })
}

async fn get_body(
    schema_type: &str,
    schema: &str,
    references: &[RegisteredReference],
    properties: Option<&HashMap<String, String>>,
    tags: Option<&HashMap<String, Vec<String>>>,
) -> String {
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
    if tags.is_some() || properties.is_some() {
        let mut metadata = Map::new();
        if let Some(properties) = properties {
            let mut props = Map::new();
            for (name, value) in properties {
                props.insert(String::from(name), json! { value });
            }
            metadata.insert(String::from("properties"), Value::Object(props));
        }
        if let Some(tags) = tags {
            let mut props = Map::new();
            for (tag, values) in tags {
                let tag_value = Value::Array(values.iter().map(|v| json!(v)).collect());
                props.insert(String::from(tag), tag_value);
            }
            metadata.insert(String::from("tags"), Value::Object(props));
        }
        root_element.insert(String::from("metadata"), Value::Object(metadata));
    }
    let schema_element = Value::Object(root_element);
    schema_element.to_string()
}

async fn call_and_get_id(sr_setting: &SrSettings, sr_call: SrCall<'_>) -> Result<u32, SRCError> {
    let raw_schema = perform_sr_call(sr_setting, sr_call).await?;
    match raw_schema.id {
        Some(v) => Ok(v),
        None => Err(SRCError::non_retryable_without_cause(&format!(
            "Could not get id from response for {:?}",
            sr_call
        ))),
    }
}

async fn call_and_get_version(
    sr_setting: &SrSettings,
    sr_call: SrCall<'_>,
) -> Result<u32, SRCError> {
    let raw_schema = perform_sr_call(sr_setting, sr_call).await?;
    match raw_schema.version {
        Some(v) => Ok(v),
        None => Err(SRCError::non_retryable_without_cause(&format!(
            "Could not get version from response for {:?}",
            sr_call
        ))),
    }
}

fn post_reference<'a>(
    sr_settings: &'a SrSettings,
    schema_type: &'a str,
    reference: SuppliedReference,
) -> BoxFuture<'a, Result<RegisteredReference, SRCError>> {
    async move {
        let references: Vec<RegisteredReference> = match stream::iter(reference.references)
            .then(|r| post_reference(sr_settings, schema_type, r))
            .collect::<Vec<_>>()
            .await
            .into_iter()
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
        let body = get_body(
            schema_type,
            &reference.schema,
            &references,
            reference.properties.as_ref(),
            reference.tags.as_ref(),
        )
        .await;
        perform_sr_call(sr_settings, SrCall::PostNew(&reference.subject, &body)).await?;
        let version = call_and_get_version(
            sr_settings,
            SrCall::PostForVersion(&reference.subject, &body),
        )
        .await?;
        Ok(RegisteredReference {
            name: reference.name,
            subject: reference.subject,
            version,
            properties: reference.properties,
            tags: reference.tags,
        })
    }
    .boxed()
}

pub async fn perform_sr_call(
    sr_settings: &SrSettings,
    sr_call: SrCall<'_>,
) -> Result<RawRegisteredSchema, SRCError> {
    let url_count = sr_settings.urls.len();
    let mut n = 0;
    loop {
        let result = perform_single_sr_call(
            &sr_settings.urls[n],
            &sr_settings.client,
            &sr_settings.authorization,
            sr_call,
        )
        .await;
        if result.is_ok() || n + 1 == url_count {
            break result;
        }
        n += 1
    }
}

async fn send_with_auth(
    builder: RequestBuilder,
    authentication: &SrAuthorization,
) -> Result<Response, SRCError> {
    let res = match authentication {
        SrAuthorization::None => builder.send().await,
        SrAuthorization::Token(token) => builder.bearer_auth(token).send().await,
        SrAuthorization::Basic(username, password) => {
            let p = match password {
                None => None,
                Some(v) => Some(v),
            };
            builder.basic_auth(username, p).send().await
        }
    }
    .map_err(|e| SRCError::retryable_with_cause(e, "http call to schema registry failed"))?;
    let st = res.status();
    if st.is_success() {
        return Ok(res);
    }
    // Handle non successful HTTP Requests
    Err(match st.as_u16() {
        502 | 503 => SRCError::retryable_with_cause(
            res.json::<RawError>().await.unwrap_or_else(|_| RawError {
                error_code: 0,
                message: "couldn't parse schema registry error json".to_owned(),
            }),
            format!("HTTP request to schema registry failed with status {}", st).as_str(),
        ),
        _ => SRCError::non_retryable_with_cause(
            res.json::<RawError>().await.unwrap_or_else(|_| RawError {
                error_code: 0,
                message: "couldn't parse schema registry error json".to_owned(),
            }),
            format!("HTTP request to schema registry failed with status {}", st).as_str(),
        ),
    })
}

async fn perform_single_sr_call(
    base_url: &str,
    client: &Client,
    authentication: &SrAuthorization,
    sr_call: SrCall<'_>,
) -> Result<RawRegisteredSchema, SRCError> {
    let url = url_for_call(&sr_call, base_url);
    let builder = match sr_call {
        SrCall::GetById(_) | SrCall::GetLatest(_) | SrCall::GetBySubjectAndVersion(_, _) => {
            client.get(&url)
        }
        SrCall::PostNew(_, body) | SrCall::PostForVersion(_, body) => client
            .post(&url)
            .body(String::from(body))
            .header(CONTENT_TYPE, "application/vnd.schemaregistry.v1+json")
            .header(ACCEPT, "application/vnd.schemaregistry.v1+json"),
    };
    let successful_response = send_with_auth(builder, authentication).await?;
    successful_response
        .json::<RawRegisteredSchema>()
        .await
        .map_err(|e| {
            SRCError::non_retryable_with_cause(e, "could not parse to RawRegisteredSchema")
        })
}

pub async fn get_all_subjects(sr_settings: &SrSettings) -> Result<Vec<String>, SRCError> {
    let url_count = sr_settings.urls.len();
    let mut n = 0;
    loop {
        let result = perform_single_subjects_call(
            &sr_settings.urls[n],
            &sr_settings.client,
            &sr_settings.authorization,
        )
        .await;
        if result.is_ok() || n + 1 == url_count {
            break result;
        }
        n += 1
    }
}

async fn perform_single_subjects_call(
    base_url: &str,
    client: &Client,
    authentication: &SrAuthorization,
) -> Result<Vec<String>, SRCError> {
    let url = format!("{}/subjects", base_url);
    let builder = client.get(url);
    let successful_response = send_with_auth(builder, authentication).await?;
    successful_response
        .json::<Vec<String>>()
        .await
        .map_err(|e| SRCError::non_retryable_with_cause(e, "could not parse to list of subjects"))
}

pub async fn get_all_versions(
    sr_settings: &SrSettings,
    subject: String,
) -> Result<Vec<u32>, SRCError> {
    let url_count = sr_settings.urls.len();
    let mut n = 0;
    loop {
        let result = perform_single_versions_call(
            &sr_settings.urls[n],
            &sr_settings.client,
            &sr_settings.authorization,
            &subject,
        )
        .await;
        if result.is_ok() || n + 1 == url_count {
            break result;
        }
        n += 1
    }
}

async fn perform_single_versions_call(
    base_url: &str,
    client: &Client,
    authentication: &SrAuthorization,
    subject: &String,
) -> Result<Vec<u32>, SRCError> {
    let url = format!("{}/subjects/{}/versions", base_url, subject);
    let builder = client.get(url);
    let successful_response = send_with_auth(builder, authentication).await?;
    successful_response
        .json::<Vec<u32>>()
        .await
        .map_err(|e| SRCError::non_retryable_with_cause(e, "could not parse to list of versions"))
}

#[cfg(test)]
mod tests {
    use mockito::Server;
    use std::time::Duration;

    use crate::async_impl::schema_registry::{
        get_schema_by_id, get_schema_by_id_and_type, SrSettings,
    };
    use crate::schema_registry_common::{
        Metadata, RawRegisteredSchema, RegisteredReference, RegisteredSchema, SchemaType,
    };

    #[tokio::test]
    async fn put_correct_url_as_second_check_header_set() {
        let mut server = Server::new_async().await;
        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .match_header("foo", "bar")
            .match_header("authorization", "Bearer some_json_web_token_for_example")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(String::from("bogus://test"))
            .add_url(server.url().parse().unwrap())
            .set_token_authorization("some_json_web_token_for_example")
            .add_header("foo", "bar")
            .set_timeout(Duration::from_secs(5))
            .no_proxy()
            .build()
            .unwrap();

        let result = get_schema_by_id(1, &sr_settings).await;

        match result {
            Ok(v) => assert_eq!(
                v.schema,
                String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#
                )
            ),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn basic_authorization() {
        let mut server = Server::new_async().await;

        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .match_header("authorization", "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .set_basic_authorization("Aladdin", Some("open sesame"))
            .no_proxy()
            .build()
            .unwrap();

        let result = get_schema_by_id(1, &sr_settings).await;

        match result {
            Ok(v) => assert_eq!(
                v.schema,
                String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#
                )
            ),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_get_schema_by_id_unauthenticated() {
        let mut server = Server::new_async().await;

        let _m = server
            .mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(401)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error_code":401,"message":"Unauthorized"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();

        let result = get_schema_by_id(1, &sr_settings).await;
        match result {
            Ok(_) => panic!("success, when error is expected"),
            Err(e) => {
                assert_eq!(
                    e.error,
                    "HTTP request to schema registry failed with status 401 Unauthorized"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_get_schema_by_id_and_type() {
        let mut server = Server::new_async().await;

        let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();

        let result = get_schema_by_id_and_type(1, &sr_settings, SchemaType::Avro).await;

        match result {
            Ok(v) => assert_eq!(
                v.schema,
                String::from(
                    r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#
                )
            ),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn test_parse_and_raw_to_registered_schema_async() {
        let json_str = r#"{
          "subject": "ietf-telemetry-message",
          "version": 1,
          "id": 28,
          "guid": "32da7798-bb83-b04e-4ecd-1216eb767df2",
          "schemaType": "YANG",
          "references": [
            { "name": "ietf-yang-types", "subject": "ietf-yang-types", "version": 1 },
            { "name": "ietf-inet-types", "subject": "ietf-inet-types", "version": 1 },
            { "name": "ietf-platform-manifest", "subject": "ietf-platform-manifest", "version": 1 }
          ],
          "metadata": {
            "tags": {
              "features": ["data-collection-manifest", "network-node-manifest"]
            }
          },
          "schema": "module ietf-telemetry-message {\n  yang-version 1.1;\n  ...}",
          "ts": 1763116523340,
          "deleted": false
        }"#;

        let expected_raw_schema = RawRegisteredSchema {
            subject: Some("ietf-telemetry-message".to_string()),
            version: Some(1),
            id: Some(28),
            schema_type: Some("YANG".to_string()),
            references: Some(vec![
                RegisteredReference {
                    name: "ietf-yang-types".to_string(),
                    subject: "ietf-yang-types".to_string(),
                    version: 1,
                    properties: None,
                    tags: None,
                },
                RegisteredReference {
                    name: "ietf-inet-types".to_string(),
                    subject: "ietf-inet-types".to_string(),
                    version: 1,
                    properties: None,
                    tags: None,
                },
                RegisteredReference {
                    name: "ietf-platform-manifest".to_string(),
                    subject: "ietf-platform-manifest".to_string(),
                    version: 1,
                    properties: None,
                    tags: None,
                },
            ]),
            schema: Some(
                "module ietf-telemetry-message {\n  yang-version 1.1;\n  ...}".to_string(),
            ),
            metadata: Some(Metadata {
                tags: Some({
                    let mut tags = std::collections::HashMap::new();
                    tags.insert(
                        "features".to_string(),
                        vec![
                            "data-collection-manifest".to_string(),
                            "network-node-manifest".to_string(),
                        ],
                    );
                    tags
                }),
                properties: None,
            }),
        };

        let expected_registered_schema = RegisteredSchema {
            id: 28,
            schema_type: SchemaType::Other("YANG".to_string()),
            schema: "module ietf-telemetry-message {\n  yang-version 1.1;\n  ...}".to_string(),
            references: expected_raw_schema.references.clone().unwrap(),
            properties: None,
            tags: Some({
                let mut tags = std::collections::HashMap::new();
                tags.insert(
                    "features".to_string(),
                    vec![
                        "data-collection-manifest".to_string(),
                        "network-node-manifest".to_string(),
                    ],
                );
                tags
            }),
        };

        let parsed: RawRegisteredSchema = serde_json::from_str(json_str).expect("parse json");
        assert_eq!(parsed, expected_raw_schema);

        let reg =
            crate::async_impl::schema_registry::raw_to_registered_schema(parsed.clone(), None)
                .await
                .expect("convert to registered");
        assert_eq!(reg, expected_registered_schema);
    }
}
