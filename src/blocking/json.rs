use std::sync::Arc;

use dashmap::mapref::entry::Entry as DashEntry;
use dashmap::DashMap;
use serde_json::Value;
use url::Url;

use crate::blocking::schema_registry::{
    get_referenced_schema, get_schema_by_guid_and_type, get_schema_by_id_and_type,
    get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::json_common::{
    main_url, reference_base, reference_depth_exceeded_error, reference_url, to_bytes,
    to_bytes_raw, to_value, ValidatorCache, MAX_REFERENCE_DEPTH,
};
pub use crate::json_common::{validate, DecodeResult, JsonSchema};
use crate::schema_registry_common::{
    get_bytes_result, invalid_bytes_error, parse_schema_id_header, schema_id_header_for,
    BytesResult, HeaderSchemaId, RegisteredSchema, SchemaIdHeader, SchemaType, SubjectNameStrategy,
};

/// Encoder that validates the value against the schema the subject name strategy resolves to,
/// and prefixes the JSON bytes with the schema id.
#[derive(Debug)]
pub struct JsonEncoder {
    sr_settings: SrSettings,
    cache: DashMap<String, Result<Arc<JsonSchema>, SRCError>>,
    validators: ValidatorCache,
}

impl JsonEncoder {
    /// Creates a new json encoder
    pub fn new(sr_settings: SrSettings) -> JsonEncoder {
        JsonEncoder {
            sr_settings,
            cache: DashMap::new(),
            validators: ValidatorCache::new(),
        }
    }
    /// Removes all non-retriable errors from the cache, useful to retry failed encodings once
    /// the underlying problem has been fixed. Retriable errors aren't cached in the first place.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Validates the value against the schema, and returns it as bytes prefixed with the schema
    /// id.
    pub fn encode(
        &self,
        value: &Value,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let schema = self.get_schema(subject_name_strategy)?;
        self.validators.validate(&schema, value)?;
        to_bytes(schema.id, value)
    }

    /// Validates the value against the schema the subject name strategy resolves to, keeping
    /// the compiled schema for the next validation.
    pub fn validate(
        &self,
        value: &Value,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<(), SRCError> {
        let schema = self.get_schema(subject_name_strategy)?;
        self.validators.validate(&schema, value)
    }

    /// Like [`JsonEncoder::encode`], but instead of prefixing the payload with the schema id
    /// (the default confluent wire format), returns the raw JSON bytes alongside a
    /// [`SchemaIdHeader`] carrying the schema's guid. Attach the header to the Kafka record
    /// using whatever Kafka client you're using -- this crate has no dependency on one. `is_key`
    /// picks between the `__key_schema_id`/`__value_schema_id` header names, matching
    /// Confluent's `HeaderSchemaIdSerializer`. See
    /// https://github.com/gklijs/schema_registry_converter/issues/139.
    ///
    /// Requires a schema registry that returns a `guid` (Confluent Schema Registry 8.0+); a
    /// registry that doesn't will make this return an error.
    pub fn encode_with_header_id(
        &self,
        value: &Value,
        subject_name_strategy: &SubjectNameStrategy,
        is_key: bool,
    ) -> Result<(Vec<u8>, SchemaIdHeader), SRCError> {
        let schema = self.get_schema(subject_name_strategy)?;
        self.validators.validate(&schema, value)?;
        let bytes = to_bytes_raw(value)?;
        let header = schema_id_header_for(schema.guid.as_deref(), is_key, &[])?;
        Ok((bytes, header))
    }

    fn get_schema(
        &self,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Arc<JsonSchema>, SRCError> {
        let key = subject_name_strategy.get_subject()?;
        match self.cache.entry(key) {
            DashEntry::Occupied(entry) => entry.get().clone(),
            DashEntry::Vacant(entry) => {
                let result = get_schema_by_subject(&self.sr_settings, subject_name_strategy)
                    .and_then(|r| get_json_schema(&self.sr_settings, r))
                    .map(Arc::new);
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

#[derive(Debug)]
pub struct JsonDecoder {
    sr_settings: SrSettings,
    cache: DashMap<u32, Result<Arc<JsonSchema>, SRCError>>,
    /// Cache for schemas looked up by guid rather than id, used by
    /// [`JsonDecoder::decode_with_header_id`]. Kept separate from `cache` (keyed by id) since a
    /// guid and an id are different keyspaces.
    guid_cache: DashMap<String, Result<Arc<JsonSchema>, SRCError>>,
    validators: ValidatorCache,
}

impl JsonDecoder {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. Retriable errors (e.g. a transient network/HTTP failure) are never cached,
    /// so those are simply retried on the next call. Non-retriable errors (e.g. a schema that
    /// doesn't exist or can't be parsed) are cached to avoid repeatedly retrying a lookup that
    /// isn't going to succeed; if the underlying problem gets fixed you can use
    /// remove_errors_from_cache to clean those out.
    pub fn new(sr_settings: SrSettings) -> JsonDecoder {
        JsonDecoder {
            sr_settings,
            cache: DashMap::new(),
            guid_cache: DashMap::new(),
            validators: ValidatorCache::new(),
        }
    }
    /// Removes all non-retriable errors from the cache. You might need/want to run this when the
    /// underlying problem has been fixed (e.g. a schema was missing and has since been
    /// registered) and want the next call to try again immediately. Retriable errors aren't
    /// stored in the cache in the first place, so there's nothing to remove for those.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| v.is_ok());
        self.guid_cache.retain(|_, v| v.is_ok());
    }
    /// Reads the bytes to get the name, and gives back the data bytes.
    pub fn decode(&self, bytes: Option<&[u8]>) -> Result<Option<DecodeResult>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => Ok(Some(self.deserialize(id, bytes)?)),
            BytesResult::Invalid(i) => Err(SRCError::non_retryable_without_cause(
                &invalid_bytes_error(i),
            )),
        }
    }
    /// Like [`JsonDecoder::decode`], but for a message that may carry its schema id/guid in a
    /// `__key_schema_id`/`__value_schema_id` header instead of (or in addition to) the payload
    /// prefix, mirroring Confluent's `DualSchemaIdDeserializer`: `header_value` present ->
    /// resolve the schema from it and treat `bytes` as the raw (unprefixed) payload;
    /// `header_value` absent -> falls straight through to [`JsonDecoder::decode`], so the only
    /// overhead for a caller who always passes `None` is this one check. See
    /// https://github.com/gklijs/schema_registry_converter/issues/139.
    pub fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResult>, SRCError> {
        let payload = match bytes {
            Some(v) => v,
            None => return Ok(None),
        };
        match header_value {
            None => self.decode(Some(payload)),
            Some(header) => {
                let (schema_id, _rest) = parse_schema_id_header(header)?;
                let schema = match schema_id {
                    HeaderSchemaId::Id(id) => self.get_schema(id)?,
                    HeaderSchemaId::Guid(guid) => self.get_schema_by_guid(guid)?,
                };
                match serde_json::from_slice(payload) {
                    Ok(value) => Ok(Some(DecodeResult { schema, value })),
                    Err(e) => Err(SRCError::non_retryable_with_cause(
                        e,
                        "could not create value from bytes",
                    )),
                }
            }
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<DecodeResult, SRCError> {
        let schema = self.get_schema(id)?;
        match serde_json::from_slice(bytes) {
            Ok(value) => Ok(DecodeResult { schema, value }),
            Err(e) => Err(SRCError::non_retryable_with_cause(
                e,
                "could not create value from bytes",
            )),
        }
    }
    /// Validates the value against the schema, including the schemas it references, like
    /// [`validate`], but keeps the compiled schemas, so validating against the same schema again
    /// is much faster. Validations through the same decoder happen one at a time.
    pub fn validate(&self, schema: &JsonSchema, value: &Value) -> Result<(), SRCError> {
        self.validators.validate(schema, value)
    }
    /// Resolves `id` into a fully self-contained, reference-resolved [`JsonSchema`], using (and
    /// populating) this decoder's own id-keyed cache.
    pub fn get_schema(&self, id: u32) -> Result<Arc<JsonSchema>, SRCError> {
        match self.cache.entry(id) {
            DashEntry::Occupied(entry) => entry.get().clone(),
            DashEntry::Vacant(entry) => {
                let result = get_schema_by_id_and_type(id, &self.sr_settings, SchemaType::Json)
                    .and_then(|r| get_json_schema(&self.sr_settings, r))
                    .map(Arc::new);
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

    /// Like [`JsonDecoder::get_schema`], but looks the schema up by guid instead of id -- used by
    /// [`JsonDecoder::decode_with_header_id`] when the header carries a guid rather than an id.
    fn get_schema_by_guid(&self, guid: String) -> Result<Arc<JsonSchema>, SRCError> {
        match self.guid_cache.entry(guid.clone()) {
            DashEntry::Occupied(entry) => entry.get().clone(),
            DashEntry::Vacant(entry) => {
                let result =
                    get_schema_by_guid_and_type(&guid, &self.sr_settings, SchemaType::Json)
                        .and_then(|r| get_json_schema(&self.sr_settings, r))
                        .map(Arc::new);
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

/// Resolves `registered_schema` into a fully self-contained [`JsonSchema`].
pub fn get_json_schema(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
) -> Result<JsonSchema, SRCError> {
    to_json_schema(sr_settings, None, registered_schema, 0)
}

fn to_json_schema(
    sr_settings: &SrSettings,
    optional_url: Option<Url>,
    registered_schema: RegisteredSchema,
    depth: usize,
) -> Result<JsonSchema, SRCError> {
    // A circular reference chain (a schema that references itself, directly or transitively)
    // would otherwise recurse here without bound.
    if depth > MAX_REFERENCE_DEPTH {
        return Err(reference_depth_exceeded_error());
    }
    let schema: Value = to_value(&registered_schema.schema)?;
    let url = match optional_url {
        Some(v) => v,
        None => main_url(
            &schema,
            sr_settings.url(),
            registered_schema.id,
            registered_schema.guid.as_deref(),
        ),
    };
    let base = reference_base(&schema, &url);
    let references = registered_schema
        .references
        .iter()
        .map(|rr| {
            let ref_url = reference_url(rr, &base)?;
            let rs = get_referenced_schema(sr_settings, rr)?;
            to_json_schema(sr_settings, Some(ref_url), rs, depth + 1)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(JsonSchema {
        id: registered_schema.id,
        url,
        schema,
        references,
        subject: registered_schema.subject,
        version: registered_schema.version,
        properties: registered_schema.properties,
        tags: registered_schema.tags,
        guid: registered_schema.guid,
    })
}

#[cfg(test)]
mod tests {
    use std::fs::{read_to_string, File};

    use serde_json::Value;

    use crate::blocking::json::{JsonDecoder, JsonEncoder};
    use crate::blocking::schema_registry::SrSettings;
    use crate::schema_registry_common::{
        build_schema_id_header, get_payload, SubjectNameStrategy, VALUE_SCHEMA_ID_HEADER,
    };
    use test_utils::{
        get_json_body, get_json_body_with_reference, json_extra_schema, json_get_extra_references,
        json_get_result_references, json_incorrect_bytes, json_result_java_bytes,
        json_result_schema, json_result_schema_with_id, json_test_ref_schema,
        json_test_ref_schema_with_extra,
    };

    #[test]
    fn test_encode_java_compatibility() {
        let mut server = mockito::Server::new();
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

        let encoded_data = encoder.encode(&result_example, &strategy).unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }

    #[test]
    fn test_encode_and_decode_with_header_id() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"subject":"testresult-value","version":1,"id":10,"guid":"cc0e0e0e-53c1-4a1a-8f1a-000000000001","schema":"{\"type\":\"object\"}","schemaType":"JSON"}"#)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings.clone());
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let value: Value = serde_json::json!({"foo": "bar"});

        let (bytes, header) = encoder
            .encode_with_header_id(&value, &strategy, false)
            .unwrap();

        assert_eq!(bytes, serde_json::to_vec(&value).unwrap());
        assert_eq!(header.name, VALUE_SCHEMA_ID_HEADER);
        assert_eq!(
            header.value,
            vec![
                0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x01,
            ]
        );

        // GET /schemas/guids/{guid} never carries an "id" field on a real registry -- only "guid".
        let _m2 = server
            .mock(
                "GET",
                "/schemas/guids/cc0e0e0e-53c1-4a1a-8f1a-000000000001?deleted=true",
            )
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"schema":"{\"type\":\"object\"}","schemaType":"JSON"}"#)
            .create();

        let decoder = JsonDecoder::new(sr_settings);
        let decoded = decoder
            .decode_with_header_id(Some(&header.value), Some(&bytes))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.value, value);

        // header absent -> falls straight through to decode(), which expects the prefix
        let _m3 = server
            .mock("GET", "/schemas/ids/10?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(r#"{"id":10,"schema":"{\"type\":\"object\"}","schemaType":"JSON"}"#)
            .create();
        let prefixed = get_payload(10, serde_json::to_vec(&value).unwrap());
        let decoded = decoder
            .decode_with_header_id(None, Some(&prefixed))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.value, value);
    }

    #[test]
    fn guid_only_schemas_without_id_do_not_collide_on_fallback_url() {
        // GET /schemas/guids/{guid} never returns an "id" (see get_schema_by_guid), so two
        // different schemas resolved only by guid and lacking their own `$id` must not collide
        // on the same fallback compile-scope url -- they used to, since both defaulted to the
        // same "id/0.json" placeholder. See
        // https://github.com/gklijs/schema_registry_converter/issues/139.
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);

        let schema_a = r#"{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"a\":{\"type\":\"string\"}},\"required\":[\"a\"]}"#;
        let schema_b = r#"{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"b\":{\"type\":\"string\"}},\"required\":[\"b\"]}"#;
        let guid_a = "cc0e0e0e-53c1-4a1a-8f1a-000000000001";
        let guid_b = "dd0e0e0e-53c1-4a1a-8f1a-000000000002";

        let _m1 = server
            .mock(
                "GET",
                format!("/schemas/guids/{guid_a}?deleted=true").as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(format!(
                r#"{{"guid":"{guid_a}","schema":"{schema_a}","schemaType":"JSON"}}"#
            ))
            .create();
        let _m2 = server
            .mock(
                "GET",
                format!("/schemas/guids/{guid_b}?deleted=true").as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(format!(
                r#"{{"guid":"{guid_b}","schema":"{schema_b}","schemaType":"JSON"}}"#
            ))
            .create();

        let header_a = build_schema_id_header(VALUE_SCHEMA_ID_HEADER, guid_a, &[]).unwrap();
        let header_b = build_schema_id_header(VALUE_SCHEMA_ID_HEADER, guid_b, &[]).unwrap();
        let value_a = serde_json::json!({"a": "x"});
        let value_b = serde_json::json!({"b": "y"});
        let bytes_a = serde_json::to_vec(&value_a).unwrap();
        let bytes_b = serde_json::to_vec(&value_b).unwrap();

        let decoded_a = decoder
            .decode_with_header_id(Some(&header_a.value), Some(&bytes_a))
            .unwrap()
            .unwrap();
        // Each message must validate against its own schema...
        assert!(decoder.validate(&decoded_a.schema, &value_a).is_ok());
        // ...and fail against the other's, proving they're genuinely different compiled schemas
        // rather than both resolving to whichever was compiled first.
        assert!(decoder.validate(&decoded_a.schema, &value_b).is_err());
        drop(decoded_a);

        let decoded_b = decoder
            .decode_with_header_id(Some(&header_b.value), Some(&bytes_b))
            .unwrap()
            .unwrap();
        assert!(decoder.validate(&decoded_b.schema, &value_b).is_ok());
        assert!(decoder.validate(&decoded_b.schema, &value_a).is_err());
    }

    #[test]
    fn test_encode_schema_with_id() {
        let mut server = mockito::Server::new();
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

        let encoded_data = encoder.encode(&result_example, &strategy).unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }

    #[test]
    fn test_encode_clean_cache() {
        let mut server = mockito::Server::new();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, &strategy);
        assert!(encoded_data.is_err());

        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 10))
            .create();

        let encoded_data = encoder.encode(&result_example, &strategy);
        assert!(encoded_data.is_err());

        encoder.remove_errors_from_cache();

        let encoded_data = encoder.encode(&result_example, &strategy);
        assert!(encoded_data.is_ok());
    }

    #[test]
    fn test_decoder_default() {
        let result_value: String = read_to_string("tests/schema/result-example.json")
            .unwrap()
            .parse()
            .unwrap();
        let mut server = mockito::Server::new();
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
        let result = decoder.decode(Some(&*get_payload(7, result_value.into_bytes())));

        let message = match result {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        decoder.validate(&message.schema, &message.value).unwrap();
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

    #[test]
    fn test_decoder_clean_cache() {
        let mut server = mockito::Server::new();
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
            .unwrap_err();

        assert!(error.cached);

        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 7))
            .create();

        let result = decoder.decode(Some(&*get_payload(7, bytes.clone())));
        let error = match result {
            Ok(Some(_)) => panic!("expected error"),
            Err(e) => e,
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert!(error.cached);

        decoder.remove_errors_from_cache();

        let result = decoder.decode(Some(&*get_payload(7, bytes)));
        let message = match result {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert!(message.value.is_object())
    }

    #[test]
    fn test_decoder_java_compatibility() {
        let mut server = mockito::Server::new();
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
        let result = decoder.decode(Some(json_result_java_bytes()));

        let message = match result {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        decoder.validate(&message.schema, &message.value).unwrap();
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

    #[test]
    fn test_decoder_value_can_not_be_read() {
        let mut server = mockito::Server::new();
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
        let result = decoder.decode(Some(json_incorrect_bytes())).unwrap_err();
        assert_eq!(
            String::from("could not create value from bytes"),
            result.error
        )
    }

    #[test]
    fn add_referred_schema() {
        let mut server = mockito::Server::new();
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

        let result = match decoder.decode(Some(&bytes)) {
            Ok(Some(v)) => v.value,
            _ => panic!("Not a value while that was expected"),
        };
        match result {
            Value::Object(v) => {
                assert_eq!(&Value::String(String::from("Java")), v.get("by").unwrap())
            }
            _ => panic!("Not an Object that was expected"),
        }
    }

    #[test]
    fn add_referred_schema_with_bare_reference_name() {
        // Confluent doesn't require a reference's `name` to be a fully qualified url -- it's
        // commonly just a bare file name like "result.json", matching what actually appears in
        // the referencing schema's own `$ref`. Resolving it should work the same way a JSON
        // Schema validator resolves a relative `$ref`: against the referencing schema's own
        // (fallback, since it has no `$id`) url. See
        // https://github.com/gklijs/schema_registry_converter/issues/139.
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);

        let schema = r#"{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"result\":{\"$ref\":\"result.json\"}},\"required\":[\"result\"]}"#;
        let reference = r#"{"name": "result.json", "subject": "result.json", "version": 1}"#;

        let _m = server
            .mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(schema, 5, reference))
            .create();
        let _m2 = server
            .mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 4))
            .create();

        let value = serde_json::json!({"result": {"up": "STRING", "down": "string"}});
        let bytes = get_payload(5, serde_json::to_vec(&value).unwrap());
        let result = match decoder.decode(Some(&bytes)) {
            Ok(Some(v)) => v.value,
            Err(e) => panic!("Error decoding: {}", e),
            _ => panic!("Not a value while that was expected"),
        };
        assert_eq!(result, value);
    }

    #[test]
    fn error_in_referred_schema() {
        let mut server = mockito::Server::new();
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

        let error = decoder.decode(Some(&bytes)).unwrap_err();
        assert!(error.error.starts_with("could not parse schema {"))
    }

    #[test]
    fn circular_reference_returns_error_instead_of_overflowing_stack() {
        // A schema that (transitively) references itself must be rejected with a clean
        // SRCError instead of recursing until the stack overflows. See
        // https://github.com/gklijs/schema_registry_converter/issues/139.
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);

        let schema = r#"{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{\"self\":{\"$ref\":\"self.json\"}}}"#;
        let reference = r#"{"name": "self.json", "subject": "circular", "version": 1}"#;

        let _m = server
            .mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(schema, 5, reference))
            .create();
        // The reference resolves to the *same* self-referencing schema, so following it
        // recurses into "self.json" again indefinitely.
        let _m2 = server
            .mock("GET", "/subjects/circular/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(schema, 5, reference))
            .create();

        let bytes = get_payload(5, br#"{"self":{}}"#.to_vec());
        let error = decoder.decode(Some(&bytes)).unwrap_err();
        assert!(
            error.error.contains("reference chain exceeded"),
            "expected a reference-depth error, got {:?}",
            error
        );
    }

    #[test]
    fn encounter_same_reference_again() {
        let mut server = mockito::Server::new();
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

        let result = match decoder.decode(Some(&bytes_id_5)) {
            Ok(Some(v)) => v.value,
            _ => panic!("Not a value while that was expected"),
        };
        match result {
            Value::Object(v) => {
                assert_eq!(&Value::String(String::from("Java")), v.get("by").unwrap())
            }
            _ => panic!("Not an Object that was expected"),
        }
        let result = match decoder.decode(Some(&bytes_id_7)) {
            Ok(Some(v)) => v.value,
            _ => panic!("Not a value while that was expected"),
        };
        match result {
            Value::Object(v) => {
                assert_eq!(&Value::String(String::from("Java")), v.get("by").unwrap())
            }
            _ => panic!("Not an Object that was expected"),
        }
    }

    #[test]
    fn same_reference_then_new_reference_in_same_schema() {
        // Regression test for https://github.com/gklijs/schema_registry_converter/issues/177:
        // once one reference is already in the decoder's (persistent) scope, later references
        // in the SAME schema's reference list must still be resolved, not silently skipped.
        let mut server = mockito::Server::new();
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = JsonDecoder::new(sr_settings);

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
            .mock("GET", "/schemas/ids/8?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body_with_reference(
                json_test_ref_schema_with_extra(),
                8,
                &format!(
                    "{}, {}",
                    json_get_result_references(),
                    json_get_extra_references()
                ),
            ))
            .create();
        let _m = server
            .mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_result_schema(), 4))
            .create();
        let _m = server
            .mock("GET", "/subjects/extra.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_json_body(json_extra_schema(), 9))
            .create();

        // First decode populates the decoder's scope with `result.json`.
        let bytes_id_5 = get_payload(
            5,
            serde_json::to_vec(&serde_json::json!({
                "id": [1, 2, 3],
                "by": "Java",
                "counter": 1,
                "input": "String",
                "results": [{"up": "STRING", "down": "string"}]
            }))
            .unwrap(),
        );
        decoder.decode(Some(&bytes_id_5)).unwrap();

        // Schema 8's references are [result.json, extra.json]: result.json is already scoped
        // from the decode above, extra.json isn't. Before the fix, the early return on the
        // already-scoped result.json skipped adding extra.json to scope entirely, so this
        // fully valid payload would fail validation with a missing-reference error.
        let payload = serde_json::json!({
            "id": [1, 2, 3],
            "by": "Java",
            "counter": 1,
            "input": "String",
            "results": [{"up": "STRING", "down": "string"}],
            "extra": {"note": "hi"}
        });
        let bytes_id_8 = get_payload(8, serde_json::to_vec(&payload).unwrap());
        let result = decoder.decode(Some(&bytes_id_8)).unwrap().unwrap();
        decoder.validate(&result.schema, &payload).unwrap();
    }

    #[test]
    fn display_encode() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let encoder = JsonEncoder::new(sr_settings);
        assert!(
            format!("{:?}", encoder)
                == "JsonEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {}, validators: ValidatorCache { .. } }"
        )
    }

    #[test]
    fn display_decode() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = JsonDecoder::new(sr_settings);
        assert_eq!(
            format!("{:?}", decoder),
            "JsonDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {}, guid_cache: {}, validators: ValidatorCache { .. } }"
        )
    }

    #[test]
    fn test_encode_not_valid() {
        let mut server = mockito::Server::new();
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

        let error = encoder.encode(&result_example, &strategy).unwrap_err();

        assert_eq!(
            error.error,
            String::from(
                r#"Value "Foo" was not valid according to the schema because ["\"Foo\" is not of type \"object\""]"#
            )
        )
    }

    #[test]
    fn test_encode_with_header_id_not_valid() {
        // encode_with_header_id must reject a non-conforming value just like encode() does,
        // rather than skipping validation because it doesn't need the schema's numeric id.
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(
                r#"{"subject":"testresult-value","version":1,"id":10,"guid":"cc0e0e0e-53c1-4a1a-8f1a-000000000001","schema":"{\"type\":\"object\"}","schemaType":"JSON"}"#,
            )
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value = Value::String(String::from("Foo"));

        let error = encoder
            .encode_with_header_id(&result_example, &strategy, false)
            .unwrap_err();

        assert_eq!(
            error.error,
            String::from(
                r#"Value "Foo" was not valid according to the schema because ["\"Foo\" is not of type \"object\""]"#
            )
        )
    }

    #[test]
    fn test_encode_missing_ref() {
        let mut server = mockito::Server::new();
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

        let error = encoder.encode(&result_example, &strategy).unwrap_err();

        assert_eq!(error.error, "error compiling schema");
        assert!(error
            .cause
            .unwrap()
            .contains("is not present in a registry"));
    }

    #[test]
    fn decode_invalid_bytes() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = JsonDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[1, 0])).unwrap_err();
        assert_eq!(String::from("Invalid bytes: payload too short (2 bytes), expected at least 5 bytes for confluent schema registry wire format"), result.error)
    }
}
