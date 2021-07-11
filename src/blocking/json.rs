use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;
use std::str::FromStr;

use serde_json::Value;
use url::Url;
use valico::json_schema::schema::ScopedSchema;
use valico::json_schema::{Scope, ValidationState};

use crate::blocking::schema_registry::{
    get_referenced_schema, get_schema_by_id_and_type, get_schema_by_subject, SrSettings,
};
use crate::error::SRCError;
use crate::json_common::{fetch_fallback, fetch_id, handle_validation, to_bytes, to_value};
use crate::schema_registry_common::{
    get_bytes_result, get_subject, BytesResult, RegisteredReference, RegisteredSchema, SchemaType,
    SubjectNameStrategy,
};

/// Encoder that works by prepending the correct bytes in order to make it valid schema registry
/// bytes. Ideally you want to make sure the bytes are based on the exact schema used for encoding
/// but you need a protobuf struct that has introspection to make that work, and both protobuf and
/// prost don't support that at the moment.
#[derive(Debug)]
pub struct JsonEncoder {
    sr_settings: SrSettings,
    cache: HashMap<String, Result<EncodeContext, SRCError>, RandomState>,
    scope: Scope,
}

impl JsonEncoder {
    /// Creates a new json encoder
    pub fn new(sr_settings: SrSettings) -> JsonEncoder {
        JsonEncoder {
            sr_settings,
            cache: HashMap::new(),
            scope: Scope::new(),
        }
    }
    /// Removes errors from the cache, can be useful to retry failed encodings.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Encodes the bytes by adding a few bytes to the message with additional information. The full
    /// names is the optional package followed with the message name, and optionally inner messages.
    pub fn encode(
        &mut self,
        value: &Value,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Result<Vec<u8>, SRCError> {
        let key = get_subject(subject_name_strategy)?;
        let (validation, id) = self.validate(key, subject_name_strategy, value)?;
        handle_validation(validation, value)?;
        to_bytes(id, value)
    }

    fn validate(
        &mut self,
        key: String,
        subject_name_strategy: &SubjectNameStrategy,
        value: &Value,
    ) -> Result<(ValidationState, u32), SRCError> {
        let cached_context = match self.cache.entry(key) {
            Entry::Occupied(e) => e.into_mut().as_ref(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_subject(&self.sr_settings, &subject_name_strategy) {
                    Ok(registered_schema) => match set_scoped_schema(
                        &mut self.scope,
                        &self.sr_settings,
                        &registered_schema,
                    ) {
                        Ok(url) => Ok(EncodeContext {
                            id: registered_schema.id,
                            url,
                        }),
                        Err(e) => Err(e.into_cache()),
                    },
                    Err(e) => Err(e.into_cache()),
                };
                e.insert(v).as_ref()
            }
        };
        match cached_context {
            Ok(context) => match self.scope.resolve(&context.url) {
                Some(schema) => Ok((schema.validate(value), context.id)),
                None => Err(SRCError::non_retryable_without_cause(
                    "could not get schema from scope",
                )),
            },
            Err(e) => Err(e.clone()),
        }
    }
}

#[derive(Debug)]
struct EncodeContext {
    id: u32,
    url: Url,
}

#[derive(Debug)]
pub struct JsonDecoder {
    sr_settings: SrSettings,
    cache: HashMap<u32, Result<Url, SRCError>, RandomState>,
    scope: Scope,
}

impl JsonDecoder {
    /// Creates a new decoder which will use the supplied url to fetch the schema's since the schema
    /// needed is encoded in the binary, independent of the SubjectNameStrategy we don't need any
    /// additional data. It's possible for recoverable errors to stay in the cache, when a result
    /// comes back as an error you can use remove_errors_from_cache to clean the cache, keeping the
    /// correctly fetched schema's
    pub fn new(sr_settings: SrSettings) -> JsonDecoder {
        JsonDecoder {
            sr_settings,
            cache: HashMap::new(),
            scope: Scope::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| v.is_ok());
    }
    /// Reads the bytes to get the name, and gives back the data bytes.
    pub fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Option<DecodeResult>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => Ok(Some(self.deserialize(id, &bytes)?)),
            BytesResult::Invalid(i) => Err(SRCError::non_retryable_without_cause(&*format!(
                "Invalid bytes: {:?}",
                i
            ))),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    fn deserialize(&mut self, id: u32, bytes: &[u8]) -> Result<DecodeResult, SRCError> {
        let schema = self.schema(id)?;
        match serde_json::from_slice(bytes) {
            Ok(value) => Ok(DecodeResult { schema, value }),
            Err(e) => Err(SRCError::non_retryable_with_cause(
                e,
                "could not create value from bytes",
            )),
        }
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn schema(&mut self, id: u32) -> Result<ScopedSchema, SRCError> {
        let url = match self.cache.entry(id) {
            Entry::Occupied(e) => &*e.into_mut(),
            Entry::Vacant(e) => {
                let v = match get_schema_by_id_and_type(id, &self.sr_settings, SchemaType::Json) {
                    Ok(r) => match set_scoped_schema(&mut self.scope, &self.sr_settings, &r) {
                        Ok(schema) => Ok(schema),
                        Err(e) => Err(e.into_cache()),
                    },
                    Err(e) => Err(e.into_cache()),
                };
                &*e.insert(v)
            }
        };
        match url {
            Ok(id) => match self.scope.resolve(id) {
                Some(schema) => Ok(schema),
                None => Err(SRCError::non_retryable_without_cause(
                    "could not get schema from scope",
                )),
            },
            Err(e) => Err(e.clone()),
        }
    }
}

fn add_refs_to_scope(
    scope: &mut Scope,
    sr_settings: &SrSettings,
    refs: &[RegisteredReference],
) -> Result<(), SRCError> {
    for rr in refs.iter() {
        let rs = get_referenced_schema(sr_settings, rr)?;
        let id = match Url::from_str(&*rr.name) {
            Ok(v) => v,
            Err(e) => return Err(SRCError::non_retryable_with_cause(e, &*format!("reference schema with subject {} and version {} has invalid id {}, it has to be a fully qualified url", rr.subject, rr.version, rr.name)))
        };
        // if it's already part of the scope, it's assumed any references are also already part of the scope.
        if scope.resolve(&id).is_some() {
            return Ok(());
        }
        add_refs_to_scope(scope, sr_settings, &rs.references)?;
        let def: Value = to_value(&*rs.schema)?;
        scope.compile_with_id(&id, def, false).unwrap();
    }
    Ok(())
}

fn set_scoped_schema(
    scope: &mut Scope,
    sr_settings: &SrSettings,
    registered_schema: &RegisteredSchema,
) -> Result<Url, SRCError> {
    add_refs_to_scope(scope, sr_settings, &registered_schema.references)?;
    let def: Value = match serde_json::from_str(&*registered_schema.schema) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                &*format!(
                    "could not parse schema {} with id {} to a value",
                    registered_schema.schema, registered_schema.id
                ),
            ))
        }
    };
    let id = match fetch_id(&def) {
        Some(url) => url,
        None => fetch_fallback(sr_settings.url(), registered_schema.id),
    };
    match scope.compile_with_id(&id, def, false) {
        Ok(_) => (),
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                &*format!("could not compile schema with id {}", registered_schema.id),
            ))
        }
    };
    Ok(id)
}

#[derive(Debug)]
pub struct DecodeResult<'a> {
    pub schema: ScopedSchema<'a>,
    pub value: Value,
}

#[cfg(test)]
mod tests {
    use std::fs::{read_to_string, File};

    use mockito::{mock, server_address};
    use serde_json::{from_str, to_string_pretty, Value};
    use valico::json_dsl;

    use crate::blocking::json::{JsonDecoder, JsonEncoder};
    use crate::blocking::schema_registry::SrSettings;
    use crate::schema_registry_common::{get_payload, SubjectNameStrategy};
    use test_utils::{
        get_json_body, get_json_body_with_reference, json_get_result_references,
        json_incorrect_bytes, json_result_java_bytes, json_result_schema,
        json_result_schema_with_id, json_test_ref_schema,
    };

    #[test]
    fn using_the_dsl_builder_might_be_used_after_decode_to_coerce() {
        let params = json_dsl::Builder::build(|params| {
            params.req_nested("user", json_dsl::array(), |params| {
                params.req_typed("name", json_dsl::string());
                params.req_typed("friend_ids", json_dsl::array_of(json_dsl::u64()))
            });
        });

        let mut obj = from_str(r#"{"user": [{"name": "Frodo", "friend_ids": ["1223"]}]}"#).unwrap();

        let state = params.process(&mut obj, None);
        if state.is_valid() {
            println!("Result object is {}", to_string_pretty(&obj).unwrap());
        } else {
            panic!("Errors during process: {:?}", state);
        }
    }

    #[test]
    fn test_encode_java_compatibility() {
        let _m = mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, &strategy).unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }

    #[test]
    fn test_encode_schema_with_id() {
        let _m = mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema_with_id(), 10))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, &strategy).unwrap();

        assert_eq!(encoded_data, json_result_java_bytes())
    }

    #[test]
    fn test_encode_clean_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/result-example.json").unwrap())
                .unwrap();

        let encoded_data = encoder.encode(&result_example, &strategy);
        assert_eq!(true, encoded_data.is_err());

        let _m = mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 10))
            .create();

        let encoded_data = encoder.encode(&result_example, &strategy);
        assert_eq!(true, encoded_data.is_err());

        encoder.remove_errors_from_cache();

        let encoded_data = encoder.encode(&result_example, &strategy);
        assert_eq!(false, encoded_data.is_err());
    }

    #[test]
    fn test_decoder_default() {
        let result_value: String = read_to_string("tests/schema/result-example.json")
            .unwrap()
            .parse()
            .unwrap();
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 7))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
        let result = decoder.decode(Some(&*get_payload(7, result_value.into_bytes())));

        let message = match result {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        let validation = message.schema.validate(&message.value);
        assert_eq!(true, validation.is_strictly_valid());
        assert_eq!(true, validation.errors.is_empty());
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
        let result_value: String = read_to_string("tests/schema/result-example.json")
            .unwrap()
            .parse()
            .unwrap();

        let bytes = result_value.into_bytes();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
        let error = decoder
            .decode(Some(&*get_payload(7, bytes.clone())))
            .unwrap_err();

        assert_eq!(true, error.cached);

        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 7))
            .create();

        let result = decoder.decode(Some(&*get_payload(7, bytes.clone())));
        let error = match result {
            Ok(Some(_)) => panic!("expected error"),
            Err(e) => e,
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(true, error.cached);

        decoder.remove_errors_from_cache();

        let result = decoder.decode(Some(&*get_payload(7, bytes)));
        let message = match result {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(true, message.value.is_object())
    }

    #[test]
    fn test_decoder_java_compatibility() {
        let _m = mock("GET", "/schemas/ids/10?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
        let result = decoder.decode(Some(json_result_java_bytes()));

        let message = match result {
            Ok(Some(x)) => x,
            Err(e) => panic!("Error: {:?}, while none expected", e),
            Ok(v) => panic!("Other value: {:?} than expected Message", v),
        };
        let validation = message.schema.validate(&message.value);
        assert_eq!(true, validation.is_strictly_valid());
        assert_eq!(true, validation.errors.is_empty());
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
        let _m = mock("GET", "/schemas/ids/10?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
        let result = decoder.decode(Some(json_incorrect_bytes())).unwrap_err();
        assert_eq!(
            String::from("could not create value from bytes"),
            result.error
        )
    }

    #[test]
    fn add_referred_schema() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
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

        let _m = mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body_with_reference(
                json_test_ref_schema(),
                5,
                json_get_result_references(),
            ))
            .create();
        let _m = mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 4))
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
    fn error_in_referred_schema() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
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

        let _m = mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body_with_reference(
                json_test_ref_schema(),
                5,
                json_get_result_references(),
            ))
            .create();
        let _m = mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(&json_result_schema()[0..30], 4))
            .create();

        let error = decoder.decode(Some(&bytes)).unwrap_err();
        assert_eq!(true, error.error.starts_with("could not parse schema {"))
    }

    #[test]
    fn encounter_same_reference_again() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
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

        let _m = mock("GET", "/schemas/ids/5?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body_with_reference(
                json_test_ref_schema(),
                5,
                json_get_result_references(),
            ))
            .create();
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body_with_reference(
                json_test_ref_schema(),
                7,
                json_get_result_references(),
            ))
            .create();
        let _m = mock("GET", "/subjects/result.json/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 4))
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
    fn display_encode() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let encoder = JsonEncoder::new(sr_settings);
        assert_eq!(true,
            format!("{:?}", encoder).starts_with("JsonEncoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {}, scope: Scope {")
        )
    }

    #[test]
    fn display_decode() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = JsonDecoder::new(sr_settings);
        assert_eq!(true,
                   format!("{:?}", decoder).starts_with("JsonDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client, authorization: None }, cache: {}, scope: Scope {")
        )
    }

    #[test]
    fn test_encode_not_valid() {
        let _m = mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_result_schema(), 10))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value = Value::String(String::from("Foo"));

        let error = encoder.encode(&result_example, &strategy).unwrap_err();

        assert_eq!(
            error.error,
            String::from(
                r#"Value "Foo" was not valid according to the schema because [WrongType { path: "", detail: "The value must be object" }]"#
            )
        )
    }

    #[test]
    fn test_encode_missing_ref() {
        let _m = mock("GET", "/subjects/testresult-value/versions/latest")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_json_body(json_test_ref_schema(), 10))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut encoder = JsonEncoder::new(sr_settings);
        let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
        let result_example: Value =
            serde_json::from_reader(File::open("tests/schema/jsontest-example.json").unwrap())
                .unwrap();

        let error = encoder.encode(&result_example, &strategy).unwrap_err();

        assert_eq!(
            true,
            error
                .error
                .ends_with("was not valid because of missing references")
        )
    }

    #[test]
    fn decode_invalid_bytes() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let mut decoder = JsonDecoder::new(sr_settings);
        let result = decoder.decode(Some(&[1, 0])).unwrap_err();
        assert_eq!(String::from("Invalid bytes: [1, 0]"), result.error)
    }
}
