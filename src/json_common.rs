use std::collections::HashMap;
use std::sync::Arc;

use serde_json::value::Value;
use url::Url;
use valico::json_schema::schema::ScopedSchema;
use valico::json_schema::validators::ValidationState;
use valico::json_schema::Scope;

use crate::error::SRCError;
use crate::schema_registry_common::{get_payload, RegisteredReference};

/// Cap on how deep a chain of JSON schema `references` is followed before giving up with an
/// `SRCError`. Without a cap, a circular reference chain (a schema that references itself,
/// directly or transitively) recurses without bound -- each level makes a network call, so this
/// is reached long before it could ever stack-overflow, turning what would otherwise be a hang/
/// crash into a clean, actionable error. 32 is generously deep for any legitimate schema.
pub(crate) const MAX_REFERENCE_DEPTH: usize = 32;

pub(crate) fn reference_depth_exceeded_error() -> SRCError {
    SRCError::non_retryable_without_cause(&format!(
        "JSON schema reference chain exceeded {} levels -- this usually means a schema \
         (transitively) references itself",
        MAX_REFERENCE_DEPTH
    ))
}

pub(crate) fn handle_validation(
    validation: ValidationState,
    value: &Value,
) -> Result<(), SRCError> {
    if validation.is_strictly_valid() {
        Ok(())
    } else if validation.errors.is_empty() {
        Err(SRCError::non_retryable_without_cause(&format!(
            "Value {} was not valid because of missing references",
            value
        )))
    } else {
        Err(SRCError::non_retryable_without_cause(&format!(
            "Value {} was not valid according to the schema because {:?}",
            value, validation.errors
        )))
    }
}

pub(crate) fn to_bytes(id: u32, value: &Value) -> Result<Vec<u8>, SRCError> {
    to_bytes_raw(value).map(|bytes| get_payload(id, bytes))
}

/// Like [`to_bytes`], but without the confluent wire-format prefix -- used when the schema
/// id/guid is carried in a Kafka header instead. See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub(crate) fn to_bytes_raw(value: &Value) -> Result<Vec<u8>, SRCError> {
    serde_json::to_vec(value)
        .map_err(|e| SRCError::non_retryable_with_cause(e, "error serialising value to bytes"))
}

pub(crate) fn fetch_id(def: &Value) -> Option<Url> {
    let id = match def {
        Value::Object(m) => {
            let v = m.get("$id")?;
            v.as_str()?
        }
        _ => return None,
    };
    Url::parse(id).ok()
}

/// `label` should uniquely identify the schema -- its numeric id when known, or its guid when
/// the numeric id isn't available (e.g. a schema resolved via [`crate::schema_registry_common`]'s
/// guid lookup, which -- unlike the id lookup -- never gets a real numeric id back from the
/// registry). A non-unique label (e.g. the same placeholder for every guid-only lookup) would
/// make two different schemas collide on the same fallback url and silently share one compiled
/// schema.
pub(crate) fn fetch_fallback(url: &str, label: &str) -> Url {
    let id = format!("{}/id/{}.json", url, label);
    Url::parse(&id).unwrap()
}

pub(crate) fn to_value(str: &str) -> Result<Value, SRCError> {
    let value: Value = match serde_json::from_str(str) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                &format!("could not parse schema {} to a value", str),
            ))
        }
    };
    Ok(value)
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
#[non_exhaustive]
pub struct JsonSchema {
    pub id: u32,
    pub url: Url,
    pub schema: Value,
    pub references: Vec<JsonSchema>,
    pub subject: Option<String>,
    pub version: Option<u32>,
    pub properties: Option<HashMap<String, String>>,
    pub tags: Option<HashMap<String, Vec<String>>>,
    /// The schema's UUID, as returned by Confluent Schema Registry 8.0+. `None` against an older
    /// registry. Used by the `_with_header_id` encode methods to build a
    /// [`crate::schema_registry_common::SchemaIdHeader`].
    pub guid: Option<String>,
}

/// This decode result is not validated yet, if you want to validate it use [`validate`].
#[derive(Debug)]
#[non_exhaustive]
pub struct DecodeResult {
    pub schema: Arc<JsonSchema>,
    pub value: Value,
}

fn add_refs_to_scope<'s>(
    scope: &'s mut Scope,
    schema: &JsonSchema,
) -> Result<ScopedSchema<'s>, SRCError> {
    for reference in &schema.references {
        add_refs_to_scope(scope, reference)?;
    }
    match scope.compile_and_return_with_id(&schema.url, schema.schema.clone(), false) {
        Ok(v) => Ok(v),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "error compiling schema",
        )),
    }
}

/// Validates the value against the schema, including the schemas it references. This compiles
/// the schema each time, so when validating many values against the same schema it might be
/// better to compile it once with `valico` directly.
pub fn validate(schema: &JsonSchema, value: &Value) -> Result<(), SRCError> {
    let mut scope = Scope::new();
    let schema = add_refs_to_scope(&mut scope, schema)?;
    let validation = schema.validate(value);
    handle_validation(validation, value)
}

/// `base` is the url the referencing schema is compiled under -- a reference's `name` is
/// resolved against it the same way a JSON Schema validator resolves a `$ref`, since that's
/// exactly what `name` is used as inside the referencing schema. Confluent doesn't require
/// `name` to be a fully qualified url itself (it's commonly just a bare file name like
/// `result.json`), only that it resolves to one relative to `base`.
pub(crate) fn reference_url(rr: &RegisteredReference, base: &Url) -> Result<Url, SRCError> {
    match Url::options().base_url(Some(base)).parse(&rr.name) {
        Ok(v) => Ok(v),
        Err(e) => Err(SRCError::non_retryable_with_cause(e, &format!("reference schema with subject {} and version {} has invalid id {}, it has to be a fully qualified url or resolve against {}", rr.subject, rr.version, rr.name, base)))
    }
}

/// `guid` is used as the fallback label instead of `id` when `id` is the `0` placeholder
/// `get_schema_by_guid` returns for a schema that has no
/// real numeric id in the registry's response -- without this, every such schema without its own
/// `$id` would collide on the same fallback url and silently share one compiled schema.
pub(crate) fn main_url(schema: &Value, registry_url: &str, id: u32, guid: Option<&str>) -> Url {
    match fetch_id(schema) {
        Some(url) => url,
        None => {
            let label = match guid {
                Some(guid) if id == 0 => guid.to_string(),
                _ => id.to_string(),
            };
            fetch_fallback(registry_url, &label)
        }
    }
}

/// The url the references of a schema compiled under `url` are resolved against: the schema's
/// own `$id` when it has one, like a JSON Schema validator resolves a relative `$ref`.
pub(crate) fn reference_base(schema: &Value, url: &Url) -> Url {
    fetch_id(schema).unwrap_or_else(|| url.clone())
}

#[cfg(test)]
mod tests {
    use crate::json_common::reference_base;
    use serde_json::json;
    use url::Url;

    #[test]
    fn test_reference_base_prefers_own_id() {
        let url = Url::parse("http://registry/id/1.json").unwrap();
        let with_id = json!({"$id": "http://example.com/schemas/a.json"});
        assert_eq!(
            reference_base(&with_id, &url).as_str(),
            "http://example.com/schemas/a.json"
        );
        assert_eq!(reference_base(&json!({}), &url), url);
    }
}
