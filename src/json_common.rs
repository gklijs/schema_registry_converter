use serde_json::value::Value;
use url::Url;
use valico::json_schema::validators::ValidationState;

use crate::error::SRCError;
use crate::schema_registry_common::get_payload;

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
