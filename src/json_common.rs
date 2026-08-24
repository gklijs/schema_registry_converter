use serde_json::value::Value;
use url::Url;
use valico::json_schema::validators::ValidationState;

use crate::error::SRCError;
use crate::schema_registry_common::get_payload;

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
        Value::Object(m) => match m.get("$id") {
            Some(v) => v.as_str()?,
            None => return None,
        },
        _ => return None,
    };
    Url::parse(id).ok()
}

pub(crate) fn fetch_fallback(url: &str, id: u32) -> Url {
    let id = format!("{}/id/{}.json", url, id);
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
