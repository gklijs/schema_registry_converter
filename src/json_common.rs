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
    match serde_json::to_vec(value) {
        Ok(bytes) => Ok(get_payload(id, bytes)),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "error serialising value to bytes",
        )),
    }
}

pub(crate) fn fetch_id(def: &Value) -> Option<Url> {
    let id = match def {
        Value::Object(m) => match m.get("$id") {
            Some(v) => match v.as_str() {
                Some(v) => v,
                None => return None,
            },
            None => return None,
        },
        _ => return None,
    };
    match Url::parse(id) {
        Ok(url) => Some(url),
        Err(_) => None,
    }
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
