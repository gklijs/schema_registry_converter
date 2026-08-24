use crate::error::SRCError;
use crate::proto_resolver::{IndexResolver, MessageResolver};
use crate::schema_registry_common::{get_payload, RegisteredSchema};
use integer_encoding::VarInt;

/// The message-index bytes for `full_name` in `encode_context`'s schema. This is the confluent
/// wire format's way of pointing at which message in a (possibly multi-message) `.proto` schema
/// a payload uses -- normally embedded in the payload right after the id, but for
/// `_with_header_id` encoding it instead gets appended to the id/guid in the header (see
/// [`crate::schema_registry_common::build_schema_id_header`]). See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub(crate) fn index_bytes(
    encode_context: &EncodeContext,
    full_name: &str,
) -> Result<Vec<u8>, SRCError> {
    match encode_context.resolver.find_index(full_name) {
        Some(v) if v.len() == 1 && v[0] == 0i32 => Ok(vec![0u8]),
        Some(v) => {
            let mut result = (v.len() as i32).encode_var_vec();
            for i in &*v {
                result.append(&mut i.encode_var_vec())
            }
            Ok(result)
        }
        None => Err(SRCError::non_retryable_without_cause(&format!(
            "could not find name {} with resolver",
            full_name
        ))),
    }
}

/// Like [`index_bytes`], for the single-message case (`to_bytes_single_message`/
/// `to_bytes_single_message_raw`).
pub(crate) fn index_bytes_single_message(
    encode_context: &EncodeContext,
) -> Result<Vec<u8>, SRCError> {
    if encode_context.resolver.is_single_message() {
        Ok(vec![0u8])
    } else {
        Err(SRCError::new(
            "Schema was no single message schema",
            None,
            false,
        ))
    }
}

pub(crate) fn to_bytes(
    encode_context: &EncodeContext,
    bytes: &[u8],
    full_name: &str,
) -> Result<Vec<u8>, SRCError> {
    let mut payload = index_bytes(encode_context, full_name)?;
    payload.extend(bytes);
    Ok(get_payload(encode_context.id, payload))
}

pub(crate) fn to_bytes_single_message(
    encode_context: &EncodeContext,
    bytes: &[u8],
) -> Result<Vec<u8>, SRCError> {
    let mut payload = index_bytes_single_message(encode_context)?;
    payload.extend(bytes);
    Ok(get_payload(encode_context.id, payload))
}

pub(crate) fn to_decode_context(registered_schema: RegisteredSchema) -> DecodeContext {
    let schema = String::from(&registered_schema.schema);
    DecodeContext {
        schema: registered_schema,
        resolver: MessageResolver::new(&schema),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodeContext {
    pub(crate) id: u32,
    /// The schema's UUID, as returned by Confluent Schema Registry 8.0+. `None` against an
    /// older registry. Used by the `_with_header_id` encode methods to build a
    /// [`crate::schema_registry_common::SchemaIdHeader`].
    pub(crate) guid: Option<String>,
    pub(crate) resolver: IndexResolver,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodeContext {
    pub(crate) schema: RegisteredSchema,
    pub(crate) resolver: MessageResolver,
}
