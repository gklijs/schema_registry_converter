use crate::error::SRCError;
use crate::proto_resolver::{IndexResolver, MessageResolver};
use crate::schema_registry_common::{get_payload, RegisteredSchema};
use integer_encoding::VarInt;

pub(crate) fn to_bytes(
    encode_context: &EncodeContext,
    bytes: &[u8],
    full_name: &str,
) -> Result<Vec<u8>, SRCError> {
    let mut index_bytes = match encode_context.resolver.find_index(full_name) {
        Some(v) if v.len() == 1 && v[0] == 0i32 => vec![0u8],
        Some(v) => {
            let mut result = (v.len() as i32).encode_var_vec();
            for i in v {
                result.append(&mut i.encode_var_vec())
            }
            result
        }
        None => {
            return Err(SRCError::non_retryable_without_cause(&*format!(
                "could not find name {} with resolver",
                full_name
            )))
        }
    };
    index_bytes.extend(bytes);
    Ok(get_payload(encode_context.id, index_bytes))
}

pub(crate) fn to_bytes_single_message(
    encode_context: &EncodeContext,
    bytes: &[u8],
) -> Result<Vec<u8>, SRCError> {
    if encode_context.resolver.is_single_message() {
        let mut index_bytes = vec![0u8];
        index_bytes.extend(bytes);
        Ok(get_payload(encode_context.id, index_bytes))
    } else {
        Err(SRCError::new(
            "Schema was no single message schema",
            None,
            false,
        ))
    }
}

pub(crate) fn to_decode_context(registered_schema: RegisteredSchema) -> DecodeContext {
    let schema = String::from(&registered_schema.schema);
    DecodeContext {
        schema: registered_schema,
        resolver: MessageResolver::new(&*schema),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EncodeContext {
    pub(crate) id: u32,
    pub(crate) resolver: IndexResolver,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodeContext {
    pub(crate) schema: RegisteredSchema,
    pub(crate) resolver: MessageResolver,
}
