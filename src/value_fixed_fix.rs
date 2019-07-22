//! This module fixes the incorrect handling of fixed values in the schema

use crate::schema_registry::SRCError;
use avro_rs::schema::{RecordField, UnionSchema};
use avro_rs::types::{ToAvro, Value};
use avro_rs::Schema;
use std::collections::HashMap;

pub trait FixedFixer {
    fn fix_fixed(self, schema: &Schema) -> Result<Self, SRCError>
    where
        Self: std::marker::Sized;
    fn fix_fixed_fixed(self, size: usize) -> Result<Self, SRCError>
    where
        Self: std::marker::Sized;
    fn fix_fixed_union(self, schema: &UnionSchema) -> Result<Self, SRCError>
    where
        Self: std::marker::Sized;
    fn fix_fixed_array(self, schema: &Schema) -> Result<Self, SRCError>
    where
        Self: std::marker::Sized;
    fn fix_fixed_map(self, schema: &Schema) -> Result<Self, SRCError>
    where
        Self: std::marker::Sized;
    fn fix_fixed_record(self, schema: &[RecordField]) -> Result<Self, SRCError>
    where
        Self: std::marker::Sized;
}

fn vec_to_fixed(vec: Vec<Value>) -> Result<Value, SRCError> {
    let mut errors: Vec<Value> = vec![];
    let mut hits: Vec<u8> = vec![];
    vec.iter().for_each(|part| match part {
        Value::Int(v) => {
            if *v >= 0 && *v <= 255 {
                hits.push((v & 0xff) as u8)
            } else {
                errors.push(Value::Int(*v))
            }
        }
        other => errors.push(other.clone()),
    });
    if errors.is_empty() {
        Ok(Value::Fixed(vec.len(), hits))
    } else {
        Err(SRCError::non_retryable_from_err(
            format!(
                "Found non-int values and/or ints that can't be converted to bytes: {:#?}",
                errors
            ),
            "could not fix fixed",
        ))
    }
}

impl FixedFixer for Value {
    /// Will go  through the schema and value, and convert arrays of the correct size to fixed
    /// equivalents in order to fix serialization for structs.
    fn fix_fixed(self, schema: &Schema) -> Result<Self, SRCError> {
        match *schema {
            Schema::Fixed { size, .. } => self.fix_fixed_fixed(size),
            Schema::Union(ref inner) => self.fix_fixed_union(inner),
            Schema::Array(ref inner) => self.fix_fixed_array(inner),
            Schema::Map(ref inner) => self.fix_fixed_map(inner),
            Schema::Record { ref fields, .. } => self.fix_fixed_record(fields),
            _ => Ok(self),
        }
    }

    fn fix_fixed_fixed(self, size: usize) -> Result<Self, SRCError> {
        match self {
            Value::Fixed(n, bytes) => {
                if n == size {
                    Ok(Value::Fixed(n, bytes))
                } else {
                    Err(SRCError::non_retryable_from_err(
                        format!("Fixed size mismatch, {} expected, got {}", size, n),
                        "could not fix fixed",
                    ))
                }
            }
            Value::Array(vec) => {
                if vec.len() == size {
                    vec_to_fixed(vec)
                } else {
                    Err(SRCError::non_retryable_from_err(
                        format!("Array size mismatch, {} expected, got {}", size, vec.len()),
                        "could not fix fixed",
                    ))
                }
            }
            other => Err(SRCError::non_retryable_from_err(
                format!("Fixed or vector expected, got {:?}", other),
                "could not fix fixed",
            )),
        }
    }

    fn fix_fixed_union(self, schema: &UnionSchema) -> Result<Self, SRCError> {
        let v = match self {
            // Both are unions case.
            Value::Union(v) => *v,
            // Reader is a union, but writer is not.
            v => v,
        };
        // Find the first match in the reader schema.
        let (_, inner) = schema.find_schema(&v).ok_or_else(|| {
            SRCError::non_retryable_from_err(
                "Could not find matching type in union",
                "could not fix fixed",
            )
        })?;
        v.fix_fixed(inner)
    }

    fn fix_fixed_array(self, schema: &Schema) -> Result<Self, SRCError> {
        match self {
            Value::Array(items) => Ok(Value::Array(
                items
                    .into_iter()
                    .map(|item| item.fix_fixed(schema))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Err(SRCError::non_retryable_from_err(
                format!("Array({:?}) expected, got {:?}", schema, other),
                "could not fix fixed",
            )),
        }
    }

    fn fix_fixed_map(self, schema: &Schema) -> Result<Self, SRCError> {
        match self {
            Value::Map(items) => Ok(Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| value.fix_fixed(schema).map(|value| (key, value)))
                    .collect::<Result<HashMap<_, _>, _>>()?,
            )),
            other => Err(SRCError::non_retryable_from_err(
                format!("Map({:?}) expected, got {:?}", schema, other),
                "could not fix fixed",
            )),
        }
    }

    fn fix_fixed_record(self, fields: &[RecordField]) -> Result<Self, SRCError> {
        let mut items = match self {
            Value::Map(items) => Ok(items),
            Value::Record(fields) => Ok(fields.into_iter().collect::<HashMap<_, _>>()),
            other => Err(SRCError::non_retryable_from_err(
                format!("Record({:?}) expected, got {:?}", fields, other),
                "Error creating record",
            )),
        }?;

        let new_fields = fields
            .iter()
            .map(|field| {
                let value = match items.remove(&field.name) {
                    Some(value) => value,
                    None => match field.default {
                        Some(ref value) => value.clone().avro(),
                        _ => {
                            return Err(SRCError::non_retryable_from_err(
                                format!("missing field {} in record", field.name),
                                "error fixing record",
                            ))
                        }
                    },
                };
                value
                    .fix_fixed(&field.schema)
                    .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Value::Record(new_fields))
    }
}
