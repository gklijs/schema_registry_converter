//! Common structures and functions of the crate

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::{Name, ResolvedSchema, Schema};
use apache_avro::types::{Record, Value};
use apache_avro::writer::datum::GenericDatumWriter;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use serde::ser::Serialize;
use serde_json::{value, Map};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use crate::error::SRCError;
use crate::schema_registry_common::{get_payload, SchemaType, SuppliedSchema};

/// Because we need both the resulting schema, as have a way of posting the schema as json, we use
/// this struct so we keep them both together.
///
/// `subject` and `version` are populated when the registry response includes them. See
/// [`crate::schema_registry_common::RegisteredSchema`] for the caveats — a schema id can be
/// registered against multiple subjects.
///
/// `properties` and `tags` carry the Confluent `metadata` block verbatim as returned by the
/// registry: `tags` is keyed by schema path (e.g. `io.confluent.field.<name>`) with the list of
/// tags on that path. Each is populated when the registry response includes it.
#[derive(Debug, PartialEq)]
pub struct AvroSchema {
    pub id: u32,
    pub raw: String,
    pub parsed: Schema,
    pub subject: Option<String>,
    pub version: Option<u32>,
    pub properties: Option<HashMap<String, String>>,
    pub tags: Option<HashMap<String, Vec<String>>>,
    /// The schema's UUID, as returned by Confluent Schema Registry 8.0+. `None` against an older
    /// registry. Used by the `_with_header_id` encode methods to build a [`crate::schema_registry_common::SchemaIdHeader`].
    pub guid: Option<String>,
}

/// A schema's named types resolved once and reused for every subsequent encode/decode of that
/// schema id, instead of redone from scratch on every call the way the now-deprecated
/// `to_avro_datum`/`from_avro_datum` free functions do internally (each builds a fresh
/// `GenericDatumReader`/`GenericDatumWriter`, which resolves the full schema tree into a
/// `ResolvedSchema` every time). See https://github.com/gklijs/schema_registry_converter/issues/190.
///
/// `schema` is `Box::leak`'d to get the `'static` borrow `ResolvedSchema` needs. That never gets
/// freed, but neither does the `Arc<AvroSchema>` this is resolved from -- `AvroEncoder`'s and
/// `AvroDecoder`'s own schema caches never evict a successfully-fetched entry either, so this
/// doesn't change the crate's existing "schemas we've seen stay resident for the process's
/// lifetime" behavior, just adds one more (much cheaper to reproduce, being just a resolved
/// lookup table rather than a freshly parsed/validated schema) copy alongside it.
#[derive(Debug)]
pub(crate) struct ResolvedContext {
    schema: &'static Schema,
    resolved: ResolvedSchema<'static>,
}

/// Per-`AvroEncoder`/`AvroDecoder`-instance cache for [`ResolvedContext`], keyed by schema id.
///
/// Deliberately *not* a single process-wide cache: a schema id is only unique within the schema
/// registry it came from, and this crate explicitly supports encoders/decoders pointed at
/// different registries coexisting in the same process (and, more mundanely, different tests in
/// this crate's own suite reusing the same small mock ids for unrelated schemas). A shared
/// global keyed on the bare id would silently hand back the wrong resolution for id collisions
/// like that -- caught by the test suite failing under `cargo test`'s default parallelism, even
/// though every individual affected test passed in isolation.
pub(crate) type ResolvedSchemaCache = DashMap<u32, Arc<ResolvedContext>>;

fn resolved_context(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
) -> Result<Arc<ResolvedContext>, SRCError> {
    match cache.entry(avro_schema.id) {
        Entry::Occupied(entry) => Ok(entry.get().clone()),
        Entry::Vacant(entry) => {
            let schema: &'static Schema = Box::leak(Box::new(avro_schema.parsed.clone()));
            let resolved = ResolvedSchema::try_from(schema).map_err(|e| {
                SRCError::non_retryable_with_cause(e, "Could not resolve Avro schema")
            })?;
            let context = Arc::new(ResolvedContext { schema, resolved });
            entry.insert(context.clone());
            Ok(context)
        }
    }
}

/// Decodes bytes into an apache_avro `Value` using the writer schema in `avro_schema`, reusing
/// its cached schema resolution (see [`resolved_context`]) rather than rebuilding it per call.
pub(crate) fn decode_bytes(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    bytes: &[u8],
) -> Result<Value, SRCError> {
    let context = resolved_context(cache, avro_schema)?;
    let mut reader = Cursor::new(bytes);
    GenericDatumReader::builder(context.schema)
        .resolved_writer_schemata(context.resolved.clone())
        .build()
        .and_then(|r| r.read_value(&mut reader))
        .map_err(|e| {
            SRCError::non_retryable_with_cause(e, "Could not transform bytes using schema")
        })
}

#[derive(Debug, PartialEq)]
pub struct DecodeResult {
    pub name: Option<Name>,
    pub value: Value,
}

#[derive(Debug, PartialEq)]
pub struct DecodeResultWithSchema {
    pub name: Option<Name>,
    pub value: Value,
    pub schema: Arc<AvroSchema>,
}

fn might_replace(
    val: value::Value,
    child: &value::Value,
    replace_values: &DashMap<String, String>,
) -> value::Value {
    match val {
        value::Value::Object(v) => replace_in_map(v, child, replace_values),
        value::Value::Array(v) => replace_in_array(&v, child, replace_values),
        value::Value::String(s) if replace_values.contains_key(&*s) => child.clone(),
        p => p,
    }
}

fn replace_in_array(
    parent_array: &[value::Value],
    child: &value::Value,
    replace_values: &DashMap<String, String>,
) -> value::Value {
    value::Value::Array(
        parent_array
            .iter()
            .map(|v| might_replace(v.clone(), child, replace_values))
            .collect(),
    )
}

fn replace_in_map(
    parent_map: Map<String, value::Value>,
    child: &value::Value,
    replace_values: &DashMap<String, String>,
) -> value::Value {
    value::Value::Object(
        parent_map
            .iter()
            .map(|e| {
                (
                    e.0.clone(),
                    might_replace(e.1.clone(), child, replace_values),
                )
            })
            .collect(),
    )
}

pub(crate) fn replace_reference(parent: value::Value, child: value::Value) -> value::Value {
    let (name, namespace) = match &child {
        value::Value::Object(v) => (v["name"].as_str(), v["namespace"].as_str()),
        _ => return parent,
    };
    let replace_values: DashMap<String, String> = DashMap::new();
    match name {
        Some(v) => match namespace {
            Some(u) => {
                let key = format!(".{}.{}", u, v);
                replace_values.insert(key.clone(), key);
                if parent["namespace"].as_str() == namespace {
                    replace_values.insert(String::from(v), String::from(v));
                }
            }
            None => {
                replace_values.insert(String::from(v), String::from(v));
            }
        },
        None => return parent,
    };
    match parent {
        value::Value::Object(v) => replace_in_map(v, &child, &replace_values),
        value::Value::Array(v) => replace_in_array(&v, &child, &replace_values),
        p => p,
    }
}

/// Encodes `record` using the (cached, resolved) writer schema, without the confluent wire-format
/// prefix -- shared by both [`to_bytes`] (prefixed, the default wire format) and the raw variants
/// used by the `_with_header_id` encode methods, where the id/guid instead goes in a Kafka header.
/// See https://github.com/gklijs/schema_registry_converter/issues/139.
fn encode_value(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    record: Value,
) -> Result<Vec<u8>, SRCError> {
    let context = resolved_context(cache, avro_schema)?;
    GenericDatumWriter::builder(context.schema)
        .resolved_schemata(context.resolved.clone())
        .build()
        .and_then(|writer| writer.write_value_to_vec(record))
        .map_err(|e| SRCError::non_retryable_with_cause(e, "Could not get Avro bytes"))
}

fn to_bytes(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    record: Value,
) -> Result<Vec<u8>, SRCError> {
    encode_value(cache, avro_schema, record).map(|v| get_payload(avro_schema.id, v))
}

/// Using the schema with a vector of values the values will be correctly deserialized according to
/// the avro specification.
pub(crate) fn values_to_bytes(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    values: Vec<(&str, Value)>,
) -> Result<Vec<u8>, SRCError> {
    to_bytes(
        cache,
        avro_schema,
        Value::from(build_record(avro_schema, values)?),
    )
}

/// Like [`values_to_bytes`], but without the confluent wire-format prefix -- used when the
/// schema id/guid is carried in a Kafka header instead. See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub(crate) fn values_to_bytes_raw(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    values: Vec<(&str, Value)>,
) -> Result<Vec<u8>, SRCError> {
    encode_value(
        cache,
        avro_schema,
        Value::from(build_record(avro_schema, values)?),
    )
}

fn build_record<'a>(
    avro_schema: &'a AvroSchema,
    values: Vec<(&str, Value)>,
) -> Result<Record<'a>, SRCError> {
    let mut record = match Record::new(&avro_schema.parsed) {
        Some(v) => v,
        None => {
            return Err(SRCError::new(
                "Could not create record from schema",
                None,
                false,
            ));
        }
    };
    for value in values {
        record.put(value.0, value.1)
    }
    Ok(record)
}

/// Serializes `item` directly against the (cached, resolved) writer schema using apache_avro's
/// schema-aware serde serializer (`write_ser_to_vec`), rather than the two-step path this used
/// to take: `to_value(item)` into an untyped intermediate `Value`, then a separate
/// `Value::resolve(&avro_schema.parsed)` pass to make it match the schema (picking union
/// variants, looking up named-type back-references, etc). For a schema with several fields
/// sharing the same named type, that `.resolve()` step alone dominates encode time -- measured
/// ~16x slower on a schema with 8 such fields than one with none, see
/// `benches/avro_bench.rs`'s `avro_encode_struct_cached_named_refs` vs
/// `avro_encode_struct_cached`. Writing directly still validates against the schema as it goes
/// (an unresolvable union variant or missing named type is still an error), it just never
/// allocates the untyped intermediate tree or makes a second pass over it. See
/// https://github.com/gklijs/schema_registry_converter/issues/117.
fn encode_item(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    item: impl Serialize,
) -> Result<Vec<u8>, SRCError> {
    let context = resolved_context(cache, avro_schema)?;
    GenericDatumWriter::builder(context.schema)
        .resolved_schemata(context.resolved.clone())
        .build()
        .and_then(|writer| writer.write_ser_to_vec(&item))
        .map_err(|e| SRCError::non_retryable_with_cause(e, "Could not get Avro bytes"))
}

/// Using the schema with an item implementing serialize the item will be correctly deserialized
/// according to the avro specification.
pub(crate) fn item_to_bytes(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    item: impl Serialize,
) -> Result<Vec<u8>, SRCError> {
    encode_item(cache, avro_schema, item).map(|v| get_payload(avro_schema.id, v))
}

/// Like [`item_to_bytes`], but without the confluent wire-format prefix -- used when the schema
/// id/guid is carried in a Kafka header instead. See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub(crate) fn item_to_bytes_raw(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    item: impl Serialize,
) -> Result<Vec<u8>, SRCError> {
    encode_item(cache, avro_schema, item)
}

pub(crate) fn record_to_bytes(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    item: Value,
) -> Result<Vec<u8>, SRCError> {
    to_bytes(cache, avro_schema, item)
}

/// Like [`record_to_bytes`], but without the confluent wire-format prefix -- used when the schema
/// id/guid is carried in a Kafka header instead. See
/// https://github.com/gklijs/schema_registry_converter/issues/139.
pub(crate) fn record_to_bytes_raw(
    cache: &ResolvedSchemaCache,
    avro_schema: &AvroSchema,
    item: Value,
) -> Result<Vec<u8>, SRCError> {
    encode_value(cache, avro_schema, item)
}

pub(crate) fn get_name(schema: &Schema) -> Option<Name> {
    match schema {
        Schema::Record(schema) => Some(schema.name.clone()),
        _ => None,
    }
}

pub fn get_supplied_schema(schema: &Schema) -> SuppliedSchema {
    let name = match get_name(schema) {
        None => None,
        Some(n) => match n.namespace() {
            None => Some(n.name().to_string()),
            Some(ns) => Some(format!("{}.{}", ns, n.name())),
        },
    };
    SuppliedSchema {
        name,
        schema_type: SchemaType::Avro,
        schema: serde_json::to_string(schema).unwrap(),
        references: vec![],
        properties: None,
        tags: None,
    }
}

/// Like [`get_supplied_schema`], but for a type implementing `apache_avro`'s own [`AvroSchema`
/// trait`](apache_avro::AvroSchema) -- most commonly via `#[derive(apache_avro::AvroSchema)]`
/// (needs apache_avro's `derive` feature) -- instead of an already-built [`Schema`]. Saves
/// hand-writing and maintaining the schema as a separate JSON string: the registered schema
/// always matches the exact shape apache_avro's own serializer will produce for `T`, so there's
/// nothing to drift out of sync with the struct.
///
/// Combine it with any of the `*WithSchema`
/// [`SubjectNameStrategy`](crate::schema_registry_common::SubjectNameStrategy) variants, which
/// already register the schema with the registry if it isn't there yet:
/// ```
/// use apache_avro::AvroSchema;
/// use serde::Serialize;
/// use schema_registry_converter::avro_common::get_supplied_schema_for;
/// use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
///
/// #[derive(Serialize, AvroSchema)]
/// struct Heartbeat {
///     beat: i64,
/// }
///
/// let strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_supplied_schema_for::<Heartbeat>());
/// ```
///
/// See https://github.com/gklijs/schema_registry_converter/issues/111.
pub fn get_supplied_schema_for<T: apache_avro::AvroSchema>() -> SuppliedSchema {
    get_supplied_schema(&T::get_schema())
}

#[cfg(test)]
mod tests {
    use apache_avro::types::Value;
    use apache_avro::Schema;
    use dashmap::DashMap;

    use test_utils::{Atype, ConfirmAccountCreation, Heartbeat};

    use crate::avro_common::{get_name, get_supplied_schema_for, values_to_bytes, AvroSchema};
    use crate::error::SRCError;
    use crate::schema_registry_common::SchemaType;

    #[test]
    fn get_supplied_schema_for_matches_derived_schema() {
        // See https://github.com/gklijs/schema_registry_converter/issues/111.
        #[derive(serde::Serialize, apache_avro::AvroSchema)]
        struct SimpleRecord {
            #[allow(dead_code)]
            count: i64,
        }

        let supplied = get_supplied_schema_for::<SimpleRecord>();
        assert_eq!(supplied.schema_type, SchemaType::Avro);
        assert!(supplied.references.is_empty());
        assert!(supplied.properties.is_none());
        assert!(supplied.tags.is_none());

        // The registered JSON must round-trip into a schema apache_avro itself considers the
        // same record `SimpleRecord::get_schema()` produces -- otherwise the registered schema
        // and the data this crate would actually send could disagree.
        let parsed = Schema::parse_str(&supplied.schema)
            .expect("get_supplied_schema_for's output must be valid Avro schema JSON");
        let derived = <SimpleRecord as apache_avro::AvroSchema>::get_schema();
        assert_eq!(get_name(&parsed), get_name(&derived));
        assert_eq!(supplied.name, get_name(&derived).map(|n| n.fullname(None)));
    }

    #[test]
    fn to_bytes_no_record() {
        let schema = AvroSchema {
            id: 5,
            raw: "".to_string(),
            parsed: Schema::Boolean,
            subject: None,
            version: None,
            properties: None,
            tags: None,
            guid: None,
        };
        let result = values_to_bytes(&DashMap::new(), &schema, vec![("beat", Value::Long(3))]);
        assert_eq!(
            result,
            Err(SRCError::new(
                "Could not create record from schema",
                None,
                false,
            ))
        )
    }

    #[test]
    fn to_bytes_no_transfer_wrong() {
        let schema = AvroSchema {
            id: 5,
            raw: String::from(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#),
            parsed: Schema::parse_str(r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#).unwrap(),
            subject: None,
            version: None,
            properties: None,
            tags: None,
            guid: None,
        };
        let err =
            values_to_bytes(&DashMap::new(), &schema, vec![("beat", Value::Long(3))]).unwrap_err();
        assert_eq!(err.error, "Could not get Avro bytes")
    }

    #[test]
    fn item_to_bytes_no_tranfer_wrong() {
        let schema = AvroSchema {
            id: 5,
            raw: String::from(
                r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#,
            ),
            parsed: Schema::parse_str(
                r#"{"type":"record","name":"Name","namespace":"nl.openweb.data","fields":[{"name":"name","type":"string","avro.java.string":"String"}]}"#,
            ).unwrap(),
            subject: None,
            version: None,
            properties: None,
            tags: None,
            guid: None,
        };
        let err =
            crate::avro_common::item_to_bytes(&DashMap::new(), &schema, Heartbeat { beat: 3 })
                .unwrap_err();
        assert_eq!(err.error, "Could not get Avro bytes")
    }

    #[test]
    fn item_to_bytes_still_broken() {
        let schema = AvroSchema {
            id: 6,
            raw: String::from(
                r#"{"type":"record","name":"ConfirmAccountCreation","namespace":"nl.openweb.data","fields":[{"name":"id","type":{"type":"fixed","name":"Uuid","size":16}},{"name":"a_type","type":{"type":"enum","name":"Atype","symbols":["AUTO","MANUAL"]}}]}"#,
            ),
            parsed: Schema::parse_str(
                r#"{"type":"record","name":"ConfirmAccountCreation","namespace":"nl.openweb.data","fields":[{"name":"id","type":{"type":"fixed","name":"Uuid","size":16}},{"name":"a_type","type":{"type":"enum","name":"Atype","symbols":["AUTO","MANUAL"]}}]}"#,
            ).unwrap(),
            subject: None,
            version: None,
            properties: None,
            tags: None,
            guid: None,
        };
        let item = ConfirmAccountCreation {
            id: [
                204, 240, 237, 74, 227, 188, 75, 46, 183, 163, 122, 214, 178, 72, 118, 162,
            ],
            a_type: Atype::Manual,
        };
        let err = crate::avro_common::item_to_bytes(&DashMap::new(), &schema, item).unwrap_err();
        assert_eq!(err.error, "Could not get Avro bytes")
    }
}
