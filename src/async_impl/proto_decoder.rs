use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use std::sync::Arc;

use crate::async_impl::schema_registry::{
    get_referenced_schemas, get_schema_by_guid_and_type, get_schema_by_id_and_type, SrSettings,
};
use crate::error::SRCError;
use crate::proto_decoder_common::{
    decode_message, to_file_descriptor, MessageIndex, ReferencedSchemas,
};
use crate::proto_resolver::to_index_and_data;
use crate::schema_registry_common::{
    get_bytes_result, parse_schema_id_header, BytesResult, HeaderSchemaId, RegisteredSchema,
    SchemaType,
};
use prost_reflect::bytes::Bytes;
use prost_reflect::{DynamicMessage, FileDescriptor, Value};

type SharedFutureContext = Shared<BoxFuture<'static, Result<Arc<DecodeContext>, SRCError>>>;

#[derive(Debug)]
pub struct ProtoDecoder {
    sr_settings: SrSettings,
    direct_cache: DashMap<u32, Arc<DecodeContext>>,
    cache: DashMap<u32, SharedFutureContext>,
    /// Cache for schemas looked up by guid rather than id, used by
    /// [`ProtoDecoder::decode_with_header_id`]. Kept separate from `direct_cache`/`cache`
    /// (keyed by id) since a guid and an id are different keyspaces.
    guid_direct_cache: DashMap<String, Arc<DecodeContext>>,
    guid_cache: DashMap<String, SharedFutureContext>,
}

impl ProtoDecoder {
    /// Creates a new decoder which will use the supplied url used in creating the sr settings to
    /// fetch the schema's since the schema needed is encoded in the binary, independent of the
    /// SubjectNameStrategy we don't need any additional data. Retriable errors (e.g. a transient
    /// network/HTTP failure) are never cached, so those are simply retried on the next call.
    /// Non-retriable errors (e.g. a schema that doesn't exist or can't be parsed) are cached to
    /// avoid repeatedly retrying a lookup that isn't going to succeed; if the underlying problem
    /// gets fixed you can use remove_errors_from_cache to clean those out.
    pub fn new(sr_settings: SrSettings) -> ProtoDecoder {
        ProtoDecoder {
            sr_settings,
            direct_cache: DashMap::new(),
            cache: DashMap::new(),
            guid_direct_cache: DashMap::new(),
            guid_cache: DashMap::new(),
        }
    }
    /// Removes all non-retriable errors from the cache. You might need/want to run this when the
    /// underlying problem has been fixed (e.g. a schema was missing and has since been
    /// registered) and want the next call to try again immediately. Retriable errors aren't
    /// stored in the cache in the first place, so there's nothing to remove for those.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
        self.guid_cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(Value::Bytes(Bytes::new())),
            BytesResult::Valid(id, bytes) => Ok(Value::Message(self.deserialize(id, bytes).await?)),
            BytesResult::Invalid(i) => Ok(Value::Bytes(Bytes::copy_from_slice(i))),
        }
    }
    /// Like [`ProtoDecoder::decode`], but for a message that may carry its schema id/guid and
    /// message index in a `__key_schema_id`/`__value_schema_id` header instead of (or in
    /// addition to) the payload prefix, mirroring Confluent's `DualSchemaIdDeserializer`:
    /// `header_value` present -> resolve the schema and message index from it and treat `bytes`
    /// as the raw (unprefixed) payload; `header_value` absent -> falls straight through to
    /// [`ProtoDecoder::decode`], so the only overhead for a caller who always passes `None` is
    /// this one check. See https://github.com/gklijs/schema_registry_converter/issues/139.
    pub async fn decode_with_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Value, SRCError> {
        let payload = match bytes {
            Some(v) => v,
            None => return Ok(Value::Bytes(Bytes::new())),
        };
        match header_value {
            None => self.decode(Some(payload)).await,
            Some(header) => {
                let (schema_id, index_bytes) = parse_schema_id_header(header)?;
                let context = match schema_id {
                    HeaderSchemaId::Id(id) => self.get_context(id).await?,
                    HeaderSchemaId::Guid(guid) => self.get_context_by_guid(guid).await?,
                };
                let (index, _empty) = to_index_and_data(index_bytes)?;
                let descriptor = context.messages.find(&index)?;
                Ok(Value::Message(decode_message(descriptor, payload)?))
            }
        }
    }
    /// Like [`ProtoDecoder::decode_with_header_id`], but returns the resolved [`DecodeContext`]
    /// (schema metadata and the compiled schema) alongside the value and
    /// full name, mirroring [`ProtoDecoder::decode_with_context`] for the header-aware path.
    /// `header_value` present -> resolve the schema and message index from it and treat `bytes`
    /// as the raw (unprefixed) payload; `header_value` absent -> falls straight through to
    /// [`ProtoDecoder::decode_with_context`].
    pub async fn decode_with_context_and_header_id(
        &self,
        header_value: Option<&[u8]>,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResultWithContext>, SRCError> {
        let payload = match bytes {
            Some(v) => v,
            None => return Ok(None),
        };
        match header_value {
            None => self.decode_with_context(Some(payload)).await,
            Some(header) => {
                let (schema_id, index_bytes) = parse_schema_id_header(header)?;
                let context = match schema_id {
                    HeaderSchemaId::Id(id) => self.get_context(id).await?,
                    HeaderSchemaId::Guid(guid) => self.get_context_by_guid(guid).await?,
                };
                let (index, _empty) = to_index_and_data(index_bytes)?;
                let descriptor = context.messages.find(&index)?;
                let full_name = descriptor.full_name().to_owned();
                let value = decode_message(descriptor, payload)?;
                Ok(Some(DecodeResultWithContext {
                    value,
                    context,
                    full_name,
                    data_bytes: payload.to_vec(),
                }))
            }
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    async fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<DynamicMessage, SRCError> {
        let context = self.get_context(id).await?;
        let (index, data) = to_index_and_data(bytes)?;
        let descriptor = context.messages.find(&index)?;
        decode_message(descriptor, &data)
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub async fn decode_with_context(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<Option<DecodeResultWithContext>, SRCError> {
        match get_bytes_result(bytes) {
            BytesResult::Null => Ok(None),
            BytesResult::Valid(id, bytes) => match self.deserialize_with_context(id, bytes).await {
                Ok(v) => Ok(Some(v)),
                Err(e) => Err(e),
            },
            BytesResult::Invalid(_) => {
                Err(SRCError::new("no protobuf compatible bytes", None, false))
            }
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    async fn deserialize_with_context(
        &self,
        id: u32,
        bytes: &[u8],
    ) -> Result<DecodeResultWithContext, SRCError> {
        let context = self.get_context(id).await?;
        let (index, data_bytes) = to_index_and_data(bytes)?;
        let descriptor = context.messages.find(&index)?;
        let full_name = descriptor.full_name().to_owned();
        let value = decode_message(descriptor, &data_bytes)?;
        Ok(DecodeResultWithContext {
            value,
            context,
            full_name,
            data_bytes,
        })
    }
    /// Gets the decode context directly or via a shared future. The direct cache main function
    /// is for performance.
    async fn get_context(&self, id: u32) -> Result<Arc<DecodeContext>, SRCError> {
        match self.direct_cache.get(&id) {
            None => {
                let result = self.get_context_by_shared_future(id).await;
                match result {
                    Ok(v) => {
                        if !self.direct_cache.contains_key(&id) {
                            self.direct_cache.insert(id, v.clone());
                            self.cache.remove(&id);
                        }
                        Ok(v)
                    }
                    // Retriable errors (transient network/HTTP issues) don't make sense to
                    // cache, so the entry is dropped and the next call issues a fresh request
                    // rather than replaying a stale failure.
                    Err(e) if e.retriable => {
                        self.cache.remove(&id);
                        Err(e)
                    }
                    Err(e) => Err(e.into_cache()),
                }
            }
            Some(result) => Ok(result.value().clone()),
        }
    }
    /// Gets the decode context by a shared future, to prevent multiple of the same calls to
    /// schema registry, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn get_context_by_shared_future(&self, id: u32) -> SharedFutureContext {
        match self.cache.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let v = async move {
                    match get_schema_by_id_and_type(id, &sr_settings, SchemaType::Protobuf).await {
                        Ok(v) => to_decode_context(&sr_settings, v).await,
                        Err(e) => Err(e),
                    }
                }
                .boxed()
                .shared();
                e.insert(v).value().clone()
            }
        }
    }

    /// Like [`ProtoDecoder::get_context`], but looks the schema up by guid instead of id
    /// -- used by [`ProtoDecoder::decode_with_header_id`] when the header carries a guid rather
    /// than an id.
    async fn get_context_by_guid(&self, guid: String) -> Result<Arc<DecodeContext>, SRCError> {
        match self.guid_direct_cache.get(&guid) {
            None => {
                let result = self.get_context_by_guid_shared_future(guid.clone()).await;
                match result {
                    Ok(v) => {
                        if !self.guid_direct_cache.contains_key(&guid) {
                            self.guid_direct_cache.insert(guid.clone(), v.clone());
                            self.guid_cache.remove(&guid);
                        }
                        Ok(v)
                    }
                    Err(e) if e.retriable => {
                        self.guid_cache.remove(&guid);
                        Err(e)
                    }
                    Err(e) => Err(e.into_cache()),
                }
            }
            Some(result) => Ok(result.value().clone()),
        }
    }

    fn get_context_by_guid_shared_future(&self, guid: String) -> SharedFutureContext {
        match self.guid_cache.entry(guid.clone()) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let v = async move {
                    match get_schema_by_guid_and_type(&guid, &sr_settings, SchemaType::Protobuf)
                        .await
                    {
                        Ok(v) => to_decode_context(&sr_settings, v).await,
                        Err(e) => Err(e),
                    }
                }
                .boxed()
                .shared();
                e.insert(v).value().clone()
            }
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct DecodeResultWithContext {
    pub value: DynamicMessage,
    pub context: Arc<DecodeContext>,
    pub full_name: String,
    pub data_bytes: Vec<u8>,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct DecodeContext {
    pub schema: RegisteredSchema,
    /// The compiled root schema, its imports can be reached via
    /// [`FileDescriptor::parent_pool`].
    pub file_descriptor: FileDescriptor,
    /// The messages of the root schema by their confluent message index, see [`MessageIndex`].
    pub(crate) messages: MessageIndex,
}

/// Compiles the schema texts into a `DecodeContext`. The result is cached per schema id/guid,
/// like the blocking decoder does, so a compile failure is cached too (as a non-retriable error,
/// see `remove_errors_from_cache`). See
/// https://github.com/gklijs/schema_registry_converter/issues/175.
fn into_decode_context(
    schema: RegisteredSchema,
    referenced_schemas: ReferencedSchemas,
) -> Result<DecodeContext, SRCError> {
    let file_descriptor = to_file_descriptor(&schema.schema, referenced_schemas)?;
    let messages = MessageIndex::new(&file_descriptor);
    Ok(DecodeContext {
        schema,
        file_descriptor,
        messages,
    })
}

async fn to_decode_context(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
) -> Result<Arc<DecodeContext>, SRCError> {
    let referenced_schemas = get_referenced_schemas(sr_settings, &registered_schema).await?;
    Ok(Arc::new(into_decode_context(
        registered_schema,
        referenced_schemas,
    )?))
}

#[cfg(test)]
mod tests {
    use crate::async_impl::proto_decoder::{into_decode_context, ProtoDecoder};
    use crate::async_impl::schema_registry::SrSettings;
    use crate::proto_decoder_common::ReferencedSchemas;
    use crate::schema_registry_common::{RegisteredSchema, SchemaType};
    use mockito::Server;
    use prost_reflect::{DynamicMessage, Value};
    use test_utils::{
        get_proto_complex, get_proto_complex_proto_test_message, get_proto_complex_references,
        get_proto_hb_101, get_proto_hb_101_empty_payload, get_proto_hb_schema,
        get_proto_money_result, get_proto_result,
    };

    fn field(message: &DynamicMessage, name: &str) -> Value {
        message.get_field_by_name(name).unwrap().into_owned()
    }

    fn dummy_registered_schema(schema: &str) -> RegisteredSchema {
        RegisteredSchema {
            id: 0,
            schema_type: SchemaType::Protobuf,
            schema: schema.to_owned(),
            references: Vec::new(),
            properties: None,
            tags: None,
            subject: None,
            version: None,
            guid: None,
        }
    }

    fn get_proto_body(schema: &str, id: u32) -> String {
        format!(
            "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":{}}}",
            schema, id
        )
    }

    fn get_proto_body_with_reference(schema: &str, id: u32, reference: &str) -> String {
        format!(
            "{{\"schema\":\"{}\", \"schemaType\":\"PROTOBUF\", \"id\":{}, \"references\":[{}]}}",
            schema, id, reference
        )
    }

    #[tokio::test]
    async fn test_decoder_default() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::U64(101u64), field(&message, "beat"))
    }

    #[tokio::test]
    async fn test_decode_with_header_id() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock(
                "GET",
                "/schemas/guids/cc0e0e0e-53c1-4a1a-8f1a-000000000001?deleted=true",
            )
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        // magic byte 0x01 + 16-byte guid + single-message index byte 0x00
        let header_value = [
            0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x00,
        ];
        let payload = &get_proto_hb_101()[6..]; // data only, no prefix/index

        let heartbeat = decoder
            .decode_with_header_id(Some(&header_value), Some(payload))
            .await
            .unwrap();

        let message = match heartbeat {
            Value::Message(x) => x,
            v => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(Value::U64(101u64), field(&message, "beat"));

        // header absent -> falls straight through to decode(), which expects the prefix
        let _m2 = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();
        let heartbeat = decoder
            .decode_with_header_id(None, Some(get_proto_hb_101()))
            .await
            .unwrap();
        let message = match heartbeat {
            Value::Message(x) => x,
            v => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(Value::U64(101u64), field(&message, "beat"));
    }

    #[tokio::test]
    async fn test_decode_with_context_and_header_id() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock(
                "GET",
                "/schemas/guids/cc0e0e0e-53c1-4a1a-8f1a-000000000001?deleted=true",
            )
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        // magic byte 0x01 + 16-byte guid + single-message index byte 0x00
        let header_value = [
            0x01, 0xcc, 0x0e, 0x0e, 0x0e, 0x53, 0xc1, 0x4a, 0x1a, 0x8f, 0x1a, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x00,
        ];
        let payload = &get_proto_hb_101()[6..]; // data only, no prefix/index

        let result = decoder
            .decode_with_context_and_header_id(Some(&header_value), Some(payload))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(Value::U64(101u64), field(&result.value, "beat"));
        assert_eq!(result.context.schema.id, 1);
        assert_eq!(result.context.schema.schema_type, SchemaType::Protobuf);
        assert_eq!(&*result.full_name, "nl.openweb.data.Heartbeat");

        // header absent -> falls straight through to decode_with_context(), which expects the
        // classic wire-format prefix
        let _m2 = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();
        let result = decoder
            .decode_with_context_and_header_id(None, Some(get_proto_hb_101()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Value::U64(101u64), field(&result.value, "beat"));
        assert_eq!(result.context.schema.id, 7);
    }

    #[tokio::test]
    async fn test_decoder_five_byte_message_gives_error_instead_of_panic() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let result = decoder.decode(Some(get_proto_hb_101_empty_payload())).await;

        assert!(result.is_err())
    }

    #[tokio::test]
    async fn test_decode_with_context_default() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode_with_context(Some(get_proto_hb_101()))
            .await
            .unwrap();

        assert!(heartbeat.is_some());

        let result = heartbeat.unwrap();

        assert_eq!(Value::U64(101u64), field(&result.value, "beat"));
        // The registered schema's id reflects the id used to look it up (from the payload's
        // prefix), not the (unrelated, in this mocked response) "id" field of the schema
        // registry response body.
        assert_eq!(result.context.schema.id, 7);
        assert_eq!(result.context.schema.schema_type, SchemaType::Protobuf);
        // get_proto_hb_schema() is pre-escaped for embedding in the mocked JSON body, so the
        // schema string coming back out has those escapes resolved.
        assert_eq!(
            result.context.schema.schema,
            get_proto_hb_schema().replace("\\\"", "\"")
        );
    }

    #[tokio::test]
    async fn test_decode_with_context_complex_returns_top_level_schema() {
        // The context's schema should be the top-level registered schema (id 2), not the
        // referenced result.proto schema (id 1) pulled in along the way.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m = server
            .mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let result = decoder
            .decode_with_context(Some(get_proto_complex_proto_test_message()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(field(&result.value, "counter"), Value::I64(1));
        assert_eq!(result.context.schema.id, 6);
        assert_eq!(
            result.context.schema.schema,
            get_proto_complex().replace("\\\"", "\"")
        );
    }

    #[tokio::test]
    async fn test_decode_with_context_uses_cached_schema_on_second_call() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .expect(1)
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);

        let first = decoder
            .decode_with_context(Some(get_proto_hb_101()))
            .await
            .unwrap()
            .unwrap();
        let second = decoder
            .decode_with_context(Some(get_proto_hb_101()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(first.context.schema.id, second.context.schema.id);
        assert_eq!(first.context.schema.schema, second.context.schema.schema);
        _m.assert_async().await;
    }

    #[tokio::test]
    async fn test_decoder_cache() {
        let mut server = Server::new_async().await;
        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let error = decoder.decode(Some(get_proto_hb_101())).await.unwrap_err();
        assert!(error.cached);

        let _m = server
            .mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let error = decoder.decode(Some(get_proto_hb_101())).await.unwrap_err();
        assert!(error.cached);

        decoder.remove_errors_from_cache();

        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::U64(101u64), field(&message, "beat"));
    }

    #[tokio::test]
    async fn test_decoder_complex() {
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m = server
            .mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let proto_test = decoder
            .decode(Some(get_proto_complex_proto_test_message()))
            .await
            .unwrap();

        let message = match proto_test {
            Value::Message(x) => x,
            v => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(field(&message, "counter"), Value::I64(1))
    }

    #[tokio::test]
    async fn test_decoder_complex_with_common_type_import_on_reference() {
        // Mirrors the blocking regression test for
        // https://github.com/gklijs/schema_registry_converter/issues/178: the *referenced*
        // schema (not the top-level one) imports a well-known/common type. The async decoder
        // already handled this correctly; kept here so both implementations stay covered.
        let mut server = Server::new_async().await;
        let _m = server
            .mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m = server
            .mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(get_proto_body(get_proto_money_result(), 1))
            .create();

        let sr_settings = SrSettings::new_builder(server.url())
            .no_proxy()
            .build()
            .unwrap();
        let decoder = ProtoDecoder::new(sr_settings);
        let proto_test = decoder
            .decode(Some(get_proto_complex_proto_test_message()))
            .await
            .unwrap();

        let message = match proto_test {
            Value::Message(x) => x,
            v => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(field(&message, "counter"), Value::I64(1))
    }

    #[test]
    fn display_decoder() {
        let sr_settings = SrSettings::new(String::from("http://127.0.0.1:1234"));
        let decoder = ProtoDecoder::new(sr_settings);
        assert!(
            format!("{:?}", decoder).starts_with("ProtoDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client {")
        )
    }

    #[test]
    fn test_into_decode_context() {
        let base_schema = "syntax = \"proto3\";\npackage a.b.c;\n\nimport \"google/protobuf/timestamp.proto\";\n\noption java_outer_classname = \"MetadataProto\";\n\nmessage Metadata {\n  string field1 = 1;\n  string field2 = 2;\n  .google.protobuf.Timestamp field3 = 3;\n}\n";
        let top_schema = "syntax = \"proto3\";\npackage a.b.c.d;\n\nimport \"a/b/c/metadata.proto\";\n\noption java_outer_classname = \"TopLevelProto\";\n\nmessage TopLevelMetadata {\n  uint64 field1 = 1;\n  .a.b.c.Metadata metadata = 3;\n\n}\n";
        let mut referenced_schemas = ReferencedSchemas::new();
        referenced_schemas.insert(
            String::from("a/b/c/metadata.proto"),
            base_schema.to_string(),
        );
        let result = into_decode_context(dummy_registered_schema(top_schema), referenced_schemas);
        assert!(result.is_ok())
    }

    #[test]
    fn test_into_decode_context_with_optional_field() {
        // Regression test for https://github.com/gklijs/schema_registry_converter/issues/147:
        // proto3 `optional` fields, which are compiled into a synthetic `oneof _<field>`.
        let schema = "syntax = \"proto3\";\n\npackage in.abc.event_entities;\n\noption java_outer_classname = \"EventSourceProto\";\n\nmessage EventSource {\n  message Actor {\n    enum UserEntity {\n      USER_ENTITY_UNSPECIFIED = 0;\n      USER_ENTITY_ADMIN = 1;\n    }\n\n    UserEntity entity = 1;\n    uint32 id = 2;\n  }\n  string system = 1;\n  optional Actor initiator = 2;\n  optional Actor proxy = 3;\n  optional string reason = 4;\n}\n";
        let result = into_decode_context(dummy_registered_schema(schema), ReferencedSchemas::new());
        assert!(result.is_ok())
    }
}
