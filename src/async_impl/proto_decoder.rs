use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use std::sync::Arc;

use crate::async_impl::schema_registry::{
    get_referenced_schema, get_schema_by_id_and_type, SrSettings,
};
use crate::error::SRCError;
use crate::proto_common_types::add_common_files;
use crate::proto_resolver::{resolve_name, to_index_and_data, MessageResolver};
use crate::schema_registry_common::{get_bytes_result, BytesResult, RegisteredSchema, SchemaType};
use protofish::context::Context;
use protofish::decode::{MessageValue, Value};

type SharedFutureSchema<'a> = Shared<BoxFuture<'a, Result<Arc<Vec<String>>, SRCError>>>;

#[derive(Debug)]
pub struct ProtoDecoder<'a> {
    sr_settings: SrSettings,
    direct_cache: DashMap<u32, Arc<Vec<String>>>,
    cache: DashMap<u32, SharedFutureSchema<'a>>,
}

impl<'a> ProtoDecoder<'a> {
    /// Creates a new decoder which will use the supplied url used in creating the sr settings to
    /// fetch the schema's since the schema needed is encoded in the binary, independent of the
    /// SubjectNameStrategy we don't need any additional data. It's possible for recoverable errors
    /// to stay in the cache, when a result comes back as an error you can use
    /// remove_errors_from_cache to clean the cache, keeping the correctly fetched schema's
    pub fn new(sr_settings: SrSettings) -> ProtoDecoder<'a> {
        ProtoDecoder {
            sr_settings,
            direct_cache: DashMap::new(),
            cache: DashMap::new(),
        }
    }
    /// Remove all the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&self) {
        self.cache.retain(|_, v| match v.peek() {
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
            BytesResult::Valid(id, bytes) => Ok(Value::Message(Box::from(
                self.deserialize(id, &bytes).await?,
            ))),
            BytesResult::Invalid(i) => Ok(Value::Bytes(Bytes::from(i))),
        }
    }
    /// The actual deserialization trying to get the id from the bytes to retrieve the schema, and
    /// using a reader transforms the bytes to a value.
    async fn deserialize(&self, id: u32, bytes: &[u8]) -> Result<MessageValue, SRCError> {
        let vec_of_schemas = self.get_vec_of_schemas(id).await?;
        let context = into_decode_context(vec_of_schemas.to_vec())?;
        let (index, data) = to_index_and_data(bytes);
        let full_name = resolve_name(&context.resolver, &index)?;
        let message_info = context.context.get_message(&full_name).unwrap();
        Ok(message_info.decode(&data, &context.context))
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
            BytesResult::Valid(id, bytes) => {
                match self.deserialize_with_context(id, &bytes).await {
                    Ok(v) => Ok(Some(v)),
                    Err(e) => Err(e),
                }
            }
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
        let vec_of_schemas = self.get_vec_of_schemas(id).await?;
        let context = into_decode_context(vec_of_schemas.to_vec())?;
        let (index, data_bytes) = to_index_and_data(bytes);
        let full_name = resolve_name(&context.resolver, &index)?;
        let message_info = context.context.get_message(&full_name).unwrap();
        let value = message_info.decode(&data_bytes, &context.context);
        Ok(DecodeResultWithContext {
            value,
            context,
            full_name,
            data_bytes,
        })
    }
    /// Gets the vector of schema's directly of via a shared future. The direct cache main function
    /// is for performance.
    async fn get_vec_of_schemas(&self, id: u32) -> Result<Arc<Vec<String>>, SRCError> {
        match self.direct_cache.get(&id) {
            None => {
                let result = self.get_vec_of_schemas_by_shared_future(id).await;
                if result.is_ok() && !self.direct_cache.contains_key(&id) {
                    self.direct_cache.insert(id, result.clone().unwrap());
                    self.cache.remove(&id);
                };
                result
            }
            Some(result) => Ok(result.value().clone()),
        }
    }
    /// Gets the vector of schema's by a shared future, to prevent multiple of the same calls to
    /// schema registry, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn get_vec_of_schemas_by_shared_future(&self, id: u32) -> SharedFutureSchema<'a> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let sr_settings = self.sr_settings.clone();
                let v = async move {
                    match get_schema_by_id_and_type(id, &sr_settings, SchemaType::Protobuf).await {
                        Ok(v) => to_vec_of_schemas(&sr_settings, v).await,
                        Err(e) => Err(e.into_cache()),
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
pub struct DecodeResultWithContext {
    pub value: MessageValue,
    pub context: DecodeContext,
    pub full_name: Arc<String>,
    pub data_bytes: Vec<u8>,
}

fn add_files<'a>(
    sr_settings: &'a SrSettings,
    registered_schema: RegisteredSchema,
    files: &'a mut Vec<String>,
) -> BoxFuture<'a, Result<(), SRCError>> {
    async move {
        for r in registered_schema.references {
            let child_schema = get_referenced_schema(sr_settings, &r).await?;
            add_files(sr_settings, child_schema, files).await?;
        }
        files.push(registered_schema.schema);
        Ok(())
    }
    .boxed()
}

#[derive(Debug)]
pub struct DecodeContext {
    pub resolver: MessageResolver,
    pub context: Context,
}

fn into_decode_context(vec_of_schemas: Vec<String>) -> Result<DecodeContext, SRCError> {
    let resolver = MessageResolver::new(vec_of_schemas.last().unwrap());
    let mut files: Vec<String> = Vec::new();
    add_common_files(resolver.imports(), &mut files);
    for s in vec_of_schemas {
        files.push(s);
    }
    match Context::parse(files) {
        Ok(context) => Ok(DecodeContext { resolver, context }),
        Err(e) => Err(SRCError::non_retryable_with_cause(
            e,
            "Error creating proto context",
        )),
    }
}

async fn to_vec_of_schemas(
    sr_settings: &SrSettings,
    registered_schema: RegisteredSchema,
) -> Result<Arc<Vec<String>>, SRCError> {
    let mut vec_of_schemas = Vec::new();
    add_files(sr_settings, registered_schema, &mut vec_of_schemas).await?;
    Ok(Arc::new(vec_of_schemas))
}

#[cfg(test)]
mod tests {
    use mockito::{mock, server_address};

    use crate::async_impl::proto_decoder::ProtoDecoder;
    use crate::async_impl::schema_registry::SrSettings;
    use protofish::prelude::Value;
    use test_utils::{
        get_proto_complex, get_proto_complex_proto_test_message, get_proto_complex_references,
        get_proto_hb_101, get_proto_hb_schema, get_proto_result,
    };

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
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }

    #[tokio::test]
    async fn test_decode_with_context_default() {
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder
            .decode_with_context(Some(get_proto_hb_101()))
            .await
            .unwrap();

        assert!(heartbeat.is_some());

        let message = heartbeat.unwrap().value;

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }

    #[tokio::test]
    async fn test_decoder_cache() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoDecoder::new(sr_settings);
        let error = decoder.decode(Some(get_proto_hb_101())).await.unwrap_err();
        assert!(error.cached);

        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let error = decoder.decode(Some(get_proto_hb_101())).await.unwrap_err();
        assert!(error.cached);

        decoder.remove_errors_from_cache();

        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value);
    }

    #[tokio::test]
    async fn test_decoder_complex() {
        let _m = mock("GET", "/schemas/ids/6?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body_with_reference(
                get_proto_complex(),
                2,
                get_proto_complex_references(),
            ))
            .create();

        let _m = mock("GET", "/subjects/result.proto/versions/1")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_result(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoDecoder::new(sr_settings);
        let proto_test = decoder
            .decode(Some(get_proto_complex_proto_test_message()))
            .await
            .unwrap();

        let message = match proto_test {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };
        assert_eq!(message.fields[1].value, Value::Int64(1))
    }

    #[test]
    fn display_decoder() {
        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = ProtoDecoder::new(sr_settings);
        assert!(
            format!("{:?}", decoder).starts_with("ProtoDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client {")
        )
    }
}
