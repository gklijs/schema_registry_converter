use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;

use bytes::Bytes;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use protofish::{Context, MessageValue, Value};

use crate::async_impl::schema_registry::{
    get_referenced_schema, get_schema_by_id_and_type, SrSettings,
};
use crate::error::SRCError;
use crate::proto_resolver::{resolve_name, to_index_and_data, MessageResolver};
use crate::schema_registry_common::{get_bytes_result, BytesResult, RegisteredSchema, SchemaType};

type SharedFutureOfSchemas<'a> = Shared<BoxFuture<'a, Result<Vec<String>, SRCError>>>;

#[derive(Debug)]
pub struct ProtoDecoder<'a> {
    sr_settings: SrSettings,
    cache: HashMap<u32, SharedFutureOfSchemas<'a>, RandomState>,
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
            cache: HashMap::new(),
        }
    }
    /// Remove al the errors from the cache, you might need to/want to run this when a recoverable
    /// error is met. Errors are also cashed to prevent trying to get schema's that either don't
    /// exist or can't be parsed.
    pub fn remove_errors_from_cache(&mut self) {
        self.cache.retain(|_, v| match v.peek() {
            Some(r) => r.is_ok(),
            None => true,
        });
    }
    /// Decodes bytes into a value.
    /// The choice to use Option<&[u8]> as type us made so it plays nice with the BorrowedMessage
    /// struct from rdkafka, for example if we have m: &'a BorrowedMessage and decoder: &'a mut
    /// Decoder we can use decoder.decode(m.payload()) to decode the payload or
    /// decoder.decode(m.key()) to get the decoded key.
    pub async fn decode(&mut self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
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
    async fn deserialize(&mut self, id: u32, bytes: &[u8]) -> Result<MessageValue, SRCError> {
        let vec_of_schemas = self.get_vec_of_schemas(id).clone().await?;
        let context = into_decode_context(&vec_of_schemas)?;
        let (index, data) = to_index_and_data(bytes);
        let full_name = resolve_name(&context.resolver, &index)?;
        let message_info = context.context.get_message(full_name).unwrap();
        Ok(message_info.decode(&data, &context.context))
    }
    /// Gets the Context object, either from the cache, or from the schema registry and then putting
    /// it into the cache.
    fn get_vec_of_schemas(&mut self, id: u32) -> &SharedFutureOfSchemas<'a> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => &*e.into_mut(),
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
                &*e.insert(v)
            }
        }
    }
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
struct DecodeContext {
    resolver: MessageResolver,
    context: Context,
}

fn into_decode_context(vec_of_schemas: &[String]) -> Result<DecodeContext, SRCError> {
    let resolver = MessageResolver::new(vec_of_schemas.last().unwrap());
    match Context::parse(vec_of_schemas) {
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
) -> Result<Vec<String>, SRCError> {
    let mut vec_of_schemas = Vec::new();
    add_files(sr_settings, registered_schema, &mut vec_of_schemas).await?;
    Ok(vec_of_schemas)
}

#[cfg(test)]
mod tests {
    use mockito::{mock, server_address};
    use protofish::Value;

    use crate::async_impl::proto_decoder::ProtoDecoder;
    use crate::async_impl::schema_registry::SrSettings;
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
        let mut decoder = ProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
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
        let mut decoder = ProtoDecoder::new(sr_settings);
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
        assert_eq!(
            true
                .to_owned(),
            format!("{:?}", decoder).starts_with("ProtoDecoder { sr_settings: SrSettings { urls: [\"http://127.0.0.1:1234\"], client: Client {")
        )
    }
}
