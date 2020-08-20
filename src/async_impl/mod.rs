#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "proto_decoder")]
pub mod proto_decoder;
pub mod schema_registry;
