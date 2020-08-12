#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "proto_decoder")]
pub mod proto_decoder;
#[cfg(feature = "proto_raw")]
pub mod proto_raw;
#[cfg(any(feature = "proto_decoder", feature = "proto_raw"))]
mod proto_resolver;
pub mod schema_registry;
