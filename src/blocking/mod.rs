//! blocking implementations. Requires `blocking` feature.
#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "proto_decoder")]
pub mod proto_decoder;
#[cfg(feature = "proto_raw")]
pub mod proto_raw;
pub mod schema_registry;
