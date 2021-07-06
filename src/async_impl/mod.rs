#[cfg(feature = "avro")]
pub mod avro;
#[cfg(all(feature = "easy", feature = "avro"))]
pub mod easy_avro;
#[cfg(all(feature = "easy", feature = "json"))]
pub mod easy_json;
#[cfg(all(feature = "easy", feature = "proto_decoder"))]
pub mod easy_proto_decoder;
#[cfg(all(feature = "easy", feature = "proto_raw"))]
pub mod easy_proto_raw;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "proto_decoder")]
pub mod proto_decoder;
#[cfg(feature = "proto_raw")]
pub mod proto_raw;
pub mod schema_registry;
