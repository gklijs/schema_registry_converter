#[cfg(feature = "avro")]
pub mod avro_consumer;
#[cfg(feature = "avro")]
mod avro_tests;
pub mod kafka_consumer;
pub mod kafka_producer;
#[cfg(feature = "proto_decoder")]
mod proto_consumer;
#[cfg(feature = "proto_decoder")]
mod proto_tests;
mod schema_registry_calls;
