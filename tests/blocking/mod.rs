#[cfg(feature = "avro")]
pub mod avro_consumer;
#[cfg(feature = "avro")]
mod avro_tests;
pub mod kafka_consumer;
pub mod kafka_producer;
