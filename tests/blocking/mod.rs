#[cfg(all(feature = "avro", feature = "kafka_test"))]
mod avro_tests;
pub mod kafka_consumer;
pub mod kafka_producer;
