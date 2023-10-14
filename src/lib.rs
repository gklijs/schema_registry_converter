//! Rust encoders and decoders in order to work with the Confluent schema registry.
//!
//! This crate contains ways to handle encoding and decoding of messages making use of the
//! [confluent schema-registry]. This happens in a way that is compatible to the
//! [confluent java serde]. As a result it becomes easy to work with the same data in both the jvm
//! and rust.
//!
//! [confluent schema-registry]: https://docs.confluent.io/current/schema-registry/docs/index.html
//! [confluent java serde]: https://github.com/confluentinc/schema-registry/tree/master/avro-serde/src/main/java/io/confluent/kafka/streams/serdes/avro
//!
//! Both the Decoder and the Encoder have a cache to allow re-use of the Schema objects used for
//! the avro transitions.
//!
//! For Encoding data it's possible to supply a schema else the latest available schema will be used.
//! For Decoding it works the same as the Java part, using the id encoded in the bytes, the
//! correct schema will be fetched and used to decode the message to a apache_avro::types::Value.
//!
//! Resulting errors are SRCError, besides the error they also contain a .cached which tells whether
//! the error is cached or not. Another property added to the error is retriable, in some cases, like
//! when the network fails it might be worth to retry the same function. The library itself doesn't
//! automatically does retries.
//!
//! [avro-rs]: https://crates.io/crates/avro-rs
#[cfg(feature = "futures")]
pub mod async_impl;
#[cfg(feature = "avro")]
pub mod avro_common;
#[cfg(feature = "blocking")]
pub mod blocking;
pub mod error;
#[cfg(feature = "json")]
mod json_common;
#[cfg(any(feature = "proto_decoder", feature = "proto_raw"))]
mod proto_common_types;
#[cfg(feature = "proto_raw")]
pub mod proto_raw_common;
#[cfg(any(feature = "proto_decoder", feature = "proto_raw"))]
pub mod proto_resolver;
pub mod schema_registry_common;
