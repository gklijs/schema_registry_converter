> #schema_registry_converter

[![Build Status](https://travis-ci.org/gklijs/schema_registry_converter.svg?branch=master)](https://travis-ci.org/gklijs/schema_registry_converter)
[![codecov](https://codecov.io/gh/gklijs/schema_registry_converter/branch/master/graph/badge.svg)](https://codecov.io/gh/gklijs/schema_registry_converter)
[![Crates.io](https://img.shields.io/crates/d/schema_registry_converter.svg?maxAge=2592000)](https://crates.io/crates/schema_registry_converter)
[![Crates.io](https://img.shields.io/crates/v/schema_registry_converter.svg?maxAge=2592000)](https://crates.io/crates/schema_registry_converter)
[![docs.rs](https://docs.rs/schema_registry_converter/badge.svg)](https://docs.rs/schema_registry_converter/)
---

This library provides a way of using the Confluent Schema Registry in a way that is compliant with the usual jvm usage.
The release notes can be found on [github](https://github.com/gklijs/schema_registry_converter/blob/master/RELEASE_NOTES.md)
Consuming/decoding and producing/encoding is supported. It's also possible to provide the schema to use when decoding. When no schema is provided, the latest
schema with the same `subject` will be used. As far as I know, it's feature complete compared to the confluent java version.
As I'm still pretty new to rust, pr's/remarks for improvements are greatly appreciated.



## Consumer

For consuming messages encoded with the schema registry, you need to fetch the correct schema from the schema registry to transform it into a record. For clarity, error handling is omitted from the diagram. 

![Consumer activity flow](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/gklijs/schema_registry_converter/master/uml/consumer.puml)

## Producer

For producing messages which can be properly consumed by other clients, the proper id needs to be encoded with the message.  To get the correct id, it might be necessary to register a new schema. For clarity, error handling is omitted from the diagram.

![Producer activity flow](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/gklijs/schema_registry_converter/master/uml/producer.puml)

# Getting Started

[schema_registry_converter.rs is available on crates.io](https://crates.io/crates/schema_registry_converter).
It is recommended to look there for the newest and more elaborate documentation.

```toml
[dependencies]
schema_registry_converter = "1.0.0"
```

...and see the [docs](https://docs.rs/schema_registry_converter) for how to use it.

# Example with consumer and producer

```rust
use rdkafka::message::{Message, BorrowedMessage};
use avro_rs::types::Value;
use schema_registry_converter::{Decoder, Encoder};
use schema_registry_converter::schema_registry::SubjectNameStrategy;

fn main() {
    let mut decoder = Decoder::new("localhost:8081".into());
    let mut encoder = Encoder::new("localhost:8081".into());
}

fn get_value<'a>(
    msg: &'a BorrowedMessage,
    decoder: &'a mut Decoder,
) -> Value{
    match decoder.decode(msg.payload()){
    Ok(v) => v,
    Err(e) => panic!("Error getting value: {}", e),
    }
}

fn get_future_record<'a>(
    topic: &'a str,
    key: Option<&'a str>,
    values: Vec<(&'static str, Value)>,
    encoder: &'a mut Encoder,
) -> FutureRecord<'a>{
    let subject_name_strategy = SubjectNameStrategy::TopicNameStrategy(topic, false);
    let payload = match encoder.encode(values, &subject_name_strategy) {
        Ok(v) => v,
        Err(e) => panic!("Error getting payload: {}", e),
    };
    FutureRecord {
        topic,
        partition: None,
        payload: Some(&payload),
        key,
        timestamp: None,
        headers: None,
    }
}
```

# Example using to post schema to schema registry

```rust
use schema_registry_converter::schema_registry::SubjectNameStrategy::post_schema;

fn main(){
    let heartbeat_schema = SuppliedSchema::new(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#.into());
    let result = post_schema("localhost:8081/subjects/test-value/versions");
}

```

# Relation to related libraries

The avro part of the conversion is handled by avro-rs.  As such, I don't include tests for every possible schema.
While I used rdkafka in combination to successfully consume from and produce to kafka, and while it's used in the example, this crate has no direct dependency on it.
All this crate does is convert [u8] <-> avro_rs::types::Value.

# Tests

Due to mockito, used for mocking the schema registry responses, being run in a separate thread, tests have to be run using ` --test-threads=1` for example like
`cargo +stable test --color=always -- --nocapture --test-threads=1`

# Integration test

The integration tests require a Kafka cluster running on the default ports. It will create topics, register schema's, produce and consume some messages.
They are marked with `kafka_test` so to include them in testing `+stable test --features kafka_test --color=always -- --nocapture --test-threads=1` need to be run.
The easiest way to run them is with the confluent cli. The 'prepare_integration_test.sh' script can be used to create the 3 topics needed for the tests, but even without those the test pass.

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Schema Registry Converter by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
