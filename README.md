> #schema_registry_converter

[![Build Status](https://travis-ci.org/gklijs/schema_registry_converter.svg?branch=master)](https://travis-ci.org/gklijs/schema_registry_converter)
[![codecov](https://codecov.io/gh/gklijs/schema_registry_converter/branch/master/graph/badge.svg)](https://codecov.io/gh/gklijs/schema_registry_converter)
[![Crates.io](https://img.shields.io/crates/d/schema_registry_converter.svg?maxAge=2592000)](https://crates.io/crates/schema_registry_converter)
[![Crates.io](https://img.shields.io/crates/v/schema_registry_converter.svg?maxAge=2592000)](https://crates.io/crates/schema_registry_converter)
[![docs.rs](https://docs.rs/schema_registry_converter/badge.svg)](https://docs.rs/schema_registry_converter/)
---

This library is provides a way of using the Confluent Schema Registry in a way that is compliant with the usual jvm useage.
Consuming/decoding and producing/encoding is supported. It's also possible to provide the schema to use when decoding. When no schema is provided the latest
schema with the same `subject` will be used. As far as I know it's feature complete compared to the confluent java version.
As I'm still pretty new to rust pr's/remarks for improvements are greatly appreciated.

# Getting Started

[schema_registry_converter.rs is available on crates.io](https://crates.io/crates/schema_registry_converter).
It is recommended to look there for the newest and more elaborate documentation.

```toml
[dependencies]
schema_registry_converter = "0.3.0"
```

...and see the [docs](https://docs.rs/schema_registry_converter) for how to use it.

# Example

```rust
extern crate rdkafka;
extern crate avro_rs;
extern crate schema_registry_converter;

use rdkafka::message::{Message, BorrowedMessage};
use avro_rs::types::Value;
use schema_registry_converter::Decoder;
use schema_registry_converter::Encoder;
use schema_registry_converter::schema_registry::SubjectNameStrategy;


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

fn main() {
    let mut decoder = Decoder::new(SERVER_ADDRESS);
    let mut encoder = Encoder::new(SERVER_ADDRESS);
    //somewhere else the above functions can be called
}
```

# Relation to related libraries

The avro part of the conversion is handled by avro-rs as such I don't include tests for every possible schema.
While I used rdkafka in combination to successfully consume from and produce to kafka, and it's used in the example this crate has no direct dependency to it.
All this crate does is convert [u8] <-> avro_rs::types::Value.

# Tests

Do to mockito, used for mocking the schema registry responses, being run in a seperate thead, tests have to be run using ` --test-threads=1` for example like
`cargo +stable test --color=always -- --nocapture --test-threads=1`

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