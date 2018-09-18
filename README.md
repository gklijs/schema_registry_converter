# schema_registry_converter

This library is provides a way of using the Confluent Schema Registry in a way that is compliant with the usual jvm useage.
Consuming/decoding and producing/encoding is supported. Encoding supposes the schema's needed are already available in the schema registry.
Feel free to open een issue/pr to add functionality to add the schema from this crate.

# Relation to related libraries

The avro part of the conversion is handled by avro-rs as such I don't include tests for every possible schema.
While I used rdkafka in combination to successfully consume from and produce to kafka this crate has no direct reference to it. It does show in some of the examples. All this crate does is convert [u8] <-> avro_rs::types::Value.

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
