## Release notes

### 2.0.0

This release has a breaking change in the SubjectNameStrategy where the supplied schema now is in a Box, to keep the size of the Enum smaller.
Another breaking change is that the protocol  (http or https) needs  to be included in the schema registry url.
Since besides avro also other ways are supported, avro is no behind a feature flag, and in its own module.
-TODO- I hope to also add support for authentication for Confluent Cloud and Protobuf for this version.

### 1.1.0

This release makes it easier to work with structs, instead of the raw Value type in a vector.
To use structs with avro you need to add `#[derive(Debug, Deserialize, Serialize)]` above your
struct and also have a dependency on serde with the derive feature enabled. like:
```toml
[dependencies.serde]
version = "1.0"
features = ["derive"]
```

#### Issues

- Added support for the decoder to also get the name of the schema, this
can be used to determine witch crate to use the values for. In the [tests](https://github.com/gklijs/schema_registry_converter/blob/master/src/lib.rs#L577)
there is an example.
- Added support the the encoder to take a struct. To do this use the `encode_struct`
instead of the `encode` function on the encoder.

### 1.0.0

#### Issues

- Made it easier to use the crate by changing some values to owed strings.
- Fixed to issues related to sending the schema to the schema registry.
- Added integration tests, to test against a kafka cluster.
- Make post_schema public so it can be used directly.

#### Contributors

- [@kitsuneninetails](https://github.com/kitsuneninetails)
- [@j-halbert](https://github.com/j-halbert)