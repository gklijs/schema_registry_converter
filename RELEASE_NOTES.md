## Release notes

### 5.0.0

Two breaking changes bump this to a major version:

`BytesResult` (in `schema_registry_common`) now borrows the payload instead of owning it --
`Invalid(Vec<u8>)` / `Valid(u32, Vec<u8>)` are now `Invalid(&'a [u8])` / `Valid(u32, &'a [u8])`,
and `get_bytes_result` returns `BytesResult<'_>` borrowing from its input, instead of copying the
whole payload into a fresh `Vec` on every call. This is on the hot path of every decode, for
every format (Avro, JSON, protobuf), so it matters more the larger your messages are -- see
#190 for measurements.

This only affects code that calls `get_bytes_result`/matches on `BytesResult` directly. The
built-in `AvroDecoder`, `JsonDecoder`, `ProtoDecoder` and `ProtoRawDecoder` (blocking and async)
are **not** affected -- their `decode`/`decode_with_schema`/etc. signatures haven't changed, and
existing code calling those is unaffected. If you do call `get_bytes_result` directly and need to
keep the bytes beyond the immediate match (store them, return them, carry them across an
`.await`), copy them out explicitly where you used to get an owned `Vec` for free:
```rust
match get_bytes_result(bytes) {
    BytesResult::Valid(id, bytes) => {
        let owned: Vec<u8> = bytes.to_vec();
        // ... use `owned` instead of `bytes` wherever it needs to outlive this match
    }
    BytesResult::Invalid(bytes) => {
        let owned: Vec<u8> = bytes.to_vec();
        // ...
    }
    BytesResult::Null => {}
}
```

Second, `apache-avro` moved from `^0.21` to `^0.22`, which turned `Name::name`/`Name::namespace`
(`apache_avro::schema::Name`, returned in `DecodeResult.name`/`DecodeResultWithSchema.name`) from
public fields into methods returning borrows (`name() -> &str`, `namespace() -> Option<&str>`)
instead of owned `String`s. If you access those on a `Name` obtained through this crate, it's not
just adding parentheses -- `.unwrap()` on the `Option<Name>` now needs `.as_ref()` first, or the
borrow from `.name()`/`.namespace()` outlives the temporary it came from:
```rust
// before (apache-avro 0.21, schema_registry_converter <5.0.0)
let name: String = decode_result.name.unwrap().name;
let namespace: Option<String> = decode_result.name.unwrap().namespace;
// after (apache-avro 0.22, schema_registry_converter >=5.0.0)
let name: &str = decode_result.name.as_ref().unwrap().name();
let namespace: Option<&str> = decode_result.name.as_ref().unwrap().namespace();
```

Not breaking, but worth knowing about: Avro encode/decode now reuse a cached, already-resolved
schema instead of re-resolving it from scratch on every single call -- internal, not observable
in the API, but measurably faster, more so the more named types (records/enums/fixed) your
schema has. See #190.

Fix a panic when decoding a 5-byte (or otherwise malformed/truncated) protobuf-framed payload
with `ProtoDecoder`/`ProtoRawDecoder` (blocking and async); such payloads now yield an `SRCError`
instead. As part of this, `proto_resolver::to_index_and_data` is now fallible: it returns
`Result<(Vec<i32>, Vec<u8>), SRCError>` instead of `(Vec<i32>, Vec<u8>)`, a breaking change for
any caller using that function directly. Fixes #176.

Fix a follow-up issue in the same area: a brace character inside a string literal in a proto
schema (e.g. a field/option default value) could desync `MessageResolver`/`IndexResolver`'s
index bookkeeping from the real message nesting, since its lexer didn't previously recognize
string literals and treated every `{`/`}` as message nesting. String literals are now skipped as
a whole when scanning for braces.

### 4.10.0

Propagate properties and tags.
Minor code improvements.

### 4.9.0

Support schema compatibility checking.
Expose subject and version in schema's.
Add clone/debug to all easy encoders/decoders.

### 4.8.0

Some dependency updates.
Better handling for comments in proto massages.
Improved error response from schema registry, using the response from schema registry.
Adding clone/debug to encoders and decoders.

### 4.7.0

Update avro dependency and add properties inside the metadata of the schema registry response.

### 4.6.1

Serialization support for SuppliedSchema

### 4.6.0

Add support for properties and tags metadata.
Update versions, most noteworthy apache avro.

### 4.5.0

Updated versions and added option to not use a proxy.

### 4.4.0

Updated versions and small code improvements.

### 4.3.0

Updated versions. Bug fixes.

### 4.2.0

Updated versions.
Add possibility to directly encode avro values.
Make it possible to have slashes in subject names.
By updating to the latest [avro-rs](https://crates.io/crates/avro-rs) being able to [have custom name validators](https://github.com/apache/avro-rs/blob/main/avro/README.md#custom-names-validators) in Avro.

### 4.1.0

Updated versions.

### 4.0.0

Opened up/added some functionality.
Updated versions.
Simplified the SubjectNameStrategy enum.

### 3.1.0

Fix a problem with missing checks on some proto common types, like timestamp.
Added functions to get the context when using protobuf.
Added functions to get the Avro schema when using avro.

### 3.0.0

Several breaking changes in the API, making it easier to use as in most places we don't need a mutable reference anymore.
Move to apache-avro for avro, which contains several fixes.
Made some additional things public for other use cases. Added some methods to the API in case the used schema is required.
Protobuf common types are 'supported' as long as the import is in the main schema, the schema's will be added to the list giving to protofish, so it can be deserialized.

### 2.1.0

Dependencies updated and ci is now run in Github Actions also some improvements where made making it easier to use, and open up some additional use cases.

#### Issues
- It's now possible to set additional options on the `reqwest` client, and use that to create the SrSettings. Mainly for custom security requirements.
- It's possible to use the `rustls_tls` feature to let `reqwest` use `rustls-tls`.
- For each async converter an `easy` variant was added. This makes it easier to use the library, as internally an arc is used, making it easier to use.
- For the protobuf encoders an `encode_single_message` method was eded to encode when the schema contains only one message. The full name of the proto message is not needed for this.

### 2.0.2

Updated dependencies

### 2.0.1

Maintenance release with mainly updated dependencies, making the blocking sr settings cloneable and no longer needs `kafka_test` feature to use both blocking and async in the same project.

### 2.0.0

This release has a breaking change in the SubjectNameStrategy where the supplied schema now is in a Box, to keep the size of the Enum smaller.
Another breaking change is that the protocol  (http or https) needs  to be included in the schema registry url.
Since besides avro also protobuf and json schema, and to some degree custom formats are supported, avro is no behind a feature flag, and in its own module.
Also add support for authentication and the use of a proxy in this version.
Another major change is by default support for async.

To use the new version of the library, and continue to use it in a blocking way like it was before, you need to use the library like:
```toml
schema_registry_converter = { version = "2.0.2", default-features = false, features = ["avro", "blocking"]}
```
Also the Converters are moved to the blocking module, and to create the converters you need a SrSettings object, which can be created with just the
schema registry url. 
```rust
let sr_settings = SrSettings::new(String::from("http://localhost:8081"));
```

#### Issues

- Add json schema support.
- Add protobuf support.
- Support references in schema registry.
- Add authentication proxies, timeouts, etc, by using reqwest instead of curl.
- Support async/non-blocking by default
- For Avro, make it possible to use the encode_struct function with primitive values.

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

- [@ahassany](https://github.com/ahassany)
- [@benmanns](https://github.com/benmanns)
- [@cbzehner](https://github.com/cbzehner)
- [@icdevin](https://github.com/icdevin)
- [@jschmid1](https://github.com/jschmid1)
- [@johnhalbert](https://github.com/johnhalbert)
- [@gab-txt](https://github.com/gab-txt)
- [@kitsuneninetails](https://github.com/kitsuneninetails)
- [@kujeger](https://github.com/kujeger)
- [@lahabana](https://github.com/lahabana)
- [@Licenser](https://github.com/Licenser)
- [@lukecampbell](https://github.com/lukecampbell)
- [@MariellHoversholm-Paf](https://github.com/MariellHoversholm-Paf)
- [@mdroogh](https://github.com/mdroogh)
- [@marioloko](https://github.com/marioloko)
- [@naamancurtis](https://github.com/naamancurtis)
- [@PookieBuns](https://github.com/PookieBuns)
- [@rodonile](https://github.com/rodonile)
- [@saiharshavellanki](https://github.com/saiharshavellanki)
- [@SergeStrashko](https://github.com/SergeStrashko)
- [@sfsf9797](https://github.com/sfsf9797)
- [@vpikulik](https://github.com/vpikulik)