//! async supported implementations. Requires `futures` feature.
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

#[cfg(test)]
mod tests {
    /// The codecs own everything their cached futures need, so they can be stored and shared
    /// (e.g. in an `Arc` across tasks) without a lifetime.
    #[allow(dead_code)]
    fn assert_owned<T: Send + Sync + 'static>() {}

    #[test]
    fn codecs_have_no_lifetime() {
        #[cfg(feature = "avro")]
        {
            assert_owned::<super::avro::AvroDecoder>();
            assert_owned::<super::avro::AvroEncoder>();
        }
        #[cfg(feature = "json")]
        {
            assert_owned::<super::json::JsonDecoder>();
            assert_owned::<super::json::JsonEncoder>();
        }
        #[cfg(feature = "proto_decoder")]
        assert_owned::<super::proto_decoder::ProtoDecoder>();
        #[cfg(feature = "proto_raw")]
        {
            assert_owned::<super::proto_raw::ProtoRawDecoder>();
            assert_owned::<super::proto_raw::ProtoRawEncoder>();
        }
    }
}
