//! Benchmarks for the blocking Avro encoder/decoder, see
//! https://github.com/gklijs/schema_registry_converter/issues/118.
//!
//! The schema registry lookup is mocked and done once before timing starts, so what's measured
//! is the cache-hit hot path: avro (de)serialization, not network/mock overhead. That's the path
//! that matters for a long-running consumer/producer once it has warmed up.

use criterion::{criterion_group, criterion_main, Criterion};
use schema_registry_converter::blocking::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::hint::black_box;
use test_utils::Heartbeat;

fn avro_benchmarks(c: &mut Criterion) {
    let mut server = mockito::Server::new();
    let _m = server
        .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(r#"{"subject":"heartbeat-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
        .create();
    // The decoder caches by schema id and looks it up independently of the encoder's
    // subject-based cache above, so it needs its own mock.
    let _m2 = server
        .mock("GET", "/schemas/ids/4?deleted=true")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"Heartbeat\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"beat\",\"type\":\"long\"}]}"}"#)
        .create();

    let sr_settings = SrSettings::new_builder(server.url())
        .no_proxy()
        .build()
        .unwrap();
    let encoder = AvroEncoder::new(sr_settings.clone());
    let strategy =
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

    // Warm the cache: the mock only ever answers once, everything timed below is served from it.
    let encoded = encoder
        .encode_struct(Heartbeat { beat: 3 }, &strategy)
        .unwrap();

    c.bench_function("avro_encode_struct_cached", |b| {
        b.iter(|| encoder.encode_struct(black_box(Heartbeat { beat: 3 }), black_box(&strategy)))
    });

    let decoder = AvroDecoder::new(sr_settings);
    // Warm the decoder's cache too, using the bytes the encoder above just produced.
    decoder.decode(Some(&encoded)).unwrap();

    c.bench_function("avro_decode_cached", |b| {
        b.iter(|| decoder.decode(black_box(Some(&encoded))))
    });
}

criterion_group!(benches, avro_benchmarks);
criterion_main!(benches);
