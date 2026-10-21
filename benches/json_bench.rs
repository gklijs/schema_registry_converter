//! Benchmarks for the blocking JSON Schema encoder/decoder, see
//! https://github.com/gklijs/schema_registry_converter/issues/118.
//!
//! As with the other benches here, the schema registry lookup is mocked and done once before
//! timing starts, so this measures the cache-hit hot path: JSON Schema validation, not
//! network/mock overhead.

use criterion::{criterion_group, criterion_main, Criterion};
use schema_registry_converter::blocking::json::{validate, JsonDecoder, JsonEncoder};
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde_json::json;
use std::hint::black_box;
use test_utils::{get_json_body, json_result_schema};

fn json_benchmarks(c: &mut Criterion) {
    let mut server = mockito::Server::new();
    let _m = server
        .mock("GET", "/subjects/testresult-value/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(get_json_body(json_result_schema(), 10))
        .create();
    // The decoder caches by schema id and looks it up independently of the encoder's
    // subject-based cache above, so it needs its own mock.
    let _m2 = server
        .mock("GET", "/schemas/ids/10?deleted=true")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(get_json_body(json_result_schema(), 10))
        .create();

    let sr_settings = SrSettings::new_builder(server.url())
        .no_proxy()
        .build()
        .unwrap();
    let encoder = JsonEncoder::new(sr_settings.clone());
    let strategy = SubjectNameStrategy::TopicNameStrategy(String::from("testresult"), false);
    let value = json!({"up": "some", "down": "other"});

    // Warm the cache: the mocks only ever answer once, everything timed below is served from it.
    let encoded = encoder.encode(&value, &strategy).unwrap();

    c.bench_function("json_encode_cached", |b| {
        b.iter(|| encoder.encode(black_box(&value), black_box(&strategy)))
    });

    let decoder = JsonDecoder::new(sr_settings);
    decoder.decode(Some(&encoded)).unwrap();

    c.bench_function("json_decode_cached", |b| {
        b.iter(|| decoder.decode(black_box(Some(&encoded))))
    });

    c.bench_function("json_decode_and_validate_cached", |b| {
        b.iter(|| {
            let result = decoder.decode(black_box(Some(&encoded))).unwrap().unwrap();
            decoder.validate(&result.schema, &result.value)
        })
    });

    // The free `validate` function compiles the schema each time.
    c.bench_function("json_decode_and_validate_uncached", |b| {
        b.iter(|| {
            let result = decoder.decode(black_box(Some(&encoded))).unwrap().unwrap();
            validate(&result.schema, &result.value)
        })
    });
}

// Raised from criterion's defaults (1% / 5%) to match this kind of environment's real noise
// floor -- see the comment in benches/avro_bench.rs and
// https://github.com/gklijs/schema_registry_converter/issues/190 for how that was measured.
criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.05).significance_level(0.02);
    targets = json_benchmarks
}
criterion_main!(benches);
