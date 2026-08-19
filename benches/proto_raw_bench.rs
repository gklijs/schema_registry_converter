//! Benchmarks for the blocking raw protobuf encoder/decoder, see
//! https://github.com/gklijs/schema_registry_converter/issues/118.
//!
//! As with the other benches here, the schema registry lookup is mocked and done once before
//! timing starts, so this measures the cache-hit hot path (index resolving + framing), not
//! network/mock overhead.

use criterion::{criterion_group, criterion_main, Criterion};
use schema_registry_converter::blocking::proto_raw::{ProtoRawDecoder, ProtoRawEncoder};
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::hint::black_box;
use test_utils::{
    get_proto_body, get_proto_hb_101, get_proto_hb_101_only_data, get_proto_hb_schema,
};

fn proto_raw_benchmarks(c: &mut Criterion) {
    let mut server = mockito::Server::new();
    let _m = server
        .mock("GET", "/subjects/nl.openweb.data.Heartbeat/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(get_proto_body(get_proto_hb_schema(), 7))
        .create();
    // The decoder caches by schema id and looks it up independently of the encoder's
    // subject-based cache above, so it needs its own mock.
    let _m2 = server
        .mock("GET", "/schemas/ids/7?deleted=true")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(get_proto_body(get_proto_hb_schema(), 7))
        .create();

    let sr_settings = SrSettings::new_builder(server.url())
        .no_proxy()
        .build()
        .unwrap();
    let encoder = ProtoRawEncoder::new(sr_settings.clone());
    let strategy =
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));

    // Warm the cache: the mock only ever answers once, everything timed below is served from it.
    encoder
        .encode(
            get_proto_hb_101_only_data(),
            "nl.openweb.data.Heartbeat",
            &strategy,
        )
        .unwrap();

    c.bench_function("proto_raw_encode_cached", |b| {
        b.iter(|| {
            encoder.encode(
                black_box(get_proto_hb_101_only_data()),
                black_box("nl.openweb.data.Heartbeat"),
                black_box(&strategy),
            )
        })
    });

    let decoder = ProtoRawDecoder::new(sr_settings);
    decoder.decode(Some(get_proto_hb_101())).unwrap();

    c.bench_function("proto_raw_decode_cached", |b| {
        b.iter(|| decoder.decode(black_box(Some(get_proto_hb_101()))))
    });
}

criterion_group!(benches, proto_raw_benchmarks);
criterion_main!(benches);
