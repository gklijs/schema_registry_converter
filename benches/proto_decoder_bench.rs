//! Benchmark for the blocking `protofish`-backed `ProtoDecoder`, see
//! https://github.com/gklijs/schema_registry_converter/issues/118.
//!
//! Kept separate from `proto_raw_bench`: `ProtoDecoder` fully parses the `.proto` schema and
//! decodes the message through `protofish`, a materially heavier path than the raw decoder's
//! index-based framing, so it's worth tracking on its own.

use criterion::{criterion_group, criterion_main, Criterion};
use schema_registry_converter::blocking::proto_decoder::ProtoDecoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use std::hint::black_box;
use test_utils::{get_proto_body, get_proto_hb_101, get_proto_hb_schema};

fn proto_decoder_benchmarks(c: &mut Criterion) {
    let mut server = mockito::Server::new();
    let _m = server
        .mock("GET", "/schemas/ids/7?deleted=true")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(get_proto_body(get_proto_hb_schema(), 7))
        .create();

    let sr_settings = SrSettings::new_builder(server.url())
        .no_proxy()
        .build()
        .unwrap();
    let decoder = ProtoDecoder::new(sr_settings);

    // Warm the cache: the mock only ever answers once, everything timed below is served from it.
    decoder.decode(Some(get_proto_hb_101())).unwrap();

    c.bench_function("proto_decoder_decode_cached", |b| {
        b.iter(|| decoder.decode(black_box(Some(get_proto_hb_101()))))
    });
}

// Raised from criterion's defaults (1% / 5%) to match this kind of environment's real noise
// floor -- see the comment in benches/avro_bench.rs and
// https://github.com/gklijs/schema_registry_converter/issues/190 for how that was measured.
criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.05).significance_level(0.02);
    targets = proto_decoder_benchmarks
}
criterion_main!(benches);
