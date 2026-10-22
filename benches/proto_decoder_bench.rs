//! Benchmark for the blocking `prost-reflect`-backed `ProtoDecoder`, see
//! https://github.com/gklijs/schema_registry_converter/issues/118.
//!
//! Kept separate from `proto_raw_bench`: `ProtoDecoder` fully parses the `.proto` schema and
//! decodes the message through `prost-reflect`, a materially heavier path than the raw decoder's
//! index-based framing, so it's worth tracking on its own.

use criterion::{Criterion, criterion_group, criterion_main};
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

    // The message at index [29] of a 30-message schema, so the cost of resolving a message
    // index that isn't the first one shows up too. The Heartbeat schema above only has [0].
    let mut many_messages = String::from(r#"syntax = \"proto3\";package nl.openweb.data;"#);
    for i in 0..30 {
        many_messages.push_str(&format!("message M{} {{uint64 beat = 1;}}", i));
    }
    let _m_many = server
        .mock("GET", "/schemas/ids/8?deleted=true")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(get_proto_body(&many_messages, 8))
        .create();
    // id 8, message index [29] (zigzag varint count 1, index 29), then beat = 101
    let last_message: &[u8] = &[0, 0, 0, 0, 8, 0x02, 0x3a, 8, 101];
    decoder.decode(Some(last_message)).unwrap();

    c.bench_function("proto_decoder_decode_cached_last_of_many", |b| {
        b.iter(|| decoder.decode(black_box(Some(last_message))))
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
