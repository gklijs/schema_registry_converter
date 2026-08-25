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
use serde::{Deserialize, Serialize};
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

/// A record type used only as a named-type back-reference below -- every `NamedRefParent` field
/// after the first refers back to this same definition by name, rather than repeating it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NamedRefChild {
    name: Option<String>,
}

/// Reproduces https://github.com/gklijs/schema_registry_converter/issues/117: a record with
/// several fields of the *same* named type, each wrapped in a nullable union
/// (`["null", "NamedRefChild"]`), the shape the reporter's own repro used. Before the fix for
/// #117, `encode_struct` went through an untyped intermediate `Value` plus a separate
/// `Value::resolve()` pass that re-resolved every one of those named-type back-references
/// against the schema on every single call (measured ~16x slower here than
/// `avro_encode_struct_cached` above, a schema with no named-type references at all) -- unlike
/// the `ResolvedSchema` used for the actual byte writing (cached since #190), that step wasn't
/// cached, since it operated on the value, not the schema. `encode_struct` now serializes
/// directly against the cached resolved schema instead (see `avro_common::encode_item`),
/// avoiding that pass entirely; this benchmark stays as a regression guard against it coming
/// back.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NamedRefParent {
    child1: Option<NamedRefChild>,
    child2: Option<NamedRefChild>,
    child3: Option<NamedRefChild>,
    child4: Option<NamedRefChild>,
    child5: Option<NamedRefChild>,
    child6: Option<NamedRefChild>,
    child7: Option<NamedRefChild>,
    child8: Option<NamedRefChild>,
}

impl NamedRefParent {
    fn sample() -> NamedRefParent {
        let child = || {
            Some(NamedRefChild {
                name: Some(String::from("name")),
            })
        };
        NamedRefParent {
            child1: child(),
            child2: child(),
            child3: child(),
            child4: child(),
            child5: child(),
            child6: child(),
            child7: child(),
            child8: child(),
        }
    }
}

fn avro_named_refs_benchmarks(c: &mut Criterion) {
    let child_schema = r#"{\"type\":\"record\",\"name\":\"NamedRefChild\",\"namespace\":\"nl.openweb.data\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}"#;
    let schema = format!(
        r#"{{\"type\":\"record\",\"name\":\"NamedRefParent\",\"namespace\":\"nl.openweb.data\",\"fields\":[{{\"name\":\"child1\",\"type\":[\"null\",{child_schema}],\"default\":null}},{{\"name\":\"child2\",\"type\":[\"null\",\"NamedRefChild\"],\"default\":null}},{{\"name\":\"child3\",\"type\":[\"null\",\"NamedRefChild\"],\"default\":null}},{{\"name\":\"child4\",\"type\":[\"null\",\"NamedRefChild\"],\"default\":null}},{{\"name\":\"child5\",\"type\":[\"null\",\"NamedRefChild\"],\"default\":null}},{{\"name\":\"child6\",\"type\":[\"null\",\"NamedRefChild\"],\"default\":null}},{{\"name\":\"child7\",\"type\":[\"null\",\"NamedRefChild\"],\"default\":null}},{{\"name\":\"child8\",\"type\":[\"null\",\"NamedRefChild\"],\"default\":null}}]}}"#
    );

    let mut server = mockito::Server::new();
    let _m = server
        .mock(
            "GET",
            "/subjects/nl.openweb.data.NamedRefParent/versions/latest",
        )
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(format!(
            r#"{{"subject":"namedrefparent-value","version":1,"id":5,"schema":"{schema}"}}"#
        ))
        .create();

    let sr_settings = SrSettings::new_builder(server.url())
        .no_proxy()
        .build()
        .unwrap();
    let encoder = AvroEncoder::new(sr_settings);
    let strategy =
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.NamedRefParent"));

    // Warm the cache: the mock only ever answers once, everything timed below is served from it.
    encoder
        .encode_struct(NamedRefParent::sample(), &strategy)
        .unwrap();

    c.bench_function("avro_encode_struct_cached_named_refs", |b| {
        b.iter(|| encoder.encode_struct(black_box(NamedRefParent::sample()), black_box(&strategy)))
    });
}

// Defaults (1% noise threshold, 5% significance level) are tuned for a quiet, dedicated
// machine. Verified via a same-binary-vs-itself control run on a laptop CPU under WSL2 --
// nothing unusual, just a common dev-machine setup -- that this environment's real run-to-run
// noise floor is more like 5-7%, well above criterion's default 1% threshold; at the default,
// completely unchanged code routinely got flagged "regressed"/"improved". Raising both here
// doesn't make measurements less noisy, it stops criterion *reporting* noise as a real change.
// See https://github.com/gklijs/schema_registry_converter/issues/190 and
// scripts/bench_compare.py, which classifies at the same 5% threshold for consistency.
criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.05).significance_level(0.02);
    targets = avro_benchmarks, avro_named_refs_benchmarks
}
criterion_main!(benches);
