#!/usr/bin/env bash
# Runs all `[[bench]]` binaries in one of three modes, for the bench-compare workflow (see
# .github/workflows/bench-compare.yml and https://github.com/gklijs/schema_registry_converter/issues/118).
#
#   plain                    Runs each binary with no criterion baseline flags. Always succeeds
#                             if the code builds -- used to get a complete, panic-proof set of
#                             current timings regardless of what main looks like.
#   save-baseline <name>     Runs each binary with `--save-baseline <name>`.
#   compare <name> <out_dir> Runs each binary with `--baseline <name>`, tolerating criterion's
#                             panic-on-missing-baseline (thrown the first time it hits a
#                             benchmark that doesn't exist under that baseline yet -- expected
#                             for any newly added benchmark). Writes each binary's output to
#                             <out_dir>/compare_<binary>.txt; scripts/bench_compare.py is what
#                             makes sense of a binary that panicked partway through.
#
# The bench list below has to be kept in sync with the `[[bench]]` entries in Cargo.toml by
# hand -- there isn't a `cargo bench --list`-style machine-readable source for name+features.
set -euo pipefail

BENCHES=(
  "avro_bench:avro,blocking"
  "proto_raw_bench:proto_raw,blocking"
  "proto_decoder_bench:proto_decoder,blocking"
  "json_bench:json,blocking"
)
# Trimmed from criterion's defaults (100 samples / 5s+3s) to keep CI time reasonable while
# still giving the significance test enough to work with.
CRITERION_ARGS=(--sample-size 60 --measurement-time 3 --warm-up-time 2)

mode="${1:?usage: $0 plain / save-baseline NAME / compare NAME OUT_DIR}"

case "$mode" in
  plain)
    for entry in "${BENCHES[@]}"; do
      bin="${entry%%:*}"
      feat="${entry##*:}"
      cargo bench --bench "$bin" --features "$feat" -- "${CRITERION_ARGS[@]}"
    done
    ;;
  save-baseline)
    name="${2:?save-baseline needs a baseline name}"
    for entry in "${BENCHES[@]}"; do
      bin="${entry%%:*}"
      feat="${entry##*:}"
      cargo bench --bench "$bin" --features "$feat" -- --save-baseline "$name" "${CRITERION_ARGS[@]}"
    done
    ;;
  compare)
    name="${2:?compare needs a baseline name}"
    out_dir="${3:?compare needs an output directory}"
    mkdir -p "$out_dir"
    for entry in "${BENCHES[@]}"; do
      bin="${entry%%:*}"
      feat="${entry##*:}"
      # `|| true`: a missing baseline makes criterion panic (exit 101) partway through the
      # binary. That's expected for a new benchmark, not a real failure -- whatever it managed
      # to print before panicking (comparisons for benchmarks that DO have a baseline) is still
      # useful, so keep going rather than losing the rest of the matrix over one panic.
      cargo bench --bench "$bin" --features "$feat" -- --baseline "$name" "${CRITERION_ARGS[@]}" \
        >"$out_dir/compare_$bin.txt" 2>&1 || true
    done
    ;;
  *)
    echo "usage: $0 {plain|save-baseline <name>|compare <name> <out_dir>}" >&2
    exit 1
    ;;
esac
