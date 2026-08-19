#!/usr/bin/env python3
"""Turns `cargo bench` output into a Markdown PR-comment comparing a PR build to main.

See https://github.com/gklijs/schema_registry_converter/issues/118 for the benchmarks
themselves, and .github/workflows/bench-compare.yml for how this is invoked in CI.

Criterion's `--baseline <name>` comparison mode is the source of truth for the "did this
regress" numbers, but it *panics* (aborting the whole bench binary, including benchmarks
listed after the missing one) the first time it meets a benchmark with no saved baseline of
that name -- which is guaranteed to happen for every benchmark on the PR that first adds
benches, and for any later PR that adds a new benchmark function. So the CI workflow always
also does one plain, comparison-free run on the PR code (which can't panic that way) to get a
complete set of current timings, and a best-effort `--baseline main` run per bench binary to
get change data for whichever benchmarks *do* have something to compare against on main. This
script merges the two: every benchmark gets a row (from the plain run), and rows for
benchmarks the comparison run actually got through get a change percentage and verdict too.

Usage: bench_compare.py <plain_run_output> <compare_run_output> [<compare_run_output> ...]
Prints a Markdown table to stdout.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass


@dataclass
class Reading:
    name: str
    time: str
    change_pct: float | None = None
    verdict: str | None = None


def _normalize_sign(token: str) -> float:
    # Criterion prints the Unicode minus sign (U+2212), not ASCII '-'.
    return float(token.replace("−", "-").rstrip("%"))


def _is_bare_name(line: str) -> bool:
    s = line.strip()
    return bool(s) and all(c.isalnum() or c == "_" for c in s)


def parse(text: str) -> dict[str, Reading]:
    """Parses one `cargo bench` run's stdout into {benchmark_name: Reading}.

    Criterion's layout varies with name length: a short name shares its line with `time:`,
    a long one gets pushed to a bare line of its own with `time:` indented below it.
    """
    readings: dict[str, Reading] = {}
    lines = text.splitlines()
    pending_name: str | None = None

    i = 0
    while i < len(lines):
        line = lines[i]
        if "time:" in line and "[" in line:
            prefix, _, rest = line.partition("time:")
            name = prefix.strip() or pending_name
            pending_name = None
            if name is None:
                i += 1
                continue
            bracket = rest[rest.find("[") + 1 : rest.find("]")]
            parts = bracket.split()
            point_time = f"{parts[2]} {parts[3]}" if len(parts) >= 4 else bracket
            reading = Reading(name=name, time=point_time)

            # An immediately following `change: [...]` + verdict line is optional -- present
            # only when this run was a `--baseline` comparison AND the benchmark existed on
            # that baseline already.
            if i + 1 < len(lines) and "change:" in lines[i + 1]:
                change_line = lines[i + 1]
                bracket2 = change_line[change_line.find("[") + 1 : change_line.find("]")]
                change_parts = bracket2.split()
                if len(change_parts) == 3:
                    reading.change_pct = _normalize_sign(change_parts[1])
                if i + 2 < len(lines):
                    reading.verdict = lines[i + 2].strip()
                i += 3
            else:
                i += 1
            readings[name] = reading
            continue

        if _is_bare_name(line):
            pending_name = line.strip()
        i += 1

    return readings


def classify(change_pct: float | None) -> tuple[str, str]:
    # 5% deliberately matches the `noise_threshold` set on the `Criterion` config in each
    # benches/*.rs file, not an independent guess -- see the comment there (and
    # https://github.com/gklijs/schema_registry_converter/issues/190) for how that number was
    # measured: a same-binary-vs-itself control run put common dev/CI-environment noise at
    # 5-7%, well above criterion's own 1% default.
    if change_pct is None:
        return "🆕", "no baseline on `main` yet"
    if change_pct >= 5:
        return "🔴", f"{change_pct:+.2f}% slower"
    if change_pct <= -5:
        return "🟢", f"{change_pct:+.2f}% faster"
    return "⚪", f"{change_pct:+.2f}% (within noise)"


def render(plain: dict[str, Reading], compared: dict[str, Reading]) -> str:
    if not plain:
        return (
            "_No benchmark results were parsed -- check the `benchmarks` job log, "
            "the output format may have changed._"
        )

    rows = []
    regressions = 0
    improvements = 0
    for name in sorted(plain):
        change_pct = compared.get(name, Reading(name, "")).change_pct
        icon, desc = classify(change_pct)
        time = plain[name].time
        rows.append(f"| `{name}` | {time} | {icon} {desc} |")
        if change_pct is not None:
            if change_pct >= 5:
                regressions += 1
            elif change_pct <= -5:
                improvements += 1

    lines = [
        "| Benchmark | PR time | vs `main` |",
        "|---|---|---|",
        *rows,
    ]
    if regressions:
        lines.append(f"\n⚠️ **{regressions} benchmark(s) regressed by 5% or more.**")
    elif improvements:
        lines.append(f"\n✅ {improvements} benchmark(s) improved by 5% or more, none regressed.")
    else:
        lines.append("\nNo benchmark changed by more than 5% (or is new, see 🆕 rows above).")
    lines.append(
        "\n<sub>Comparison uses criterion's own significance test; ±5% is treated as noise. "
        "Numbers come from a shared GitHub Actions runner, so treat them as directional, not "
        "authoritative -- see [issue #118](https://github.com/gklijs/schema_registry_converter/issues/118).</sub>"
    )
    return "\n".join(lines)


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
        plain = parse(f.read())

    compared: dict[str, Reading] = {}
    for path in sys.argv[2:]:
        with open(path, encoding="utf-8", errors="replace") as f:
            compared.update(parse(f.read()))

    print(render(plain, compared))


if __name__ == "__main__":
    main()
