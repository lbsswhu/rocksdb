#!/usr/bin/env python3
"""Summarize completed benchmark_zero_range.py runs, excluding warmup round 0."""
import argparse
import csv
import json
from pathlib import Path
import random
import re
import statistics


def number(pattern, text, default=None, last=False):
    values = re.findall(pattern, text)
    return float(values[-1 if last else 0]) if values else default


def bootstrap_interval(a, b):
    rng = random.Random(1001)
    samples = sorted((statistics.median(rng.choices(b, k=len(b))) /
                      statistics.median(rng.choices(a, k=len(a))) - 1) * 100
                     for _ in range(10000))
    return [samples[250], samples[9749]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    results = json.loads((args.root / "results.json").read_text())
    for record in results:
        case = Path(record["case"])
        text = (case / "bench.log").read_text()
        timing = (case / "time.txt").read_text()
        # The first histogram is the foreground write operation distribution.
        # P95 below is separately named: it comes from the DB::Write histogram.
        percentile_line = re.search(r"^Percentiles: (.*)$", text, re.M)
        if percentile_line:
            for p in ("50", "99", "99.9"):
                record["write_op_p" + p + "_us"] = number(
                    r"P" + re.escape(p) + r": ([\d.]+)", percentile_line[1])
        record["db_write_p95_us"] = number(
            r"rocksdb\.db\.write\.micros P50 : [\d.]+ P95 : ([\d.]+)", text)
        for key, pattern in {
            "flush_us": r"flush_micros=(\d+)",
            "compactall_us": r"compactall_micros=(\d+)",
            "close_us": r"db_close_micros=(\d+)",
            "drain_total_us": r"waitforcompaction_total_micros=(\d+)",
            "stall_us": r"rocksdb\.stall\.micros COUNT : (\d+)",
            "flush_bytes": r"rocksdb\.flush\.write\.bytes COUNT : (\d+)",
            "compaction_bytes": r"rocksdb\.compact\.write\.bytes COUNT : (\d+)",
            "allocation_us": r"rocksdb\.zero\.range\.preallocation\.micros .* SUM : (\d+)",
            "truncate_us": r"rocksdb\.preallocation\.truncate\.micros .* SUM : (\d+)",
            "final_sync_us": r"rocksdb\.preallocation\.final\.sync\.micros .* SUM : (\d+)",
        }.items():
            record[key] = number(pattern, text, last=True)
        for key, pattern in {
            "cpu_user_seconds": r"User time \(seconds\): ([\d.]+)",
            "cpu_system_seconds": r"System time \(seconds\): ([\d.]+)",
            "max_rss_kib": r"Maximum resident set size \(kbytes\): (\d+)",
            "filesystem_output_blocks": r"File system outputs: (\d+)",
        }.items():
            record[key] = number(pattern, timing)
        record.update(json.loads((case / "bench.space.json").read_text()))
        for ticker in ("zero.range.preallocation.failures", "preallocation.truncate.failures"):
            count = number("rocksdb\\." + re.escape(ticker) + r" COUNT : (\d+)", text)
            if count not in (0, None):
                raise RuntimeError("nonzero error counter in " + str(case))
    (args.root / "metrics.json").write_text(json.dumps(results, indent=2))
    with (args.root / "metrics.csv").open("w") as out:
        fields = sorted(set().union(*(r.keys() for r in results)))
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)

    groups = list(dict.fromkeys(r["group"] for r in results))
    comparators = [g for g in ("A1", "P", "A0") if g in groups and "B" in groups]
    rounds = int(json.loads((args.root / "environment.json").read_text())["args"]["rounds"])
    lines = ["# ZERO_RANGE write-only benchmark results", "",
             f"Medians of {rounds} measured runs; round 0 excluded.",
             "A0: original; A1: optimized flag off; P: frozen preoptimization flag on; B: optimized flag on.",
             "Each executable explicitly selects its colocated shared library (environment.json).",
             "Write-only foreground; end-to-end includes compaction, flush, drain, close and fixed 5 second wait.",
             "Current runner uses CLOCK_MONOTONIC for process time; raw GNU time is retained separately.",
             "No fixed wait is subtracted. Offline checksum/full scans are excluded from timings.", "",
             "| Scenario | Operations | Threads | Batch | Write buffer bytes |",
             "|---|---:|---:|---:|---:|"]
    for scene in dict.fromkeys(r["scenario"] for r in results):
        r = next(r for r in results if r["scenario"] == scene)
        lines.append(f"| {scene} | {r['operations']} | {r['threads']} | {r.get('batch_size', 1)} | {r.get('buffer_size', 'see commands')} |")
    aggregate = {}
    for scene in dict.fromkeys(r["scenario"] for r in results):
        grouped = {g: [r for r in results if r["scenario"] == scene and
                        r["group"] == g and r["round"] > 0] for g in groups}
        if any(len(v) != rounds for v in grouped.values()):
            raise RuntimeError("incomplete measured rounds: " + scene)
        med = {g: {k: statistics.median(r[k] for r in rs if r.get(k) is not None)
                   for k in rs[0] if isinstance(rs[0][k], (int, float))}
               for g, rs in grouped.items()}
        data = {"medians": med}
        for g in comparators:
            data["b_vs_" + g.lower() + "_percent"] = (med["B"]["ops_sec"] / med[g]["ops_sec"] - 1) * 100
            data["b_vs_" + g.lower() + "_bootstrap_95_percent"] = bootstrap_interval(
                [r["ops_sec"] for r in grouped[g]], [r["ops_sec"] for r in grouped["B"]])
        aggregate[scene] = data
    for metric, label in (("ops_sec", "Foreground throughput"), ("end_to_end_ops_sec", "End-to-end throughput")):
        lines += ["", label, "", "| Scenario | " + " | ".join(groups) +
                  " | " + " | ".join("B/" + g for g in comparators) + " |",
                  "|---|" + "---:|" * (len(groups) + len(comparators))]
        for scene, data in aggregate.items():
            med = data["medians"]
            cells = [f"{med[g][metric]:.0f}" for g in groups]
            cells += [f"{(med['B'][metric]/med[g][metric]-1)*100:+.2f}%" for g in comparators]
            lines.append("| " + scene + " | " + " | ".join(cells) + " |")
    lines += ["", "Foreground change and 95% independent bootstrap interval (10000 resamples, seed 1001):", "",
              "| Scenario | Comparison | Change | 95% interval |", "|---|---|---:|---|"]
    for scene, data in aggregate.items():
        for g in comparators:
            delta = data["b_vs_" + g.lower() + "_percent"]
            ci = data["b_vs_" + g.lower() + "_bootstrap_95_percent"]
            lines.append(f"| {scene} | B/{g} | {delta:+.2f}% | [{ci[0]:+.2f}%, {ci[1]:+.2f}%] |")
    lines += ["", "Intervals are exploratory without multiple-comparison correction; not a portability claim.",
              "P99 is the median of per-run P99 values, not pooled. Peak space is sampled, not a device high-water mark.", "",
              "| Scenario | Group | P99 us | Process s | Close ms | User/system CPU s | Peak allocated MiB |",
              "|---|---|---:|---:|---:|---|---:|"]
    for scene, data in aggregate.items():
        for g, m in data["medians"].items():
            close = m.get("close_us")
            close_text = f"{close/1000:.2f}" if close is not None else "unavailable"
            lines.append(f"| {scene} | {g} | {m['write_op_p99_us']:.2f} | {m['process_seconds']:.2f} | {close_text} | "
                         f"{m['cpu_user_seconds']:.2f}/{m['cpu_system_seconds']:.2f} | {m['peak_allocated_bytes']/2**20:.1f} |")
    (args.root / "analysis.json").write_text(json.dumps(aggregate, indent=2))
    notes = args.root / "post-measurement-notes.json"
    if notes.exists():
        lines += ["", "Build and validation notes:", ""] + json.loads(notes.read_text())
    (args.root / "analysis.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
