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

    lines = ["# ZERO_RANGE write-only benchmark results", "",
             "Each cell is the median of five measured runs; round 0 is excluded.",
             "A0: original release; A1: new release, flag off; B: same release, flag on.",
             "Foreground threads: 1 or 4. Compaction/flush/drain/close are included",
             "in process time, including the identical fixed 5-second wait.",
             "All groups run final compactall to flush every active memtable, including",
             "the original db_bench whose single-CF flush benchmark is a no-op.", "",
             "Scenarios: main1/main4 = async random writes with WAL; sync1/sync4 =",
             "sync=true random writes; sst = WAL disabled; direct = async with WAL",
             "and direct SST I/O; directsst = WAL disabled and direct SST I/O;",
             "seq = sequential writes; overwrite = independent copies of one seed DB.",
             "Normal cases: 600000 writes, 64 MiB write buffer/target SST, 256 MiB level base.",
             "Sync cases: 10000 writes, 1 MiB write buffer/target SST, 4 MiB level base.",
             "Keys/values: 16/1024 bytes, no compression, batch=1, background jobs=2.", "",
             "Foreground write throughput (excludes final compaction/flush/drain/close):", "",
             "| Scenario | A0 ops/s | A1 ops/s | B ops/s | B/A1 | 95% bootstrap interval | B/A0 |",
             "|---|---:|---:|---:|---:|---|---:|"]
    aggregate = {}
    for scene in dict.fromkeys(r["scenario"] for r in results):
        grouped = {g: [r for r in results if r["scenario"] == scene and
                        r["group"] == g and r["round"] > 0]
                   for g in ("A0", "A1", "B")}
        if any(len(v) != 5 for v in grouped.values()):
            raise RuntimeError("expected exactly five measured runs: " + scene)
        med = {g: {k: statistics.median(r[k] for r in rs if r.get(k) is not None)
                   for k in rs[0] if isinstance(rs[0][k], (int, float))}
               for g, rs in grouped.items()}
        ci = bootstrap_interval([r["ops_sec"] for r in grouped["A1"]],
                                [r["ops_sec"] for r in grouped["B"]])
        a0, a1, b = (med[g]["ops_sec"] for g in ("A0", "A1", "B"))
        delta = (b / a1 - 1) * 100
        lines.append(f"| {scene} | {a0:.0f} | {a1:.0f} | {b:.0f} | {delta:+.2f}% | "
                     f"[{ci[0]:+.2f}%, {ci[1]:+.2f}%] | {(b/a0-1)*100:+.2f}% |")
        aggregate[scene] = {"medians": med, "b_vs_a1_percent": delta,
                            "b_vs_a1_bootstrap_95_percent": ci}
    lines += ["", "Intervals use independent bootstrap resampling of five per-run",
              "throughput samples, seed 1001, 10000 resamples; exploratory, without",
              "multiple-comparison correction. They do not establish portability.", "",
              "End-to-end throughput includes all finalization stages and fixed waits.",
              "A1/B additionally require two three-sample idle checks (~0.4 seconds).",
              "That overhead is shared by A1/B, and is included when comparing to A0;",
              "no wait time has been subtracted from any group.", "",
              "| Scenario | A0 ops/s | A1 ops/s | B ops/s | B/A1 |",
              "|---|---:|---:|---:|---:|"]
    for scene, data in aggregate.items():
        a0, a1, b = (data["medians"][g]["end_to_end_ops_sec"]
                      for g in ("A0", "A1", "B"))
        lines.append(f"| {scene} | {a0:.0f} | {a1:.0f} | {b:.0f} | {(b/a1-1)*100:+.2f}% |")
    lines += ["",
              "P99 below is the median of five per-run P99 values, not a pooled P99.", "",
              "Peak space is sampled from directory st_blocks every 100ms, not an exact",
              "device-wide high-water mark. Raw time.txt records CPU/RSS/filesystem output.", "",
              "| Scenario | A1/B P99 us | A1/B process s | A1/B close ms | A1/B peak allocated MiB |",
              "|---|---:|---:|---:|---:|"]
    for scene, data in aggregate.items():
        a, b = data["medians"]["A1"], data["medians"]["B"]
        lines.append(f"| {scene} | {a['write_op_p99_us']:.2f}/{b['write_op_p99_us']:.2f} | "
                     f"{a['process_seconds']:.2f}/{b['process_seconds']:.2f} | "
                     f"{a['close_us']/1000:.2f}/{b['close_us']/1000:.2f} | "
                     f"{a['peak_allocated_bytes']/2**20:.1f}/{b['peak_allocated_bytes']/2**20:.1f} |")
    (args.root / "analysis.json").write_text(json.dumps(aggregate, indent=2))
    notes = args.root / "post-measurement-notes.json"
    if notes.exists():
        lines += ["", "Build and validation notes:", ""] + json.loads(notes.read_text())
    (args.root / "analysis.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
