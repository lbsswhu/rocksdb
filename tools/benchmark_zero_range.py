#!/usr/bin/env python3
"""Write-only, isolated A0/A1/B db_bench runs; at most four foreground threads.

Timing stops before offline validation. Round zero is warmup. Keep all raw
outputs, independent DB directories and commands for inspection/reproduction.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shutil
import statistics
import subprocess
import time


def run(command, output, monitor_dir=None):
    output.with_suffix(".command.json").write_text(json.dumps(command, indent=2))
    with output.open("w") as stream:
        begin = time.monotonic()
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        peak_logical = peak_allocated = 0
        pidfd = os.pidfd_open(process.pid) if hasattr(os, "pidfd_open") else None
        while process.poll() is None:
            if monitor_dir is not None and monitor_dir.exists():
                logical = allocated = 0
                for path in monitor_dir.iterdir():
                    try:
                        info = path.stat()
                        logical += info.st_size
                        allocated += info.st_blocks * 512
                    except FileNotFoundError:
                        pass  # Concurrent compaction deletes obsolete files.
                peak_logical = max(peak_logical, logical)
                peak_allocated = max(peak_allocated, allocated)
            if pidfd is not None:
                select.select([pidfd], [], [], 0.1)
            else:
                try:
                    process.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    pass
        elapsed = time.monotonic() - begin
        if pidfd is not None:
            os.close(pidfd)
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, command)
        if monitor_dir is not None:
            output.with_suffix(".space.json").write_text(json.dumps({
                "peak_logical_bytes": peak_logical,
                "peak_allocated_bytes": peak_allocated,
                "sample_interval_seconds": 0.1,
            }))
        return elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--previous", type=Path,
                        help="frozen preoptimization ZERO_RANGE binary (group P)")
    parser.add_argument("--groups", help="subset/order of A0,A1,P,B")
    parser.add_argument("--geometry-mib", type=int,
                        help="fixed write buffer/target size for BOTH sync and async")
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--cost-probe", type=Path,
                        help="diagnostic LD_PRELOAD shim; not a production performance run")
    parser.add_argument("--cost-mode", default="zero", choices=(
        "zero", "keep_all", "keep_wal", "keep_flush", "keep_compaction"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--num", type=int, default=600000)
    parser.add_argument("--sync-num", type=int, default=10000)
    parser.add_argument("--scenarios", default="main1,main4,sync1,sync4,sst,direct,directsst,seq,overwrite")
    args = parser.parse_args()
    if args.rounds < 1 or args.num < 1 or args.sync_num < 1:
        parser.error("rounds and operation counts must be positive")
    if args.batch_size < 1 or (args.geometry_mib is not None and args.geometry_mib < 1):
        parser.error("batch size and geometry must be positive")
    binary, baseline = args.binary.resolve(), args.baseline.resolve()
    previous = args.previous.resolve() if args.previous else None
    groups = args.groups.split(",") if args.groups else (
        ["A0", "A1", "P", "B"] if previous else ["A0", "A1", "B"])
    if not groups or len(groups) != len(set(groups)) or any(
            g not in ("A0", "A1", "P", "B") for g in groups):
        parser.error("groups must be unique entries from A0,A1,P,B")
    if "P" in groups and previous is None:
        parser.error("group P requires --previous")
    def executable_command(executable, library_dir=None, extra_env=()):
        # CMake RUNPATH can point back to the original build directory after a
        # binary is copied. Bind every run to its own frozen shared library.
        return ["env", "LD_LIBRARY_PATH=" + str(library_dir or executable.parent)
                ] + list(extra_env) + [str(executable)]

    def find_tool(name):
        for candidate in (binary.parent / name, binary.parent / "tools" / name):
            if candidate.is_file():
                return str(candidate)
        parser.error("missing offline validation tool: " + name)
    sst_dump, ldb = find_tool("sst_dump"), find_tool("ldb")
    args.root.mkdir(parents=True, exist_ok=False)
    env = {
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "uname": os.uname(),
        "baseline_sha256": hashlib.sha256(baseline.read_bytes()).hexdigest(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "previous_sha256": hashlib.sha256(previous.read_bytes()).hexdigest() if previous else None,
        "groups": groups,
        "library_sha256": {str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest()
                           for executable in (binary, baseline) + ((previous,) if previous else ())
                           for p in executable.parent.glob("librocksdb.so.*")},
        "args": {k: str(v) for k, v in vars(args).items()},
    }
    env["resolved_libraries"] = {
        str(executable): subprocess.check_output(
            ["env", "LD_LIBRARY_PATH=" + str(executable.parent), "ldd", str(executable)],
            text=True) for executable in (binary, baseline) + ((previous,) if previous else ())}
    (args.root / "environment.json").write_text(json.dumps(env, indent=2))
    for name, command in {
        "mount": ["findmnt", "-T", str(args.root)],
        "cpu": ["lscpu"], "memory": ["free", "-h"],
        "space": ["df", "-h", str(args.root)],
        "devices": ["lsblk", "-o", "NAME,MODEL,SIZE,FSTYPE,MOUNTPOINTS"],
    }.items():
        run(command, args.root / (name + ".txt"))

    scenarios = {
        "main1": ("fillrandom", 1, False, False, False),
        "main4": ("fillrandom", 4, False, False, False),
        "sync1": ("fillrandom", 1, True, False, False),
        "sync4": ("fillrandom", 4, True, False, False),
        "sst": ("fillrandom", 1, False, True, False),
        "direct": ("fillrandom", 1, False, False, True),
        "directsst": ("fillrandom", 1, False, True, True),
        "seq": ("fillseq", 1, False, False, False),
        "overwrite": ("overwrite", 1, False, False, False),
    }
    results = []
    for scenario in args.scenarios.split(","):
        if scenario not in scenarios:
            parser.error("unknown scenario: " + scenario)
        benchmark, threads, sync, no_wal, direct = scenarios[scenario]
        total = args.sync_num if sync else args.num
        if total % (threads * args.batch_size):
            parser.error("operation count must be divisible by threads * batch size")
        # Use the design's normal 64 MiB geometry for sustained writes. Sync
        # cases use a fixed smaller geometry so 10k durable writes also create
        # multiple flushes and real compaction outputs within a practical run.
        buffer_size = (args.geometry_mib << 20) if args.geometry_mib else (
            1 << 20 if sync else 64 << 20)
        common = [
            "--benchmarks=" + benchmark + ",compactall,flush,waitforcompaction,stats",
            "--num=" + str(total), "--writes=" + str(total // threads),
            "--threads=" + str(threads), "--batch_size=" + str(args.batch_size), "--seed=1001",
            "--duration=0", "--key_size=16", "--value_size=1024",
            "--compression_type=none", "--write_buffer_size=" + str(buffer_size),
            "--max_write_buffer_number=4", "--target_file_size_base=" + str(buffer_size),
            "--max_bytes_for_level_base=" + str(buffer_size * 4), "--max_background_jobs=2",
            "--level0_file_num_compaction_trigger=4", "--disable_auto_compactions=0",
            "--options_file=", "--disable_wal=" + str(int(no_wal)),
            "--sync=" + str(int(sync)), "--use_fsync=0",
            "--use_direct_io_for_flush_and_compaction=" + str(int(direct)),
            "--bytes_per_sync=0", "--wal_bytes_per_sync=0", "--histogram=1",
            "--statistics=1", "--stats_interval_seconds=0",
        ]
        seed = args.root / (scenario + "-seed")
        if benchmark == "overwrite":
            run(executable_command(baseline) + common + ["--benchmarks=fillseq,compactall,flush,waitforcompaction",
                "--db=" + str(seed), "--wal_dir=" + str(seed)],
                args.root / "overwrite-seed.log")
        for round_number in range(args.rounds + 1):
            # Rotate every group through the beginning/end of a round.
            offset = round_number % len(groups)
            order = groups[offset:] + groups[:offset]
            for group in order:
                case = args.root / (scenario + "-r" + str(round_number) + "-" + group)
                case.mkdir()
                database = case / "db"
                if benchmark == "overwrite":
                    shutil.copytree(seed, database)
                executable = baseline if group == "A0" else previous if group == "P" else binary
                probe_env = []
                if args.cost_probe:
                    probe_env = ["LD_PRELOAD=" + str(args.cost_probe.resolve()),
                                 "ROCKSDB_ZERO_COST_MODE=" + args.cost_mode,
                                 "ROCKSDB_ZERO_COST_OUTPUT=" + str((case / "cost.json").resolve())]
                command = executable_command(executable, extra_env=probe_env) + common + [
                    "--db=" + str(database), "--wal_dir=" + str(database),
                    "--use_existing_db=" + str(int(benchmark == "overwrite")),
                ]
                if group != "A0":
                    command.append("--enable_zero_range_preallocation=" + str(int(group in ("P", "B"))))
                monitored_wall = run(["/usr/bin/time", "-v", "-o", str(case / "time.txt")] + command,
                           case / "bench.log", monitor_dir=database)
                timing = (case / "time.txt").read_text()
                elapsed_text = re.search(r"Elapsed \(wall clock\) time .*: ([\d:.]+)", timing)
                if elapsed_text is None:
                    raise RuntimeError("missing process elapsed time")
                wall = 0.0
                for component in elapsed_text[1].split(":"):
                    wall = wall * 60 + float(component)
                # GNU time uses wall time, which can jump in a VM. Retain its
                # raw value; the end-to-end primary metric uses CLOCK_MONOTONIC.
                gnu_wall = wall
                wall = monitored_wall
                text = (case / "bench.log").read_text()
                match = re.search(r"^" + benchmark + r"\s*:\s*([\d.]+) micros/op ([\d.]+) ops/sec", text, re.M)
                if not match:
                    raise RuntimeError("missing benchmark throughput: " + str(case))
                operations = re.search(r"benchmark_operations=(\d+)", text)
                if operations is not None:
                    operation_count = int(operations[1])
                else:
                    # Original db_bench only reports the histogram count. One
                    # histogram sample represents one completed batch.
                    batches = re.search(r"Count: (\d+)", text)
                    operation_count = int(batches[1]) * args.batch_size if batches else None
                if operation_count != total:
                    raise RuntimeError("actual operation count mismatch: " + str(case))
                options_text = "\n".join(p.read_text() for p in database.glob("OPTIONS-*"))
                if not re.search(r"recycle_log_file_num\s*=\s*0\b", options_text):
                    raise RuntimeError("WAL recycling is not verified disabled")
                def ticker(name):
                    found = re.findall(re.escape(name) + r" COUNT : (\d+)", text)
                    return int(found[-1]) if found else None
                calls = ticker("rocksdb.zero.range.preallocation.calls")
                fallback = ticker("rocksdb.zero.range.preallocation.fallbacks")
                if group in ("P", "B") and (not calls or fallback != 0):
                    raise RuntimeError("B did not exercise ZERO_RANGE without fallback")
                flush_calls = ticker("rocksdb.zero.range.preallocation.flush.calls")
                compaction_calls = ticker("rocksdb.zero.range.preallocation.compaction.calls")
                wal_calls = ticker("rocksdb.zero.range.preallocation.wal.calls")
                if group in ("P", "B") and (not flush_calls or
                                      (benchmark != "fillseq" and not compaction_calls) or
                                      (not no_wal and not wal_calls)):
                    raise RuntimeError("missing ZERO_RANGE on a required output path")
                if group in ("P", "B") and no_wal and wal_calls != 0:
                    raise RuntimeError("WAL allocation occurred in WAL-disabled workload")
                if group == "A1" and calls != 0:
                    raise RuntimeError("A1 unexpectedly exercised ZERO_RANGE")
                # Offline full SST checksums and reopen/full scan are excluded
                # from timed write-only performance.
                run(executable_command(Path(sst_dump), binary.parent) + ["--file=" + str(database),
                     "--command=check", "--verify_checksum"], case / "checksum.log")
                run(executable_command(Path(ldb), binary.parent) + ["--db=" + str(database),
                     "dump", "--count_only"], case / "scan.log")
                scan = (case / "scan.log").read_text()
                keys = re.search(r"Keys in range: (\d+)", scan)
                if keys is None or int(keys[1]) == 0:
                    raise RuntimeError("offline full scan is empty or failed")
                expected_file = args.root / (scenario + "-expected-keys.json")
                if benchmark in ("fillseq", "overwrite") and int(keys[1]) != total:
                    raise RuntimeError("deterministic full scan key count mismatch")
                # Random keys depend on each thread's fixed RNG stream; write
                # ordering changes values but must not change the unique keys.
                if expected_file.exists():
                    if int(keys[1]) != json.loads(expected_file.read_text()):
                        raise RuntimeError("A0/A1/B full scan key count mismatch")
                else:
                    expected_file.write_text(json.dumps(int(keys[1])))
                record = {"scenario": scenario, "round": round_number, "group": group,
                          "threads": threads, "operations": total,
                          "batch_size": args.batch_size, "buffer_size": buffer_size,
                          "ops_sec": float(match[2]),
                          "micros_op": float(match[1]), "process_seconds": wall,
                          "end_to_end_ops_sec": total / wall,
                          "monitored_wall_seconds": monitored_wall,
                          "gnu_time_wall_seconds": gnu_wall,
                          "zero_calls": calls, "fallback": fallback,
                          "wal_calls": wal_calls, "flush_calls": flush_calls,
                          "compaction_calls": compaction_calls,
                          "scanned_keys": int(keys[1]),
                          "final_bytes": sum(p.stat().st_size for p in database.iterdir() if p.is_file()),
                          "case": str(case)}
                results.append(record)
                (args.root / "results.json").write_text(json.dumps(results, indent=2))
                print(json.dumps(record), flush=True)
    lines = ["Write-only throughput medians; round zero excluded.",
             "A0 original; A1 optimized flag off; P frozen preoptimization flag on; B optimized flag on.", ""]
    for metric, label in (("ops_sec", "Foreground (excludes final compaction/drain/close)"),
                          ("end_to_end_ops_sec", "End to end (includes fixed 5 second wait)")):
        lines += [label, "", "| Scenario | " + " | ".join(groups) +
                  " | B/A1 | B/P |", "|---|" + "---:|" * (len(groups) + 2)]
        for scenario in args.scenarios.split(","):
            medians = {g: statistics.median(r[metric] for r in results
                       if r["scenario"] == scenario and r["group"] == g and r["round"] > 0)
                       for g in groups}
            changes = ["{:+.2f}%".format((medians["B"] / medians[g] - 1) * 100)
                       if "B" in medians and g in medians else "n/a" for g in ("A1", "P")]
            lines.append("| " + scenario + " | " + " | ".join(
                "{:.0f}".format(medians[g]) for g in groups) + " | " + " | ".join(changes) + " |")
        lines.append("")
    (args.root / "summary.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
