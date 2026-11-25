#!/usr/bin/env python3
"""Run an isolated db_bench matrix. Never run two copies at once."""
import argparse
import hashlib
import json
import os
import random
import re
import resource
import shutil
import subprocess
import time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--duration', type=int, default=10)
p.add_argument('--threads', type=int, default=32)
p.add_argument('--pipelined-write', choices=['false', 'true', 'both'], default='both')
p.add_argument('--repeats', type=int, default=3)
p.add_argument('--cpus', default=None)
p.add_argument('--root', default='/data/lb/async-memtable-bench-85ab')
p.add_argument('--out', default='benchmark_results/async_memtable_write')
args = p.parse_args()
if args.cpus is None:
    args.cpus = ','.join(str(c) for c in sorted(os.sched_getaffinity(0)))
out = Path(args.out)
out.mkdir(parents=True, exist_ok=True)
root = Path(args.root)
common = ['--num=1000000', '--key_size=16', '--value_size=100',
          '--compression_type=none',
          '--disable_wal=false', '--cache_size=0', '--histogram=true',
          '--stats_interval=1000000000', '--ops_between_duration_checks=100',
          '--write_buffer_size=67108864', '--max_write_buffer_number=4',
          '--max_background_jobs=4', '--statistics=false']
configs = []
for sync in (False, True):
    for concurrent in (False, True):
        configs.append(dict(sync=sync, concurrent=concurrent, async_=False, workers=0))
        for workers in (1, 2, 4, 8, 16):
            if not concurrent and workers > 1:
                continue
            for lazy in (False, True):
                configs.append(dict(sync=sync, concurrent=concurrent, lazy=lazy, workers=workers, async_=True))
pipelines = ['false', 'true'] if args.pipelined_write == 'both' else [args.pipelined_write]
configs = [dict(c, pipeline=pipeline) for c in configs for pipeline in pipelines]
manifest = dict(args=vars(args), commit=subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode(),
               binary_sha256=hashlib.sha256(Path('db_bench').read_bytes()).hexdigest(),
               common=common, configs=configs)
manifest['runtime'] = dict(cgroup=Path('/proc/self/cgroup').read_text(),
                           affinity=sorted(os.sched_getaffinity(0)),
                           cpu_quota=Path('/sys/fs/cgroup/cpu/cpu.cfs_quota_us').read_text(),
                           cpu_period=Path('/sys/fs/cgroup/cpu/cpu.cfs_period_us').read_text())
if (out / 'manifest.json').exists():
    previous = json.loads((out / 'manifest.json').read_text())
    if previous['args'] != manifest['args'] or previous['binary_sha256'] != manifest['binary_sha256']:
        raise RuntimeError('Existing output directory has a different configuration or binary')
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2))
completed = set()
if (out / 'results.jsonl').exists():
    for line in (out / 'results.jsonl').read_text().splitlines():
        r = json.loads(line)
        completed.add((r['name'], r['repeat']))
for rep in range(args.repeats):
    jobs = [(work, c) for work in ('readrandom', 'readrandomwriterandom', 'overwrite') for c in configs]
    random.Random(42 + rep).shuffle(jobs)
    for work, c in jobs:
        name = f"{work}_s{int(c['sync'])}_c{int(c['concurrent'])}_a{int(c['async_'])}_w{c['workers']}_l{int(c.get('lazy', True))}_p{c['pipeline']}"
        if (name, rep) in completed:
            continue
        logfile = out / (name + f'_r{rep}.log')
        # Use a fresh process and fresh DB for every repeat.
        db = root / 'active'
        shutil.rmtree(db) if db.exists() else None
        def copy(src, dst):
            # SST files are immutable; all other files are copied.
            if src.endswith('.sst'):
                os.link(src, dst)
            else:
                shutil.copy2(src, dst)
        shutil.copytree(root / 'seed', db, copy_function=copy)
        cmd = ['taskset', '-c', args.cpus, './db_bench'] + common + [
            f'--db={db}', '--use_existing_db=true', f'--duration={args.duration}',
            f'--threads={args.threads}', '--readwritepercent=50', f'--seed={42}',
            f"--sync={str(c['sync']).lower()}", f"--allow_concurrent_memtable_write={str(c['concurrent']).lower()}",
            f"--async_memtable_write={str(c['async_']).lower()}",
            f"--enable_pipelined_write={c['pipeline']}",
            f"--lazy_advance_sequence={str(c.get('lazy', True)).lower()}",
            f"--async_memtable_write_threads={c['workers']}"]
        (out / (name + f'_r{rep}.command.json')).write_text(json.dumps(cmd + [f'--benchmarks={work}'], indent=2))
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.monotonic()
        result = subprocess.run(cmd + [f'--benchmarks={work}'], capture_output=True, text=True, timeout=args.duration + 180)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        logfile.write_text(result.stdout + result.stderr)
        match = re.search(r'^' + work + r'\s+:\s+([\d.]+) micros/op\s+([\d.]+) ops/sec', result.stdout, re.M)
        if result.returncode or not match:
            raise RuntimeError(f'{name}: {result.stderr}')
        if c['async_']:
            assert 'pending_groups=0 pending_bytes=0 terminal_error=0' in result.stdout
        if re.search(r'get error|put error', result.stdout + result.stderr):
            raise RuntimeError('read failure')
        row = dict(c, name=name, repeat=rep, workload=work, micros=match.group(1), ops=match.group(2))
        row['cpu'] = after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime
        row['wall_seconds'] = time.monotonic() - start
        row['p99_us'] = dict(re.findall(r'Microseconds per (\w+):.*?P99: ([\d.]+)', result.stdout, re.S))
        drain = re.search(r'Async commit drain: (\d+) us', result.stdout)
        row['drain_us'] = int(drain.group(1)) if drain else 0
        with (out / 'results.jsonl').open('a') as f:
            f.write(json.dumps(row) + '\n')
        print(row, flush=True)
