#!/usr/bin/env python3
"""Same-writer flag-only diagnostic; build zero_range_cost_probe.c first.

Use one optimized B bench.command.json as the workload template. All cases retain
its geometry/seal/sync strategy. Only kernel fallocate flags change. This is not
a production mode and its results must stay separate from the formal benchmark.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import statistics

from benchmark_zero_range import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--template', type=Path, required=True)
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=5)
    parser.add_argument('--modes', default='zero,keep_all,keep_wal,keep_flush,keep_compaction')
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error('rounds must be positive')
    command = json.loads(args.template.read_text())
    command = command[command.index('env'):]
    binary = next(Path(s) for s in command if s.endswith('/db_bench'))
    if '--enable_zero_range_preallocation=1' not in command:
        parser.error('template must be a ZERO_RANGE-enabled optimized run')
    for name in ('sst_dump', 'ldb'):
        if not (binary.parent / 'tools' / name).exists():
            parser.error('missing validation tool: ' + name)
    args.root.mkdir(parents=True, exist_ok=False)
    modes = args.modes.split(',')
    if len(set(modes)) != len(modes) or any(m not in (
            'zero', 'keep_all', 'keep_wal', 'keep_flush', 'keep_compaction', 'skip_flush')
            for m in modes):
        parser.error('invalid or duplicate diagnostic modes')
    if 'skip_flush' in modes and '--use_direct_io_for_flush_and_compaction=1' in command:
        parser.error('skip_flush is a buffered-I/O diagnostic only')
    (args.root / 'environment.json').write_text(json.dumps({
        'template': str(args.template.resolve()), 'command': command,
        'probe_sha256': hashlib.sha256(args.probe.read_bytes()).hexdigest(),
        'rounds': args.rounds, 'modes': modes,
        'note': 'instrumented diagnostic, NOT production feature results'}, indent=2))
    results = []
    for round_number in range(args.rounds + 1):
        offset = round_number % len(modes)
        for mode in modes[offset:] + modes[:offset]:
            case = args.root / f'r{round_number}-{mode}'
            case.mkdir()
            db = (case / 'db').resolve()
            adjusted = ['--db=' + str(db) if s.startswith('--db=') else
                        '--wal_dir=' + str(db) if s.startswith('--wal_dir=') else s
                        for s in command]
            adjusted[1:1] = ['LD_PRELOAD=' + str(args.probe.resolve()),
                             'ROCKSDB_ZERO_COST_MODE=' + mode,
                             'ROCKSDB_ZERO_COST_OUTPUT=' + str((case / 'cost.json').resolve())]
            elapsed = run(['/usr/bin/time', '-v', '-o', str(case / 'time.txt')] + adjusted,
                          case / 'bench.log', monitor_dir=db)
            costs = json.loads((case / 'cost.json').read_text())['sources']
            for source in ('wal', 'flush', 'compaction'):
                skipped = costs[source].get('skipped_calls', 0)
                if mode == 'skip_flush' and source == 'flush':
                    if not skipped or costs[source]['zero_calls'] or costs[source]['keep_calls']:
                        raise RuntimeError('flush skip simulation did not match: ' + str(case))
                    continue
                if skipped:
                    raise RuntimeError('unexpected skipped allocation: ' + str(case))
                keep = mode in ('keep_all', 'keep_' + source)
                active = 'keep_calls' if keep else 'zero_calls'
                other = 'zero_calls' if keep else 'keep_calls'
                if costs[source][active] == 0 or costs[source][other] or costs[source]['failures']:
                    raise RuntimeError('incorrect mode/source attribution: ' + str(case))
            if costs['other']['zero_calls']:
                raise RuntimeError('unattributed ZERO_RANGE call: ' + str(case))
            text = (case / 'bench.log').read_text()
            match = re.search(r'^fillrandom\s*:\s*([\d.]+) micros/op ([\d.]+) ops/sec', text, re.M)
            if not match:
                raise RuntimeError('template must use fillrandom')
            prefix = ['env', 'LD_LIBRARY_PATH=' + str(binary.parent)]
            run(prefix + [str(binary.parent / 'tools' / 'sst_dump'), '--file=' + str(db),
                          '--command=check', '--verify_checksum'], case / 'checksum.log')
            run(prefix + [str(binary.parent / 'tools' / 'ldb'), '--db=' + str(db),
                          'dump', '--count_only'], case / 'scan.log')
            keys = re.search(r'Keys in range: (\d+)', (case / 'scan.log').read_text())
            if not keys or (results and int(keys[1]) != results[0]['keys']):
                raise RuntimeError('scan key count mismatch')
            record = dict(round=round_number, mode=mode, ops_sec=float(match[2]),
                          process_seconds=elapsed, sources=costs, keys=int(keys[1]), case=str(case))
            results.append(record)
            (args.root / 'results.json').write_text(json.dumps(results, indent=2))
            print(json.dumps(record), flush=True)
    lines = ['Instrumented flag-only diagnostic; same geometry/seal/sync policy.',
             'One warmup, ' + str(args.rounds) + ' measured rounds. Not production performance.', '',
             '| Mode | Median ops/s | Process seconds | WAL/Flush/Compaction allocation us |',
             '|---|---:|---:|---|']
    for mode in modes:
        rows = [r for r in results if r['mode'] == mode and r['round'] > 0]
        costs = '/'.join(f"{statistics.median(r['sources'][s]['syscall_nanos']/1000 for r in rows):.0f}"
                         for s in ('wal', 'flush', 'compaction'))
        lines.append(f"| {mode} | {statistics.median(r['ops_sec'] for r in rows):.0f} | "
                     f"{statistics.median(r['process_seconds'] for r in rows):.3f} | {costs} |")
    (args.root / 'summary.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
