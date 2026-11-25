#!/usr/bin/env python3
"""Run the user's YCSB common.sh functions against isolated database copies."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--probe', action='store_true')
p.add_argument('--seconds', type=int, default=60)
p.add_argument('--repeats', type=int, default=3)
p.add_argument('--full-suite', action='store_true', help='Full warmup followed by A-F on the same database')
p.add_argument('--out', default='benchmark_results/ycsb_async')
args = p.parse_args()
repo = Path(__file__).resolve().parents[1]
out = (repo / args.out).resolve()
root = Path('/data/lb/ycsb-async-full-85ab' if args.full_suite else '/data/lb/ycsb-async-85ab')
ycsb = Path('/home/lb/YCSB')
backup = Path('/data/lb/workload_backup_data')
assert '/docker/' in Path('/proc/self/cgroup').read_text()
assert sorted(os.sched_getaffinity(0)) == list(range(16))
for d in (out / 'configs', out / 'native_tmp', out / 'empty_library_path', out / 'script_logs', root / 'driver/bin'):
    d.mkdir(parents=True, exist_ok=True)
driver = root / 'driver'
shim = driver / 'bin/ycsb'
shim.write_text('''#!/bin/bash
set -e
cd /home/lb/YCSB
exec python3 ./bin/ycsb "$@" -p maxexecutiontime="$YCSB_LIMIT_SECONDS" -jvm-args "-Xms2g -Xmx4g -Djava.library.path=$YCSB_EMPTY_LIB $YCSB_EXTRA_JVM"
''')
shim.chmod(0o755)
wrapper = driver / 'run.sh'
wrapper.write_text('''#!/bin/bash
set -eo pipefail
source /home/lb/YCSB/test_scripts/inc/common.sh
YCSB_HOME="$1"
ROCKSDB_DATA="$2"
ROCKSDB_RUN_CONFIG="$3"
LOGDIR="$4"
if [ "$5" = warm ]; then warm_up; else run_workload "$5" 16; fi
''')
configs = [dict(name='native_p0', async_=False, workers=0, lazy=True, pipeline=False),
           dict(name='native_p1', async_=False, workers=0, lazy=True, pipeline=True),
           dict(name='async_w1_lazy1', async_=True, workers=1, lazy=True, pipeline=False),
           dict(name='async_w2_lazy1', async_=True, workers=2, lazy=True, pipeline=False),
           dict(name='async_w2_lazy0', async_=True, workers=2, lazy=False, pipeline=False)]
original = (ycsb / 'test_scripts/inc/configs/dbsnappyrun.ini').read_text()
for c in configs:
    flags = dict(async_memtable_write=c['async_'], async_memtable_write_threads=c['workers'],
                 lazy_advance_sequence=c['lazy'], enable_pipelined_write=c['pipeline'],
                 allow_concurrent_memtable_write=True)
    added = ''.join(f'  {k}={str(v).lower()}\n' for k, v in flags.items())
    (out / 'configs' / (c['name'] + '.ini')).write_text(original.replace('[DBOptions]\n', '[DBOptions]\n' + added))
native = repo / 'java/target/librocksdbjni-linux-aarch64.so'
native_hash = hashlib.sha256(native.read_bytes()).hexdigest()
env = dict(os.environ, LD_LIBRARY_PATH='', JAVA_HOME='/usr/lib/jvm/java-11-openjdk',
           ROCKSDB_SHAREDLIB_DIR=str(out / 'native_tmp'), YCSB_EMPTY_LIB=str(out / 'empty_library_path'),
           YCSB_EXTRA_JVM='-Xlog:class+load=info' if args.probe else '')
manifest = dict(commit=subprocess.check_output(['git', '-c', 'safe.directory='+str(repo), '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip(),
                args=vars(args), configs=configs, native_sha256=native_hash,
                cgroup=Path('/proc/self/cgroup').read_text(), cpus=sorted(os.sched_getaffinity(0)),
                quota=Path('/sys/fs/cgroup/cpu/cpu.cfs_quota_us').read_text(),
                period=Path('/sys/fs/cgroup/cpu/cpu.cfs_period_us').read_text(),
                source_files={str(f): hashlib.sha256(f.read_bytes()).hexdigest() for f in
                    [ycsb/'test_scripts/inc/common.sh', ycsb/'test_scripts/run.sh',
                     ycsb/'test_scripts/inc/configs/dbsnappyrun.ini', ycsb/'rocksdb/target/rocksdb-binding-0.18.0-SNAPSHOT.jar',
                     repo/'java/target/rocksdbjni-6.26.1-linux64.jar']})
(out / ('probe_manifest.json' if args.probe else 'manifest.json')).write_text(json.dumps(manifest, indent=2))

def restore(db):
    if db.exists():
        shutil.rmtree(db)
    def copy(src, dst):
        if src.endswith('.sst') and not args.full_suite:
            os.link(src, dst)
        else:
            shutil.copy2(src, dst)
            if args.full_suite:
                # Evict only our private copy instead of changing host-wide caches.
                with open(dst, 'rb') as f:
                    os.fsync(f.fileno())
                    os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    shutil.copytree(backup, db, copy_function=copy)

def run(work, c, db, label, seconds):
    cmd = ['bash', str(wrapper), str(driver), str(db), str(out/'configs'/(c['name']+'.ini')),
           str(out/'script_logs'), work]
    log = out / (label + '.log')
    (out/(label+'.command.json')).write_text(json.dumps(cmd, indent=2))
    process_env = dict(env, YCSB_LIMIT_SECONDS=str(seconds))
    with log.open('w') as f:
        proc = subprocess.Popen(cmd, env=process_env, stdout=f, stderr=subprocess.STDOUT)
        mapped = []
        start = time.monotonic()
        while proc.poll() is None:
            if not mapped:
                # This benchmark is the only Java workload started by this runner.
                for pid in Path('/proc').iterdir():
                    if not pid.name.isdigit():
                        continue
                    try:
                        command = (pid/'cmdline').read_bytes()
                        if b'site.ycsb.Client' not in command or str(db).encode() not in command:
                            continue
                        maps = (pid/'maps').read_text()
                        libs = sorted({line.split()[-1] for line in maps.splitlines() if 'librocksdb' in line and '/' in line})
                        if libs:
                            for lib in libs:
                                assert hashlib.sha256(Path(lib).read_bytes()).hexdigest() == native_hash, lib
                            mapped = dict(pid=pid.name, libraries=libs, sha256=native_hash,
                                          cgroup=(pid/'cgroup').read_text(), status=(pid/'status').read_text())
                    except (FileNotFoundError, ProcessLookupError):
                        continue
            if seconds > 0 and time.monotonic()-start > seconds+240:
                raise RuntimeError('YCSB timed out: ' + label)
            time.sleep(0.5)
    assert proc.returncode == 0, log
    assert mapped, 'Did not verify actual JNI mapping: ' + label
    (out/(label+'.native.json')).write_text(json.dumps(mapped, indent=2))
    text = log.read_text()
    assert 'Exception in thread' not in text and '[ERROR]' not in text and '[main] ERROR' not in text, log
    metrics = {}
    for section, key, value in re.findall(r'^\[([^]]+)\], ([^,]+), ([^\n]+)$', text, re.M):
        metrics.setdefault(section, {})[key] = value.strip()
    assert 'Throughput(ops/sec)' in metrics.get('OVERALL', {}), log
    if seconds == 0:
        logical_ops = (float(metrics['OVERALL']['Throughput(ops/sec)']) *
                       float(metrics['OVERALL']['RunTime(ms)']) / 1000)
        assert abs(logical_ops - 100000000) < 0.01, (label, logical_ops)
    for section, fields in metrics.items():
        for key, value in fields.items():
            if key.startswith('Return='):
                assert key == 'Return=OK' or int(value) == 0, (section, key, value)
    shutil.copy2(db/'LOG', out/(label+'.rocksdb.log'))
    # Preserve the actual effective option values written by native RocksDB.
    db_log = (db/'LOG').read_text(errors='replace')
    for key, value in [('async_memtable_write_threads', str(c['workers'])),
                       ('async_memtable_write', str(int(c['async_']))),
                       ('lazy_advance_sequence', str(int(c['lazy'])))]:
        assert re.search(r'Options\.'+key+r':\s*'+value+r'\b', db_log), (key, value)
    return metrics

if args.probe:
    db = root/'probe'
    if not db.exists(): restore(db)
    c = configs[3]
    metrics = run('workloada', c, db, 'probe', 15)
    print(json.dumps(metrics), flush=True)
elif args.full_suite:
    assert args.repeats == 1, 'Full suite is one pass per configuration'
    results = out/'results.jsonl'
    assert not results.exists(), 'Use a fresh output directory for a full suite'
    for c in configs:
        db = root/'active'
        restore(db)
        label = c['name']+'_full_warm'
        run('warm', c, db, label, 0)
        print(label, '100000000 operations completed', flush=True)
        for work in 'abcdef':
            label = f"{work}_{c['name']}_r0"
            metrics = run('workload'+work, c, db, label, 0)
            row = dict(c, workload=work, repeat=0, logical_operations=100000000, metrics=metrics)
            with results.open('a') as f: f.write(json.dumps(row)+'\n')
            print(label, metrics['OVERALL'], flush=True)
else:
    completed = set()
    results = out/'results.jsonl'
    if results.exists():
        completed = {(r['name'], r['workload'], r['repeat']) for r in map(json.loads, results.read_text().splitlines())}
    for rep in range(args.repeats):
        jobs = [(work, c) for work in 'abcdef' for c in configs]
        random.Random(4200+rep).shuffle(jobs)
        for work, c in jobs:
            if (c['name'], work, rep) in completed: continue
            label = f"{work}_{c['name']}_r{rep}"
            db = root/'active'
            restore(db)
            run('warm', c, db, label+'_warm', 10)
            metrics = run('workload'+work, c, db, label, args.seconds)
            row = dict(c, workload=work, repeat=rep, metrics=metrics)
            with results.open('a') as f: f.write(json.dumps(row)+'\n')
            print(label, metrics['OVERALL'], flush=True)
