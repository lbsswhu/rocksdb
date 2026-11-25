#!/usr/bin/env python3
"""Summarize the repeated runs and compare to matched native baselines."""
import csv
import argparse
import json
import statistics as st
from collections import defaultdict
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--out', default='benchmark_results/async_memtable_write')
args = p.parse_args()
base = Path(args.out)
groups = defaultdict(list)
for path in base.rglob('results.jsonl'):
    for line in path.read_text().splitlines():
        r = json.loads(line)
        r.setdefault('pipeline', 'false')
        groups[(r['name'], r['pipeline'])].append(r)
summary = []
for (name, pipeline), rows in groups.items():
    first = rows[0]
    ops = [float(r['ops']) for r in rows]
    s = {k: first[k] for k in ('workload', 'sync', 'concurrent', 'async_', 'workers')}
    s.update(name=name, pipeline=pipeline, lazy=first.get('lazy'), repeats=len(rows),
             median_ops=st.median(ops), min_ops=min(ops), max_ops=max(ops),
             cv=st.stdev(ops)/st.mean(ops) if len(ops)>1 else 0,
             median_us=st.median(float(r['micros']) for r in rows),
             cpu_seconds=st.median(r['cpu'] for r in rows),
             max_drain_us=max(r['drain_us'] for r in rows))
    for op in ('read', 'write'):
        lat = [float(r['p99_us'][op]) for r in rows if op in r['p99_us']]
        s['p99_' + op] = st.median(lat) if lat else None
    summary.append(s)

def match_key(r):
    return r['workload'], r['sync'], r['concurrent'], r['pipeline']

baselines = {match_key(r): r for r in summary if not r['async_']}
def latency(value):
    return '—' if value is None else f'{value:.2f}'
for r in summary:
    b = baselines.get(match_key(r))
    r['speedup'] = r['median_ops'] / b['median_ops'] if b else 0
summary.sort(key=lambda r: (r['workload'], r['pipeline'], r['sync'], r['concurrent'], r['workers'], str(r['lazy'])))
with (base / 'summary.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(summary[0]), lineterminator='\n')
    writer.writeheader()
    writer.writerows(summary)
report = '''# async_memtable_write

Runtime: lb_rocksdb_16u container, CPU quota 16, cpuset 0-15 (8 physical cores / 16 SMT threads).
Full methodology: [README.md](README.md).

Source: 97fc7d395; zero range reverted. Release -O2.
32 clients, 16B keys, 100B values, 1M keys. 10 seconds per run.
Three independent repetitions, randomized order; throughput is the median.
WAL enabled. SST seed. P99 is API latency; throughput includes async drain.
Speedup uses the same workload, sync, concurrent and pipeline native baseline.
P99 columns show the median P99 across runs, in microseconds.
'''
count = sum(r['repeats'] for r in summary)
report = report.replace('# async_memtable_write\n', f'# async_memtable_write\n\nStatus: {count}/504 runs collected.\n', 1)
report += '\n## Best async configuration for each publication mode\n\n'
report += '|Workload|Sync|Lazy|Native ops/s|Async ops/s|Change|Workers / C / pipeline|Read P99|Write P99|\n'
report += '|---|---|---|---|---|---|---|---|---|\n'
for work in ('readrandom', 'readrandomwriterandom', 'overwrite'):
    for sync in (False, True):
        native = [r for r in summary if r['workload']==work and r['sync']==sync and not r['async_']]
        if not native:
            continue
        b = max(native, key=lambda r: r['median_ops'])
        for lazy in (False, True):
            candidates = [r for r in summary if r['workload']==work and r['sync']==sync and r['async_'] and r['lazy']==lazy]
            if not candidates:
                continue
            a = max(candidates, key=lambda r: r['median_ops'])
            report += f"|{work}|{sync}|{lazy}|{b['median_ops']:.0f}|{a['median_ops']:.0f}|{a['median_ops']/b['median_ops']-1:+.1%}|{a['workers']}/{a['concurrent']}/{a['pipeline']}|{latency(a['p99_read'])}|{latency(a['p99_write'])}|\n"
report += '\n## Best configurations (across pipeline and concurrency settings)\n\n'
report += '|Workload|Sync|Best native ops/s|Best async ops/s|Change|Async workers / lazy / C / pipeline|\n'
report += '|---|---|---|---|---|---|\n'
for work in ('readrandom', 'readrandomwriterandom', 'overwrite'):
    for sync in (False, True):
        subset = [r for r in summary if r['workload'] == work and r['sync'] == sync]
        native = [r for r in subset if not r['async_']]
        asynchronous = [r for r in subset if r['async_']]
        if not native or not asynchronous:
            continue
        b = max(native, key=lambda r: r['median_ops'])
        a = max(asynchronous, key=lambda r: r['median_ops'])
        report += f"|{work}|{sync}|{b['median_ops']:.0f}|{a['median_ops']:.0f}|{a['median_ops']/b['median_ops']-1:+.1%}|{a['workers']} / {a['lazy']} / {a['concurrent']} / {a['pipeline']}|\n"
for work in ('readrandom', 'readrandomwriterandom', 'overwrite'):
    report += '\n## ' + work + '\n'
    report += '|pipeline|sync|concurrent|workers|lazy|ops/s|speedup|P99 read/write|CV|runs|\n'
    report += '|---|---|---|---|---|---|---|---|---|---|\n'
    for r in summary:
        if r['workload'] == work:
            report += f"|{r['pipeline']}|{r['sync']}|{r['concurrent']}|{r['workers']}|{r['lazy']}|{r['median_ops']:.0f}|{r['speedup']:.3f}|{latency(r['p99_read'])}/{latency(r['p99_write'])}|{r['cv']:.1%}|{r['repeats']}|\n"
(base / 'report.md').write_text(report)
(base / 'results.html').write_text('''<!doctype html><meta charset="utf-8">
<title>async_memtable_write benchmark</title>
<style>body{font:15px system-ui;margin:32px}select{margin:8px;padding:6px}
table{border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:right}
th{position:sticky;top:0;background:#eee}.good{background:#d9f2df}.bad{background:#f6dede}</style>
<h1>async_memtable_write</h1>
<p>Median ops/s, same-configuration native baseline = 1.0×. P99 is API latency (μs).
32 clients · 1M keys · 100B values · 10s/run · 3 repeats.</p>
<label>Workload <select id="workload"><option>readrandom</option><option>readrandomwriterandom</option><option>overwrite</option></select></label>
<label>WAL sync <select id="sync"><option value="false">false</option><option value="true">true</option></select></label>
<label>Concurrent <select id="concurrent"><option value="true">true</option><option value="false">false</option></select></label>
<table><thead><tr><th>Pipeline</th><th>Workers (0=native)</th><th>Lazy</th><th>ops/s</th><th>Speedup</th><th>P99 read</th><th>P99 write</th><th>CV</th><th>Runs</th></tr></thead><tbody id="rows"></tbody></table>
<script>const data=''' + json.dumps(summary) + ''';
function render(){
const val=id=>document.getElementById(id).value;
document.getElementById('rows').innerHTML=data.filter(r=>r.workload===val('workload')&&String(r.sync)===val('sync')&&String(r.concurrent)===val('concurrent')).map(r=>`<tr><td>${r.pipeline}</td><td>${r.workers}</td><td>${r.lazy??'—'}</td><td>${Math.round(r.median_ops).toLocaleString()}</td><td class="${r.speedup>1.05?'good':r.speedup<0.95?'bad':''}">${r.speedup.toFixed(3)}×</td><td>${r.p99_read??'—'}</td><td>${r.p99_write??'—'}</td><td>${(100*r.cv).toFixed(1)}%</td><td>${r.repeats}</td></tr>`).join('');}
document.querySelectorAll('select').forEach(s=>s.onchange=render);render();</script>''')
print(f'{len(summary)} combinations, {sum(r["repeats"] for r in summary)} runs')
