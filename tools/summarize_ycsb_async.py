#!/usr/bin/env python3
"""Summarize YCSB measurements without mixing in JNI probes or warmups."""
import csv
import argparse
import json
from pathlib import Path
import statistics as st
from collections import defaultdict

p = argparse.ArgumentParser()
p.add_argument('--out', default='benchmark_results/ycsb_async')
args = p.parse_args()
out = (Path(__file__).resolve().parents[1] / args.out).resolve()
manifest = json.loads((out/'manifest.json').read_text())
full = manifest['args'].get('full_suite', False)
repeats = manifest['args']['repeats']
expected = len(manifest['configs']) * 6 * repeats
groups = defaultdict(list)
for line in (out/'results.jsonl').read_text().splitlines():
    r = json.loads(line)
    groups[r['workload'], r['name']].append(r)
rows = []
for (work, name), samples in sorted(groups.items()):
    rates = [float(r['metrics']['OVERALL']['Throughput(ops/sec)']) for r in samples]
    row = dict(workload=work, name=name, repeats=len(samples), median_ops=st.median(rates),
               min_ops=min(rates), max_ops=max(rates), cv=st.stdev(rates)/st.mean(rates) if len(rates)>1 else None)
    for section in ('READ','UPDATE','INSERT','SCAN','READ-MODIFY-WRITE','CLEANUP'):
        for metric in ('Operations','AverageLatency(us)','99thPercentileLatency(us)'):
            values = [float(r['metrics'][section][metric]) for r in samples if metric in r['metrics'].get(section,{})]
            row[section+'_'+metric] = st.median(values) if values else None
    rows.append(row)
for r in rows:
    native = [x for x in rows if x['workload']==r['workload'] and x['name'].startswith('native_')]
    best = max(native, key=lambda x:x['median_ops']) if native else None
    same_pipeline = next((x for x in native if x['name']=='native_p0'), None)
    r['vs_best_native'] = r['median_ops']/best['median_ops'] if best else None
    r['vs_native_p0'] = r['median_ops']/same_pipeline['median_ops'] if same_pipeline else None
with (out/'summary.csv').open('w') as f:
    writer = csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator='\n')
    writer.writeheader(); writer.writerows(rows)
report = '# YCSB async_memtable_write 测试\n\n'
protocol = '每组完整1亿次操作、1次；表中为单次值' if full else f'每组最多{manifest["args"]["seconds"]}秒、{repeats}次；表中吞吐为样本中位数'
report += f'采集：{sum(len(x) for x in groups.values())}/{expected} 轮。{protocol}。预检和预热不计入性能样本。\n\n'
report += '16 CPU容器、16前台线程、1亿条初始数据；使用原脚本common.sh的run_workload/warm_up函数、原A–F和dbsnappyrun.ini。详细配置差异见README.md。\n\n'
report += '## 各负载最佳配置\n\n|负载|最佳原生|原生ops/s|最佳async|async ops/s|吞吐变化|\n|---|---|---|---|---|---|\n'
for work in 'abcdef':
    native = [x for x in rows if x['workload']==work and x['name'].startswith('native_')]
    asynchronous = [x for x in rows if x['workload']==work and x['name'].startswith('async_')]
    if not native or not asynchronous: continue
    b=max(native,key=lambda r:r['median_ops']);a=max(asynchronous,key=lambda r:r['median_ops'])
    report+=f"|{work.upper()}|{b['name']}|{b['median_ops']:,.0f}|{a['name']}|{a['median_ops']:,.0f}|{a['median_ops']/b['median_ops']-1:+.1%}|\n"
report+='\n## 全部配置\n\nP99单位µs。vs native使用该负载最佳原生吞吐。\n\n'
report+='|负载|配置|ops/s|vs native|读P99|Update P99|Scan P99|CV|样本数|\n|---|---|---|---|---|---|---|---|---|\n'
fmt=lambda v:'—' if v is None else f'{v:.2f}'
for r in rows:
    ratio='—' if r['vs_best_native'] is None else f"{r['vs_best_native']:.3f}"
    cv = '—' if r['cv'] is None else f"{r['cv']:.1%}"
    report+=f"|{r['workload'].upper()}|{r['name']}|{r['median_ops']:,.0f}|{ratio}|{fmt(r['READ_99thPercentileLatency(us)'])}|{fmt(r['UPDATE_99thPercentileLatency(us)'])}|{fmt(r['SCAN_99thPercentileLatency(us)'])}|{cv}|{r['repeats']}|\n"
(out/'report.md').write_text(report)
print(f'{len(rows)} configurations, {sum(len(x) for x in groups.values())} samples')
