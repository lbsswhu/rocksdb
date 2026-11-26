import csv,hashlib,json,re,statistics,struct
from pathlib import Path
p=Path('/home/lb/ycsb-zero-range-20261125');rows=json.loads((p/'results.json').read_text());env=json.loads((p/'environment.json').read_text());v=json.loads((p/'jar-verification.json').read_text())
assert len(rows)==36 and len({(r['round'],r['group'],r['workload']) for r in rows})==36
jar=Path(v['runtime_jar']);assert hashlib.sha256(jar.read_bytes()).hexdigest()==v['jar_sha256']
native=(p/'jni-aarch64.so').read_bytes();assert hashlib.sha256(native).hexdigest()==v['native_sha256']
assert struct.unpack('<H',native[18:20])[0]==183
for name in ('rocksdbjni-6.26.1.jar','rocksdbjni-6.26.1-linux64.jar'):
 f=Path('/home/lb/.m2/repository/org/rocksdb/rocksdbjni/6.26.1')/name
 assert hashlib.sha256(f.read_bytes()).hexdigest()==v['jar_sha256']
 assert hashlib.sha1(f.read_bytes()).hexdigest()==f.with_suffix('.jar.sha1').read_text().strip()
for r in rows:
 text=Path(r['log']).read_text();assert 'Maximum time elapsed. Requesting stop' in text
 assert not re.search(r'Return=(?!OK\b)[^,\n]+, [1-9]\d*|Exception in thread|Error initializing datastore|UnsatisfiedLinkError|\[ERROR\]',text)
 assert 118000<=r['metrics']['OVERALL.RunTime(ms)']<=135000,r
 cmd=json.loads(Path(r['log']).with_suffix('.command.json').read_text());assert 'run_workload '+r['workload']+' 16' in cmd[-1]
 opts=Path(r['log']).with_suffix('.OPTIONS').read_text()
 assert re.search(r'enable_zero_range_preallocation\s*=\s*'+('true' if r['group']=='B' else 'false'),opts)
 assert re.search(r'allow_concurrent_memtable_write\s*=\s*true',opts)
for key in ('memory/memory.failcnt','memory/memory.oom_control'):
 assert env['cgroup_before'][key]==env['cgroup_after'][key]
assert re.search(r'^total_swap 0$',env['cgroup_after']['memory/memory.stat'],re.M)
v.update(native_elf_machine='AArch64 (183)',source_tree_verified_clean=True,source_commit_verified_after_run='0a47210279850a86ee97e8fc1dae846d4a110db3',embedded_git_sha='',provenance_note='The native git SHA stamp is empty; provenance is established by the clean target-commit source tree, recorded build command, jar/native SHA256 and runtime loading verification.')
(p/'jar-verification.json').write_text(json.dumps(v,indent=2))
flat=[]
for r in rows:
 d={k:x for k,x in r.items() if k!='metrics'};d.update(r['metrics']);flat.append(d)
with (p/'results.csv').open('w') as f:
 fields=sorted(set().union(*(r.keys() for r in flat)));w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(flat)
text=(p/'summary.md').read_text()
text+='\n## 结论\n\n'
text+='本轮中位数：B +3.03%、E +2.23%；A -1.37%、D -0.49%、F -1.99%，只读 C +0.30%。未观察到普遍提升。E 三轮均为正（+0.61% 至 +3.66%），B 有一轮接近零；A/F 的回退也应纳入取舍。三轮样本未用于统计显著性检验，配对变化范围不是置信区间。\n\n'
text+='## 操作延迟 P99\n\n以下为三轮各自 P99 的中位数，非合并直方图；负的变化表示延迟降低。\n\n'
text+='| workload | 操作 | 关闭 P99 us | 开启 P99 us | 变化 |\n|---|---|---:|---:|---:|\n'
latency=[]
for work in ['workload'+c for c in 'abcdef']:
 d={g:[r for r in rows if r['group']==g and r['workload']==work] for g in ('A1','B')}
 keys=sorted(set.intersection(*(set(r['metrics']) for r in d['A1']+d['B'])))
 for key in keys:
  if not key.endswith('.99thPercentileLatency(us)') or key.startswith('CLEANUP.'):continue
  m={g:statistics.median(r['metrics'][key] for r in rs) for g,rs in d.items()};delta=(m['B']/m['A1']-1)*100
  text+=f"| {work} | {key.split('.')[0]} | {m['A1']:.0f} | {m['B']:.0f} | {delta:+.2f}% |\n"
  latency.append(dict(workload=work,operation=key.split('.')[0],p99_us=m,delta_percent=delta))
text+='\n## 校验与复现\n\n'
text+=f"测试时间：{env['started_at']} 至 {env['completed_at']}。36 项正式负载及 6 次预热均成功；未发生非 OK 操作返回值或异常，未发生 OOM、内存 failcnt 增长或 swap 使用。\n\n"
text+='计时期间不运行编译、搜索、磁盘扫描、性能采样器或并行基准。低频进度读取绑定在宿主机 CPU16，位于测试 cpuset 0-15 之外；全部日志解析在单项计时结束后进行。未强制隔离宿主机其他进程。\n\n'
text+='原有 Maven artifact 使用 Java 11 class version 55，容器 Java 8 无法编译；现已备份至 previous-maven-artifact，并将 Maven 中带/不带 linux64 classifier 的 jar 及 SHA1 替换为重新构建的 Java 8 jar。原 YCSB 目录的历史 target 产物未用于本轮。运行使用隔离分发目录 runtime，强制空 java.library.path 以从 jar 提取 native，避免加载系统残留 JNI。\n\n'
text+=f"实际 jar SHA256：`{v['jar_sha256']}`；native SHA256：`{v['native_sha256']}`。ELF 为 AArch64，Java class major=52；运行时 native 映射哈希与 jar 内内容一致。native 的 git SHA 字符串为空，来源由目标提交的干净源码、构建命令及这些哈希记录确认。\n\n"
text+='预检中关闭组 ZERO_RANGE=0；开启组 WAL/flush/compaction 调用分别为 2/2/1，fallback/failure 均为 0。正式阶段使用同一 jar，逐项核对 OPTIONS 的开关与 allow_concurrent_memtable_write=true。\n\n'
text+='只读预热用原 warm_up 的 8 线程；正式负载用原 run_workload 的 16 线程。recordcount/operationcount 保留 100000000，并增加 maxexecutiontime=120。沿用原脚本函数和配置，跳过 setup_envir 中对固定数据目录的删除及宿主机 drop_caches；数据恢复由控制器在独立目录完成。此轮没有重新执行 1 亿记录 load，使用已有 117 GiB 备份库。\n\n'
text+='`run_matrix.py` 是本次控制器，`timed-common.sh` 是限时适配，`original-scripts` 保留原脚本和配置；`ycsb-src` 保存用于 Maven 构建的源码副本。复跑需使用新的结果/数据目录并相应调整控制器中的 R、D，避免覆盖本次结果。\n\n'
text+='完整逐项指标见 results.json/results.csv；jar 校验见 jar-verification.json、probe-A1.log、probe-B.log、ycsb-jar-smoke.log；Maven 构建与测试见 maven-correct-jar.log；每项原始命令与输出在 r*-A1/r*-B 下。\n'
(p/'summary.md').write_text(text)
(p/'latency-summary.json').write_text(json.dumps(latency,indent=2))
(p/'audit.json').write_text(json.dumps(dict(formal_trials=36,warmups=6,non_ok_operations=0,jar_unchanged=True,native_arch='aarch64',memory_failcnt_increase=0,oom_kills=0,swap_bytes=0,status='passed'),indent=2))
print(json.dumps(json.loads((p/'aggregate.json').read_text()),indent=2));print('audit passed')
