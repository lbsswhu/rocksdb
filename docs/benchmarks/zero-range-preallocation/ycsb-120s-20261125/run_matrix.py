#!/usr/bin/env python3
import csv,hashlib,json,os,re,shutil,statistics,subprocess,time
from pathlib import Path
R=Path('/home/lb/ycsb-zero-range-20261125');D=Path('/data/lb/ycsb-zero-range-20261125/workload_data')
S=Path('/data/lb/workload_backup_data');C=Path('/sys/fs/cgroup')
def cg():
 return {f:(C/f).read_text() for f in ('memory/memory.limit_in_bytes','memory/memory.memsw.limit_in_bytes','memory/memory.failcnt','memory/memory.oom_control','memory/memory.stat','cpu/cpu.cfs_quota_us','cpu/cpu.cfs_period_us','cpuset/cpuset.cpus')}
def run(cmd,log,cwd=None,timeout=1800):
 log.with_suffix('.command.json').write_text(json.dumps(cmd,indent=2))
 with log.open('w') as f:
  start=time.monotonic();subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,cwd=cwd,check=True,timeout=timeout)
 return time.monotonic()-start
stats=cg();assert int(stats['memory/memory.limit_in_bytes'])==32*2**30
assert stats['memory/memory.limit_in_bytes']==stats['memory/memory.memsw.limit_in_bytes']
assert int(stats['cpu/cpu.cfs_quota_us'])==1600000
manifest=json.loads((R/'jar-verification.json').read_text());jar=Path(manifest['runtime_jar'])
assert hashlib.sha256(jar.read_bytes()).hexdigest()==manifest['jar_sha256']
common=(R/'original-scripts/inc/common.sh').read_text().replace('"operationcount=100000000"','"operationcount=100000000" -p "maxexecutiontime=120" -jvm-args "-Xms4g -Xmx8g -XX:ActiveProcessorCount=16 -Djava.library.path='+str(R/'empty-native')+'"')
(R/'timed-common.sh').write_text(common)
env={'started_at':time.strftime('%FT%T%z'),'cgroup_before':stats,'jar':manifest,'method':'A1/B same target jar; original common.sh functions; independent seed per series; A-F sequential; 120s warmup and each workload; 3 measured rounds; no drop_caches'}
(R/'environment.json').write_text(json.dumps(env,indent=2));results=[]
def parse(log):
 text=log.read_text();m=re.search(r'^\[OVERALL\], Throughput\(ops/sec\), ([\d.]+)',text,re.M);assert m,log
 assert not re.search(r'Return=(?!OK\b)[^,\n]+, [1-9]\d*',text),(log,'non-OK operations')
 assert not re.search(r'Exception in thread|Error initializing datastore|UnsatisfiedLinkError|\[ERROR\]',text),log
 metrics={}
 for a,b,v in re.findall(r'^\[([^]]+)\], ([^,]+), ([\d.]+)$',text,re.M):metrics[a+'.'+b]=float(v)
 return float(m[1]),metrics
for rnd in range(1,4):
 for group in (('A1','B') if rnd%2 else ('B','A1')):
  case=R/f'r{rnd}-{group}';case.mkdir(exist_ok=False)
  (R/'progress.json').write_text(json.dumps(dict(round=rnd,group=group,phase='restoring seed')))
  # First copy was made and checked in preparation; all subsequent series copy afresh.
  if not (rnd==1 and group=='A1'):
   assert D.parent==Path('/data/lb/ycsb-zero-range-20261125');shutil.rmtree(D)
   run(['cp','-rp','--reflink=never',str(S),str(D)],case/'restore.log')
  prefix='source '+str(R/'timed-common.sh')+'\n'+f'YCSB_HOME={R}/runtime\nROCKSDB_DATA={D}\nROCKSDB_RUN_CONFIG={R}/{group}.ini\nLOGDIR={case}\n'+f'export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.472.b08-5.oe2403sp3.aarch64\nexport LD_LIBRARY_PATH=\n'
  (R/'progress.json').write_text(json.dumps(dict(round=rnd,group=group,phase='read-only warmup')))
  run(['bash','-o','pipefail','-c',prefix+'warm_up'],case/'warmup.log',timeout=900);parse(case/'warmup.log')
  for workload in ['workload'+x for x in 'abcdef']:
   (R/'progress.json').write_text(json.dumps(dict(round=rnd,group=group,phase=workload)))
   wall=run(['bash','-o','pipefail','-c',prefix+f'run_workload {workload} 16'],case/(workload+'.log'),timeout=900)
   ops,metrics=parse(case/(workload+'.log'))
   opts='\n'.join(p.read_text() for p in D.glob('OPTIONS-*'))
   assert re.search(r'enable_zero_range_preallocation\s*=\s*'+('true' if group=='B' else 'false'),opts)
   (case/(workload+'.OPTIONS')).write_text(opts)
   row=dict(round=rnd,group=group,workload=workload,ops_sec=ops,process_seconds=wall,metrics=metrics,log=str(case/(workload+'.log')))
   results.append(row);(R/'results.json').write_text(json.dumps(results,indent=2))
   print(json.dumps({k:v for k,v in row.items() if k!='metrics'}),flush=True)
  shutil.copyfile(D/'LOG',case/'RocksDB.LOG')
 env['last_completed_series']=str(case);(R/'environment.json').write_text(json.dumps(env,indent=2))
env['cgroup_after']=cg();env['completed_at']=time.strftime('%FT%T%z');(R/'environment.json').write_text(json.dumps(env,indent=2))
assert len(results)==36
lines=['# YCSB ZERO_RANGE A1/B 对照','', '目标提交 0a47210279850a86ee97e8fc1dae846d4a110db3，同一个经过验证的 JNI jar；A1 关闭 ZERO_RANGE，B 开启。',
 '16 CPU / 32 GiB / 无 swap；原始 1 亿记录备份，A→F 顺序；每组先只读预热 120 秒，每项正式负载 120 秒，三轮。',
 '每轮每组重新复制同一备份，组内保持原脚本的连续 A→F；未清空宿主机缓存。JVM heap 4–8 GiB。',
 '统计为三轮中位数；不是父提交与目标提交的整个版本对比。原 binding 为默认 WAL 开启、sync=false。','',
 '| workload | A1 ops/s | B ops/s | B/A1 | 三轮配对变化 |','|---|---:|---:|---:|---|']
summary=[]
for w in ['workload'+x for x in 'abcdef']:
 d={g:[x for x in results if x['group']==g and x['workload']==w] for g in ('A1','B')}
 med={g:statistics.median(x['ops_sec'] for x in v) for g,v in d.items()}
 delta=100*(med['B']/med['A1']-1);paired=[100*(b['ops_sec']/a['ops_sec']-1) for a,b in zip(d['A1'],d['B'])]
 lines.append(f"| {w} | {med['A1']:.1f} | {med['B']:.1f} | {delta:+.2f}% | "+' / '.join(f'{x:+.2f}%' for x in paired)+' |')
 summary.append(dict(workload=w,median_ops_sec=med,delta_percent=delta,paired_delta_percent=paired))
lines+=['','全部运行检查非 OK 返回值和异常；原始日志含 READ/UPDATE/INSERT/SCAN/READ-MODIFY-WRITE 的延迟分位数。',
 'YCSB Throughput 使用其 OVERALL 指标，包含该版本客户端线程内的 datastore init/cleanup；未额外强制全库 compactall。',
 'jar-verification.json 记录 jar/native SHA256、Java class 版本、来源提交；preflight 日志验证 native 映射哈希与 jar 内 native 一致及真实 ZERO_RANGE 调用。',
 '共享宿主机与操作系统缓存未完全隔离；每种 workload 顺序继承前面 workload 的状态，因此后段是整套脚本效果，不是独立 workload 的因果隔离。',
 '原脚本/配置快照见 original-scripts；限时适配见 timed-common.sh；运行命令见 *.command.json。']
(R/'summary.md').write_text('\n'.join(lines)+'\n');(R/'aggregate.json').write_text(json.dumps(summary,indent=2))
(R/'progress.json').write_text(json.dumps(dict(status='complete',trials=len(results))))
print('COMPLETE',flush=True)
