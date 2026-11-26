#!/usr/bin/env python3
import csv,hashlib,json,re,shutil,subprocess,time,traceback
from pathlib import Path
R=Path('/home/lb/ycsb-zero-range-full-20261126')
OLD=Path('/home/lb/ycsb-zero-range-20261125')
DATA=Path('/data/lb/ycsb-zero-range-full-20261126')
SEED=Path('/data/lb/workload_backup_data')
EXPECTED=100000000
C=Path('/sys/fs/cgroup')
def snapshot():
 return {f:(C/f).read_text() for f in ('memory/memory.limit_in_bytes','memory/memory.memsw.limit_in_bytes','memory/memory.failcnt','memory/memory.oom_control','memory/memory.stat','cpu/cpu.cfs_quota_us','cpu/cpu.cfs_period_us','cpuset/cpuset.cpus')}
def write_json(path,data):
 temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(json.dumps(data,indent=2));temp.replace(path)
def progress(**kw):
 write_json(R/'progress.json',dict(updated_at=time.strftime('%FT%T%z'),expected_operations=EXPECTED,**kw))
def run(cmd,log):
 cmd=list(map(str,cmd));write_json(log.with_suffix('.command.json'),cmd)
 with log.open('w') as stream:
  begin=time.monotonic()
  # Blocking wait: no sampling/analysis/competing tasks during the workload.
  subprocess.run(cmd,stdout=stream,stderr=subprocess.STDOUT,check=True)
 return time.monotonic()-begin
def parse(log,workload):
 text=log.read_text()
 assert not re.search(r'Return=(?!OK\b)[^,\n]+, [1-9]\d*|Exception in thread|Error initializing datastore|UnsatisfiedLinkError|\[ERROR\]',text),log
 assert 'Maximum time elapsed. Requesting stop' not in text,log
 metrics={a+'.'+b:float(v) for a,b,v in re.findall(r'^\[([^]]+)\], ([^,]+), ([\d.]+)$',text,re.M)}
 ops_sec=metrics['OVERALL.Throughput(ops/sec)'];actual=round(ops_sec*metrics['OVERALL.RunTime(ms)']/1000)
 assert actual==EXPECTED,(log,actual)
 names={'workloada':['READ','UPDATE'],'workloadb':['READ','UPDATE'],'workloadc':['READ'],'workloadd':['READ','INSERT'],'workloade':['SCAN','INSERT'],'workloadf':['READ']}[workload]
 assert sum(metrics.get(x+'.Operations',0) for x in names)==EXPECTED,(log,'operation histogram counts')
 return ops_sec,actual,metrics

def main():
 before=snapshot()
 assert int(before['memory/memory.limit_in_bytes'])==32*2**30
 assert before['memory/memory.limit_in_bytes']==before['memory/memory.memsw.limit_in_bytes']
 assert int(before['cpu/cpu.cfs_quota_us'])==1600000
 assert int(before['cpu/cpu.cfs_period_us'])==100000
 assert before['cpuset/cpuset.cpus'].strip()=='0-15'
 manifest=json.loads((R/'jar-verification.json').read_text());jar=Path(manifest['runtime_jar'])
 assert hashlib.sha256(jar.read_bytes()).hexdigest()==manifest['jar_sha256']
 assert not DATA.exists(),'Refusing to overwrite existing test data'
 assert not (R/'results.json').exists(),'Refusing to overwrite results'
 DATA.mkdir(parents=True)
 env=dict(started_at=time.strftime('%FT%T%z'),cgroup_before=before,jar=manifest,rounds=1,
   operations_per_workload=EXPECTED,warmup_operations=EXPECTED,maxexecutiontime=0,
   method='A1/B same target JNI jar, off/on; independent seed copies; full 100M read-only warmup; full A-F sequential, no global cache drop, no extra compactall')
 write_json(R/'environment.json',env);rows=[];warmups=[]
 run(['findmnt','-T',str(DATA)],R/'filesystem.log')
 for group in ('A1','B'):
  case=R/group;case.mkdir();db=DATA/group
  progress(status='running',group=group,phase='restore',log=str(case/'restore.log'))
  run(['cp','-rp','--reflink=never',SEED,db],case/'restore.log')
  prefix='source '+str(R/'full-common.sh')+'\n'+f'YCSB_HOME={OLD}/runtime\nROCKSDB_DATA={db}\nROCKSDB_RUN_CONFIG={R}/{group}.ini\nLOGDIR={case}\n'+f'export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.472.b08-5.oe2403sp3.aarch64\nexport LD_LIBRARY_PATH=\n'
  progress(status='running',group=group,phase='warmup',log=str(case/'warmup.log'))
  wall=run(['bash','-o','pipefail','-c',prefix+'warm_up'],case/'warmup.log')
  rate,actual,metrics=parse(case/'warmup.log','workloadc')
  warmups.append(dict(group=group,actual_operations=actual,ops_sec=rate,process_seconds=wall));write_json(R/'warmups.json',warmups)
  for work in ['workload'+x for x in 'abcdef']:
   progress(status='running',group=group,phase=work,log=str(case/(work+'.log')),completed_trials=len(rows))
   wall=run(['bash','-o','pipefail','-c',prefix+f'run_workload {work} 16'],case/(work+'.log'))
   rate,actual,metrics=parse(case/(work+'.log'),work)
   options='\n'.join(f.read_text() for f in db.glob('OPTIONS-*'))
   assert re.search(r'enable_zero_range_preallocation\s*=\s*'+('true' if group=='B' else 'false'),options)
   assert re.search(r'allow_concurrent_memtable_write\s*=\s*true',options)
   (case/(work+'.OPTIONS')).write_text(options)
   row=dict(group=group,workload=work,actual_operations=actual,ops_sec=rate,process_seconds=wall,metrics=metrics,log=str(case/(work+'.log')))
   rows.append(row);write_json(R/'results.json',rows)
   print(json.dumps({k:v for k,v in row.items() if k!='metrics'}),flush=True)
  shutil.copyfile(db/'LOG',case/'RocksDB.LOG')
 assert len(rows)==12 and len(warmups)==2
 assert hashlib.sha256(jar.read_bytes()).hexdigest()==manifest['jar_sha256']
 env.update(completed_at=time.strftime('%FT%T%z'),cgroup_after=snapshot());write_json(R/'environment.json',env)
 flat=[]
 for row in rows:
  item={k:v for k,v in row.items() if k!='metrics'};item.update(row['metrics']);flat.append(item)
 with (R/'results.csv').open('w') as f:
  writer=csv.DictWriter(f,fieldnames=sorted(set().union(*(set(x) for x in flat))));writer.writeheader();writer.writerows(flat)
 lines=['# YCSB 完整 1 亿操作对照','',
   '目标提交 0a47210279850a86ee97e8fc1dae846d4a110db3，同一个已验证 JNI jar；A1 关闭 ZERO_RANGE，B 开启。',
   '16 CPU / 32 GiB / 无 swap；每组从同一 1 亿记录备份独立复制；先完成 1 亿次只读预热（8 线程），再依次完成 A-F 各 1 亿次操作（16 线程）。',
   '无执行时间上限，实际操作数逐项核对为 100000000；不额外执行 compactall，不清空宿主机缓存。使用原 common.sh 的 warm_up/run_workload，JVM heap 4–8 GiB。',
   '这是关闭/开启各一轮完整测试，不是三轮中位数，也不是父提交/目标提交的整个版本对比。WAL 开启、sync=false、concurrent memtable write=true。','',
   '| workload | 关闭 ops/s | 开启 ops/s | 开启/关闭 | 关闭/开启 RunTime 秒 |',
   '|---|---:|---:|---:|---:|']
 summary=[]
 for work in ['workload'+x for x in 'abcdef']:
  a,b=[next(x for x in rows if x['group']==g and x['workload']==work) for g in ('A1','B')]
  delta=100*(b['ops_sec']/a['ops_sec']-1)
  lines.append(f"| {work} | {a['ops_sec']:.1f} | {b['ops_sec']:.1f} | {delta:+.2f}% | {a['metrics']['OVERALL.RunTime(ms)']/1000:.3f}/{b['metrics']['OVERALL.RunTime(ms)']/1000:.3f} |")
  summary.append(dict(workload=work,off_ops_sec=a['ops_sec'],on_ops_sec=b['ops_sec'],delta_percent=delta))
 lines+=['','## P99 延迟','', '| workload | 操作 | 关闭 P99 us | 开启 P99 us | 变化 |','|---|---|---:|---:|---:|']
 for work in ['workload'+x for x in 'abcdef']:
  a,b=[next(x for x in rows if x['group']==g and x['workload']==work) for g in ('A1','B')]
  for key in sorted(set(a['metrics']) & set(b['metrics'])):
   if key.endswith('.99thPercentileLatency(us)') and not key.startswith('CLEANUP.'):
    av,bv=a['metrics'][key],b['metrics'][key]
    lines.append(f"| {work} | {key.split('.')[0]} | {av:.0f} | {bv:.0f} | {(bv/av-1)*100:+.2f}% |")
 lines+=['','每项数据已检查错误返回值、异常、实际操作数、OPTIONS 开关；jar SHA256 在执行前后复核。',
   '开关两组分别独立恢复备份，但组内 A-F 按原脚本继承前面的数据库状态；一轮结果不能描述轮间波动，后段差异也包含前序负载引起的状态差异。',
   'YCSB 吞吐采用其 OVERALL 指标；process_seconds 另含启动与脚本开销。完整原始指标见 results.json/results.csv，命令及日志在 A1、B 目录，环境见 environment.json。',
   '计时期间没有其他编译、扫描、分析或并行基准负载；低频进度读取在宿主机 CPU16（测试 cpuset 之外）进行。共享宿主机其他活动没有强制隔离。']
 (R/'summary.md').write_text('\n'.join(lines)+'\n');write_json(R/'aggregate.json',summary)
 write_json(R/'audit.json',dict(status='passed',formal_trials=12,warmups=2,expected_and_actual_operations_each=EXPECTED,jar_unchanged=True))
 progress(status='complete',completed_trials=12);print('COMPLETE',flush=True)
try:main()
except BaseException as exc:
 previous=json.loads((R/'progress.json').read_text()) if (R/'progress.json').exists() else {}
 progress(status='failed',previous=previous,error=repr(exc));traceback.print_exc();raise
