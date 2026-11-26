#!/usr/bin/env python3
"""Run inside the constrained container; do no monitoring during timed work."""
import csv, hashlib, itertools, json, random, re, shutil, subprocess, sys, time
from pathlib import Path
B=Path('/home/lb/rocksdb-bench-0a4721')
R=Path(sys.argv[1]); R.mkdir(exist_ok=False)
C=Path('/sys/fs/cgroup')
assert (C/'memory/memory.limit_in_bytes').read_text().strip()==str(32*2**30)
assert (C/'memory/memory.memsw.limit_in_bytes').read_text().strip()==str(32*2**30)
assert (C/'cpu/cpu.cfs_quota_us').read_text().strip()=='1600000'
def run(cmd, log):
    cmd=list(map(str,cmd)); log.with_suffix('.command.json').write_text(json.dumps(cmd,indent=2))
    with log.open('w') as f:
        start=time.monotonic()
        subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=300)
    return time.monotonic()-start
def ticker(t,n):
    v=re.findall(re.escape('rocksdb.'+n)+r' COUNT : (\d+)',t)
    return int(v[-1]) if v else None
def cg():
    return {f:(C/f).read_text().strip() for f in ('memory/memory.limit_in_bytes',
      'memory/memory.memsw.limit_in_bytes','memory/memory.failcnt','memory/memory.oom_control',
      'memory/memory.stat','cpu/cpu.cfs_quota_us','cpu/cpu.cfs_period_us','cpuset/cpuset.cpus')}
env={'container':'lb_rocksdb_16u','started_at':time.strftime('%FT%T%z'),'cgroup_before':cg(),'builds':{}}
for name in ('parent','commit'):
    d=B/('build-'+name); s=B/('src-'+name)
    assert not subprocess.check_output(['git','-C',str(s),'status','--porcelain'],text=True).strip()
    env['builds'][name]={'sha':subprocess.check_output(['git','-C',str(s),'rev-parse','HEAD'],text=True).strip(),
      'sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in (d/'db_bench',d/'librocksdb.so.6.26.1')},
      'flags':(d/'CMakeFiles/db_bench.dir/flags.make').read_text()}
    run(['ldd',d/'db_bench'],R/('ldd-'+name+'.log'))
(R/'environment.json').write_text(json.dumps(env,indent=2))
run(['findmnt','-T',R],R/'filesystem.log')
run(['lscpu'],R/'cpu.log')
common=['--num=600000','--writes=600000','--reads=600000','--threads=16','--batch_size=1','--seed=1001',
 '--key_size=16','--value_size=1024','--compression_type=none','--write_buffer_size=67108864',
 '--max_write_buffer_number=4','--target_file_size_base=67108864','--max_bytes_for_level_base=268435456',
 '--max_background_jobs=2','--level0_file_num_compaction_trigger=4','--disable_auto_compactions=0',
 '--disable_wal=0','--use_fsync=0','--bytes_per_sync=0','--wal_bytes_per_sync=0','--histogram=1',
 '--statistics=1','--stats_interval_seconds=0','--options_file=','--readwritepercent=50',
 '--ops_between_duration_checks=100','--use_direct_io_for_flush_and_compaction=0',
 '--use_direct_reads=0','--enable_pipelined_write=0']
seed=R/'seed'
run([B/'build-parent/db_bench']+common+['--benchmarks=fillseq,compactall','--threads=1',
 '--duration=0','--sync=0','--db='+str(seed),'--wal_dir='+str(seed)],R/'seed.log')
rows=[]
scenes=list(itertools.product(('write','mixed'),(0,1),(0,1)));random.Random(1001).shuffle(scenes)
for work,sync,cmw in scenes:
    bench='fillrandom' if work=='write' else 'readrandomwriterandom'
    for rnd in range(4):
        groups=['A0','A1','B']; shift=rnd%3; groups=groups[shift:]+groups[:shift]
        for g in groups:
            case=R/f'{work}-sync{sync}-cmw{cmw}'/f'r{rnd}-{g}';case.mkdir(parents=True);db=case/'db'
            if work=='mixed':shutil.copytree(seed,db)
            build=B/('build-parent' if g=='A0' else 'build-commit')
            flags=common+[f'--benchmarks={bench},compactall,stats',f'--sync={sync}',
              f'--allow_concurrent_memtable_write={cmw}',f'--duration={2 if rnd==0 else 5}',
              f'--use_existing_db={int(work=="mixed")}','--db='+str(db),'--wal_dir='+str(db)]
            if g!='A0':flags.append(f'--enable_zero_range_preallocation={int(g=="B")}')
            wall=run(['/usr/bin/time','-v','-o',case/'time.txt',build/'db_bench']+flags,case/'bench.log')
            t=(case/'bench.log').read_text()
            assert not re.search(r'(?:get|put|write) error:|CompactRange failed:',t),case
            m=re.search(r'^'+bench+r'\s*:\s*([\d.]+) micros/op (\d+) ops/sec',t,re.M);assert m,case
            hist={m[1]:{'count':int(m[2]),'p50_us':float(m[3]),'p99_us':float(m[4]),'p999_us':float(m[5])}
              for m in re.finditer(r'Microseconds per (\w+):\nCount: (\d+).*?'
                r'Percentiles: P50: ([\d.]+).*?P99: ([\d.]+) P99.9: ([\d.]+)',t,re.S)}
            assert 'write' in hist,(case,hist)
            ops=sum(v['count'] for v in hist.values())
            counters={n:ticker(t,n) for n in ('zero.range.preallocation.calls','zero.range.preallocation.wal.calls',
              'zero.range.preallocation.flush.calls','zero.range.preallocation.compaction.calls',
              'zero.range.preallocation.fallbacks','zero.range.preallocation.failures','preallocation.truncate.failures','stall.micros')}
            for n in ('zero.range.preallocation.fallbacks','zero.range.preallocation.failures','preallocation.truncate.failures'):
                assert counters[n] in (0,None),(case,n)
            if g=='B':
                assert all(counters['zero.range.preallocation.'+n]>0 for n in ('calls','wal.calls','flush.calls','compaction.calls')),case
            if g=='A1':assert counters['zero.range.preallocation.calls']==0
            if g!='A0':assert re.findall(r'db_close_micros=\d+ status=(.*)',t)[-1]=='OK'
            options='\n'.join(p.read_text() for p in db.glob('OPTIONS-*'));(case/'OPTIONS.txt').write_text(options)
            for k,v in (('allow_concurrent_memtable_write',str(bool(cmw)).lower()),('recycle_log_file_num','0')):
                assert re.search(r'\b'+k+r'\s*=\s*'+v+r'\b',options),(case,k)
            run([build/'tools/sst_dump','--file='+str(db),'--command=check','--verify_checksum'],case/'checksum.log')
            run([build/'tools/ldb','--db='+str(db),'dump','--count_only'],case/'scan.log')
            keys=int(re.search(r'Keys in range: (\d+)',(case/'scan.log').read_text())[1])
            assert keys>0 and (work!='mixed' or keys==600000),(case,keys)
            row=dict(workload=work,sync=sync,concurrent=cmw,round=rnd,group=g,ops_sec=float(m[2]),
              operations=ops,process_seconds=wall,end_to_end_ops_sec=ops/wall,histograms=hist,
              scanned_keys=keys,counters=counters,case=str(case))
            rows.append(row);(R/'results.json').write_text(json.dumps(rows,indent=2))
            print(json.dumps({k:v for k,v in row.items() if k not in ('histograms','counters','case')}),flush=True)
            shutil.rmtree(db)
env['completed_at']=time.strftime('%FT%T%z');env['cgroup_after']=cg()
(R/'environment.json').write_text(json.dumps(env,indent=2))
with (R/'results.csv').open('w') as f:
    fields=[k for k in rows[0] if k not in ('histograms','counters')]
    w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
assert len(rows)==96
print('COMPLETE: 96 validated trials',flush=True)
