#!/bin/bash
source inc/common.sh

function load() {
  # 用纯读来预热数据，让数据达到稳定状态
  echo "加载数据开始====================================================="
  date
  cd $YCSB_HOME && ./bin/ycsb load rocksdb -s -P workloads/workloada -p rocksdb.dir=$ROCKSDB_DATA -threads 5 -p rocksdb.optionsfile=$ROCKSDB_LOAD_CONFIG -p rocksdb.cfs=default -p table=default -p "recordcount=100000000" -p "operationcount=100000000"
  cd -
}


function warmup() {
  echo "预热开始====================================================="
  date

  LOG=/$ROCKSDB_DATA/LOG
  while grep -q "compaction_started" "$LOG"; do
      cd $YCSB_HOME && ./bin/ycsb run rocksdb -s -P workloads/workloadc -p rocksdb.dir=$ROCKSDB_DATA -threads 3 -p rocksdb.optionsfile=$ROCKSDB_RUN_CONFIG -p rocksdb.cfs=default -p table=default -p "recordcount=100000000" -p "operationcount=100000000"
  done
  echo "========================"
  echo "数据转储完毕！"
  echo "========================"
  date
}

[ -d $ROCKSDB_BACKUP ]  && rm -rf $ROCKSDB_BACKUP
load
warmup
cp -rp --reflink=never $ROCKSDB_DATA $ROCKSDB_BACKUP
