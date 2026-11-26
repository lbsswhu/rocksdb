#!/bin/bash
YCSB_HOME=/root/YCSB/
ROCKSDB_LOAD_CONFIG=/$YCSB_HOME/test_scripts/inc/configs/dbsnappyload.ini
ROCKSDB_RUN_CONFIG=/$YCSB_HOME/test_scripts/inc/configs/dbsnappyrun.ini
ROCKSDB_DATA=/data/lb/workload_data
ROCKSDB_BACKUP=/data/lb/workload_backup_data
LOGDIR=$YCSB_HOME/test_scripts/runlog

function setup_envir() {
  [ -d $ROCKSDB_DATA ] && rm -rf $ROCKSDB_DATA
  cp -rp --reflink=never $ROCKSDB_BACKUP $ROCKSDB_DATA
  [ ! -d $LOGDIR ] && mkdir -p $LOGDIR
  echo 3 >/proc/sys/vm/drop_caches
  echo 3 >/proc/sys/vm/drop_caches
}

function warm_up() {
  echo "预热开始====================================================="
  date
  cd $YCSB_HOME && ./bin/ycsb run rocksdb -s -P workloads/workloadc -p rocksdb.dir=$ROCKSDB_DATA -threads 8 -p rocksdb.optionsfile=$ROCKSDB_RUN_CONFIG -p rocksdb.cfs=default -p table=default -p "recordcount=100000000" -p "operationcount=100000000"

  echo "========================"
  echo "数据预热完毕！"
  echo "========================"

  date
}

function run_workload() {
  workload=$1
  threads=$2
  logsuffix=`date +%Y%m%d_%H-%M-%S`
  # [ ! -d $LOGDIR/${workload} ] && mkdir -p $LOGDIR/${workload}
  cd $YCSB_HOME && ./bin/ycsb run rocksdb -s -P workloads/$workload -p rocksdb.dir=$ROCKSDB_DATA -threads ${threads} -p rocksdb.optionsfile=$ROCKSDB_RUN_CONFIG -p rocksdb.cfs=default -p table=default -p "recordcount=100000000" -p "operationcount=100000000"|tee $LOGDIR/${workload}_${threads}_${logsuffix}.log
}
