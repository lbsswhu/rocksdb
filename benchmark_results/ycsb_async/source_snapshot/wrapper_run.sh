#!/bin/bash
set -eo pipefail
source /home/lb/YCSB/test_scripts/inc/common.sh
YCSB_HOME="$1"
ROCKSDB_DATA="$2"
ROCKSDB_RUN_CONFIG="$3"
LOGDIR="$4"
if [ "$5" = warm ]; then warm_up; else run_workload "$5" 16; fi
