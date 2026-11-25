#!/bin/bash 
source inc/common.sh

threads=(16)

function run_tests() {
  workload=$1
  for thread in ${threads}; do 
    run_workload "$workload" $thread
    sleep 1
  done 
} 

setup_envir
warm_up
run_tests "workloada"
run_tests "workloadb"
run_tests "workloadc"
run_tests "workloadd"
run_tests "workloade"
run_tests "workloadf"
