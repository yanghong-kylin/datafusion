#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# run tcph benchmark on local machine
  # testDatafusion
  # testBallistaWithExecutors with one executor
  # testBallistaWithExecutors with four executor

# Need two args
#  arg1: parquet data location
#  arg2: result file location

function schedulerCount() {
  scheduler_count=`ps | grep "ballista-scheduler" | grep -v grep | wc -l`
  echo "$scheduler_count"
}

function schedulerAliveCount() {
  scheduler_count=`ps | grep "target.*ballista-scheduler" | grep -v grep | wc -l`
  echo "$scheduler_count"
}

function executorCount() {
  executor_count=`ps | grep "ballista-executor" | grep -v grep | wc -l`
  echo "$executor_count"
}

function startScheduler() {
  total=$1
  scheduler_count=$(schedulerCount)
      while(($scheduler_count<$total))
      do
        nohup cargo run  --release --bin ballista-scheduler > /tmp/ballista_scheduler.out 2>&1 & echo $! > /tmp/ballista_scheduler.pid
        sleep 2s
        scheduler_count=$(schedulerCount)
        echo "$scheduler_count"
      done
  echo "start scheduler success!"
}

function startExecutorWithNumber() {
  total=$1

  flag=$(schedulerAliveCount)

  #wait scheduler startup
  while (($flag<1))
  do
    sleep 10s
    flag=$(schedulerAliveCount)
  done

  executor_count=$(executorCount)
  start_port=50020
  while (($executor_count<$total))
  do
  ((start_port=$start_port+1))
  echo "bind_port: $start_port"
  nohup cargo run  --release --bin ballista-executor -- --bind-port "$start_port" > /tmp/ballista_executor"$executor_count".out 2>&1 & echo $! > /tmp/ballista_executor"$executor_count".pid
  sleep 2s
  executor_count=$(executorCount)
  done
}

function testDatafusion() {
  echo "##### Start run datafusion"
  pushd ..
  for query in 1 3 5 6 7 10 12 13
  do
    RUST_LOG=INFO cargo run  --release --bin tpch -- benchmark datafusion --query  $query --path "$data_path" --format parquet
  done
  popd
}

function testBallistaWithExecutors() {
  executor_number=$1
  echo "##### Start run ballista $executor_number execurtor"
  pushd ../..
  startScheduler 1
  startExecutorWithNumber $executor_number
  popd

  pushd ..
  for query in 1 3 5 6 7 10 12 13
  do
    RUST_LOG=INFO cargo run  --release --bin tpch -- benchmark ballista --host localhost --port 50050  --query  $query --path "$data_path" --format parquet --debug
  done
  popd
}

function killAllProcess() {
  kill -9 $(ps -ef | grep "ballista-executor" | grep -v grep | awk '{print $2}')
  kill -9 $(ps -ef | grep "ballista-scheduler" | grep -v grep | awk '{print $2}')
}

# main function
  set -e

  if [ $# != 2 ] ; then
  echo "USAGE: arg0: parquet data path \n    arg1: output path"
  exit 1;
  fi
  data_path=$1
  out_file=$2

 if [ ! -f "$out_file" ] ; then
    touch $out_file
 else
   echo "File exits, please check and delete"
   exit 1;
 fi

  killAllProcess || { echo "Ballista Process not exit, kill cmd fail not mind"; }

  testDatafusion >> $out_file 2>&1
  testBallistaWithExecutors 1 >> $out_file 2>&1
  testBallistaWithExecutors 4 >> $out_file 2>&1

  summary_suffix=".summary"
  summary_file=$out_file$summary_suffix
  touch "$summary_file"
  cat "$out_file" | grep "### \|Query" >> "$summary_file"

  killAllProcess