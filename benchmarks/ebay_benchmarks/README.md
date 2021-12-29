<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# EBAY DataFusion and Ballista Benchmarks

## Benchmark derived from TPC-H

These benchmarks are derived from the [TPC-H][1] benchmark.

## Generating Test Data

TPC-H data can be generated using the `tpch-gen.sh` script, which creates a Docker image containing the TPC-DS data
generator.

```bash
./tpch-1g-oneFile-parquet-gen.sh
```

Data will be generated into the `data` subdirectory and will not be checked in because this directory has been added
to the `.gitignore` file.

## Running the DataFusion Benchmarks

```bash
 ./tpch-run.sh  data_abosulte_path  output_file_path
```
This will run
 ####testDatafusion
 ####testBallistaWithExecutors with one executor
 ####testBallistaWithExecutors with four executor

will generate scheduler and executor log file under `/tmp/ballista_scheduler.out` and `/tmp/ballista_executor.out`

##Expected output
The result of shell should produce the following output in `$output_file_path.summary`:
```
##### Start run ballista 1 execurtor
Query 1 iteration 0 took 837.1 ms
Query 1 iteration 1 took 835.7 ms
Query 1 iteration 2 took 836.3 ms
Query 1 avg time: 836.37 ms
Query 3 iteration 0 took 1576.8 ms
Query 3 iteration 1 took 1157.0 ms
Query 3 iteration 2 took 1460.2 ms
Query 3 avg time: 1398.04 ms
Query 5 iteration 0 took 1882.9 ms
Query 5 iteration 1 took 2906.0 ms
Query 5 iteration 2 took 2400.2 ms
Query 5 avg time: 2396.37 ms
```
