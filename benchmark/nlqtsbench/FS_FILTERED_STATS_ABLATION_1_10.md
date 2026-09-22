<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Filtered stats ablation on NLQTSBench 1–10

This ablation compares the previous `typed-page-compact` filesystem arm with
`typed-filtered-stats`. Both arms use the same model, DSH configuration, fixed
IoTDB object path, compact page encoding, task prompts, imported databases, and
single run per task. The treatment adds typed time and TAG filters to `stats`
and exposes the aggregate allowlist `count`, `min`, `max`, `sum`, `avg`, and
`median`.

The filtered stats arm completed all ten questions correctly. Nine questions
used one `stats` call. Question 7 first requested `help` and then used one
`stats` call. No trial read raw rows, paginated, retried a failed tool call, or
received truncated output.

| Metric | Typed page compact | Typed filtered stats | Change |
| --- | ---: | ---: | ---: |
| Correct | 4/10 | 10/10 | +6 tasks |
| Completed | 10/10 | 10/10 | 0 |
| Tool calls | 167 | 11 | -93.4% |
| Tool errors | 9 | 0 | -100% |
| Total wall time | 756.49 s | 75.02 s | -90.1% |
| Median wall time | 69.77 s | 5.97 s | -91.5% |
| Total CLI time | 200.99 s | 14.60 s | -92.7% |
| Median CLI time | 14.17 s | 1.44 s | -89.8% |
| Total tokens | 1,645,740 | 39,575 | -97.6% |
| Median tokens | 148,297.5 | 3,772 | -97.5% |
| Input tokens | 315,761 | 13,824 | -95.6% |
| Output tokens | 110,139 | 5,783 | -94.7% |

| # | Operation | Compact correct | Filtered stats correct | Compact calls | Filtered stats calls | Compact wall | Filtered stats wall | Compact tokens | Filtered stats tokens |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | median | no | yes | 6 | 1 | 45.82 s | 5.84 s | 40,263 | 3,838 |
| 2 | min | yes | yes | 11 | 1 | 46.67 s | 5.34 s | 171,303 | 3,560 |
| 3 | range | yes | yes | 14 | 1 | 53.43 s | 6.09 s | 183,583 | 3,584 |
| 4 | avg | no | yes | 12 | 1 | 86.10 s | 5.79 s | 140,355 | 3,860 |
| 5 | range | yes | yes | 10 | 1 | 41.39 s | 8.30 s | 53,352 | 4,960 |
| 6 | median | no | yes | 23 | 1 | 120.12 s | 5.54 s | 127,139 | 3,393 |
| 7 | avg | no | yes | 13 | 2 | 88.81 s | 8.95 s | 241,018 | 5,475 |
| 8 | median | no | yes | 50 | 1 | 120.12 s | 6.64 s | 76,696 | 3,765 |
| 9 | min | yes | yes | 7 | 1 | 33.91 s | 16.79 s | 156,240 | 3,779 |
| 10 | max | no | yes | 21 | 1 | 120.12 s | 5.74 s | 455,791 | 3,361 |

The result supports the task-decomposition hypothesis for these single-query
aggregation tasks. Compact typed rows reduce representation cost per page, but
the agent still has to fetch many pages and perform arithmetic over them.
Filtered stats moves the deterministic scan and aggregation behind one typed FS
operation, which removes most tool turns and prevents manual median, average,
and extrema calculation errors.

The current implementation still scans the visible rows in the CLI process and
computes exact median client-side. It reduces agent and model work rather than
database scan work. A later optimization can push supported aggregates and
predicates into the IoTDB query plan while preserving this controlled FS
contract. These are single-run validation results; publication measurements
should repeat each condition and report dispersion.

Server artifacts:

- Treatment: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-filtered-stats-v1`
- Call audit: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-filtered-stats-v1/fs-call-audit-v1`
- Control: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-page-compact-v1`
