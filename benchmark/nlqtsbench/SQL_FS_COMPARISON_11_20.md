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

# SQL and filesystem comparison on NLQTSBench 11–20

This single-run validation compares the SQL arm with the previously recorded
`typed-filtered-stats` filesystem arm on the same ten source questions,
imported IoTDB databases, `deepseek-flash` model, DSH metrics adapter, and
120-second task timeout. Both arms expose exactly one controlled read-only tool.
The SQL arm permits one tool call and received a task-independent IoTDB TABLE
dialect system-prompt overlay derived from the IoTDB-TimeSeek skill set. The
overlay records syntax verified on the experiment endpoint for qualified table
names, epoch-millisecond predicates, `MIN`, `MAX`, `AVG`, exact `PERCENTILE`,
range, and `ROUND`.

The full skill source snapshot is stored on the server at
`/data_01/iotdb-fs-exp/config/iotdb_skills`. The injected overlay is
`benchmark/llm-sql-fs-comparison/dsh-iotdb-table-sql.patch.yml`, with SHA-256
`b4cbd6bfe05dd4f58fd7471926aa97ee3daa05637495c59cbf98c1e4de15217d`.
It is appended only for SQL trials. Keeping the DSH skill loader disabled means
the model-facing tool set remains exactly `iotdb_sql`; the skill snapshot does
not provide file access or a second tool.

Both arms answered all ten tasks correctly with one successful tool call per
task and no retries, tool-policy violations, validation errors, CLI errors, or
timeouts.

| Metric | SQL + IoTDB skill | FS filtered stats | SQL change from FS |
| --- | ---: | ---: | ---: |
| Correct | 10/10 | 10/10 | 0 tasks |
| Completed | 10/10 | 10/10 | 0 tasks |
| Tool calls | 10 | 10 | 0 |
| Tool errors | 0 | 0 | 0 |
| Total wall time | 49.26 s | 83.36 s | -40.9% |
| Median wall time | 4.89 s | 7.12 s | -31.4% |
| Total model API time | 28.02 s | 45.15 s | -37.9% |
| Median model API time | 2.74 s | 4.39 s | -37.6% |
| Total tool time | 10.40 s | 15.71 s | -33.8% |
| Median tool time | 1.05 s | 1.55 s | -32.5% |
| Total CLI time | 9.58 s | 14.90 s | -35.7% |
| Median CLI time | 0.97 s | 1.47 s | -34.2% |
| Total tokens | 28,211 | 41,555 | -32.1% |
| Median tokens | 2,859 | 4,118.5 | -30.6% |
| Input tokens | 8,966 | 14,297 | -37.3% |
| Cache-read tokens | 14,592 | 20,096 | -27.4% |
| Output tokens | 4,653 | 7,162 | -35.0% |
| Reasoning tokens | 3,703 | 5,389 | -31.3% |
| Cache-hit model requests | 19/20 | 20/20 | -1 request |
| Weighted input-cache ratio | 61.9% | 58.4% | +3.5 points |

The SQL arm was faster and used fewer tokens on every task. Its paired
geometric wall-time ratio was 0.628 and its paired geometric token ratio was
0.678. The filesystem arm's question 19 contains a known 10.56-second DSH
process-exit delay after `turn/end`; after excluding that task from both arms,
SQL total wall time remains 34.2% lower. The robust median comparison therefore
supports the same direction without depending on the outlier.

The first SQL request incurred the only cache miss because the dialect overlay
created a new prompt prefix. All later SQL requests and all FS requests reported
cache hits. The aggregate SQL advantage therefore was not produced by a more
favorable request-level cache-hit count.

| # | Operation | SQL wall | FS wall | SQL CLI | FS CLI | SQL tokens | FS tokens |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 11 | max | 5.24 s | 12.02 s | 1.02 s | 1.52 s | 2,986 | 5,265 |
| 12 | median | 5.04 s | 6.69 s | 0.97 s | 1.47 s | 2,972 | 4,043 |
| 13 | min | 4.89 s | 7.09 s | 0.97 s | 1.42 s | 2,818 | 4,194 |
| 14 | min | 4.73 s | 7.65 s | 0.92 s | 1.27 s | 2,615 | 4,588 |
| 15 | range | 4.33 s | 5.99 s | 0.97 s | 1.47 s | 2,505 | 3,563 |
| 16 | range | 4.88 s | 5.94 s | 0.97 s | 1.42 s | 2,900 | 3,924 |
| 17 | max | 4.03 s | 5.64 s | 0.97 s | 1.77 s | 2,180 | 3,397 |
| 18 | avg | 5.74 s | 7.40 s | 0.97 s | 1.52 s | 3,353 | 4,203 |
| 19 | avg | 6.09 s | 17.80 s | 0.92 s | 1.57 s | 3,360 | 4,502 |
| 20 | avg | 4.28 s | 7.15 s | 0.92 s | 1.47 s | 2,522 | 3,876 |

All SQL calls used a fully qualified `<database>.raw_data` table, a quoted
`channel_id`, and half-open UTC epoch ranges. They performed the requested
aggregation and three-decimal rounding inside IoTDB. The model did not probe
schema or alternative function names. Every recorded request header exposed
only `iotdb_sql`.

Two effects explain most of the difference. First, the dialect overlay gives
the model one verified expression for each operation, reducing deliberation and
epoch-related self-correction. Second, SQL executes filtering and aggregation
in IoTDB, whereas the current FS `stats` implementation scans visible rows and
aggregates in the CLI process. The approximately 0.50-second difference in
median CLI time is consistent with that implementation difference, although
this small fixed dataset does not establish scaling behavior.

This run does not isolate the causal effect of the dialect overlay because
there is no SQL 11–20 no-skill control. It compares the intended operational
baselines: SQL with the required IoTDB dialect knowledge and FS with its typed
interface contract. Questions 11–20 are all `Global Aggregation` over a single
`DOUBLE` field, so the result should not be generalized to later benchmark
families until those slices are run.

Server artifacts:

- SQL: `/data_01/iotdb-fs-exp/results/nlqts-dsh-sql-11-20-iotdb-skill-v1`
- FS: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-11-20-typed-filtered-stats-v1`
