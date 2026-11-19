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

# Filesystem validation on NLQTSBench 11–20

This observation run extends the `typed-filtered-stats` filesystem arm from
source questions 1–10 to questions 11–20. It uses the same DeepSeek Harness
configuration, `deepseek-flash` model, controlled `iotdb_fs` tool, imported
IoTDB databases, prompt contract, and 120-second task timeout. The deployed CLI
also contains the statistics type and numeric-boundary fixes. Each task was run
once, so the timing measurements are validation signals rather than
publication-ready estimates.

All ten tasks completed and matched the expected answer. Every task used one
successful `stats` call. There were no retries, tool-policy violations,
validation rejections, CLI errors, or truncated tool outputs.

| Metric | Questions 1–10 | Questions 11–20 | Change |
| --- | ---: | ---: | ---: |
| Correct | 10/10 | 10/10 | 0 tasks |
| Completed | 10/10 | 10/10 | 0 tasks |
| Tool calls | 11 | 10 | -9.1% |
| Tool errors | 0 | 0 | 0 |
| Total wall time | 75.02 s | 83.36 s | +11.1% |
| Median wall time | 5.97 s | 7.12 s | +19.4% |
| Total model API time | 47.85 s | 45.15 s | -5.6% |
| Median model API time | 3.34 s | 4.39 s | +31.5% |
| Total CLI time | 14.60 s | 14.90 s | +2.1% |
| Median CLI time | 1.44 s | 1.47 s | +1.7% |
| Total tokens | 39,575 | 41,555 | +5.0% |
| Median tokens | 3,772 | 4,118.5 | +9.2% |
| Input tokens | 13,824 | 14,297 | +3.4% |
| Cache-read tokens | 19,968 | 20,096 | +0.6% |
| Output tokens | 5,783 | 7,162 | +23.8% |
| Reasoning tokens | 4,001 | 5,389 | +34.7% |
| Weighted input-cache ratio | 59.1% | 58.4% | -0.7 points |

The CLI cost is stable: its median changed by only 25 ms, and its per-task
range was 1.27–1.77 seconds. The higher median wall time and token count come
primarily from model-side reasoning. The model manually converted calendar
ranges into epoch milliseconds in every date-filtered question. Question 11
used 1,107 reasoning tokens after correcting its own epoch arithmetic, and
question 14 used 777 reasoning tokens. This suggests that a controlled UTC date
representation or deterministic date-to-epoch conversion would reduce cost and
risk without exposing raw SQL or another general-purpose tool.

| # | Operation | Answer | Correct | Calls | Wall | CLI | Tokens |
| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: |
| 11 | max | 0.772 | yes | 1 | 12.02 s | 1.52 s | 5,265 |
| 12 | median | 84.300 | yes | 1 | 6.69 s | 1.47 s | 4,043 |
| 13 | min | 4.050 | yes | 1 | 7.09 s | 1.42 s | 4,194 |
| 14 | min | -23.986 | yes | 1 | 7.65 s | 1.27 s | 4,588 |
| 15 | range | 1.000 | yes | 1 | 5.99 s | 1.47 s | 3,563 |
| 16 | range | 0.989 | yes | 1 | 5.94 s | 1.42 s | 3,924 |
| 17 | max | 3.610 | yes | 1 | 5.64 s | 1.77 s | 3,397 |
| 18 | avg | 0.062 | yes | 1 | 7.40 s | 1.52 s | 4,203 |
| 19 | avg | 0.795 | yes | 1 | 17.80 s | 1.57 s | 4,502 |
| 20 | avg | 0.594 | yes | 1 | 7.15 s | 1.47 s | 3,876 |

Question 19 is a runner-level wall-time outlier. Its DSH session lasted 6.33
seconds, including 4.60 seconds of model API time and 1.66 seconds of tool time,
but the subprocess exited 10.56 seconds after the recorded `turn/end` event.
The model answer and tool result had already completed. This delay therefore
does not indicate a slow FS scan; it should be investigated in DSH process
shutdown or benchmark subprocess cleanup before repeated timing runs.

Six tasks requested aggregates that were not required for the answer, such as
`count` alongside `avg` or both extrema for a single-extremum question. These
responses remained small (102–141 bytes), so this did not materially affect the
run. The two range tasks still required the model to subtract `min` from `max`.
Adding a controlled `range` aggregate would remove that remaining arithmetic.

Questions 1–20 all belong to the `Global Aggregation` family, and questions
11–20 all use a `DOUBLE` value field. The result therefore supports high FS
accuracy for filtered single-series aggregation, but it does not yet validate
other benchmark families or exercise the new exact `INT64` statistics path.
The next validation slices should retain the frozen ordering and explicitly
report when the task family or value type changes.

The generated runner summary has two presentation defects for single-arm runs:
it reports ten `paired_tasks` even though the SQL arm was absent, and its
generated `REPORT.md` text is hard-coded to questions 1–10. The underlying
trial records and per-arm metrics are unaffected.

Server artifacts:

- Run: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-11-20-typed-filtered-stats-v1`
- Call audit: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-11-20-typed-filtered-stats-v1/fs-call-audit-v1`
- Comparison run: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-filtered-stats-v1`
