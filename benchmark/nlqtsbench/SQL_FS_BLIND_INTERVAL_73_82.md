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

# SQL and filesystem blind comparison on NLQTSBench 73–82

This run uses the first ten `Interval Discovery` questions, source ordinals
73–82. It is a materially harder follow-up to the `Global Aggregation` slice at
11–20. Every task asks for the longest contiguous run above a threshold and is
scored both by exact endpoint equality and by the benchmark's interval IoU.

Each DSH session received the question, assigned database, and ordinary
Text2SQL schema information: `raw_data(time TIMESTAMP, channel_id STRING TAG,
value DOUBLE FIELD)`. Neither arm received an oracle answer or task-specific
SQL. The SQL arm exposed only `iotdb_sql`, allowed one read-only `SELECT`, and
received task-independent IoTDB TABLE dialect facts. The filesystem arm exposed
only typed `iotdb_fs`, with a fixed object path and controlled time, TAG,
measurement, limit, offset, and aggregate parameters. Both arms used
`deepseek-flash` and a 120-second per-task limit. Arm order alternated by source
ordinal.

The final non-overlapping run is stored at
`/data_01/iotdb-fs-exp/results/nlqts-dsh-sql-fs-73-82-interval-blind-clean-v1`.
Two earlier directories named `interval-blind-v2` and
`74-82-interval-blind-v1` are excluded because a remote process survived a
local SSH interruption and overlapped the later run. They are debugging
artifacts, not benchmark evidence.

## Results

| Metric | SQL | Filesystem |
| --- | ---: | ---: |
| Exact correct | 6/10 | 4/10 |
| Mean official IoU | 0.600 | 0.400 |
| Completed with an answer | 6/10 | 4/10 |
| Tool calls | 6 | 280 |
| Recorded tool errors | 0 | 2 |
| Total wall time | 383.22 s | 988.39 s |
| Median wall time, all trials | 38.74 s | 113.77 s |
| Median wall time, correct trials | 38.49 s | 77.41 s |
| Total CLI time | 6.11 s | 385.67 s |
| Total tokens | 130,041 | 1,336,439 |
| Median tokens, all trials | 12,580.5 | 103,030 |
| Median tokens, correct trials | 15,864 | 175,189 |
| Model requests | 16 | 75 |
| Retries | 0 | 0 |

The all-trial filesystem median is 2.94 times the SQL wall time and 8.19
times the SQL token count. Among correct trials, filesystem is 2.01 times
slower and uses 11.04 times as many tokens. No incorrect interval received a
partial IoU score: every completed answer exactly matched both public
endpoints, while failures produced no parseable interval.

| # | SQL result | FS result | SQL calls | FS calls | SQL wall (s) | FS wall (s) | SQL tokens | FS tokens |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 73 | correct | timeout | 1 | 39 | 35.78 | 120.12 | 15,098 | 110,282 |
| 74 | correct | max tokens | 1 | 10 | 38.68 | 65.94 | 17,153 | 164,916 |
| 75 | max tokens | timeout | 0 | 43 | 40.39 | 120.12 | 9,182 | 36,373 |
| 76 | correct | correct | 1 | 8 | 42.90 | 76.25 | 13,891 | 255,970 |
| 77 | max tokens | timeout | 0 | 42 | 40.74 | 120.12 | 9,182 | 96,559 |
| 78 | correct | timeout | 1 | 43 | 26.68 | 120.12 | 11,270 | 108,847 |
| 79 | max tokens | correct | 0 | 11 | 38.80 | 78.57 | 9,194 | 280,530 |
| 80 | correct | correct | 1 | 8 | 38.29 | 59.63 | 16,630 | 94,408 |
| 81 | max tokens | timeout | 0 | 52 | 37.64 | 120.12 | 9,194 | 97,213 |
| 82 | correct | correct | 1 | 24 | 43.32 | 107.43 | 19,247 | 91,341 |

All six SQL trials that issued a query returned the exact answer in one call.
The other four consumed the model's 8,192-token per-response limit before
calling the tool. SQL accuracy is therefore limited here by query-generation
stability rather than database execution or answer formatting. The first
diagnostic attempt also established one necessary dialect fact: IoTDB accepts
the nested running-window query but rejects direct `TIMESTAMP - TIMESTAMP`;
duration expressions must cast timestamps to `INT64`. This task-independent
fact was added before the clean run. The dialect overlay SHA-256 is
`bd4c29ef7a745df60f83622885fd9b89ad511f10a498398246b4fbd7e831aa26`.

## Filesystem call behavior

The metadata-only call audit recorded 280 calls: 215 `stats`, 46 `cat`, 9
`help`, 5 `schema`, 2 `tail`, 2 `meta`, and 1 `count`. The adapter returned 47
compact pages containing 18,273 rows and 511,563 model-facing bytes. No page
was truncated and no CLI execution failed. Two calls were rejected before CLI
because the model supplied time or TAG filters to commands that do not accept
them; one final call was interrupted by a timeout.

The model used two distinct decomposition strategies:

- Questions 74, 76, 79, and 80 primarily paginated filtered `cat` results and
  succeeded in 7–10 `cat` pages.
- Question 82 used 20 bounded `stats` calls to prune the time range, then read
  only 107 rows in three `cat` pages. It solved an 89-day range in 24 calls.
- The failed long-range searches made 33–48 `stats` calls or 39–52 total calls
  before reaching the wall-time limit. This shows that filtered scalar stats
  can support task decomposition, but the current one-window-per-call contract
  creates too much model and process overhead when pruning requires many
  intervals.

The audit artifacts are under the clean run's `fs-call-audit-v1` directory.

The 39–52-call failures are not filesystem-tree traversal. Their assigned
object path is fixed, and these trials issued no `ls`, `tree`, or `find` calls.
Of their combined 219 calls, 195 (89.0%) were `stats` requests against the same
CSV object with different time bounds. Only 11 were `cat` calls. The model was
therefore searching the one-dimensional time domain rather than discovering
files.

| # | Total calls | `stats` | `cat` | Tree-search calls | CLI time | Model time |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 73 | 39 | 33 | 4 | 0 | 55.15 s | 45.70 s |
| 75 | 43 | 39 | 1 | 0 | 62.40 s | 33.68 s |
| 77 | 42 | 35 | 4 | 0 | 58.85 s | 49.97 s |
| 78 | 43 | 40 | 1 | 0 | 60.39 s | 48.25 s |
| 81 | 52 | 48 | 1 | 0 | 69.25 s | 44.92 s |

The typical strategy was to query a coarse interval, split mixed intervals
into calendar-sized windows, and then refine selected windows to daily ranges.
A scalar `min` can prove that a complete window is above the threshold, and a
scalar `max` can prove that it contains no qualifying observation. A mixed
window reveals neither the number nor boundaries of qualifying runs, so the
agent must split it again or fetch rows. This makes the number of calls approach
the number of inspected time buckets when crossings are frequent.

The audit found no exact duplicate command, but some probes were still
semantically avoidable. Question 73 first queried daily `max` and then repeated
ten of the same daily ranges with `min,max`; questions 75, 78, and 81 scanned
many adjacent windows sequentially instead of prioritizing only promising
regions. Because every request launches a CLI process, 40–52 calls consumed
59–69 seconds in CLI time alone before model reasoning was counted.

## Comparison with questions 11–20

Questions 11–20 were ten single-channel scalar aggregations and both arms
scored 10/10. Moving to interval discovery reduced SQL exact accuracy by 40
percentage points and filesystem accuracy by 60 points. SQL median wall time
rose from 4.89 to 38.74 seconds and median tokens from 2,859 to 12,580.5.
Filesystem median wall time rose from 7.12 to 113.77 seconds and median tokens
from 4,118.5 to 103,030.

This slice is already difficult enough for the stated validation goal. Removing
the table schema, column roles, or UTC boundary convention would test schema
guessing and prompt ambiguity rather than normal Text2SQL. Difficulty should
therefore remain unchanged for the next validation run.

The strongest next filesystem improvement is a batched or bucketed `stats`
operation that accepts one controlled time range and returns min/max/count per
fixed sub-window. It preserves the FS decomposition model while replacing tens
of sequential scalar calls with one bounded response. A smaller cleanup is to
expose only commands whose typed parameters are valid for this task family,
which would eliminate the rejected `count` and `tail` calls. For SQL, the next
experiment should address the agent's pre-tool reasoning limit independently
of database correctness, for example by enforcing an action budget or using a
non-reasoning Text2SQL decoding profile; adding the task's gap-and-island query
template would contaminate the blind comparison.
