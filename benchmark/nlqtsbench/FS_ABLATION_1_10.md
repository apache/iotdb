<!--

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

# FS interface ablation on NTQTSBench tasks 1–10

Run date: 2026-09-21–22 (Asia/Shanghai)

This diagnostic experiment evaluates the two FS interface changes separately:

1. **Historical**: free-text `command` plus raw output.
2. **Typed raw**: fixed object path and typed parameters, with raw CSV output.
3. **Typed page**: typed parameters plus structured pages and `next_offset`.

All FS trials used the same imported and equivalence-verified data, the same
120-second per-task deadline, and the resolved model
`deepseek-official/deepseek-flash` with a 1,000,000-token context window. Only
`iotdb_fs` was exposed. The historical run was executed earlier, so it is a
diagnostic reference rather than a randomized concurrent control.

## Aggregate results

| Variant | Correct | Completed | FS calls | Session tool errors | Audited anomalous calls | Median wall time | Median model time | Median CLI time | Median total tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Historical | 1/10 | 2/10 | 387 | 144 | 146 | 120.1 s | 68.9 s | 28.0 s | 167,878 |
| Typed raw | 4/10 | 4/10 | 153 | 2 | 5 | 84.1 s | 62.7 s | 13.7 s | 98,519 |
| Typed page | 4/10 | 4/10 | 146 | 11 | 12 | 77.6 s | 51.6 s | 14.2 s | 167,135 |

Introducing the typed interface accounts for the main improvement. Relative to
the historical run, typed raw improved accuracy from 10% to 40%, reduced calls
by 60.5%, reduced audited anomalous calls by 96.6%, reduced median wall time by
30.0%, and reduced median tokens by 41.3%.

Structured paging did not improve accuracy in this run. Relative to typed raw,
it reduced calls by 4.6%, median wall time by 7.8%, and median model time by
17.8%, while median tokens increased by 69.6%. Audited anomalous calls increased
from 5 to 12. Ten of the page-mode validation or CLI errors came from invalid
parameter combinations such as applying time or TAG filters to `stats`, `count`,
or `stat`; one used `value` as a TAG column. One additional anomaly was an
unmatched result when a trial timed out.

### Token increase diagnosis

| Measurement across 10 trials | Typed raw | Typed page | Change |
| --- | ---: | ---: | ---: |
| Model requests | 63 | 81 | +28.6% |
| Model-visible data-result bytes | 577,505 | 720,862 | +24.8% |
| Successful data-read calls | 111 | 89 | -19.8% |
| Input tokens | 271,282 | 316,948 | +16.8% |
| Cache-read tokens | 725,888 | 1,233,792 | +70.0% |
| Output tokens | 143,705 | 116,212 | -19.1% |
| Reasoning tokens (included in output) | 121,399 | 98,371 | -19.0% |
| Derived total tokens | 1,140,875 | 1,666,952 | +46.1% |

Cache reads account for 507,904 of the 526,077 net added tokens, or 96.5%.
The increase is therefore not caused by the model emitting more reasoning. Two
effects enlarge the repeatedly cached conversation:

1. JSON row arrays add quotes, brackets, and commas. On tasks with comparable
   row counts and call counts, the structured representation was approximately
   25%–35% larger than raw CSV.
2. `next_offset` makes each continuation depend on the preceding result. The
   page run used 81 model requests versus 63 for raw mode, so earlier pages were
   read from cache more times even though page mode made fewer data-read calls.

Validation retries amplified the effect but were not the primary cause. For
example, task 3 had no tool error but gained 125,568 cache-read tokens. The page
run's output and reasoning token totals were both lower than raw mode.

The current derived total counts cached input at full token volume. It measures
context traffic and agent work, but it should be reported separately from
uncached input and provider cost because cache hits may have different latency
and pricing.

## Per-task results

Each cell shows `answer status / FS calls / wall seconds`.

| Task | Historical | Typed raw | Typed page |
| ---: | --- | --- | --- |
| 1 | correct / 12 / 44.3 | correct / 4 / 33.6 | invalid / 4 / 36.1 |
| 2 | invalid / 43 / 120.1 | correct / 9 / 36.8 | correct / 10 / 34.0 |
| 3 | invalid / 51 / 120.1 | correct / 8 / 39.9 | correct / 12 / 61.8 |
| 4 | invalid / 51 / 120.1 | invalid / 12 / 96.0 | invalid / 14 / 110.4 |
| 5 | invalid / 18 / 78.2 | invalid / 13 / 82.4 | correct / 8 / 32.2 |
| 6 | invalid / 29 / 120.1 | invalid / 31 / 120.1 | invalid / 17 / 120.1 |
| 7 | invalid / 30 / 120.1 | invalid / 10 / 85.9 | invalid / 12 / 93.4 |
| 8 | invalid / 75 / 120.1 | invalid / 20 / 120.1 | invalid / 17 / 120.1 |
| 9 | invalid / 27 / 80.3 | correct / 7 / 40.1 | correct / 7 / 29.6 |
| 10 | invalid / 51 / 120.1 | invalid / 39 / 120.1 | invalid / 45 / 120.1 |

Typed raw answered tasks 1, 2, 3, and 9 correctly. Typed page answered tasks 2,
3, 5, and 9 correctly. The equal aggregate accuracy therefore masks a task-1
regression and a task-5 improvement. On task 1, page mode had no tool error, but
the model read rows instead of using the whole-object `stats` operator and
reached its generation-token limit while manually processing the page.

## Paging contract evidence

The typed-page run returned 89 structured data pages containing 22,182 rows;
78 pages supplied a non-null `next_offset`. The largest model-facing tool result
was 16,381 bytes, below the 40,000-byte page budget. There were no adapter
truncations. Parsing the canonical `tool/result` message content found no
model-visible server path or spill notice in any of the three variants.

The page format therefore achieved bounded, explicit continuation without
exposing an inaccessible artifact path. It did not by itself make the model use
the right decomposition. Most remaining work is operator selection: the model
must prefer whole-object aggregates when they are valid, and it needs an
explicit windowed aggregate capability when the requested task cannot be
answered by whole-object `stats` or `count`. Repeating this ten-task experiment
with multiple runs is required before treating latency or accuracy differences
as publication evidence.

## Recommended interface changes

The highest-impact change is a filtered aggregate operation. Tasks 1–10 are all
aggregations over a time range and one channel, while the current `stats` and
`count` commands only operate on the whole object. The agent therefore has to
transfer rows to compute an answer. Extend `stats`, or add a typed `aggregate`
command, with:

- the existing `startMs`, `endMs`, `measurement`, and TAG predicate fields;
- an allowlisted `metrics` enum containing `count`, `sum`, `min`, `max`, `avg`,
  and `median`;
- a small structured result containing only the requested aggregates and the
  effective filter scope.

Minimum, maximum, average, and range then require no row transfer. Exact median
must be computed by the backend; page medians cannot be merged into an exact
global median. This operation remains read-only and task-independent, and it
does not expose arbitrary SQL.

The next changes should reduce avoidable context growth:

1. Use a command-discriminated input schema so unsupported parameter
   combinations cannot be generated. The flat schema currently presents every
   parameter alongside every command and produced most page-mode tool errors.
2. Put the complete contract in the command schemas. Keep `help` for recovery,
   but do not encourage one 2.6-KiB help call in every trial and then carry that
   text through every later model request.
3. Make fallback pages compact: emit numeric values as numbers, omit redundant
   fields (`returned_rows`, `has_more`, and the echoed `limit`), and represent a
   constant filtered TAG once instead of repeating it in every row.
4. Add `summary-only` page mode with mergeable page state (`count`, `sum`,
   `min`, and `max`) for computations that are not supported by a global
   aggregate. Raw rows should be opt-in. Median still requires a backend exact
   aggregate or a clearly labeled approximate quantile sketch.
5. If raw paging remains necessary, return total matched rows with a fixed page
   size so the agent can issue known offsets in one tool-producing model step.
   This reduces repeated model requests, although server-side reduction remains
   preferable.

A follow-up ablation should evaluate compact pages, discriminated schemas, and
filtered aggregates separately. The expected success signal for filtered
aggregates is near-zero data pages and one small aggregate result per task,
rather than merely fewer pagination calls.

The compact-page follow-up has now been completed. It reduced serialized bytes
per row by 12.6% but reduced total tokens by only 1.3% because the agent fetched
30.3% more rows. See [FS_COMPACT_ABLATION_1_10.md](FS_COMPACT_ABLATION_1_10.md).

## Server artifacts

- Historical: `/data_01/iotdb-fs-exp/results/nlqts-dsh-1-10-pilot-v1`
- Typed raw: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-raw-v1`
- Typed page: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-page-v1`

Each directory contains `trials.json`, `summary.json`, `REPORT.md`, per-trial DSH
sessions, and an `fs-call-audit-v1` directory.
