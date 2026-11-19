<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Gap/session and associated extreme-row ablation — 2026-11-19

The native CLI now computes complete non-null observation gaps and sessions and returns values associated with an extreme observation. The four-arm, eight-question pilot completed all 32 trials with the same questions and budgets. Opening both capabilities reduced total wall time by **21.6%** and reported tokens by **50.6%** versus the contemporaneous base arm. The clearest improvement is session computation: the agent stops guessing continuity from sampled windows. The median task did not become faster, and the experiment does not establish general FS superiority over SQL.

## Implementation and semantic contract

See [the frozen protocol](FS_TEMPORAL_PROTOCOL.md) and native `help` for the full interface. These changes compute in the CLI over ordered raw SELECT streams; no database-kernel modification or aggregate SQL pushdown was added.

| Command | Calculation and returned result | Boundaries and grain |
| --- | --- | --- |
| `gaps PATH --field F` | Compute every successive non-null observation gap; return distinct first/last/min/max observations per window, with earliest ties, `selected_as`, `observed_at`, `previous_at`, exact decimal `gap_ms` | Gap belongs to its later observation; preserve the real predecessor outside the output window. This is full computation with M4-style reduction, **not all gap-detail rows**. |
| `sessions PATH --field F --gap-ms G` | Stream completed sessions with start/end, count, mean and first/last values; retain only the current accumulator per entity | Split only when gap **> G**; equality joins. Skip NULL target. Apply start/end before segmentation, so bounded calls produce clipped sessions and restart indices. No windows argument. |
| `extreme PATH --field F --mode min\|max -m associated` | Return the extreme observation timestamp, target and associated values; exact INT64 comparison, earliest tie, associated NULL retained | Per entity/window; skip NULL target, reject non-finite numeric target. Empty windows have no row. No cross-entity ranking. |

Tree grain is the device; Table grain is the complete TAG tuple including NULL. Results preserve object identity, TAGs and window keys. Windows use **inclusive epoch-millisecond endpoints**: SQL `[a,b)` maps to `[a,b-1]`. Duplicate/overlapping windows retain their indices (maximum 256). Native paging counts result rows; the adapter uses one-row lookahead and `next_offset` (default 200, maximum 2,000). Cross-call paging requires unchanged data; it does not provide a transactional snapshot.

`stats.min_time/max_time` remain non-null observation time bounds, not timestamps of min/max values. `extreme` supplies the latter. A source field named `Time` is not necessarily the physical IoTDB timestamp. Empty SQL global aggregates may return a NULL row where these FS operators return no rows; SQL tie rules must explicitly match earliest-time selection. No universal SQL-equivalence claim is made.

## Validation and deployed versions

Before any LLM trial, `verify_temporal_base.py` passed **20 native invocations** with 512 MiB heap/128 MiB direct memory: four original Tree gold queries plus inclusive/duplicate/empty windows, pagination, outside-window predecessors, clipped sessions and exact threshold equality. The affected gold queries were Gas 001, NDBC 005/028 and PVDAQ 006. The gate checks NDBC's 48 gap rows and PVDAQ's two sessions against actual SQL output. Table TAG/NULL isolation, ties, INT64 precision, overflowing timestamp differences and streaming cancellation have unit coverage; **live Table SQL equivalence was not tested**.

The frozen experiment build (V1) passed 340 targeted Java tests, 11 Python tests and English/Chinese full-reactor test compilation. After all 32 LLM trials, an additional boundary regression exposed finite means becoming infinity when the intermediate floating sum overflowed. V2 divides the wide decimal sum before narrowing to double. Two added tests reproduced the defect before the fix; the final targeted suite passes **342 Java tests and 12 Python tests**. Full-reactor English/Chinese compilation and CLI packaging also pass; XML formatting was skipped for these reactor checks. The first V2 Chinese attempt was blocked by the local Maven cache lock permission; the normal-permission retry passed.

V2 also fixes the adapter treating Java OOM exit code 1 from `grep` as an ordinary no-match, and corrects the metadata label for an explicitly selected base arm. These changes occurred **after** the frozen LLM trials. The complete 20-invocation SQL gate was repeated on V2; all 14 FS invocation outputs are identical to V1. V2 is deployed on the server. **There was no LLM rerun after these fixes**, and V1 errors/costs remain in the reported results.

| Artifact | SHA-256 |
| --- | --- |
| Frozen V1 CLI used in all 32 trials | `1bf6f59bf92b1bfa1621d4ae119453fd681f7ea2a64f4e7333d822cedff003c4` |
| Final V2 CLI, SQL-verified and deployed | `837533a393a8f97d96dbcdff5bf8854d54a4b1d103782ba00dd348fdceb28965` |

## Fixed experiment

Same eight original validation questions, one stochastic trial per question/arm, serial execution with rotating arm order. The arms are base, base + gap/session (`temporal`), base + associated extreme rows (`extreme`), and both. Capability gates apply in both the advertised schema and the dispatcher. Common help explains entity/window grain in every arm. This is a capability-and-contract ablation, not an instruction-only control or a replay of the older historical adapter.

All arms use `deepseek-official/deepseek-flash`, identical provider-settings hashes, public context and selection, 900 seconds/task, 120 tool calls, **32,768 tokens per generation** (not a total task token cap), 180 seconds/FS process, 8 GiB heap, 512 MiB direct memory, 40,000 output characters and 2,000 rows/page. Only `dataset_context` and `iotdb_fs` are exposed; no SQL, shell, source, gold or external calculator. Protocol hashes, feature gates and public-context equality all verify. No question replacement, selective rerun or code change occurred during the matrix.

## Aggregate results

“Core” reviews the requested winner/values; “strict” also requires the requested entity/window interpretation to match the frozen SQL answer. These are manual answer reviews, **not official executable-SQL accuracy**. Final command strings are not uniformly replayable and some cited call IDs are descriptive labels. All 32 tasks returned a final answer; completion is not correctness.

| Arm | Strict aligned / 8 | Additional partial / 8 | Core correct / 8 | Total wall (s) | Median wall (s) | Total tokens | Tool attempts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| base | 5 | 0 | 5 | 948.40 | 74.11 | 2,693,417 | 194 |
| temporal | 6 | 1 | 7 | 841.04 | 77.70 | 2,110,109 | 202 |
| extreme | 5 | 0 | 5 | 989.73 | 78.45 | 2,872,890 | 235 |
| both | 6 | 1 | 7 | 743.35 | 76.22 | 1,331,312 | 149 |

The combined arm cuts model/API latency by 30.3% (751.83 → 524.25 s), tool attempts by 23.2%, and `stats` calls from 68 to 34. Summed tool intervals **increase** from 185.43 to 208.25 s; median wall time rises from 74.11 to 76.22 s. The reduction is concentrated in difficult cases, not a uniform native-query speedup. Temporal-only reduces total wall by 11.3%; extreme-only increases it by 4.4%.

| Arm | Model/API seconds | Tool interval sum (s) | FS / context calls | Reported tool errors | OOM calls | Truncated outputs | Model requests | Transport retries |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| base | 751.83 | 185.43 | 126 / 59 | 2 | 1 | 2 | 105 | 0 |
| temporal | 604.49 | 220.25 | 130 / 72 | 6 | 0 | 1 | 94 | 0 |
| extreme | 790.19 | 189.21 | 148 / 87 | 2 | 0 | 2 | 95 | 0 |
| both | 524.25 | 208.25 | 95 / 54 | 3 | 0 | 1 | 87 | 0 |

Base has nine additional pre-dispatch rejected attempts (194 attempts versus 185 admitted FS/context calls). Its reported error count excludes one OOM misclassified by the V1 adapter; the separate OOM counter preserves that failure. Error categories should not be added blindly across counters. Tool intervals can overlap and do not necessarily sum to task wall time; model/API latency includes provider/network time.

| Arm | Uncached input | Cached input | Output | Reasoning (subset of output) | Input cache hit ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| base | 235,897 | 2,313,472 | 144,048 | 88,781 | 90.75% |
| temporal | 176,590 | 1,814,528 | 118,991 | 70,963 | 91.13% |
| extreme | 241,182 | 2,489,984 | 141,724 | 95,562 | 91.17% |
| both | 127,258 | 1,107,584 | 96,470 | 66,447 | 89.69% |

Total tokens include cached input; reasoning is already included in output. Provider prefix-cache state was not reset or controlled. The sampled host resource log starts after the first few trials; per-call resource and DSH logs cover the whole matrix.

## Per-question results and trace interpretation

| Question | Base wall (s) | Temporal | Extreme | Both | Answer review |
| --- | ---: | ---: | ---: | ---: | --- |
| iotdb_cmapss-qa-013 | 39.76 | 42.55 | 68.54 | 77.36 | All four align |
| iotdb_cmapss-qa-030 | 80.91 | 111.00 | 47.91 | 64.80 | All four align |
| iotdb_gas_dynamic-qa-001 | 161.13 | 105.92 | 88.37 | 74.26 | All four align |
| iotdb_gas_dynamic-qa-021 | 67.32 | 98.12 | 45.80 | 101.47 | All four fail fixed gold entity grain |
| iotdb_ndbc-qa-005 | 65.91 | 57.27 | 28.83 | 63.50 | All four requested answers align |
| iotdb_ndbc-qa-028 | 39.70 | 51.18 | 101.05 | 75.09 | Only temporal/both align |
| iotdb_pvdaq-qa-006 | 291.84 | 36.81 | 346.50 | 87.99 | Only temporal/both align |
| iotdb_pvdaq-qa-040 | 201.83 | 338.18 | 262.72 | 198.89 | Same core values; temporal/both have window-timezone mismatch |

### Complete session computation removes a concrete reasoning bottleneck

PVDAQ 006 has 850,675 observations in two sessions, separated by 16,740,000 ms (4 h 39 min), exceeding the four-hour threshold. Gold boundaries/means are:

| Start epoch ms | End epoch ms | Count | Mean |
| --- | --- | ---: | ---: |
| 1385764560000 | 1407053520000 | 118,248 | 60.00006511738033 |
| 1407070260000 | 1595797200102 | 732,427 | 59.90763809908739 |

Base spends 291.84 s/36 attempts/872,078 tokens and extreme-only 346.50 s/46 attempts/1,061,536 tokens, yet both incorrectly infer one session from sampled windows. Temporal completes correctly in 36.81 s/14 attempts/69,633 tokens; both in 87.99 s/18 attempts/97,827 tokens. Each successful arm invokes `sessions` once and `gaps` once. Model/API time falls from 263.70 s in base to 26.63 s in temporal or 71.94 s in both; tool intervals fall from 26.33 s to 9.08/14.88 s. This trace supports moving exact continuity computation into the command, rather than leaving repeated approximate reasoning to the model.

### Gap predecessor semantics improve correctness, not always latency

For NDBC 028, a gap is assigned to its later observation; the first hourly sample can have a predecessor in the preceding hour/day. Native SQL and `gaps` return 48 distinct rows: the HH:00 row covers first/min/max roles and HH:50 covers last, all 600,000 ms. Base/extreme shift the first gap to HH:10 and fail. Temporal/both return the correct 48 timestamps and values, taking 51.18/75.09 s versus the incorrect base's 39.70 s. Full predecessor handling is necessary even when the correct answer costs more than a shortcut.

### Associated extrema work, but gains depend on the rest of the trace

Gas 001 needs timestamp 5,020,520 ms and associated sensor_02 = -4.39 at sensor_01's maximum. Extreme/both reduce wall time from 161.13 s to 88.37/74.26 s. NDBC 005 needs 1675516800000 (2023-02-04 13:20 UTC), associated wtmp = 20.3: extreme-only takes 28.83 s versus base 65.91 s, but both takes 63.50 s. Base eventually gets both requested answers right using multiple calls. Extra available commands do not guarantee fewer calls: extreme-only makes 17 `extreme` calls over the matrix, while both makes six.

Base Gas 001 also hits an 8 GiB Java OOM on a broad legacy `grep` over approximately 4.2 million rows. V1 mistakes its exit code 1 for a no-match; final answer recovery does not erase the failure. The new operators did not OOM. The V2 adapter now distinguishes fatal Java errors from ordinary grep no-match, but generic text paths (`grep`/`cut` and related materialization) are **not all streaming or memory-bounded**. Part of the extrema benefit is avoiding this legacy path.

### Entity grain and window interpretation remain unresolved at the task layer

All four Gas 021 answers pool the two experiments by window and rank 62–64, 60–62, 68–70. The frozen SQL ranks separate (experiment, window) rows: methane experiment windows 68–70, 60–62, 62–64. The native commands preserve entity keys; the agent chooses a different reduction despite the common grain note. The phrase “across both” is ambiguous, so this is a fixed-gold mismatch, not sufficient evidence of a native operator defect.

For PVDAQ 040, all arms identify metric_2745/inv6_temp, the June-15-start 14-day period, first 27, last 47.566667 and increase 20.566667. Temporal/both describe the bucket in America/New_York midnight terms, four hours offset from the gold UTC bucket starting 1560556800000. Equal values caused by nighttime gaps do not make the window labels equivalent: these two answers are **partial**, not strict passes. Base/extreme align on the requested bucket interpretation. Base additionally gives incorrect unrequested opening/closing UTC details; those are recorded in its review, not treated as evidence that its prose is entirely error-free. Base NDBC 005 also makes an unnecessary incorrect claim that `min_time/max_time` are inconsistent; their documented meaning is observation bounds.

PVDAQ 040 remains expensive (198.89–338.18 s; 35–80 attempts). The natural-language request does not fully specify the gold's non-overlapping 14-day grid anchored at 2019-06-01 UTC over `[1559347200000,1569888000000)` (2019-06-01 through 2019-10-01 UTC). Traces investigate rolling starts, timezone shifts and alternate phases, generate long window arrays and manually compute deltas. Some alternate grids find a larger delta. These investigations cannot all be classified as useless search without first resolving the task's intended window family. Inputs/gold were not changed mid-experiment.

## Next work supported by this pilot

1. Keep exact gaps/sessions and associated extrema as base capabilities. Preserve the distinction between full scans and reduced output, and between observation timestamps and source-time fields.
2. Make window construction explicit in a generic typed contract: epoch origin, width, stride, timezone, endpoint convention and entity keys. Return these descriptors with every result; do not silently infer a gold-specific grid from this validation set.
3. Add explicit cross-entity reduction/ranking and endpoint-difference semantics only after specifying keys and SQL equivalence. This targets Gas 021's pooling mismatch and PVDAQ 040's manual delta/ranking work. It must remain distinguishable from per-entity statistics.
4. Stream or explicitly bound legacy text materialization and retain fatal-error diagnostics. V2 fixes error classification; it does not yet eliminate the underlying broad-grep OOM.
5. After freezing the contract, repeat paired validation trials and use held-out questions for generalization. One trial/cell, eight validation questions, common-contract changes and uncontrolled caches cannot establish statistical significance or performance against SQL.

## Evidence and reproduction

Server root: `/data/zzy/iotdb-fs-exp` on `zzy@101.6.42.165`.

- `results/tqts-fs-temporal-20261119/`: schedule/completion, four arm protocols and complete DSH/tool traces, summaries, `answer-review.json`, `comparison.json`, V1 `validation/`, V2 `validation-final/`, `deployment.json`, evidence notes and resource logs.
- `experiments/tqts-fs-temporal-20261119/scripts/`: frozen executed V1 scripts. `iotdb-cli.frozen-ablation.jar` preserves V1; `final-boundary-fix/` holds V2 and its separate evidence. Never substitute V2 when reproducing the reported V1 matrix.
- Local `.tmp/tqts-fs-temporal-20261119/`: credential-excluding evidence mirror. `.tmp/fs-temporal-v2-*.log` retain build/test results, including the failing-before-fix numeric regression.
- `evidence.tar.gz` plus SHA-256 manifest: complete experiment evidence excluding private DSH homes, credentials, workspaces and private spill directories. Private provider settings remain on the server; only hashes are archived.
- The base arm's original preparation metadata inherited an obsolete historical-comparison label. The frozen root protocol, schedule and actual hashes identify the contemporaneous matrix. The original record is retained with an explicit correction note; V2 fixes future labels.

Review comments and partial-credit judgments are saved alongside every question rather than modifying the benchmark or the agent traces. Current changes are uncommitted on `fs/inner-view`, on top of `d5e2e05c15`; pre-existing work is preserved.
