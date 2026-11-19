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


# FS composition ablation — 2026-11-19

The new composition interface did **not** improve aggregate efficiency in this
small development set. Relative to the contemporaneous grid-reduce control,
median wall time increased **83.96%**, total wall time **36.66%**, and total tokens
**39.50%**. Frame operations were adopted in 3/8 questions; explicit read and
verify were never used. These are descriptive results from one trial per cell,
not an estimate of general interface superiority.

## 4.1 small-collection test: main comparison

Both arms use the same validated CLI, eight original questions, public context,
DeepSeek configuration and budgets. This is a fresh two-arm matrix, **not** the
historical grid-reduce run already reported in the earlier ablation.

| Arm | Original-matcher final-table matches | Median wall (s) | Total wall (s) | Total tokens | Tool calls |
|---|---:|---:|---:|---:|---:|
| grid-reduce | 1/8 | 65.87 | 755.10 | 2,586,274 | 182 |
| composition | 2/8 | 121.17 | 1031.95 | 3,607,808 | 210 |

**The match counts are not substantive answer accuracy.** This is the frozen
FS final-table extension using the original comparator, not native TQTS SQL
execution. Ten of the sixteen submissions are rejected at pivot binding, often
before the correctly reported numerical facts can be compared. All sixteen
submissions contain a valid designated table. No alternative prose, earlier tool
result or repaired table was substituted for scoring. The apparent 1/8→2/8
increase must not be promoted as an accuracy gain from the new arithmetic.

## Detailed metrics

| Metric | grid-reduce | composition |
|---|---:|---:|
| Completed model tasks | 8 | 8 |
| Model API time, sum (s) | 594.80 | 685.92 |
| Tool interval time, sum (s) | 151.00 | 335.56 |
| Median tokens/task | 124,784.50 | 150,373.00 |
| Uncached input tokens | 195,464 | 371,843 |
| Cached input tokens | 2,280,064 | 3,103,360 |
| Output tokens | 110,746 | 132,605 |
| Reasoning tokens, included in output | 74,328 | 85,171 |
| Input cache-hit ratio | 92.10% | 89.30% |
| Model requests | 88 | 99 |
| FS calls | 115 | 149 |
| Public-context calls | 67 | 61 |
| Tool errors | 6 | 5 |
| Transport retries | 0 | 0 |
| Truncated tool outputs | 1 | 3 |
| Tool timeouts / heap OOM | 0 / 0 | 0 / 0 |
| Total tool response characters | 539,374 | 988,885 |

Total tokens include cached input; reasoning is already part of output and is
not added twice. Tool interval sums and per-process call times are accounting
metrics, not an additive causal decomposition of wall time. Alias resolved as
`deepseek-flash`; provider serving variability and cache state were not controlled.

| Question | Control wall (s) | Composition wall (s) | Control tokens | Composition tokens | Matcher control/new | Frame calls |
|---|---:|---:|---:|---:|---|---:|
| iotdb_cmapss-qa-013 | 56.83 | 196.98 | 192,880 | 677,813 | 0 / 0 | 9 |
| iotdb_cmapss-qa-030 | 76.37 | 157.50 | 136,894 | 851,237 | 0 / 1 | 14 |
| iotdb_gas_dynamic-qa-001 | 59.57 | 96.30 | 50,420 | 89,474 | 0 / 0 | 0 |
| iotdb_gas_dynamic-qa-021 | 93.53 | 88.47 | 155,899 | 121,060 | 0 / 0 | 0 |
| iotdb_ndbc-qa-005 | 40.21 | 55.50 | 112,675 | 109,116 | 0 / 0 | 0 |
| iotdb_ndbc-qa-028 | 63.35 | 146.03 | 110,142 | 179,686 | 1 / 1 | 0 |
| iotdb_pvdaq-qa-006 | 68.39 | 60.01 | 81,292 | 115,973 | 0 / 0 | 0 |
| iotdb_pvdaq-qa-040 | 296.85 | 231.16 | 1,746,072 | 1,463,449 | 0 / 0 | 4 |

## What the traces establish

1. **Broad discovery is a major observed cost.** In composition Cmapss 013,
   `tree /root/iotdb_cmapss --max-depth 5` takes 80.48s and a subsequent broad
   `find` takes 15.04s; both truncate. The control's depth-4 tree takes 4.17s.
   Composition Cmapss 030 also spends 39.37s on a broad tree. Across all tasks,
   tree/find call times sum to 136.64s versus 6.59s in control. This is evidence
   of different traversal choices and expensive metadata discovery, not proof
   that frame arithmetic is slow or that the feature gate caused those choices.

2. **Native composition is fast, but its representations are large.** All 27
   frame calls sum to 13.41s. Their responses contain 242,738 characters:
   entities 4,430; windows 11,717; reduce 122,061; combine 66,860; derive 37,670.
   They are about 24.5% of composition's tool response characters. Retained
   source columns and provenance help inspection, but repeated full result rows
   enlarge later model contexts. This is a candidate mechanism, not a measured
   causal attribution of all token growth.

3. **A missing result-selection step causes rescans.** Cmapss 030 first captures
   a two-entity, eight-window table (16 rows). At step 9 it self-joins this result
   on window_index alone and correctly receives DUPLICATE_JOIN_KEY. It then
   creates two singleton entity sets and rescans at step 11, before performing
   the intended per-window arithmetic. The same-key safety check is appropriate;
   there is no simple controlled entity subset/projection of a saved result in
   the current interface. This is a generic producer/consumer grain mismatch.

4. **Similar command families invite parameter confusion.** Cmapss 030 twice
   sends frame's value_unit/time_unit/time_unit_ms to ordinary reduce, which
   correctly rejects them. Other errors include unsupported pagination fields,
   a context-page limit and missing window bounds. No new native numeric crash,
   OOM or query timeout occurs in the completed matrix.

5. **Reuse is only partly adopted.** Frame operations: entities 7, windows 3,
   reduce 8, combine 6, derive 3; read 0, verify 0. Five successful combines and
   three derives consume saved inputs without DB scans. In Cmapss 013, two
   same-step reduce requests differ only by spelling out default limit=200;
   native duplicate_request marks the second and scan_performed remains true.
   There is no evidence yet that duplicate hints reduce future calls, or that
   the uninvoked verify mechanism reduces verification cost.

6. **Window interpretation remains a long reasoning branch.** PVDAQ 040 costs
   296.85s/65 calls in control and 231.16s/58 calls in composition. Both explore
   alternative window arrangements and ultimately submit a New York boundary,
   not the gold's UTC boundary. Composition uses frame only late in the task.
   Fast computation and richer metadata did not settle that semantic choice.
   NDBC 028's composition gaps call returns all 48 rows in 0.82s, yet its full
   task takes 146.03s: complete data computation is not the remaining bottleneck.

## Scoring and answer diagnosis

The unchanged matcher passes NDBC 028 in both arms and Cmapss 030 in composition.
It rejects five questions per arm at pivot binding: Cmapss 013, Gas 001, NDBC 005,
PVDAQ 006 and PVDAQ 040. Both Gas 021 tables also omit the required Device column;
control Cmapss 030 omits the required physical window timestamp.

These are **non-grading observations of the primary submitted tables**, not
replacement accuracy labels:

- Cmapss 030: both arms compute /23 rates correctly. Control submits cycle
  identifiers but no physical window start; composition retains start_time_ms
  and matches. This does not show correction of a /24 arithmetic error in the
  new control run.
- Gas 001: both primary tables contain 5020520ms and associated sensor_02=-4.39.
- NDBC 005: both contain 1675516800000ms and sea temperature 20.3.
- PVDAQ 006: both contain the two gold physical session endpoints and means
  within tolerance. Column/pivot binding still rejects these final tables.
- Cmapss 013: reported window means align, but native identity/field/pivot and
  time representation do not satisfy the frozen normalization contract.
- Gas 021: final tables lack Device. Composition explores both pooled and
  entity reductions, then submits the pooled presentation. Control also loses
  entity identity in final output. Correct time positions alone are insufficient.
- PVDAQ 040: both identify metric 2745 and endpoint values 27/47.566667, but
  submit June 15 midnight America/New_York rather than UTC. It has a real window
  semantic mismatch in addition to its pivot-binding rejection.

For example, NDBC 005's policy declares fields observed_at/sea_temperature_c in
its reshape stage; names=ignore at the column comparison stage does not remove
that earlier binding constraint. A generic JSON table alone is therefore not a
complete gold-independent compatibility layer for the SQL-oriented evaluator.
We preserve all failures and do not retrospectively rename/recover answer cells.

## Next direction suggested by this evidence

Prioritize bounded tree/find discovery with useful partial results and narrowing
feedback, compact result projections with provenance referenced once, and typed
selection/partitioning of captured entity rows. Distinguish ordinary reduce from
frame reduce in the parameter schema to prevent cross-family arguments. These
address trace patterns, not constants or answers from the eight questions.

Before claiming correctness gains, define and validate a public semantic result
contract plus deterministic serialization that the original matcher can consume
without per-question gold-dependent repair. Keep its historical scores separate.
Do not add more operators or force frame use on tasks that already have a direct
extreme/gaps/sessions solution. Repeat a frozen matched comparison before drawing
causal conclusions about interface exposure or progressive discovery.

## Reproducibility and limitations

Protocol: [FS_COMPOSITION_ABLATION_PROTOCOL_20261119.md](FS_COMPOSITION_ABLATION_PROTOCOL_20261119.md).
The matrix is 8 reused development questions × 2 arms × 1 trial, alternating arm
order, serial execution. Per task: 900s, 120 calls, 32768 tokens/generation;
180s/CLI process, 8GiB heap, 512MiB direct memory, 40000 raw output characters.
All 16 exposure audits show only dataset_context/iotdb_fs; recorded generation
limits are 32768. Source, CLI, settings, selection and public-context integrity
checks all pass. All 20 evaluator source hashes and eight saved gold hashes pass.
No live gold queries or interface/prompt changes occur during the matrix.

The initial attempt is archived separately: one baseline trial completed
(55.46s, 103,556 tokens), then composition failed during DSH schema registration
before a model call. Only required/additionalProperties authoring syntax was
fixed, and all four feature configurations compiled with the actual DSH package
without model requests. A separate complete 16-cell replacement matrix was run;
no cells were selectively rerun or merged. The abort is excluded from matrix
metrics, retained as setup cost, and can have affected provider prefix caches.

Final server root: `/data/zzy/iotdb-fs-exp/results/tqts-fs-composition-20261119-r2/`.
It contains comparison.json, schedules/protocols, per-trial evaluation.json and
submitted-final-table.json, full DSH sessions, requests/results, resource metrics
and native frame captures. Original credentials/settings and raw command resource
logs are excluded from the exported evidence archive. CLI SHA-256:
`0908b1014059b2d9ba41ad912a8e22b793d48e782962d963edd3bbf56e97c54e`.
Evidence archive SHA-256:
`03691cdb0dc61817e9457ad313013e837f83300166a4378b3a897ffd8fb1632d`.

Local evidence: `.tmp/fs-composition-20261119-r2/evidence/` in the IoTDB checkout.
The shared experiment CLI remains on the validated new artifact. The aborted
attempt and pre-deployment CLI backup remain in the sibling non-r2 directory.
This small collection remains section 4.1 evidence, not a full paper experiment
or a claim of automatic interface evolution/generalization.
