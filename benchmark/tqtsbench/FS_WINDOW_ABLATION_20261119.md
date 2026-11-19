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

# Explicit window grids and entity reduction — 2026-11-19

The native FS interface now exposes explicit window origin/timezone and reduction grain, global ranking, and endpoint differences. These are general commands: no benchmark IDs, expected winners, task-specific units, candidate lists or gold-dependent defaults are embedded in their implementation or help. Calculations run in the CLI over raw SELECT streams; the database kernel and benchmark gold remain unchanged.

The 32-trial matrix completed. Compared with the contemporaneous base, opening both capabilities reduced total wall time by **33.0%** and reported tokens by **43.3%**, but strict answer alignment fell from **7/8 to 5/8** (each additionally has one partial answer). Median task wall time increased from **66.84 to 77.05 s**. This is evidence of lower cost on some traces, **not an overall effectiveness improvement**.

## Evaluation clarification (2026-11-19)

The counts below remain historical **manual answer review**, not official TQTS execution accuracy. The authoritative evaluation audit is [the frozen eight-question contract](TQTS_EVALUATION_CONTRACT_20261119.md). All eight original policies ignore row order; Gas 021 differs in entity grain/required Device, not merely ordering. Partial numeric alignment and alternative interpretations are trace annotations, not benchmark credit. A canonical FS final-table submission protocol was not frozen for these runs, so no official FS accuracy is retroactively assigned. No new commands are planned from this eight-question audit; the interface remains frozen pending evaluation alignment.

## Contract

See [the frozen protocol](FS_WINDOW_PROTOCOL.md) for the complete semantics.

| Capability | Contract |
| --- | --- |
| `windows` | Explicit origin, width/step, `ms`, calendar `day` or `month`, and timezone. Select grid starts inside the requested range and clip effective ends. Return nominal half-open and effective inclusive bounds, local labels and origin. Reject ambiguous/nonexistent calendar boundaries and overflow. Maximum 256 windows. |
| `reduce` | Numeric field with count/sum/avg/min/max/first/last/delta/elapsed_ms/rate_per_ms. Entity grain means Tree device or complete Table TAG tuple; pooled grain merges raw observations and gives sample-weighted means. |
| Endpoints | First/last non-null target by physical timestamp, with deterministic entity tie breaks. Delta is last minus first; elapsed time is last_at minus first_at, not window width or sample count. Zero-span rate is NULL. Exact decimal differences and wide elapsed arithmetic prevent INT64 overflow; division uses DECIMAL128. |
| Ranking | Aggregate completely, then rank globally across entity/window result rows. Explicit metric and direction; NULL last; deterministic window/entity ties; row_number semantics. Apply top before pagination. |
| Bounds | State grows with nonempty result groups (maximum 65,536), not sample count. Results preserve entity/endpoint identities and window descriptors. Empty groups are omitted. |

The first version reduces one numeric field at a time. It does not evaluate arbitrary expressions, infer cycle/unit conversions, select domain-specific entity subsets, or guarantee a snapshot across separate calls. Those are explicit limits, not hidden question-dependent behavior.

## SQL validation before agent execution

The final frozen CLI passed 347 targeted Java tests, 13 Python tests, CLI packaging, and English/Chinese full-reactor test compilation. Spotless and diff checks passed. The native SQL gate passed 31 invocations, including fixture setup and inspection (not 31 independent test cases), at 512 MiB heap/128 MiB direct memory.

Live Tree/Table fixtures cover unequal entity sample counts, sample-weighted pooling, NULL targets/TAGs, deterministic endpoint ties, endpoint differences/rates, duplicate and empty windows, ranking/top/offset, and DST boundaries. Table `date_bin` verifies ordered window results directly against SQL; a New York local-midnight day spanning spring DST has 23 hours. Real Cmapss data verifies window endpoints and differences, and PVDAQ data verifies cross-entity/window ranking.

Two server findings limit the oracle. Tree `ALIGN BY DEVICE ORDER BY aggregate` drops groups on the small fixture, reproduced in the existing server and a fresh isolated process using the same server jars. Complete unsorted Tree aggregates are therefore used for numeric checks; Tree top-k is independently sorted from those complete SQL results. Direct ordered-ranking SQL equivalence is checked on Table. The existing server also fails Table field creation with `ClassNotFoundException` for EncodingInferenceUtils despite the class being present on disk; a fresh isolated process succeeds. Root causes are not established. Diagnostics are retained; neither the original server kernel nor benchmark gold was altered. This is scoped SQL semantic validation, not a claim to reproduce defective Tree ORDER BY behavior.

The isolated validation server was stopped before the LLM matrix. Frozen CLI SHA-256: `457e7373b5704cb3a1865c4a24e324646dfb374771dad4ed6a713d5d3e840340`.

## Fixed experiment

All four arms share the previous complete gaps/sessions/extreme capabilities. `grid-base` adds neither feature; `grid-only` exposes window construction; `reduce-only` exposes reduction with explicit bounds; `grid-reduce` exposes both, including inline grids in reduction. This tests available capability plus its advertised contract, not an instruction-only change.

The original eight questions, prompts, public context and DeepSeek configuration are unchanged. One trial per cell, 32 serial trials with rotated arm order; no selective reruns or in-run source changes. All use `deepseek-official/deepseek-flash`, 900 s/task, 120 tool attempts, 32,768 tokens per generation, 180 s/FS process, 8 GiB heap/512 MiB direct memory, 40,000 output characters and 2,000 rows/page. Only dataset_context and iotdb_fs are exposed. SQL, shell, code, gold and external calculators are unavailable to task agents.

## Results

All 32 trials completed under the frozen budgets. Manual requested-answer review is separate from completion; strict alignment includes the window/timezone and entity grain.

| Arm | Strict / 8 | Additional partial | Core / 8 | Total wall (s) | Median wall (s) | Total tokens | Tool attempts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| grid-base | 7 | 1 | 8 | 872.34 | 66.84 | 2,028,815 | 217 |
| grid-only | 6 | 1 | 7 | 651.53 | 68.63 | 1,595,361 | 152 |
| reduce-only | 6 | 1 | 7 | 819.46 | 72.93 | 1,996,119 | 180 |
| grid-reduce | 5 | 1 | 6 | 584.83 | 77.05 | 1,151,312 | 151 |

| Arm | Model/API seconds | Tool interval sum (s) | FS / context calls | Tool errors | OOM | Truncated outputs | Requests / retries |
| --- | ---: | ---: | --- | ---: | ---: | ---: | --- |
| grid-base | 676.23 | 186.65 | 136 / 81 | 3 | 0 | 2 | 88 / 0 |
| grid-only | 453.43 | 188.15 | 102 / 49 | 9 | 0 | 1 | 89 / 0 |
| reduce-only | 658.38 | 151.97 | 120 / 60 | 3 | 0 | 1 | 83 / 0 |
| grid-reduce | 404.54 | 164.90 | 106 / 44 | 3 | 0 | 0 | 86 / 0 |

| Arm | Uncached input | Cached input | Output | Reasoning (included in output) | Cache hit ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| grid-base | 189,072 | 1,711,616 | 128,127 | 78,013 | 90.05% |
| grid-only | 136,526 | 1,381,376 | 77,459 | 43,906 | 91.01% |
| reduce-only | 199,733 | 1,680,640 | 115,746 | 73,156 | 89.38% |
| grid-reduce | 131,071 | 946,176 | 74,065 | 41,234 | 87.83% |

Total tokens include cached input; reasoning is already included in output. Model/API time includes provider/network latency. Tool intervals may overlap. Protocol hashes, feature gates and public-context equality pass; policy audits pass. Host resource monitoring covers this entire matrix.

| Question | Base wall (s) / review | Grid | Reduce | Grid + reduce |
| --- | --- | --- | --- | --- |
| iotdb_cmapss-qa-013 | 40.32 / strict | 30.10 / strict | 48.24 / strict | 36.35 / strict |
| iotdb_cmapss-qa-030 | 67.01 / strict | 91.92 / strict | 67.39 / strict | 51.24 / fail |
| iotdb_gas_dynamic-qa-001 | 62.45 / strict | 94.93 / strict | 75.56 / strict | 84.06 / strict |
| iotdb_gas_dynamic-qa-021 | 131.16 / strict | 82.35 / fail | 106.10 / fail | 93.17 / fail |
| iotdb_ndbc-qa-005 | 21.27 / strict | 54.91 / strict | 42.76 / strict | 49.30 / strict |
| iotdb_ndbc-qa-028 | 66.67 / strict | 54.51 / strict | 90.59 / strict | 86.78 / strict |
| iotdb_pvdaq-qa-006 | 220.89 / strict | 50.63 / strict | 70.30 / strict | 70.03 / strict |
| iotdb_pvdaq-qa-040 | 262.59 / partial | 192.19 / partial | 318.52 / partial | 113.90 / partial |



## What the traces distinguish

### Correct endpoint arithmetic does not resolve the rate denominator

For Cmapss 030, 24 observations span 23 cycle intervals. All four arms recover the correct eight endpoint changes. Base/grid-only/reduce-only give the gold rate with denominator 23. The combined arm chooses denominator 24 in its primary answer, while placing the correct /23 rates in an alternative note. That answer is a strict failure, not rescued by the alternative. The new contract explicitly defines rate from physical endpoint elapsed time; a model can still choose a different denominator when composing a derived multi-entity result. This motivates preserving and consuming unit/denominator metadata through composition, rather than embedding a 23-cycle special case in the command.

### Explicit pooling makes a wrong grain auditable

Gas 021 asks for a difference of two sensor means. Frozen gold ranks separate (experiment, window) rows; methane windows 68–70, 60–62 and 62–64 minutes are the top three. Base reports that correct per-entity ranking and then separately discusses pooling. The other three arms choose pooled-by-window answers, ranking 62–64, 60–62 and 68–70. Reduce-capable arms explicitly call `group_by=pooled`; the computation is correct for that choice, but the choice differs from gold. Thus opening a reduction operator does not itself repair an ambiguous natural-language interpretation. The single-field interface also leaves subtraction of two independently computed means and subsequent ranking to the model; no arbitrary expression was silently added for this question.

### Rechecking a different time axis can dominate a solved task

Base PVDAQ 006 obtains the complete two-session result in its eighth admitted tool call (native sessions takes about 1.57 s), then continues to 46 attempts and 220.89 s. It repeatedly inspects physical Time versus source_time_ms, duplicate shifts, boundaries and alternate measurements. The final requested physical-Time answer is correct, but an unrequested source-time date conversion is wrong. Both axes are real dataset concepts; these extra checks cannot all be dismissed as meaningless. The trace nevertheless shows that native capability completion and agent stopping criteria are separate bottlenecks. A reusable time-axis contract and explicit result provenance may reduce this investigation more directly than adding another session operator.

### Window/ranking composition helps one expensive trace, not every arm

PVDAQ 040 takes 262.59 s / 81 attempts / 943,154 tokens in base, 192.19 s / 35 / 850,142 with grid-only, 318.52 s / 64 / 1,212,395 with reduce-only, and 113.90 s / 36 / 492,614 with both. All four return the correct stream, first/last values and delta, but their primary answer selects June 15 midnight in America/New_York (1560571200000), not gold UTC midnight (1560556800000). Every arm is partial, not strict. Checking a UTC alternative or observing equal endpoint values does not make the two grids identical.

Base invokes stats 47 times on this question. Reduce-only invokes reduce 28 times: seven streams across four manually specified window families (UTC June-1 grid, local June-1 grid, local June-3 grid and a local daily-shifted subset). Combined invokes reduce 12 times, 11 using inline origin/width/timezone and one using explicit bounds; it narrows the per-stream results with delta ranking. This is consistent with easier composition of grid generation and reduction, but one stochastic trace cannot identify a causal interaction. Alternate grid exploration partly reflects an under-specified question and should not all be labeled wasted search.

The temperature streams are a subset of the parent folder. Agents still call individual stream paths and manually merge their candidates; ranking the entire folder would include non-temperature measurements. Native cross-entity ranking is verified by SQL but is not fully exploited here. A general explicit entity-set selector would let callers preserve a discovered candidate set without embedding benchmark-specific names.

### Adoption, output and interface consistency

Across eight tasks, grid-only calls windows 8 times. Reduce-only calls reduce 41 times, including 28 ranked calls. Combined calls windows 4 times (one rejected for unsupported path/field parameters) and reduce 21 times, including 13 inline grids and 10 ranked calls. Both reduction arms use pooled grain twice. Reduce-only requests rate_per_ms twice; combined never requests it, despite having the capability. Four grid-only windows calls are used as generic timezone-label conversion for session endpoints, illustrating reuse beyond the initial window-ranking case.

Reduce-only emits 187,821 response characters from reduce calls, versus 87,739 in combined. Grid-only windows emits 38,247 characters. Repeated descriptors and retyping window arrays therefore remain output/composition costs. Common gap computation returns the correct 48 distinct selected timestamps in every arm; all hourly endpoint/predecessor values were checked. Presentation length still varies substantially.

The reported error counts exclude one pre-dispatch rejected attempt in grid-only and one in combined. Most admitted errors are inconsistent parameter use across legacy commands (limit on stats/find, measurements on tail, bounds/windows on count), dotted rather than slash paths, or missing field. Combined also tries stats at a schema-only parent and passes path/field to windows. These errors remain in the costs; no retry was removed. There are no observed OOM calls or provider transport retries. Peak per-call RSS is about 2,106 / 6,162 / 441 / 516 MiB for base/grid/reduce/combined respectively; availability of streaming operators does not make every legacy command memory-bounded.

## Next direction supported by the traces

1. Preserve the explicit origin/timezone, physical time axis, entity keys and endpoint elapsed-time contract through a typed composition layer. Add generic operations for aligning keyed summaries, combining numeric results and ranking an explicitly selected entity set, instead of leaving subtraction and denominator selection in prose.
2. Allow a generated grid or candidate set to be referenced by later commands, with compact shared metadata. This targets repeated window arrays and per-row descriptors without changing the mathematical operation.
3. Return clear computation coverage and time-axis provenance so the agent can distinguish a complete result from a sample and avoid repeated validation of an already solved task. Test stopping behavior rather than assuming a faster native command makes the whole task faster.
4. Harmonize supported parameters where semantics permit, and improve bounded legacy text paths. Keep rejected calls and memory use observable.
5. Replicate under a frozen contract and evaluate held-out tasks. Annotate ambiguous natural-language grain/timezone interpretations symmetrically for SQL and FS before any new experiment; do not privately teach the FS arm the gold choice. Retain strict and partial scores separately.

## Review convention and limits

Manual review evaluates the primary requested answer against the frozen SQL result, including entity grain and epoch/window labels. A correct value with a wrong timezone/window label is reported as partial rather than a strict pass. A primary wrong rate with the correct rate only in an alternative note is not counted as correct. Explicitly reporting the correct per-entity ranking before a separate pooling alternative can satisfy the requested ranking. Completion, a valid native computation and final-answer correctness remain distinct measures.

This small validation matrix has one stochastic trial per cell. It cannot establish significance, held-out generalization or superiority over SQL. Provider prefix caches are not reset; latency includes provider/network effects. Command adoption is measured separately, because exposing a command does not mean the agent used it. Existing natural-language ambiguity is retained rather than retrospectively changing the prompt to fit gold.

## Evidence

Server root: `/data/zzy/iotdb-fs-exp` on `zzy@101.6.42.165`.

- `results/tqts-fs-window-20261119/`: four arm protocols, full task/tool traces, schedule/completion, resource monitoring, answer reviews, comparisons, native validation and SQL-oracle diagnostics.
- `experiments/tqts-fs-window-20261119/scripts/`: frozen executed adapter and orchestrator. Source/config/selection/jar hashes identify the actual intervention.
- `validation-final/` contains the successful scoped gate; prior failed attempts and their diagnoses are preserved separately. They were resolved before any LLM trial.
- Credential-excluding archive and SHA-256 manifest preserve reproducible evidence. Raw GNU time command lines are excluded because they contain connection arguments; structured resource metrics remain. Private credentials/settings stay on the server; only settings hashes are shared.
- The report and README are written after source freezing; they do not alter the frozen execution sources.

Changes are uncommitted on `fs/inner-view`, on top of `2f89f01e2f`.
