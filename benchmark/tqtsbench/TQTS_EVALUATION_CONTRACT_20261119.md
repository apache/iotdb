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

# TQTS eight-question evaluation contract — 2026-11-19

This audit uses the **frozen benchmark snapshot used by the experiments**, not a newly downloaded version. The eight question texts, gold queries and match policies exactly match their original package `qa.json` entries. All 20 downloaded evaluator-source file hashes match `code-snapshot.json`. The source repository recorded by that snapshot is https://anonymous.4open.science/r/TQTS-Bench-00CD.

## What the benchmark scores

The contract consists of the original question and public dataset context, the executable gold query, each question's `match_policy`, and the frozen evaluator implementation. Gold and match policies remain evaluator-only: do not give the task agent a question-specific denominator, winning entity, window origin or expected result extracted from them.

`evaluator._evaluate_generated` executes the gold and generated query and calls `results.compare_outputs` on their outputs. Correctness is a binary result match, not SQL-string equality, an LLM prose judgment, or partial credit. The report computes `passed / completed` and separately records `planned_total`. Gold failure/empty output, generated-query failure and comparison/resource failures have distinct failure stages; the native report still includes these failed records in its completed denominator. Report environment failures separately; do not silently remove them or repair gold and call the resulting metric unchanged official accuracy.

All eight policies use:

- `profile=relational`, `rows.mode=unordered`: presentation order is ignored. This does not allow missing/extra observations or silently dropping duplicates. Declared pivot normalization can change the physical long/wide layout while preserving identity facts.
- `columns.mode=gold_subset`, `columns.names=ignore`: match the focused gold columns through the implemented column mapping. Ordinary query aliases are not the target; extra columns may be tolerated. Pivot-bound metric/entity identities have additional constraints, so this is not permission to discard identities or relabel measurements.
- Numeric tolerance: `abs(a-b) <= max(0.01, 1e-9 * max(abs(a), abs(b)))`, using Decimal arithmetic in the comparator.
- Strict NULL handling, including when `nulls` is omitted: NULL, absent field and zero are distinct.
- Timestamp normalization: recognized temporal strings compare as the same UTC instant. Epoch numbers are not universally typed timestamps; ordinary numeric cells can follow numeric tolerance, while pivot identity keys follow the identity comparison. Reuse the actual matcher rather than substituting a custom timestamp tolerance.
- Only Gas 021 explicitly excludes the generic `Time` column. Its selected `block_start_ms` and `Device` remain required. Other questions retain the time columns their gold outputs actually produce.

There is no benchmark-level partial-correct category for a correct temperature paired with the wrong window, or a correct alternative buried in explanatory prose. `gold_subset` can accept extra result columns under its rules, but it does not authorize a reviewer to select a favorable alternative from arbitrary prose or an earlier tool call.

## Per-question contract

The output shapes and example values below come from the saved execution of unchanged gold SQL on our imported data. They are observed reference outputs, not a new certification of the benchmark's original IoTDB 1.3.3 environment; the experimental server is 2.0.11-SNAPSHOT.

| Question | Contract encoded by original gold | Focused output in the saved reference |
| --- | --- | --- |
| Cmapss 013 | FD001 training engines 59 and 60, `sensor_02`; cycles 100–159; physical `Time` in `[100000,160000)`, 10-second windows (10 cycles), `avg` separately per engine, `FILL(PREVIOUS)`. | 6 rows: window start + the two engine means. No empty window occurs in these data, so this instance does not exercise filling. |
| Cmapss 030 | FD001 training engines 1 and 2; signed gap `engine_1.sensor_03 - engine_2.sensor_03`; `[1000,193000)` in 24-second windows. Each result is `(last_value(gap)-first_value(gap))/23`, not `/24`. | 8 rows: window start + rate. The 24 observations span 23 cycle intervals. |
| Gas 001 | Ethylene–CO experiment; maximize `sensor_01`; return `max_by(Time,sensor_01)` and `max_by(sensor_02,sensor_01)`. | 1 row: `5020520` ms and associated `sensor_02=-4.39`. The maximum sensor_01 value is not itself a selected gold column. No explicit general tie-break rule is added beyond the original `max_by` behavior. |
| Gas 021 | Both experiment devices; `[3600000,4200000)` in 120000-ms windows; require `count(sensor_05)>0`; globally select 3 **device-window rows** by `avg(sensor_06)-avg(sensor_05)` descending with `ALIGN BY DEVICE`. Do not pool observations across devices. | 3 rows after excluding generic Time: `Device` + `first_value(source_time_ms+sample_ordinal)` (`block_start_ms`). All three devices are `ethylene_methane`, with starts `{3600000,3720000,4080000}`. Output order is ignored; the sensor-mean differences are ranking expressions, not selected answer columns. |
| NDBC 005 | Station 41001: `min_by(Time,atmp)` and `min_by(wtmp,atmp)` from the same minimum-air-temperature selection. | 1 row: `1675516800000` (2023-02-04 13:20 UTC), sea temperature `20.3`. Preserve the associated-row semantics; do not substitute the minimum sea temperature. |
| NDBC 028 | Station 42001 `M4(time_difference(wspd))`, hourly display windows on Jan 1, 2023 UTC. Read begins at `1672530600000` (previous day 23:50) to retain the first gap's predecessor; display begins at `1672531200000` and ends exclusively at `1672617600000`. First/last/shortest/longest gaps; earliest tied extrema; selected observations are distinct. | 48 rows: selected observation timestamp + gap, all gaps `600000` ms. Each hour selects HH:00 and HH:50; first/min/max coincide at HH:00 and are reported once. A gap is located at the later observation. |
| PVDAQ 006 | System 1200, metric 4199; `GROUP BY SESSION(4h)` over physical Time. A gap **greater than**, not equal to, four hours starts a new session. Return session start, `__endTime`, `avg(value)`. | 2 rows: `(1385764560000,1407053520000,60.00006511738033)` and `(1407070260000,1595797200102,59.90763809908739)`. Source timestamps do not replace physical endpoints. |
| PVDAQ 040 | System 1199 metrics `{2720,2725,2730,2735,2740,2745,2750}`; `[1559347200000,1569888000000)` (June 1–October 1 UTC), fixed 1209600000-ms tumbling windows anchored at June 1 UTC, final window clipped. Nonempty device-window rows; select global top 1 by `last_value(value)-first_value(value)`. | 1 row: window start `1560556800000` (June 15 00:00 UTC), full Device for metric 2745, first `27`, last `47.566667`. New York midnight `1560571200000` is a different window even with equal endpoint values. |

The complete original question, SQL and policy objects, source hashes and saved gold rows are in the evaluator-only `contracts.json` audit artifact. Long/wide reshape/pivot rules are preserved there verbatim; they are not replaced with a handwritten simplified policy.

## Consequences for existing FS results

The earlier 7/8 (base) and 5/8 (combined) figures are **historical manual requested-answer alignment**, not native TQTS execution accuracy. The FS runs did not freeze a single canonical final-result table submission protocol: answers mix prose, nested objects, final commands and evidence references. Do not retroactively convert these figures into official accuracy, or use gold to decide which intermediate result/alternative should count as the final submission.

Specific behavior annotations are retained without changing the score contract:

- Cmapss 030: the combined arm chooses 24 as the primary denominator and lists /23 rates secondarily. Record the denominator choice and alternative separately. Substituting /24 into the saved gold table fails the original comparator (some rows happen to fall inside tolerance, but the whole result does not).
- Gas 021: base discusses the gold entity-window interpretation and a pooled alternative; the other three arms choose pooling. Both interpretations select the same three time positions here. **Different ordering is not a failure under this policy.** Missing/wrong `Device` identity is the substantive mismatch. The old manual review's mention of different ranking order is descriptive only, not an official rejection reason.
- PVDAQ 040: all four primary answers use New York boundaries. Record origin, timezone, selected physical bounds, and any UTC recheck. Public context says the database timezone is UTC; the source's New York fallback describes import conversion and does not replace the gold's numeric window bounds. The imported clock and query-window clock are distinct concepts.
- These are observations about the chosen interpretation and output. One run per cell cannot attribute them causally to interface design or model randomness; apparent ambiguity is not a scoring exemption.

For the next frozen protocol, preserve the exact original gold and matcher. Require a designated final table with column and row values from each arm, apply only fixed, gold-independent serialization, and use the same comparator. Keep SQL final-query execution equivalence as a separate measure where available. Native SQL-query evaluation and an FS final-table extension must be labeled distinctly; sharing the comparator does not make an agent/FS extension the unmodified official benchmark track. Do not introduce new FS computation commands for these eight questions.

## Audit evidence and checks

Local artifact root: `.tmp/tqts-contract-audit-20261119/` in the IoTDB checkout. Server mirror: `results/tqts-contract-audit-20261119/` under `/data/zzy/iotdb-fs-exp`.

- `contracts.json`: eight unmodified source contracts and saved gold outputs with SHA-256 provenance.
- `comparator-checks.json`: 22 deterministic checks using the unmodified comparator. Eight gold self-matches and eight reversed-row comparisons pass. Gas exclusion of generic Time passes; losing Device or adding a duplicate row fails. Cmapss /24 and PVDAQ shifted/missing window starts fail as expected. These are comparator checks, not new agent scores.
- `trace-annotations/<arm>/<question>.json`: 32 sidecars referencing the original final result hash; 12 carry targeted denominator/grain/timezone observations and source excerpts/call references. Empty annotations on the other trials do not certify correctness. Every sidecar marks the audit as post-run, non-grading, and not model-authored.
- `manifest.json`: hashes for this additional audit bundle. Original traces, answer reviews, frozen sources and their archived evidence remain unchanged.

The initial resource-isolated attempt stopped at admission with `resource_busy: global query admission timed out`, before producing a comparison result. For this small local offline check, the original comparator then ran in process with `TSDB_RESOURCE_GUARD=0`; this avoids its subprocess isolation and does not modify matching functions. No model request, live database query, benchmark resampling, prompt change or command implementation change was made.
