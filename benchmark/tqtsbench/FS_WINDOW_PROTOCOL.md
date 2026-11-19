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

# Window-grid and reduction ablation (2026-11-19)

Revision 2 of the temporal capability study. The previous complete gaps/sessions/extreme build is now the common baseline. Keep the original eight validation questions, unmodified initial prompts/public context, DeepSeek settings, 900 seconds/task, 120 tool calls, 32768 tokens per generation, 180 seconds per FS process, 8 GiB heap/512 MiB direct, 40000 output characters and 2000-row pages. Four arms: `grid-base`, `grid-only`, `reduce-only`, `grid-reduce`; one trial per cell, rotated serial order, no in-run changes or selective reruns. All use the same frozen jar and adapter. Baseline capabilities always include gaps/sessions/extreme. Grid enables native `windows` and explicit origin parameters; reduction enables native `reduce` with explicit bounds; combined allows direct grid construction in `reduce`. This compares capability plus advertised contract, not an instruction-only intervention.

## General semantics, fixed before execution

No benchmark identifiers, expected winners, task-specific intervals, units, entity lists or gold-dependent defaults enter command implementations/help. Native CLI computes over raw SELECT, with no aggregate SQL pushdown.

`windows`: explicit `origin` (epoch ms or ISO datetime), positive width/step, unit `ms|day|month`, timezone (UTC default), inclusive start/end. Fixed ms arithmetic is distinct from calendar day/month arithmetic. Grid starts are `origin + k*step` for integer k, anchored rather than iteratively drifting at month ends. Select starts inside the requested range and clip each effective end to the requested end; no left partial window beginning outside the range. Return nominal half-open bounds, effective inclusive bounds, timezone/local labels and grid specification. Calendar local boundaries must exist and be unambiguous; reject DST gap/overlap boundaries rather than silently shifting. Maximum 256 windows; reject timestamp overflow. No date/origin/timezone inference from task language.

`reduce`: explicit field, metrics count/sum/avg/min/max/first/last/delta/elapsed_ms/rate_per_ms; default count,first,last,delta. `group_by=entity` (default) preserves Tree device or full Table TAG tuple, including NULL; `pooled` merges raw observations per window, so average is sample-weighted, not mean-of-entity-means. NULL target skipped; finite numeric target required. Omit empty groups; do not fabricate a COUNT=0/NULL SQL aggregate row. First/last refer to non-null target at physical time, not source timestamp fields; ties at identical timestamps choose lexicographically smallest entity tuple, NULL TAG first. delta=last-first; elapsed_ms=last_at-first_at; rate_per_ms=delta/elapsed_ms, NULL for zero span. Return endpoints' identities/times. Exact decimal arithmetic avoids INT64 difference/sum overflow; division uses DECIMAL128. No implicit cycle/unit normalization.

Ranking sorts the chosen result metric only after aggregation, globally across result entity/window rows. Direction asc/desc (default desc); NULL scores last; ties use window index then entity identity. Rank is deterministic row_number, not SQL rank/dense_rank. Apply top before output limit/offset. Maximum 65536 nonempty result groups; state bounded by result groups, not input samples. Windows and reduction support structured pagination. Explicit windows remain inclusive and may overlap/repeat. Plain start/end denote one window. Generated grids exclude explicit windows.

## Validation gate

Before model execution: compare native results with actual IoTDB SQL on benchmark data and on a separate synthetic validation fixture, covering two unequal-size entities, weighted pooling, NULL targets/TAGs, ties, endpoints, negative/large values, windows, clipping, DST/calendar and pagination. Use explicit SQL grouping/tie rules; do not claim equivalence for ambiguous natural-language interpretations or floating summation bit patterns. Report Tree/Table validation separately. Freeze source/config/CLI/selection hashes and keep existing experiments unchanged.

## Interpretation

Review requested answers including entity grain and epoch/window labels; report partial alignment separately. Retain all 32 traces, wall/model/tool time, token/cache counters, command adoption/errors, resource usage and remaining reasoning. Eight validation questions with one stochastic trial/cell cannot establish statistical significance, held-out generalization or superiority over SQL. Under-specified window/aggregation language remains an evaluation caveat; do not rewrite questions mid-run to enforce gold.

## Pre-run validation finding and oracle scope

The prepared server's Tree `ALIGN BY DEVICE ORDER BY aggregate` loses result groups on a small fixture, reproduced in both the existing process and a fresh isolated process. Unsorted Tree aggregation returns the complete groups. Preserve this diagnostic rather than forcing FS to reproduce missing rows. Tree numerical/window checks use complete unsorted native SQL aggregates; real Tree top-k is independently sorted from those complete SQL results. Direct SQL global window ranking, ties, top/offset and pooled/entity semantics are verified with Table `date_bin` on an isolated instance using the same server jars. The existing process cannot create Table fields (ClassNotFound for EncodingInferenceUtils despite the class being present in its on-disk jar); a fresh isolated instance works. Do not change the original server/kernel or benchmark gold to conceal these findings. This is limited SQL semantic validation, not proof of identical behavior to the defective Tree ORDER BY implementation.
