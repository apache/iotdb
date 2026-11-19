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

# Temporal capability ablation (2026-11-19)

## Frozen comparison

The same eight original TQTS questions selected on 2026-11-18 are the validation set. Each question is run once in each arm: base (streaming statistics), temporal (base plus gaps/sessions), extreme (base plus associated extreme rows), both. All arms use the same new CLI jar and common typed adapter; capabilities are gated in both the advertised tool schema and the dispatcher. Common help explicitly preserves object/window grain. This is a contemporaneous capability ablation, not an exact replay of the historical tool contract.

Question order is frozen; arm order rotates by question index. All 32 tasks run serially. Same DeepSeek configuration, public context, read-only accounts, 900 s/task, 120 calls, 32,768 generation tokens, 180 s/FS process, 8 GiB heap, 512 MiB direct memory, 40,000 output characters and 2,000-row maximum page. No source, SQL, external computation or gold is accessible to the task agent. No optimization during execution; complete traces and protocol hashes are retained.

## Semantic gate

Before LLM execution, run `verify_temporal_base.py` against the imported database with a 512 MiB heap. Compare the four affected original gold queries (Gas 001, NDBC 005/028, PVDAQ 006), plus inclusive endpoints, empty/duplicate windows, pagination, outside-window predecessor, clipped sessions and exact threshold equality. Unit tests cover Table TAG tuples including NULL, exact INT64 extrema, tied extrema, NULL associated fields, non-finite rejection, gap overflow and streaming cancellation. Table SQL equivalence is not inferred from Tree benchmark checks.

## Command semantics

All ranges use inclusive epoch-ms endpoints. Convert SQL `[a,b)` to `[a,b-1]` at millisecond precision. Windows may overlap or repeat; results retain their zero-based index. Tree entities are devices; Table entities are complete TAG tuples, including NULL. Commands never pool entities or perform global ranking. Paging is over output rows on an unchanged data snapshot; it is not a transactional snapshot guarantee across calls.

- `gaps PATH --field F [--windows a:b,... | --start a --end b]`: scan every non-null F observation through the largest upper bound, including the predecessor outside each output window. Assign a gap to its later observation. Return distinct first/last/min/max gap observations per entity/window, with earliest ties and role labels. No observation/no predecessor means no gap row. This is complete gap computation with an M4-style output reduction, not an API returning every gap. Durations are exact decimal strings, including differences beyond signed INT64.
- `sessions PATH --field F --gap-ms G [--start a --end b]`: finite numeric F, NULL skipped; split iff successive gap > G. Time filtering occurs before segmentation: bounded calls return clipped sessions and restart indices. Return start/end, count, mean, first/last per entity/session. No windows argument. One active accumulator per entity; completed sessions stream to the output page.
- `extreme PATH --field F --mode min|max [-m associated] [--windows a:b,... | --start a --end b]`: finite numeric comparison field, earliest timestamp on ties, retain associated NULLs, target always included. Return entity/window keys, timestamp, extreme value and associated values. Empty windows have no row; exact comparison avoids INT64 precision loss. It does not imply identical tie behavior to every SQL `min_by/max_by` implementation; an explicit earliest-time SQL tie rule defines the contract.

All commands support `-f ndjson -n N --offset K`; typed adapter returns `next_offset` and fetches one lookahead row. Native integer timestamps/counts/durations preserve exact decimal encoding. Non-finite target values in sessions/extreme are rejected explicitly. SQL equivalence is limited to supported finite numeric inputs, the same non-null observation domain and explicit entity/window/tie rules. No aggregate SQL pushdown is added.

## Measurement and interpretation

Review final answers at the requested entity and temporal grain against frozen SQL outputs; record incomplete output separately from the core winner/value correctness. Report task wall time, summed model latency, tool time, tokens, cache counters, tool calls/retries and use of new commands. Distinguish native execution cost from remaining model reasoning. One stochastic trial per cell and uncontrolled provider prefix-cache state do not establish a general speedup or a statistical causal estimate. Keep all failures and do not replace questions or selectively rerun failures.
