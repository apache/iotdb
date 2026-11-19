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

# Two-arm composition ablation — frozen before execution

Eight unchanged original TQTS pilot questions, one trial per arm (16 cells).
Arms: `grid-reduce` versus `composition` (adds the complete frame interface).
Same validated CLI artifact SHA-256
`0908b1014059b2d9ba41ad912a8e22b793d48e782962d963edd3bbf56e97c54e`
in both arms. All existing native functions and non-frame contracts are identical.
This estimates the added capability/advertised-interface bundle, not individual
contributions of entity sets, reuse, arithmetic or verification metadata.

Use existing deepseek-official/deepseek-flash configuration and original public
context. Per trial: 900s, 120 calls, 32768 generation tokens, 180s/FS process,
8GiB heap, 512MiB direct memory, 40000 output characters, page limit 2000.
Serial schedule alternates arm order by question index. No model warm-up,
selective reruns, new commands or prompt/interface changes after starting.
Stop on infrastructure or tool-policy failure; retain all prior cells. Settings,
source, CLI, selected questions and public context are hashed. Credentials stay
private. Task agent sees only dataset_context and controlled iotdb_fs.

## Shared final submission and scoring

Both arms get the same generic final-table-v1 instruction in addition to the
original question/prompt. A single `final_table={columns:[...],rows:[[...],...]}`
is designated as the primary answer. The frozen serializer rejects malformed,
missing or ambiguous tables; it converts rows to JSON objects without inferring
missing times/entities, changing units, recovering tool results or choosing prose
alternatives. NULLs and duplicate rows are preserved. No question-specific output
shape or gold information is supplied to the model.

Offline only: use saved unchanged gold outputs and original match_policy with the
snapshot's unmodified comparator. Validate its source hashes first. Record binary
match and failure details. This is a **FS final-table extension using the original
matcher**, not the original SQL-execution track. Existing manual prose-alignment
scores are historical context, not directly comparable accuracy. No DB gold query
runs concurrently with the model tasks.

Primary descriptive outcomes: matched final tables / 8, wall-time median and sum,
token sum/median, model/tool time, calls/errors/retries/cache input. Include failed
or unfinished cells in planned denominators. Inspect frame adoption and operation
counts, explicit reads, duplicate reductions and repeated integrity verifications;
trace classifications remain post-run observations, not scoring exceptions.

One stochastic trial per cell, eight reused development questions, provider/cache
and host variation uncontrolled. Do not claim generalization, causal component
attribution or statistically established speedup. Any next change requires a new
frozen run; failed cells are not silently replaced.

## Infrastructure amendment before replacement run

The first launch completed one baseline cell, then composition failed before any
model call because the installed DSH value-schema DSL rejects JSON-Schema-style
required arrays and requires explicit additionalProperties on object nodes.
Correct only this authoring syntax; native CLI, prompts, budgets and final-table
serializer stay identical. Validate all feature schemas using the real installed
DSH compiler without LLM requests. Archive the aborted attempt; start a separate
replacement matrix of all 16 cells, not a selective rerun or merged result.
