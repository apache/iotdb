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

# Native typed FS result composition

This capability completes the native CLI substrate. It does not change benchmark
questions, gold answers, scoring, model prompts, or any historical run. No new
LLM ablation is part of this implementation validation.

## Interface

Native entry: `frame --spec '<JSON object>'`. The controlled DSH tool exposes
`command="frame"` plus typed fields under the optional `composition` feature.
The adapter validates and serializes parameters; all selection, reduction,
arithmetic, persistence and verification execute in the native Java CLI.
`help frame` and tool `help.frame_contract` enumerate parameters and semantics.

| op | Required fields | Result |
|---|---|---|
| entities | entries | Immutable explicit entity set; exact Tree device paths, or Table path plus complete TAG tuple (string/NULL) |
| windows | start_ms, end_ms, origin, width | Complete reusable native window grid |
| reduce | entities_ref, windows_ref, field | All nonempty entity/window reductions, with physical endpoint metadata |
| combine | left_ref, right_ref, keys, expression | Unique-key exact/inner/left alignment, computed `value` and provenance |
| derive | input_ref, expression | Unary typed computation; optional value ranking |
| read | ref | Captured result page, no database scan |
| verify | ref | Content integrity, declared coverage and repeated-verification count |

All operations accept `limit` (1..2000, default 200) and `offset` (0..65536).
Responses contain opaque `result_id`, `kind`, `metadata`, `captured_at`, `rows`,
`total_rows`, `returned_rows`, `has_more`, `next_offset`, `reused`, and
`scan_performed`. Verification returns no rows. Pages are bounded by both rows
and serialized size; the actual continuation is `next_offset`.

`entities` stores a declaration, not a database-existence assertion. Reduction
validates numeric schema and exact Tree device scope; Table TAG names must equal
the complete schema tuple. Multiple selected TAG tuples for one table share its
scan. No parent-folder expansion or implicit entity pooling. Empty windows or
entities with no non-NULL target observation produce no reduction row; omission
is not zero. Metadata retains selected-entity count and this omission rule.

`windows` uses existing `windows` semantics: inclusive requested bounds,
origin-aligned starts, clipped inclusive ends, fixed-ms or calendar-day/month
steps, explicit timezone/DST validation, at most 256 windows. The saved grid
contains **all** windows even when the response displays one page. Reduction
stores all groups before pagination, so later joins/ranks never operate on an
accidentally truncated first page.

## Keys, arithmetic and time contracts

`combine.keys` must include `window_index` and normally `entity`; the user must
explicitly omit entity to align different entities. Duplicate keys are rejected
on either side, preventing accidental Cartesian products. `exact` (default)
requires identical key sets; `inner`/`left` report unmatched counts. Window and
physical-time contracts must match exactly. This conservative check can reject
equivalent but differently expressed grids; reuse the same windows reference.

Expression AST (no SQL, shell or arbitrary evaluation):

- Column: `{"source":"left","column":"delta"}` (or right).
- Constant: `{"constant":"1000","unit":"ms"}`.
- Binary: `{"op":"add|sub|mul|div","left":<expr>,"right":<expr>}`.
- Rate: `{"op":"rate","numerator":<expr>,"source":"left|right"}`.

`derive` binds its input as left. Numeric values remain decimal strings. Addition,
subtraction and multiplication preserve decimal precision within bounded numeric
size; division uses DECIMAL128. NULL propagates; zero division yields NULL.
Add/sub require equal declared unit labels; there is no inferred unit conversion.
Endpoint-derived arithmetic requires aligned physical first/last timestamps.
Derived values retain endpoint spans and denominator provenance through chaining.

Rate divides by **elapsed_ms / time_unit_ms**, never sample count or nominal window
width. `time_unit=ms,time_unit_ms="1"` is the default. Any other label requires
an explicit positive scale; caller-declared domain units are not inferred from
question text. `sample_count`, `elapsed_ms`, `first_at`, `last_at`, physical time
axis, source entities and result references remain inspectable.

`rank_by="value"`, `order=asc|desc` (default desc), and optional `top` apply after
computation and before paging. NULL sorts last; ties use deterministic canonical
row order. Ranking is row-number selection, not SQL RANK/DENSE_RANK tie expansion.

## Reuse and repeated checks

Each immutable capture has a SHA-256 content identifier. `read` explicitly reuses
that capture; it never asserts database freshness. Repeating a reduction performs
a fresh scan, returning `duplicate_request` and `previous_result_id` rather than
silently substituting a cached answer. Request identity ignores page controls.
`verify` checks saved content integrity and declared operation coverage, reports
`previous_verifications`, and explicitly sets database freshness and task
correctness checks to false. It is neither an answer oracle nor a forced stopping
rule. This makes repeated checking observable without inventing new evidence.

The workspace is bounded to 128 captures / 64 MiB, with 16 MiB per capture and
65536 result rows. Capacity errors never mean partial success. Native interactive
use defaults to memory. Operators can set `IOTDB_FS_WORKSPACE` and
`IOTDB_FS_CONTEXT` for cross-process reuse. The adapter uses task-private storage
bound to package, account, server, dialect and CLI artifact hash. Models receive
opaque references, never local paths. File locks, atomic writes, content hashes,
context checks and symlink rejection guard the private capture store.

Captures from different scans do **not** constitute a transactional database
snapshot. `scope_complete` means the declared operation completed (with explicit
join/top metadata), not that all relevant entities were discovered or the task
answer is correct. Selection/context discovery remains the agent's responsibility.
The present result API captures reductions and grids; it does not promise generic
raw-row materialization, arbitrary relational algebra or automatic fresh caching.

## Validation

Run focused Java FS tests and the adapter boundary tests. `verify_frame_base.py`
uses port 33867 on an isolated fixture database; it creates its own Tree/Table
namespaces and compares two-window cross-entity endpoint differences with native
SQL. It also checks endpoint-denominator rates, pagination, repeated reduction,
immutable result reuse and duplicate verification across separate CLI processes.
No LLM calls or benchmark answers are involved. Actual outcomes are recorded in
`FS_FRAME_VALIDATION_20261119.md` after execution.
