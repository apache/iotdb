<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# NLQTSBench 1–10 filesystem call audit

This is a diagnostic run for FS optimization, not a benchmark result. The run used
the controlled `iotdb_fs` interface and original source questions 1–10. It did not
expose source files, oracle files, SQL, shell, Web, or local filesystem tools to the
model. The SQL-arm dialect issue is outside this audit and is not rerun here.

Server evidence:

- Run: `/data_01/iotdb-fs-exp/results/nlqts-dsh-1-10-pilot-v1`
- Metadata-only call audit: `fs-call-audit-v1/fs-call-audit.json`
- Human-readable audit: `fs-call-audit-v1/FS_CALL_AUDIT.md`

## Observed outcome

The strict answer result was 1/10. Task 5 also contained the correct numeric answer
but violated the required single-number output format. Seven tasks reached the
120-second deadline, one ended at the model token limit, and two completed.

| # | Operation | Scoped rows | Result | Calls | Call errors | Wall s | Tokens |
| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | median, 12 days | 1,152 | correct | 12 | 2 | 44.3 | 50,627 |
| 2 | minimum, month | 2,880 | timeout | 43 | 16 | 120.1 | 250,868 |
| 3 | range, month | 2,976 | timeout | 51 | 17 | 120.1 | 225,683 |
| 4 | average, month | 2,880 | timeout | 51 | 26 | 120.1 | 142,750 |
| 5 | range, 30 days | 2,880 | correct value, invalid format | 18 | 2 | 78.2 | 177,752 |
| 6 | median, year | 35,040 | timeout | 29 | 3 | 120.1 | 127,908 |
| 7 | average, month | 2,880 | timeout | 30 | 8 | 120.1 | 158,004 |
| 8 | median, year | 35,040 | timeout | 75 | 39 | 120.1 | 184,750 |
| 9 | minimum, month | 2,976 | model token limit | 27 | 10 | 80.3 | 131,413 |
| 10 | maximum, year | 35,040 | timeout | 51 | 23 | 120.1 | 178,577 |

The ten FS trials consumed 1,043.6 seconds and 1,628,332 tokens. DSH attributed
651.0 seconds to model requests and 328.5 seconds to tool intervals. The model made
387 FS calls.

## Where the calls came from

| FS command | Calls | Errors | Main use |
| --- | ---: | ---: | --- |
| `cat` | 128 | 40 | bounded probes and raw-row pagination |
| `help` | 105 | 44 | repeated discovery of command syntax and missing operators |
| `head` | 47 | 14 | schema/value probes and attempted bounded reads |
| `stats` | 34 | 23 | repeated attempts to add time/tag filters |
| `count` | 19 | 11 | repeated attempts to add time/tag filters |
| `meta` / `schema` / `ls` | 38 | 9 | object and schema discovery already supplied in the prompt |
| other | 16 | 5 | `tail`, `find`, `file`, `stat`, and rejected aggregate names |

The exact error sources were:

| Source | Count | Interpretation |
| --- | ---: | --- |
| Invalid or disallowed `help` target | 44 | The free-form interface invited shell-style capability discovery, while the validator only allowed `help <whitelisted-command>`. |
| Unsupported option on a valid command | 33 | Mostly time/tag filters on `stats`/`count`, or invented aggregation/filter flags on `cat`/`head`. |
| ISO/date text passed to `--start` | 31 | FS accepts epoch-millisecond bounds, but the prompt described UTC timestamps without stating the accepted representation. |
| Missing, guessed, or out-of-scope path | 26 | Includes guessed subpaths/query strings and five attempts to read DSH spill files, which the database-only boundary correctly blocked. |
| FIELD/TAG/filter misuse | 6 | The model attempted FIELD predicates through TAG-only filters or aggregate expressions as measurement names. |
| Disallowed command/follow mode | 4 | `sum`, `avg`, `grep`, or `tail -f`; all were rejected before execution. |
| Unmatched at process timeout | 2 | The process deadline interrupted an in-flight call. |

Of 146 call-level failures, 94 were rejected before the CLI started and 52 were CLI
errors. Exact command duplication was low; the high count came from trying many
slightly different syntaxes rather than repeating one request.

## Protocol and integration overhead

The current free-form `command` parameter is the first optimization target:

- `help` alone accounted for 27.1% of all calls. Sixty-one help calls succeeded and
  forty-four tried a form rejected by the controlled adapter.
- Seventy-three `cat` calls requested only 1–96 rows. Only fifteen calls used an
  offset, so most probes did not advance a complete reduction over the requested
  interval.
- Six returned outputs crossed the adapter limit. DSH exposed a local spill-file
  path, but the controlled FS tool correctly refused five attempts to read it.
  The agent therefore received a recovery path it was not authorized to use.
- Successful and failed CLI attempts together consumed 303.5 seconds. The primary
  DSH result reported only 267.8 seconds because plugin metadata is lost when the
  plugin throws a tool error. Error-inclusive CLI time is underreported by 35.7
  seconds (11.8%).

These failures can be reduced without changing IoTDB query capability: use typed
arguments for command, fixed task path, integer time bounds, measurement, limit and
offset; embed a compact filtered command grammar; and return bounded pages with an
explicit next offset instead of a local spill path.

## Remaining FS capability bottleneck

Documentation and typed arguments do not remove the main computational gap.
`stats` and `count` operate on the whole object and cannot consume the time range
selected by `cat`. The ten questions cover 123,744 scoped rows. Consequently, the
agent must transfer raw rows and maintain min/max/sum/count or an ordered median
across pages. A yearly question contains 35,040 rows; at the observed median `cat`
CLI time of about 1.32 seconds, row pagination alone cannot fit comfortably within
the 120-second trial budget, before model processing.

For the next FS optimization iteration, the smallest useful database-side
composition primitive is a read-only time/tag-scoped view that `stats` can consume,
or equivalently time/tag options on `stats`. It must cover count, min, max, sum and
mean; the three median questions additionally require an exact percentile/median
operator. A `filter -> ephemeral inner view -> stats/percentile` design preserves
the FS task-decomposition hypothesis better than exposing a question-specific
solver. Typed paging remains useful for tasks whose answers require rows rather
than aggregates.

## Optimization order

1. Replace the free-form FS command string with a typed, database-scoped read API
   and inline its complete allowed grammar.
2. Use structured bounded pages with `next_offset`; never give the model an
   inaccessible local spill path.
3. Add a general read-only scoped-view plus statistics/percentile composition path.
4. Preserve error metadata and CLI timing for rejected CLI executions.
5. Rerun the same frozen 1–10 questions and compare invalid-call rate, tool calls,
   transferred bytes, wall time, tokens and strict correctness against this run.
