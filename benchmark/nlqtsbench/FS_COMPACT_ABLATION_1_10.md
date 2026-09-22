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

# Typed-page compact encoding ablation on NTQTSBench tasks 1–10

Run date: 2026-09-22 (Asia/Shanghai)

This experiment isolates the row-cell encoding change from the earlier
structured-page implementation. Both variants retain the same commands, page
fields, `next_offset` behavior, maximum requested page size, and 40,000-byte
model-output budget.

- **Typed page** represents every CSV cell as a JSON string.
- **Typed page compact** represents timestamps and the selected numeric
  measurement as JSON numbers, preserves TAG values as strings, and represents
  the CSV `\N` sentinel as JSON null.

The compact encoder only converts the requested measurement, so numeric-looking
TAG values such as `"007"` keep their string identity. All trials used the same
imported and equivalence-verified data, the same 120-second deadline, and
`deepseek-official/deepseek-flash`. Only `iotdb_fs` was exposed.

## Encoding verification

A direct differential call over the same 500 IoTDB rows produced:

| Encoding | Model page bytes | First row | Runtime types |
| --- | ---: | --- | --- |
| Typed page | 15,924 | `["1589932800000","147","0.141"]` | string/string/string |
| Typed page compact | 13,924 | `[1589932800000,"147",0.141]` | integer/string/float |

The isolated payload reduction was 12.6%. A real DSH smoke trial exposed only
`iotdb_fs`, made one error-free tool call, consumed the mixed-type page, and
returned the expected `0.141` answer.

## Benchmark result

| Metric | Typed page | Typed page compact | Change |
| --- | ---: | ---: | ---: |
| Correct | 4/10 | 4/10 | unchanged |
| Completed | 4/10 | 4/10 | unchanged |
| Model requests | 81 | 83 | +2.5% |
| FS calls | 146 | 167 | +14.4% |
| Session tool errors | 11 | 9 | -18.2% |
| Structured data pages | 89 | 102 | +14.6% |
| Returned rows | 22,182 | 28,905 | +30.3% |
| Total page bytes | 720,862 | 821,220 | +13.9% |
| Page bytes per returned row | 32.50 | 28.41 | -12.6% |
| Maximum page bytes | 16,381 | 15,617 | -4.7% |
| Median wall time | 77.6 s | 69.8 s | -10.1% |
| Total wall time | 757.8 s | 756.5 s | -0.2% |
| Median model time | 51.6 s | 41.1 s | -20.4% |
| Total model time | 530.6 s | 489.1 s | -7.8% |
| Median total tokens | 167,135 | 148,298 | -11.3% |
| Total tokens | 1,666,952 | 1,645,740 | -1.3% |
| Cache-read tokens | 1,233,792 | 1,219,840 | -1.1% |

Both variants answered tasks 2, 3, 5, and 9 correctly. Encoding therefore did
not change the observed capability boundary in this run.

The compact representation reduced bytes per row by the same 12.6% measured by
the controlled smoke test. It did not materially reduce total tokens or total
wall time because the compact run chose a different read strategy: it fetched
30.3% more rows and issued 14.4% more FS calls. Task 8 alone made 35 `head`
calls, and task 10 accumulated 455,791 tokens before timing out. Neither pattern
was caused by a serialization or tool-schema failure.

Median values improved, but the aggregate values are the safer interpretation
for this single non-deterministic run. Total tokens fell only 1.3% and total wall
time was effectively unchanged. Multiple randomized repetitions are required
before assigning a stable latency effect to compact encoding.

## Conclusion

Native JSON scalar encoding is correct and reduces the page payload by about
12.6% for this dataset. It is worth retaining because it is semantically clearer
and strictly more compact. It cannot solve the main performance problem: the
model still transfers raw rows to answer filtered aggregation questions.

The next optimization should extend the controlled `stats` operation with the
existing time, measurement, and TAG filters plus allowlisted aggregate metrics.
That change removes row transfer and manual arithmetic; further page encoding
changes can only reduce the cost of a scan that should usually be avoided.

## Server artifacts

- Typed page: `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-page-v1`
- Typed page compact:
  `/data_01/iotdb-fs-exp/results/nlqts-dsh-fs-1-10-typed-page-compact-v1`
- Direct encoding smoke:
  `/data_01/iotdb-fs-exp/results/typed-page-compact-runner-smoke-v1`
- DSH compact smoke:
  `/data_01/iotdb-fs-exp/results/typed-fs-dsh-compact-smoke-v1`

Each benchmark run contains `trials.json`, `summary.json`, per-trial canonical
DSH sessions, and an `fs-call-audit-v1` directory.
