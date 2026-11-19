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

# FS typed composition: implementation validation (2026-11-19)

Status: implementation and deterministic functional validation complete. **No new
LLM run or correctness/wall-time comparison** is reported. Historical ablations
and official benchmark scoring contracts remain unchanged.

## Implemented

The native `frame --spec JSON` command captures exact entity sets, reusable window
grids and reductions; performs unique-key arithmetic and ranking; propagates
physical endpoints, declared time scales and denominators; and exposes explicit
result reuse, duplicate computation and repeated integrity verification.
See [FS_FRAME_PROTOCOL.md](FS_FRAME_PROTOCOL.md) for full contracts and limits.
The controlled adapter enables this through `--features composition`. Computation
is in Java, not hidden in Python or the evaluation layer.

## Checks and outcomes

| Check | Outcome |
|---|---|
| Focused CLI regression: shell, parser, Tree/Table providers, reduction, frame, JDBC scan | 175 tests, zero failures/errors |
| Additional bounded-workspace check and rerun of frame tests | 9 frame tests, zero failures/errors; 8 overlap the above suite, 1 adds capacity exhaustion/recovery |
| Benchmark Python tests | 15 tests passed, including 10 FS boundary/contract tests |
| JS tool registration, composition-only field exposure and syntax | Passed without model invocation |
| Full reactor test compilation, default and Chinese locale | Both 50-module builds passed |
| FsFrameMessages EN/ZH key parity | 4/4 constants present in both locales |
| Live Tree + Table native CLI/SQL comparison | 29 separate CLI calls, passed on isolated port 33867 |

The live fixture has two selected entities and two windows. Native SQL computes
cross-entity endpoint differences independently. FS results agree in both data
models. Derived rates are 3 and 2 value-units/second for the known three-second
spans (two samples per span). Tests also verify that saved results retain the
second window when the first response is limited to one row, later reads do not
scan, repeated reductions do scan and report their prior result ID, and repeated
integrity checks report previous counts while disclaiming freshness/answer checks.
Table fixtures include an unselected NULL-tag entity; native unit tests separately
select the NULL-tag tuple and reject incomplete TAG tuples.

Unit tests reject duplicate join keys, unmatched exact joins, incompatible grids,
units and endpoint spans, invalid numeric limits and references. They cover NULL
left joins, inner joins, zero-time rates, chained denominator/span propagation,
disk reuse, context isolation, content tampering and capacity exhaustion without
invalidating earlier captures. Existing reduction tests cover INT64 subtraction
overflow and origin/timezone/DST semantics.

The first live attempt passed Tree checks but its Table reference SQL failed with
`DOUBLE vs unknown` for CASE ELSE NULL. Explicit `CAST(NULL AS DOUBLE)` corrected
the reference expression; no native FS behavior changed. Both attempts are kept.

## Reproduction and evidence

Local entry points:

```sh
mvn test -pl iotdb-client/cli -Dxml-format.skip=true \
  -Dtest=FsFrameTest,FsReductionTest,FilesystemCommandParserTest,TreeFilesystemSchemaProviderTest,TableFilesystemSchemaProviderTest,FilesystemShellTest,JdbcSqlExecutorTest
python3 -m unittest discover -s benchmark/tqtsbench -p 'test*.py'
node --check benchmark/tqtsbench/fs-discovery-tools.mjs
```

Server evidence directory:
`/data/zzy/iotdb-fs-exp/results/tqts-fs-frame-20261119/`

- `iotdb-cli-2.0.11-SNAPSHOT.jar`: validated artifact, SHA-256
  `0908b1014059b2d9ba41ad912a8e22b793d48e782962d963edd3bbf56e97c54e`.
- `validation-final/summary.json` and `call-*.json`: all 29 calls, commands and raw
  results from `verify_frame_base.py`; no model credentials or gold answers.
- `validation/`: first attempt and the Table reference type error.
- `frozen-source/`: source archive, manifest, contracts and local test/build logs.

The artifact is installed in this isolated validation directory; the shared
benchmark runtime and frozen historical results were not replaced. Fixtures use
`root.fs_frame_validation_20261119` / `fs_frame_validation_20261119` on the isolated
server. The new artifact can be explicitly selected for the next registered run.

## Limits of the evidence

Correct generic computation and explicit provenance do not establish lower LLM
wall time or higher answer accuracy. The next authorized same-budget run should
measure discovery calls, repeated reductions versus reads, duplicate verification,
manual arithmetic tokens, and semantic alignment failures. The reference interface
remains bounded and conservative: no implicit Cartesian joins, automatic pooling,
unit inference, transactional cross-query snapshot or task-correctness oracle.
