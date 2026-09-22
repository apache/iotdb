<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Deterministic data migration and answer verification

This stage imports the complete CSV for each of the 274 L1 candidates, exports
one Table TsFile per task, and compares full data and deterministic answers.
It makes no model calls and does not modify the original tasks or gold answers.
The output is verification evidence, not an agent-performance result.

The completed run is summarized in [EQUIVALENCE.md](EQUIVALENCE.md), with
per-task hashes and answer comparisons in
[equivalence-summary.json](equivalence-summary.json). The server configuration
snapshot is [migration-server-49.json](../environment/migration-server-49.json).

## Data contract

- Source: Sonar tasks at `99866c43cd7ceb91a929de745e9f49cea92962e5`, with
  `mrtan/NLQTSBench` CSVs at `81571171c5c614d01698497626bff8eb6ad8dcf4`.
- All 274 source files contain one channel, totaling 43,666,522 rows. Import the
  **entire file**, including observations outside the question's time range.
  Do not trim data using hidden arguments or gold answers.
- One database per task: `nlqts_v1_<original numeric suffix>`; table `raw_data`.
  Example: task `L1_T1_Global_Aggregation_00007` maps to
  `nlqts_v1_00007.raw_data`.
- TABLE schema: built-in `time TIMESTAMP TIME`, `channel_id STRING TAG`,
  `value DOUBLE FIELD`. Original channel identifiers remain strings.
- Interpret naive source timestamps as UTC. Store exact epoch milliseconds;
  reject sub-millisecond input, duplicate timestamps and unordered rows.
  These inputs have no NULLs, non-finite values or irregular sampling according
  to the full static scan. No imputation or deduplication is performed.
- Source floating-point strings are parsed with round-trip precision and emitted
  with 17 significant digits. Verification compares binary64 values, not
  rounded display strings.

`tools/import-data.sh -ft csv -sql_dialect table` is the only bulk import path.
Python normalizes files and orchestrates the CLI; it never inserts data through
Python sessions or row-by-row SQL. A per-task intent/checkpoint records the source
hash and expected schema. Unowned existing databases are refused. Repeating the
same verified source after an interrupted import is safe under IoTDB's identical
timestamp/tag-key semantics; full content checks remain mandatory.

## Four representations of the same data

1. Original source CSV converted to the declared logical schema.
2. IoTDB SQL export, ordered by timestamp, using `tools/export-data.sh`.
3. IoTDB FS `cat -f csv /<database>/raw_data.csv`.
4. Table TsFile exported by `tools/export-data.sh -ft tsfile`, independently
   read through Apache TsFile Python 2.4.0 / its C++ reader.

Each representation must match row count, NULL count, minimum/maximum time and a
SHA-256 over the channel identifier followed by ordered little-endian int64
timestamps and IEEE binary64 values. A separate SHA-256 identifies each physical
source CSV and TsFile. SQL export's literal STRING quotes are removed as a
serialization convention; data values are not changed. TsFile reading consumes
every batch and every row.

The [Apache TsFile reader interface](https://github.com/apache/tsfile/blob/develop/python/examples/example.py)
is used only for independent read verification. Exported files contain the same
`raw_data` table/schema and can be reused by standalone TsFile tooling. They are
not copied directly from live DataNode storage directories.

The current IoTDB FS provider reads database virtual CSV paths. Its `cat` does
not treat an operating-system `.tsfile` path as a database table. Local TsFiles
are separate prepared artifacts; `sketch` identified an exported probe as a
Table TsFile (see `logs/migration-tsfile-sketch.log`). This stage does not
substitute local-file access for the database-backed FS arm.

## Answer contract

The private verifier may read original `meta.args` and gold; neither is a
model-facing tool input. Operation names and thresholds correspond to the public
question. Time boundaries are deliberately evaluated in two tracks:

- **Public:** a year/month is half-open; an explicitly named date range includes
  the whole final calendar day. SQL uses `[start,end)` and FS uses inclusive
  `--end end-1ms`. The actual FS time-filtered output is also checked against the
  corresponding source slice.
- **Private diagnostic:** the exact hidden Timestamp tuple uses both endpoints,
  represented as `[start,end+1ms)`. This track helps identify gold/question
  inconsistencies. It must never silently replace the public question.

The operational definitions are:

| Family | Definition | IoTDB SQL implementation |
| --- | --- | --- |
| Global aggregation | MIN/MAX/mean/range/exact median; compare final 3-decimal answer | MIN/MAX/AVG/PERCENTILE(0.5) |
| Temporal localization | Earliest extremum; first strictly-above point or last strictly-below point | Filter + ORDER BY + LIMIT |
| Interval discovery | Longest consecutive strictly-above run; NULL breaks a run; earliest start breaks duration ties | Cumulative failure count + grouped MIN/MAX |
| Sliding window | Integral K-day sample count at native regular cadence; complete windows only; sample variance `ddof=1`; earliest end on exact ties | ROWS window + COUNT completeness guard + AVG/MIN/MAX/VAR_SAMP |

The threshold convention is a point predicate, not a test of a previous sample's
crossing direction. This convention is checked against the original gold; the
wording still requires disclosure when freezing an evaluation corpus. A K-day
window with N samples spans K days minus one sampling interval between its
reported endpoints. Shorter partial windows are not allowed.

SQL produces each answer with one SELECT statement, including nested/window
operations when needed. The independent CSV oracle uses NumPy/pandas. The FS
route reads through the real FS CLI, then applies external generic deterministic
computation (scalar aggregation or linear-time rolling sums/variance/deques).
**FS + composition is not native FS operator coverage.** These verification
functions are private harness code, not proposed question-specific agent tools.
Formal experiments must expose generic operators and equal computation budgets.

Report strict equality separately from the original `score_one` metric.
An IoU between zero and one is partial credit, not an equivalent answer. Numeric
answers follow the question's three-decimal format; point/interval endpoints
must match exactly. Floating-point or tied-optimum differences remain visible.

## Reproduce on the experiment server

```bash
source /data_01/iotdb-fs-exp/config/environment.sh
PY=/data_01/iotdb-fs-exp/envs/nlqts/bin/python
cd "$EXP_ROOT/src/iotdb"

"$PY" -m unittest discover -s benchmark/nlqtsbench -p 'test_*.py'
"$PY" benchmark/nlqtsbench/migrate.py \
  --iotdb-home "$IOTDB_HOME" \
  --sonar-root "$EXP_ROOT/src/Sonar-TS-iotdb" \
  --data-root "$EXP_ROOT/datasets/nlqtsbench" \
  --output "$EXP_ROOT/results/migration-v1" --workers 8

"$PY" benchmark/nlqtsbench/summarize_migration.py \
  --sonar-root "$EXP_ROOT/src/Sonar-TS-iotdb" \
  --output "$EXP_ROOT/results/migration-v1"
```

The default namespace is `nlqts_v1_`. Use a new `--prefix` and output directory
for changed source data. `--ids` selects fixed probes; `--import-only` checks the
import stage; `--recheck` re-executes verification. Source and implementation
hashes prevent silently reusing stale completed results. Per-task locks serialize
concurrent preparation of the same task. A CLI zero exit code alone is insufficient:
the runner checks error messages, rejected import rows and full exported data.

Each task directory retains normalized input, commands with redacted passwords,
stdout/stderr, SQL text, both CSV representations, TsFile, and `result.json`.
`equivalence-summary.json`, `equivalence.csv`, and `EQUIVALENCE.md` are private
review artifacts containing answers. Do not place them in a model workspace.
`experiment_ready` stays false until semantic review and controlled agent-tool
integration are separately completed.

## Resource adjustment during preparation

The initial DataNode had a 6,552 MiB heap and approximately 1,640 MiB direct-memory
cap. Region creation failed after the first small batch because the configured
direct-buffer accounting budget was only 1,375,731,712 bytes. Each isolated
database creates Schema/Data Regions with fixed buffer reservations.

The experiment DataNode was gracefully stopped and restarted with the **same
6,552 MiB heap and a 32 GiB direct-memory cap**, preserving database isolation,
WAL settings and the original data. This is a cap, not a claim of 32 GiB actual
resident memory. The server had approximately 364 GiB available at adjustment.
Configuration evidence is in `config/migration-memory-adjustment.json`; the
owned `bin/start-iotdb` wrapper persists the change. No other server instance was
restarted. The original failed import attempts and restart logs are retained;
affected tasks are replayed and fully verified rather than counted as successes.

Use this final configuration consistently for both experimental arms. The
installation and validation timings include background imports, cold starts and
retries, and must not be published as SQL-versus-FS performance measurements.

During validation, the SQL generator's original digit-only channel check was
also corrected to accept the audited named channels (for example `MUFL` and
`m_06`) as STRING tag values. A regression test covers both numeric and named
channels. The complete corpus is reverified with the corrected implementation;
old results are invalidated by the implementation hash.
