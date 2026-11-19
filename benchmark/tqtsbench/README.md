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

# TQTS-Bench discovery pilot

This pilot collects end-to-end agent traces using the existing DSH/DeepSeek
deployment. It is an exploratory SQL-agent workload with on-demand public dataset
context, **not an official benchmark score or a paired SQL/FS experiment**.

The frozen 2026-11-18 selection contains eight original IoTDB questions: the first
medium and first hard question in original QA order for each of the C-MAPSS, gas
dynamic, NDBC and PVDAQ packages. Failed questions are not replaced. No easy
questions, repetitions, or optimization runs are included.

## Agent boundary

The initial prompt contains the original question, assigned database root, engine
version, native time precision and output contract. It does not preload schema or
dataset documentation. Exactly two tools are exposed:

- `dataset_context`: search and paginate the original `manifest.model_context`,
  including field meanings, units, device mappings and time conventions. Responses
  retain JSON-pointer identities, and related records can be fetched by pointer.
- `iotdb_sql`: run bounded read-only Tree SQL and schema inspection through a
  dedicated per-package account. Both textual validation and database ACLs constrain
  access. This account cannot read other package data or write to the database.

There is no shell, arbitrary file access, web, source code, skill loader, subagent,
gold SQL or reference answer tool. The public context is necessary because schema
alone does not describe physical sensor meanings. It is a separate document tool,
not an IoTDB filesystem implementation. In particular, the old SQL dialect skill
pack is not loaded in this pilot; dialect errors cannot establish intrinsic SQL
limitations. `SHOW FUNCTIONS` is also outside the current allowlist.

Budgets are 240 seconds per task, 30 **admitted** tool calls, 45 seconds per query,
24,000 characters of SQL output and up to 40 context entries per page. Requests
after call 30 are rejected; attempted calls can therefore exceed 30. Full raw SQL
output is retained for offline analysis, while the model sees a truncation marker
and must narrow or paginate a query. No local spill path is exposed.

## Deployment and evidence

On `zzy@101.6.42.165`, the experiment root is `/data/zzy/iotdb-fs-exp`.

- Frozen source: `experiments/tqts-discovery-pilot-20261118/scripts/`.
- Output: `results/tqts-discovery-pilot-20261118/`.
- `selection.json` and `protocol.json`: original tasks, matching policies,
  selection rule, limits and source hashes. These are not model-facing resources.
- `trials/<id>/`: prompt, DSH stdout/stderr, complete `session.v3.jsonl`, metrics,
  per-call requests/results and full SQL stdout/stderr, and tool exposure audit.
- `validation/<id>/`: original gold SQL and final SQL execution, kept separate
  from agent calls and agent metrics.
- `summary.json`: per-question metrics and official-matcher execution comparison.
- `credentials/`: private database accounts; exclude this directory from evidence
  bundles and never publish credential values.

The `prepare-only` step creates accounts and configurations; `smoke-only` checks
the actual model and tool path. `--run` resumes this output directory, skipping
tasks that already have `dsh_result.json`. A fresh deployment needs the migrated
TQTS package data, IoTDB runtime, existing DSH adapter and private provider settings.
The account names are fixed per package, so a different output directory on the
same deployment requires explicit account/config reuse rather than blindly
provisioning duplicate users.

```bash
source /data/zzy/iotdb-fs-exp/config/environment.sh
cd /data/zzy/iotdb-fs-exp
python experiments/tqts-discovery-pilot-20261118/scripts/run_pilot.py \
  --root /data/zzy/iotdb-fs-exp \
  --output /data/zzy/iotdb-fs-exp/results/tqts-discovery-pilot-20261118 --run

TSDB_HOST_RESERVE_BYTES=4294967296 TSDB_QUERY_MEMORY_BYTES=536870912 \
python experiments/tqts-discovery-pilot-20261118/scripts/analyze_pilot.py \
  --root /data/zzy/iotdb-fs-exp \
  --output /data/zzy/iotdb-fs-exp/results/tqts-discovery-pilot-20261118
```

The offline matcher reserves 4 GiB host headroom and limits its worker to 512 MiB
on this 32 GiB server. The bundled matcher's 64 GiB default headroom prevents
admission here. Changing these resource settings does not change matching rules.

## Interpretation

`analyze_pilot.py` uses the original TQTS matching policy and reference SQL without
rewriting them. A failed or empty gold result is an environment/reference issue,
not an agent correctness failure. Final SQL equivalence is also **not** final
answer correctness: agents can retrieve intermediate rows and compute a valid
answer afterward. Such answers require separately recorded review against gold
values, units, device identity, window boundaries and ordering.

Report cached input, uncached input and output tokens separately. Total tokens
include cache reads; reasoning tokens are a subset of output tokens and must not
be added again. API retries, rejected tool attempts and semantic query retries
are different quantities. Tool intervals are summed durations, not automatically
the critical-path portion of wall time. Costs include context discovery and all
correction attempts; smoke checks and offline validation are excluded.

Local checks:

```bash
python3 -m unittest discover -s benchmark/tqtsbench -p 'test_*.py'
node --check benchmark/tqtsbench/discovery-tools.mjs
```

## Generous-resource FS control (frozen pre-streaming implementation)

`run_fs_pilot.py` reuses the frozen SQL selection, public contexts and read-only
accounts. The separate output is `results/tqts-fs-control-20261118/`; it never
overwrites SQL traces. Its exact protocol records 900 seconds per question,
120 admitted calls, 32,768 tokens per generation, 180 seconds per FS process,
8 GiB Java heap and 512 MiB direct memory. Questions run serially. These deliberate
resource differences make this a bottleneck comparison, not an equal-budget
performance comparison.

The only model tools are `dataset_context` and `iotdb_fs`. Typed parameters map
to existing read-only native Tree FS operations, including discovery, schema,
statistics, paginated reads and native text commands. Virtual paths stay within
`/root/<package>`; no SQL escape, local files, writes or external computation is
exposed. The adapter adds no window/session/ranking operator or query pushdown.
Pages use native NDJSON values and report `next_offset`; all raw output is retained
offline. Native `stats`/filtered reads may materialize data in the CLI before
filtering. OOM and timeouts are observations, not silently retried by the adapter.

DSH stored settings override plugin patches. The runner therefore creates an
experiment-private `private-dsh-home`, reusing the existing credential file with
mode 0600 and changing only its private generation limit. **Exclude that entire
directory from evidence archives.** The original shared settings remain intact.
The first smoke revealed an effective 8,192-token limit and is preserved as
`smoke-8192`; after correcting the configuration, the second smoke verified the
actual 32,768-token request header before any question was run. Both smokes are
excluded from task metrics.

`analyze_fs_pilot.py` analyzes saved logs without database queries.
`monitor_fs_resources.py` samples host available memory and FS subprocess resource
values every five seconds; it never persists command lines or credentials. The
per-call GNU time records provide CPU and peak RSS; a killed process may have no
final GNU time output, so retain the sampled resource log as well. Original SQL
gold outputs are reused for post-run answer review.

The completed eight-question control, answer review and bottleneck comparison are
reported in [FS_CONTROL_20261118.md](FS_CONTROL_20261118.md).

## Streaming FS base (2026-11-18)

The current development adapter adds native typed `stats.windows` and exposes
`first`, `last`, `min_time`, `max_time`, and `null_count`. Tree/Table native reads
use streaming cursors, time-range predicates and early page termination. CLI
aggregation remains local; exact median uses private external merge runs.
See [implementation, contract, validation and limitations](STREAMING_BASE_20261118.md).

The earlier eight-question control used the frozen scripts and CLI from that
run. Do not substitute these development scripts when reproducing it. On the
server, the new adapter is isolated under
`experiments/fs-streaming-base-20261118/scripts/`, and the previous CLI is kept as
`experiments/fs-streaming-base-20261118/iotdb-cli.pre-streaming.jar`.
The runtime CLI now uses the verified streaming build. No new model benchmark
has been run with this build.

Run the read-only deterministic check on the prepared experiment server:

```bash
source /data/zzy/iotdb-fs-exp/config/environment.sh
python3 experiments/fs-streaming-base-20261118/verify_streaming_base.py \
  --root /data/zzy/iotdb-fs-exp \
  --jar /data/zzy/iotdb-fs-exp/experiments/fs-streaming-base-20261118/iotdb-cli-final.jar \
  --output /data/zzy/iotdb-fs-exp/results/fs-streaming-base-new-run
```

This check reuses the private read-only accounts of the SQL discovery pilot,
never invokes an LLM, and records scalar equivalence, window equivalence,
median checks, wall time, peak RSS and temporary-file cleanup.

## Same-budget streaming FS pilot (2026-11-19)

Use `run_fs_pilot.py --fs-baseline <previous-fs-result>` with a **new** output
folder to compare the streaming implementation against the frozen FS control.
This reuses its private provider settings and verifies the same selection and
FS resource limits. The initial question prompt is unchanged; the current typed
tool contract advertises multi-window statistics. The CLI jar, tool source and
settings hashes are frozen before task execution. Never publish the private
settings or credential directories.

`compare_fs_pilots.py --before <previous-fs-result> --after <new-fs-result>`
compares saved `summary.json` files without database or model calls. It reports
paired wall time, model/tool intervals, token categories, errors, and adoption of
multi-window calls. A historical single-run comparison does not isolate provider,
cache or host variation, or the contributions of individual implementation changes.

The completed eight-question streaming pilot is documented in
[FS_STREAMING_PILOT_20261119.md](FS_STREAMING_PILOT_20261119.md).

## Gap/session and associated extreme rows

The [temporal capability protocol](FS_TEMPORAL_PROTOCOL.md) defines entity grain,
NULL/tie rules, inclusive windows, output pagination and the four-arm validation-set
ablation. Native `gaps`, `sessions` and `extreme` compute from ordered complete
streams in the CLI; no aggregate SQL pushdown is introduced. `gaps` returns an
M4-style first/last/min/max reduction of the fully computed gaps.

Run `verify_temporal_base.py` before `run_temporal_ablation.py`; the orchestrator
requires a passed SQL-equivalence report matching the deployed CLI hash. Feature
gates are enforced both in the DSH schema and typed dispatcher. Full traces and
per-arm frozen protocols are retained. `analyze_temporal_ablation.py` summarizes
the matrix without issuing database queries or LLM requests.

The completed 32-trial matrix, answer-alignment review, final deployment and
remaining bottlenecks are recorded in
[FS_TEMPORAL_ABLATION_20261119.md](FS_TEMPORAL_ABLATION_20261119.md).
