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

# SQL/FS experiment server

Deployment prepared on 2026-09-20 on `root@192.168.99.49` (`timecho`).
The experiment root is `/data_01/iotdb-fs-exp`; `/data_01` had approximately
3.4 TiB available at setup. This is an installation and smoke-test environment,
not a completed model evaluation or a performance result.

## Pinned components

| Component | Version / provenance |
| --- | --- |
| IoTDB | Local `fs/inner-view`, `ce9b227b860d2af3beb5e0b603ed39c716a7f855`, 2.0.11-SNAPSHOT |
| Sonar-TS-iotdb | Local `feat/iotdb`, `99866c43cd7ceb91a929de745e9f49cea92962e5` |
| DeepSeek Harness | Official npm `@deepseek-ai/dsh@0.1.5-rc.2`, MIT; source tag `dsh-v0.1.5-rc.2`, `fb2c4b9e698e30edb738bca4cf0618587db7d203` |
| Java / Maven | Existing OpenJDK 21.0.12 / Maven 3.8.7; Java compilation target 17 |
| Python | Existing Python 3.12.3, isolated `envs/benchmark` virtual environment |
| Node / npm | Node 22.23.2 / npm 10.9.8, installed under `envs/node`; official archive SHA-256 verified |
| NLQTSBench | `mrtan/NLQTSBench`, revision `81571171c5c614d01698497626bff8eb6ad8dcf4`; all 274 L1 CSVs, 1,147,254,133 bytes |

IoTDB and Sonar were transferred as Git bundles to preserve exact local commits.
The server's existing SSH identity could authenticate to GitHub; no local private
SSH key was copied. Harness source was fetched over SSH and pinned to the release
tag matching the installed npm package, rather than current `master`.

The server could not reach Hugging Face directly. The L1 data were fetched locally
at the pinned revision, checked against the source Git blob hashes, and transferred
over SSH. `datasets/nlqtsbench/dataset-manifest.json` records every file's SHA-256.
L2–L4 data are outside the current selection scope and are not provisioned.

## Directory layout

All paths below are relative to `/data_01/iotdb-fs-exp`.

| Path | Purpose |
| --- | --- |
| `src/iotdb`, `src/Sonar-TS-iotdb`, `src/deepseek-harness` | Pinned source checkouts |
| `runtime/iotdb` | Symlink to the built all-in-one distribution; configuration, data and server logs |
| `runtime/agent` | Installed Harness npm package and `package-lock.json` |
| `runtime/dsh-home` | Isolated profiles and durable agent session logs |
| `runtime/agent-workspace` | Default disposable working directory for installation smoke tests |
| `envs/benchmark`, `envs/nlqts`, `envs/node` | Harness support, independent data verification, and Node environments |
| `datasets/nlqtsbench/ts_data` | Original L1 CSVs, outside agent workspace |
| `config/requirements.lock.txt` | Exact installed Python package versions |
| `config/agent.patch.yml` | Headless provider/model configuration using environment references |
| `config/environment.sh`, `bin/` | Environment activation and launch wrappers |
| `downloads/`, `cache/`, `tmp/` | Source bundles, archives and installation caches |
| `logs/`, `results/` | Build, verification and smoke-test evidence |

## Use the environment

```bash
ssh root@192.168.99.49
source /data_01/iotdb-fs-exp/config/environment.sh

iotdb-sql -e 'SHOW VERSION;SHOW CLUSTER'
iotdb-sql -e 'SELECT count(*) FROM fs_exp_smoke.telemetry'
iotdb-fs -e 'count /fs_exp_smoke/telemetry.csv'
iotdb-fs -e 'head -n 5 -f csv /fs_exp_smoke/telemetry.csv'
dsh-exp --version
```

The isolated cluster uses loopback RPC `127.0.0.1:32867`; ConfigNode uses
32871/32872 and DataNode internal services use 32873–32876. Its cluster name is
`iotdb-fs-exp`. ConfigNode has a 2 GiB configured memory budget. DataNode was
initially configured for 8 GiB; importing the isolated benchmark databases
required increasing its direct-memory cap. Its current settings are a 6,552 MiB
heap and a 32 GiB direct-memory cap. The cap is not actual resident usage.
See [migration resource evidence](../nlqtsbench/MIGRATION.md#resource-adjustment-during-preparation).
The instance is started manually, without adding an OS boot service:

```bash
/data_01/iotdb-fs-exp/bin/start-iotdb
/data_01/iotdb-fs-exp/bin/stop-iotdb
```

Run only the required command; do not restart during measurements. For access
from a workstation, use an SSH tunnel to loopback:

```bash
ssh -N -L 32867:127.0.0.1:32867 root@192.168.99.49
```

The CLI wrappers accept `IOTDB_USERNAME` and `IOTDB_PASSWORD`; their defaults are
the fresh installation's administrative account. The controlled runner below
enforces the SQL/FS restrictions. Provision dedicated read-only credentials before
formal evaluation as a second database-side boundary.

## Agent entry point

The [official Harness](https://github.com/deepseek-ai/deepseek-harness/tree/dsh-v0.1.5-rc.2)
provides a one-task `headless` profile. The installed release prints the final
answer to stdout and diagnostics to stderr, with durable logs in `DSH_HOME`.
Do not assume the newer development branch's `headless --json` flag is available
in this release. Framework code and its dependency lock are available locally.

The current server stores the model selection and endpoint in
`runtime/dsh-home/settings.yaml`, while `runtime/dsh-home/.credentials.yaml`
stores the API-key value under the reference named by `apiKeyEnv`. Both files
are owner-only (`0600`) and remain outside the agent workspace. Run a task with:

```bash
run-agent 'Your installation test question'
```

`EXP_AGENT_WORKSPACE` can select another working directory. `EXP_LLM_MODEL`,
`EXP_LLM_BASE_URL`, and `EXP_LLM_API_KEY` remain optional per-run overrides;
inherited environment values take precedence over the stored files. The
patch disables telemetry, session-log upload, plugin inventory upload and the
auxiliary title-generation model call. Each headless invocation creates a new
session. Full session logs, rather than final stdout alone, are needed for tool
calls and token/cost analysis.

The DSH measurement adapter is installed from
`benchmark/llm-sql-fs-comparison/dsh_client.py`. It creates one isolated canonical
session log per trial and derives wall time, provider token/cache usage, tool calls,
tool errors and provider retries. Formal trials must use its controlled baseline;
the convenience `run-agent` entry point retains the general-purpose headless tools
and is only for installation checks.

Run a measured SQL or filesystem trial with a fresh output directory:

```bash
python3 /data_01/iotdb-fs-exp/src/iotdb/benchmark/llm-sql-fs-comparison/dsh_client.py \
  --dsh /data_01/iotdb-fs-exp/bin/dsh-exp \
  --patch /data_01/iotdb-fs-exp/config/agent.patch.yml \
  --workspace /data_01/iotdb-fs-exp/runtime/agent-workspace \
  --prompt-file /absolute/path/to/prompt.txt \
  --output /data_01/iotdb-fs-exp/results/trials/sql-example \
  --baseline sql \
  --database '<task-database>'

# For the filesystem arm, use a different fresh output path:
#   --baseline filesystem --database '<task-database>'
```

The final baseline patch disables every installed default tool provider and
registers exactly one tool: `iotdb_sql` for SQL or `iotdb_fs` for filesystem.
The Python runner validates the command, starts the matching wrapper without a
shell, scopes access to the selected database, and passes no model API credentials
to the CLI process. The adapter audits every canonical `request/header` and fails
the trial if another tool is exposed or invoked.

## Verification and remaining work

- The 37-module IoTDB distribution reactor completed successfully with
  `mvn -B -ntp -Dmaven.repo.local=/data_01/iotdb-fs-exp/cache/maven -pl distribution -am -DskipTests package`.
  The build log and exit status are in `logs/iotdb-build.*`. Maven tests were skipped.
- `SHOW VERSION` returned build `ce9b227`; ConfigNode and DataNode both reported
  `Running`. See `logs/iotdb-health.log`.
- The existing CLI comparison fixture contains 200 rows in `fs_exp_smoke.telemetry`.
  All 9 scenarios × 2 interfaces completed with zero exit status, with
  `WARMUP=0`, `REPEAT=1`. These timings are installation checks only.
- SQL count and temperature min/max were 200, 20.0 and 21.99; FS statistics
  returned the matching per-device counts and extrema.
- Python `pip check` passed; all 14 selector tests passed.
- `bin/smoke_agent.py` uses a loopback mock API and a dummy credential to verify
  actual Harness startup, Bash tool execution of both IoTDB CLI interfaces, and
  final-answer handling. Both interfaces returned the expected 200-row count.
  Its result is explicitly marked `local_mock_not_model_evaluation`.
- `results/dsh-adapter-smoke-v1` records a real-model adapter smoke trial. It
  completed with the exact requested answer, one successful Bash tool call, two
  model requests, zero provider retries, and provider-reported token/cache usage.
  This result validates collection and remains outside the formal benchmark.
- `results/dsh-baseline-sql-smoke-v7` and
  `results/dsh-baseline-fs-smoke-v3` are real-model controlled-tool smoke trials.
  Their request headers expose only `iotdb_sql` and `iotdb_fs`, respectively,
  and both returned the expected 200-row count with one successful tool call.
  `results/dsh-baseline-fs-smoke-v2` separately records a disallowed `wc` command
  being rejected as a tool error before the CLI started. These results are setup
  checks, not benchmark samples.
- `results/selection-initial` preserves selection before data arrival;
  `results/selection-with-data` records the full static scan after upload.
  All 274 L1 CSVs passed the static data checks. That historical scan records
  `experiment_ready=false` and does not contain runtime verification. Subsequent
  import and answer evidence is documented in [MIGRATION.md](../nlqtsbench/MIGRATION.md).
  The scan found 220 tasks requiring FS composition, 53 whose
  whole-file scope needs review, and 1 covered with scalar arithmetic.

The adjacent `server-49-manifest.json` is a copy of the server's
`config/deployment-manifest.json`: it records source revisions, artifact/config
hashes, installed versions, smoke results and the selection summary at initial
deployment. The later memory adjustment and verification environment are recorded
separately in the migration evidence; the initial manifest is preserved.

## Completed L1 data preparation

All 274 L1 databases now contain the complete source series: 43,666,522 rows.
Source CSV, SQL export, FS output and independently read TsFile data match for
every task. All 274 SQL and FS-plus-composition answers match the declared oracle.
Original gold agrees with 250 public-question answers; 17 discrepancies are
explained by hidden time bounds and 7 by incomplete gold windows. No model ran.

See [EQUIVALENCE.md](../nlqtsbench/EQUIVALENCE.md) for the complete discrepancy
list and [migration-server-49.json](migration-server-49.json) for the final
configuration, dependency versions and evidence hashes. Server evidence lives
under `results/migration-v1`, including each task's `tsfile/raw_data0.tsfile`.
All 24 selector/verification unit tests and the verification environment's
`pip check` passed. After a successful `FLUSH`, both cluster nodes were `Running`.
Formal agent evaluation remains pending semantic review and trial scheduling.

Real-model connectivity and stored credentials were verified with minimal exact-answer
smoke calls; these are connectivity checks, not benchmark results. The server also
hosts other workloads, so formal latency
experiments need recorded background load or a reserved measurement window.
The native Harness sandbox reported partial enforcement on the host's older
Landlock ABI. Controlled trials therefore rely on removal of the general-purpose
tools plus the command validator and restricted child-process environment; the
canonical tool inventory is audited in every trial.
