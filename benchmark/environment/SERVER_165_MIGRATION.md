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

# Dedicated experiment server migration

Migrated on 2026-11-10 from `root@192.168.99.49:/data_01/iotdb-fs-exp`
to `zzy@101.6.42.165:/data/zzy/iotdb-fs-exp`. The old copy is preserved.
This is an environment migration and functional validation, not a model evaluation.

## Access and lifecycle

```bash
ssh -i ~/.ssh/id_ed25519 zzy@101.6.42.165
source /data/zzy/iotdb-fs-exp/config/environment.sh
iotdb-sql -e 'SHOW VERSION;SHOW CLUSTER'
iotdb-fs -e 'count /fs_exp_smoke/telemetry.csv'
dsh-exp --version
```

The cluster is running. Start or stop it manually when needed:

```bash
/data/zzy/iotdb-fs-exp/bin/start-iotdb
/data/zzy/iotdb-fs-exp/bin/stop-iotdb
```

No boot service was installed. RPC remains `127.0.0.1:32867`; ConfigNode
uses 32871/32872 and DataNode internal services use 32873–32876.
Other users' services were left untouched. Use an SSH tunnel for remote RPC access.

## Migrated contents and provenance

The full environment was copied, including runtime database storage, source
checkouts and uncommitted changes, benchmark data/import files, materialized
TsFiles, DSH profiles/settings/credentials, and previous results/traces.
The source database was stopped when copying began. Before relocation, SHA-256,
size and symlink-target comparison matched all 273,847 entries, representing
25,607,403,416 regular-file content bytes, with no missing, extra or changed entries.

| Component | Migrated version |
| --- | --- |
| IoTDB | `fs/inner-view`, `ce9b227b860d2af3beb5e0b603ed39c716a7f855` plus the source server's uncommitted changes; 2.0.11-SNAPSHOT |
| Sonar-TS-iotdb | `feat/iotdb`, `99866c43cd7ceb91a929de745e9f49cea92962e5` |
| DeepSeek Harness | 0.1.5-rc.2; source `fb2c4b9e698e30edb738bca4cf0618587db7d203` |
| Java / Maven | Reused target OpenJDK 21.0.12.1 / Maven 3.9.12 |
| Node | Copied bundled 22.23.2 |
| Python | Standalone 3.12.16, replacing source 3.12.3; both virtual environments retain their installed packages |

This preserves the old server's implementation; it does not upgrade it to the
local checkout's later HEAD. Source working-tree patches and status snapshots
are saved with the migration evidence.

## Host-specific changes

The target has approximately 30 GiB RAM, 20 logical CPUs and a 3.6 TiB `/data`
partition. Absolute environment paths and internal links were relocated; stale
PID files were archived. Both Python environments' launchers were rebuilt and
`pip check` passed, including native NumPy, pandas, PyArrow and TsFile imports.

DataNode retains a 6,552 MiB heap, with direct memory reduced from 32 GiB to
12 GiB. ConfigNode retains a 2 GiB memory budget. The first DataNode startup
failed during direct-buffer reservation for 556 data regions. Setting
`wal_buffer_size_in_byte=4194304` (4 MiB per region) allowed recovery without
disabling WAL. These settings differ from the source host and must be reported
when comparing performance across servers.

The activation script sets `TZ=Asia/Shanghai`, matching the source environment.
The target system timezone remains `America/New_York`.

**Keep `/data/zzy/fs-exp-migration-20261110`**: besides evidence, its
`python/cpython-3.12.16-linux-x86_64-gnu` directory contains the live Python
runtime used by both virtual environments. It is not a disposable transfer cache.

DSH settings and credentials are owned by `zzy` with mode `0600`. No SSH private
key was copied. Existing real-provider configuration was retained; provider
credentials were not revalidated with a successful real model request.

## Validation

| Check | Result |
| --- | --- |
| IoTDB cluster | ConfigNode and DataNode Running; SQL smoke count 200 |
| NLQTS database counts | 274/274 match; 43,666,522 rows |
| NLQTS materialized TsFiles | All 274 copied; three complete-file readback fingerprints match (175,296, 175,296 and 56,958 rows) |
| TQTS native IoTDB packages | All four pass device-count and every field's non-null-count validation |
| FS | Count and head commands successfully read migrated data |
| Adapter/tool tests | 28 passed |
| NLQTS tests | 30 passed |
| DSH local mock | Two requests, successful tool execution and final answer; exit 0 |

| TQTS package | Questions | Devices | Materialized rows |
| --- | ---: | ---: | ---: |
| NDBC | 62 | 20 | 5,025,580 |
| Gas dynamic | 64 | 2 | 8,386,765 |
| PVDAQ | 63 | 328 | 119,469,815 |
| C-MAPSS | 63 | 1,416 | 265,256 |
| Total | 252 | 1,766 | 133,147,416 |

TQTS validation used `--validate-only`; it did not reimport data or rerun gold
answers. Full file checksums establish copy fidelity; aggregate counts and
sampled readback establish runtime access, not a new full answer-equivalence study.

The old mock initially followed durable provider settings and sent an invalid
dummy key to the default endpoint. Its routing was corrected using a separate
mock-only DSH home and loopback provider. The successful mock exercises general
headless tools; controlled SQL/FS restrictions are covered separately by the unit
tests. Formal trials must still use the controlled DSH adapter, not `run-agent`.
No successful real model generation or formal model experiment was performed.

## Evidence and continuation

Evidence is under `/data/zzy/fs-exp-migration-20261110`:

- `migration-receipt.json`, `copy-verification.json`, and source/target manifests;
- `relocation.json`, original configuration backups, working-tree patches;
- `health.log`, `validation/nlqts-counts/summary.json`, `validation/tsfile-readback.json`;
- `tqts-workspace/validation/import-status.json`;
- `validation/dsh-tests.log`, `validation/nlqts-tests.log`, FS smoke logs;
- `results/agent-smoke.json` and the initial mock-routing failure record.

Existing experiment commands in this directory's README remain applicable after
replacing `/data_01/iotdb-fs-exp` with `/data/zzy/iotdb-fs-exp`. Historical results
retain their original provenance and are not new-server performance measurements.
