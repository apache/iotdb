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

# Logical backup

Use `tools/logical-backup.sh` on Linux/macOS or
`tools/windows/logical-backup.bat` on Windows. `--help` lists the supported options.
Commands exit with 0 on success and 1 on failure. Errors go to stderr; `--json`
provides a machine-readable result on stdout.

## Capture, check, transport, restore

```sh
tools/logical-backup.sh dump --host source-host --port 6667 --user root \
  --password-stdin --backup-dir /mnt/shared-backups --backup-id migration_001 \
  --scope config,schema,data --model tree,table --mode snapshot

tools/logical-backup.sh inspect --input /mnt/shared-backups/migration_001 --json
tools/logical-backup.sh verify --input /mnt/shared-backups/migration_001 --json
tools/logical-backup.sh pack --input /mnt/shared-backups/migration_001 \
  --output /archives/migration_001.zip

# Local verification; no target connection is opened.
tools/logical-backup.sh restore --input /archives/migration_001.zip --dry-run

# Target version, precision, authentication and protocol capability checks.
tools/logical-backup.sh restore --input /archives/migration_001.zip \
  --host target-host --port 6667 --user root --password-stdin --dry-run

tools/logical-backup.sh restore --input /archives/migration_001.zip \
  --host target-host --port 6667 --user root --password-stdin \
  --checkpoint /archives/migration_001.restore.json

# After an interrupted restore, retain all connection and input options and add --resume.
tools/logical-backup.sh restore --input /archives/migration_001.zip \
  --host target-host --port 6667 --user root --password-stdin \
  --checkpoint /archives/migration_001.restore.json --resume
```

Passwords are read from stdin or `--password-env NAME`. Plaintext password
arguments are rejected. Manifests and checkpoints contain no connection secrets.

`dump` generates a directory, and `pack` only packages an existing directory.
`pack` rejects connection options. Output and checkpoint files must be outside
the input backup, and existing output archives are never overwritten.

All source ConfigNodes, DataNodes, and the CLI must access the same backup
directory at the specified path. This is a shared filesystem workflow, not a
remote download protocol. Do not modify a directory while it is being checked,
packed or restored.

## Scope and boundaries

`--scope` is required and selects `config`, `schema`, `data`, or a comma-separated
combination. Schema capture includes its ConfigRegion definitions, such as
databases and tables. `--model` defaults to `tree,table`.

Authorization objects are excluded by default. Capturing them requires
`--scope config` (possibly combined with other scopes) and `--include-auth true`.
Restoring them also requires `--include-auth true`; inspect/dry-run reports this
selection. Legacy config archives are conservatively treated as containing
authorization objects.

Object filters require a single model. Tree data accepts source path inclusion
and exclusion lists; when capturing config/schema, the source supports a single
prefix or full path without exclusions. Table filters use
`database-regex/table-regex`; exclusions must match either whole databases or
whole table-name patterns. Unsupported combinations fail before task creation.
Data `--start-time` and `--end-time` are inclusive and use source timestamp
precision. These filters do not remove the schema needed to describe selected
objects.

`snapshot` captures finite historical streams. `continuous` requires an ISO-8601
`--until` boundary. Each source stops accepting realtime work at its capture
boundary, drains accepted work, and acknowledges completion before task removal.
The boundary uses source clocks; streams do not represent a global transaction.
`--stop-sequence` is reserved and rejected because this server does not expose a
stable global source sequence. `sourceHighWatermark` records each stream's durable
archive sequence; it is not a cluster-wide source log position.

The root `manifest.json` is the portable entry point. It declares scope,
models, filters, capture settings, streams, relative file paths, full-file SHA-256
digests, counts, status and capture/seal times. Stream manifests describe the
versioned `.pwal` frames, CRCs, segment digests and committed event groups. Legacy
stream-only directories remain readable; packing them adds a root manifest
without changing the input.

Config and Schema snapshots are decoded into logical plans before capture;
data files are decoded into logical tablet requests. Each Region has its own
ordered stream and writer. Pipe parameter validation creates no backup files.

## Interrupted work and conflicts

`dump --resume fail-if-exists` is the default. `new` chooses a new backup identity
if the requested directory exists. `append` requires the same identity, scope
and writing settings. A timed-out command reports the backup, directory and
Pipe identity; query `SHOW PIPE` or rerun the same dump with `--resume append`.
An already sealed matching backup is returned after verification.

Writer sizes, fsync policy and batch/period combinations are validated before
writing. Unknown events fail by default; `--on-unsupported skip` records a gap
and seals the result as `INCOMPLETE`. Inspect, verify, pack and restore reject
incomplete backups unless `--allow-incomplete` is explicit. That flag never
bypasses checksum errors, truncated segments, missing files or invalid groups.

Restore validates locally before connecting, then replays config, schema and
data, sorting equal-type streams by stream ID. Dry-run sends no replay requests.
Target preflight checks version, precision, authentication and protocol
handshakes; object conflicts and object-level permissions are checked during
replay. Only `--conflict fail` is supported. Target errors stop replay and retain
the checkpoint; no schema is silently deleted, overwritten or skipped.

Checkpoints bind the archive/content digest, backup format and identity, target
host/port/user, completed groups and the next request within a group. A request
that may have succeeded immediately before an interrupted checkpoint update can
be retried; only that uncertain request accepts an idempotent-conflict response.
There is no transaction across requests or streams. Completed checkpoints remain
available for audit and repeat invocation.

`export` and `import` are deprecated compatibility aliases. Offline `export` is
`pack`; connected `export` performs `dump` then `pack`; `import` is `restore`.
Both aliases print a migration notice on stderr.
