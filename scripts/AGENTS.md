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

# Script Guidelines

These instructions apply to `scripts/` and supplement the
[repository instructions](../AGENTS.md).

## Shared sources and platform behavior

- `conf/` holds environment setup and shared helpers, `sbin/` holds launchers, and `tools/`
  holds operational utilities. Their Windows counterparts live in the respective `windows/`
  directories. Review the counterpart when changing shared behavior and keep arguments,
  configuration handling, and exit status consistent where applicable.
- Scripts are copied into several distributions. Read
  [distribution/AGENTS.md](../distribution/AGENTS.md) before changing packaged file names,
  locations, or dependencies. Update original sources here rather than copies under `target/`.
- Resolve installation paths relative to the script or the supported environment override,
  not the caller's working directory. Preserve `IOTDB_HOME`, `IOTDB_CONF`, data/log directory,
  and `JAVA_HOME` handling. Quote paths and forwarded arguments, including paths with spaces.
- Reuse `conf/iotdb-common.sh` and `conf/windows/iotdb-common.bat` for shared behavior. Preserve
  Maven-filtered placeholders such as `@tsfile.locale.opt@`; inspect the packaged result when
  changing them. Keep supported Linux/macOS branches and explicitly guard platform-specific
  operations such as systemd integration.

## Edge lifecycle and cleanup

- Edge startup must use the configuration-aware ConfigNode and DataNode port checks in the
  shared helpers. Preserve custom configuration paths and fail before launching when required
  validation fails; do not substitute hardcoded default-port checks.
- Edge stop logic must match both the `org.apache.iotdb.edge.EdgeNode` main class and the exact
  installation `IOTDB_HOME`, including when recovering from a missing/stale PID file. Validate
  ownership again before forced termination; never fall back to a global process-name kill.
- Preserve Edge daemon tracking of the forked JVM through `edge.pid`. During cleanup, stop only
  the matching installation's systemd service before stopping the process so it cannot restart
  while its data is being removed. Propagate stop/service failures instead of continuing.
- Preserve cleanup confirmation and force-option behavior. Read effective configured data
  paths, including legacy configuration and multiple data tiers, and validate every target
  before deleting any. Retain protection for filesystem/home roots, parent directories, and
  symlink aliases. Report deletion failures instead of printing a success message.

## Verification

- Run `bash -n` on each edited Bash script. This checks syntax only; exercise changed argument,
  path, process, and failure behavior with an isolated installation or the existing harness.
- For Edge operational scripts or their assembly selection, run from the repository root:

  ```bash
  python3 .github/scripts/test-edge-ops.py
  ```

  The harness covers cleanup, daemon behavior, and assembly selection with temporary fixtures.
  Keep new cases similarly isolated; do not use an existing installation or real system service
  as a cleanup fixture.
- For Windows Edge launchers, also run on Windows from the repository root:

  ```powershell
  powershell -NoProfile -File .github/scripts/test-edge-windows.ps1
  ```

  These checks are wired into [Edge IT](../.github/workflows/edge-it.yml). Report platform-specific
  checks that could not run; Unix results do not establish Windows behavior.
- For packaged launcher or environment changes, rebuild the affected distribution and smoke-test
  its extracted scripts. Verify custom configuration, an unrelated working directory, occupied
  ports, and isolation from another installation when those behaviors are affected. Follow
  [distribution verification](../distribution/AGENTS.md#packaging-verification) for archive checks
  and [Edge IT instructions](../integration-test/AGENTS.md#edge-tests) for Tree/Table read-write
  verification.
