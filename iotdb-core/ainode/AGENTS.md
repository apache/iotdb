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

# AINode Guidelines

These instructions apply to `iotdb-core/ainode/` and supplement the
[repository instructions](../../AGENTS.md).

## Python environment and source ownership

- Use an isolated Python environment matching [pyproject.toml](pyproject.toml); the runtime
  currently requires Python 3.11 (`>=3.11.0,<3.12.0`). The Python version in the formatting-only
  CI job does not define the supported runtime.
- AINode implementation lives in `iotdb/ainode/core/`. Keep configuration parsing and defaults
  (`config.py` and `constant.py` there) aligned with `resources/conf/iotdb-ainode.properties`.
- Maven copies the Python client and generated Thrift modules from `iotdb-client/client-py/iotdb`
  into this module. Fix their original sources or Thrift IDL rather than the copied files under
  `iotdb/`. Build metadata under `iotdb/ainode/conf/` is generated from `resources/pom.properties`
  and Git; do not edit generated metadata or files under `build/`, `dist/`, or `target/`.

## Formatting

Always run Black and isort after editing Python files here. Use the versions pinned by
[AINode Code Style Check](../../.github/workflows/greeting-ainode.yml), currently
`black==25.1.0` and `isort==6.0.1`. Use a separate formatter environment if the Black dependency
in `pyproject.toml` differs from the CI pin.

From `iotdb-core/ainode/`, apply and check formatting:

```bash
black .
isort --profile black .
black --check .
isort --check-only --profile black .
```

## Targeted verification

- Existing Python tests use `unittest`. From this module directory, with the runtime environment
  active, select the affected test file, for example:

  ```bash
  python -m unittest discover -s tests -p 'test_config.py'
  ```

- Model tests such as `tests/test_sundial_flow_loss.py` require the relevant ML dependencies.
  Preserve checkpoint parameter names/shapes and tensor shape/gradient contracts when changing
  model code. Keep small unit cases independent of model downloads or a running cluster where
  possible; do not treat a CPU-only run as GPU validation.
- For registration, RPC, or cluster inference changes, read
  [integration-test/AGENTS.md](../../integration-test/AGENTS.md). The cluster verification setup
  is in [Cluster IT - 1C1D1A](../../.github/workflows/cluster-it-1c1d1a.yml): it uses
  `AIClusterIT`, `with-integration-tests`, and `with-ainode`, and runs on a GPU runner.

## Binary packaging

- AINode is opt-in through `with-ainode`. From the repository root, build the module and its
  dependencies with a Python 3.11 executable available under the selected name:

  ```bash
  mvn clean package -pl iotdb-core/ainode -am -P with-ainode -DskipTests -Dpython.exe.bin=python3.11
  ```

- The Maven `compile` phase invokes `build_binary.py`, which installs Poetry/dependencies,
  regenerates `poetry.lock`, and builds or restores a PyInstaller binary. It is not a lightweight
  Python syntax check. The script stores its environment and binary cache under
  `~/.cache/iotdb-ainode-build/` on Unix or `%LOCALAPPDATA%\iotdb-ainode-build\` on Windows;
  `TMPDIR` and pip cache settings do not relocate that script-managed directory. Check cache
  placement against the active workspace policy before invoking it.
- For dependency, dynamic-import, or bundled-resource changes, inspect `ainode.spec` and
  `ainode.xml` and verify the packaged executable, not only imports from the source tree.
  Keep AINode-specific NOTICE and license resources aligned with the bundled contents.
- Read [distribution/AGENTS.md](../../distribution/AGENTS.md) when changing the final AINode
  distribution, and [scripts/AGENTS.md](../../scripts/AGENTS.md) when changing its launchers.
