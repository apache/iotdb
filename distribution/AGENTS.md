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

# Distribution Guidelines

These instructions apply to `distribution/` and supplement the
[repository instructions](../AGENTS.md).

## Assembly ownership and artifact identity

- Treat [pom.xml](pom.xml) and `src/assembly/` as the sources of package layout. Edit the
  original configuration or script source, not staged files or archives under `target/`.
- Preserve the default `all-bin`, `library-udf-bin`, `library-pipe-bin`, and `edge-bin`
  assemblies. `with-ainode` and `with-cpp` add their own distributions; keep profile descriptors
  additive so enabling one does not replace the default descriptor list.
- Assembly IDs determine attached artifact classifiers. Keep each ID unique, especially
  `edge-bin`, and check final archive/base-directory names and SHA-512 checksum includes when
  adding or renaming an artifact. Preserve existing outputs for every affected profile.
- Keep LICENSE, NOTICE, and bundled dependency licenses in the resulting archives; reuse
  `src/assembly/common-files.xml` and the AINode-specific component where applicable.
- Preserve executable file modes and Maven filtering for shared environment scripts, including
  `iotdb-common.sh` and `windows/iotdb-common.bat`. Check includes/excludes against both Unix
  and Windows paths. Read [scripts/AGENTS.md](../scripts/AGENTS.md) when changing script contents
  or availability, and [AINode instructions](../iotdb-core/ainode/AGENTS.md) for AINode packaging.

## Edge package boundaries

- [src/assembly/edge.xml](src/assembly/edge.xml) produces
  `apache-iotdb-<version>-edge-bin.zip` for the combined ConfigNode/DataNode process.
- Edge system defaults come from
  `iotdb-core/node-commons/src/assembly/resources/conf/edge/`; Edge logging configuration comes
  from `distribution/src/assembly/resources/conf-edge/`. Keep these separate from standard
  configuration. Standard node, all-in-one, and integration-test packages must not acquire
  Edge defaults; preserve `edge/**` exclusions wherever shared configuration directories are
  copied recursively.
- Package the Edge start/stop pair and compatible CLI/tools. Exclude tools requiring standalone
  node launchers or environment files, including standalone daemon, destroy, and health-check
  scripts. The explicit Edge daemon/destroy replacements are intentional exceptions; retain
  their inclusion without restoring the standalone tools.
- Check the standalone node and integration-test assembly descriptors as well when changing
  shared configuration or script selection; a correct Edge archive does not establish that
  other packages are unaffected.

## Packaging verification

For changes to assembly descriptors, packaged resources, or artifact wiring:

1. Run the clean distribution build from the
   [root build instructions](../AGENTS.md#build-commands). Enable any affected optional profile
   as well; an old archive in `target/` is not evidence of a successful new build.
2. Inspect the produced archives and SHA-512 files under `distribution/target/`. Verify all
   expected artifacts for the active profiles, archive roots, configuration, dependency jars,
   licenses, Unix executable modes, and Windows entries. Confirm unwanted tools and Edge-only
   defaults are absent from the other packages.
3. Inspect the Edge zip and verify startup/shutdown from a freshly extracted package using an
   isolated installation. For Edge package selection changes, also run the script/assembly
   checks described in [scripts/AGENTS.md](../scripts/AGENTS.md#verification).
4. For Edge runtime changes, follow the build and Tree/Table read-write verification in
   [integration-test/AGENTS.md](../integration-test/AGENTS.md#edge-tests). Keep the IT commands
   there rather than duplicating them here.
