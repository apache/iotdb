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

# AGENTS.md

This file provides repository-wide guidance for coding agents working on Apache IoTDB.
For setup and compatibility notes, see [Coding agents](CONTRIBUTING.md#coding-agents).

## Project Overview

Apache IoTDB is a time series database for IoT data. It uses a distributed architecture with ConfigNodes (metadata/coordination) and DataNodes (storage/query). Data is stored in TsFile columnar format (separate repo: https://github.com/apache/tsfile). Current version is 2.0.11-SNAPSHOT.

## Branch Relationship and Backports

- `master` is the primary development branch for the 2.x line. `dev/1.3` maintains the 1.3.x
  line. They have diverged in supported features, implementation, APIs, and dependencies
  (including TsFile); `dev/1.3` is not a mirror of `master`.
- A request to cherry-pick (CP) a `master` PR to `dev/1.3` authorizes only that PR's changes and
  the minimum compatibility adaptations needed to preserve their logic on the target branch.
  **The backport must be logically and behaviorally equivalent to the original PR. Do not make
  additional changes or use the backport as an opportunity to improve the surrounding code.**

### Cherry-picking from master to dev/1.3

1. Read the source PR's final diff, relevant surrounding code, and regression tests. Identify
   the exact source commit(s) and target base revision before editing. Base the backport branch
   on the requested `dev/1.3` revision, and follow that branch's applicable instructions and
   build/test configuration.
2. Prefer `git cherry-pick -x` for the requested commits. When conflicts or branch differences
   require manual adaptation, preserve source traceability in the commit/PR description and
   resolve each change by its intended behavior. Do not blindly choose one side of a conflict
   or copy whole files from `master` over target-branch implementations.
3. Preserve the source PR's conditions, execution order, return values, exceptions/status codes,
   side effects, boundary cases, and concurrency/resource-lifecycle guarantees. Adapt package
   names, method signatures, or APIs only where the target requires it, using equivalent target
   facilities. A clean cherry-pick or successful compilation alone does not prove equivalence.
4. Keep the diff limited to the requested backport. Do not add unrelated fixes, refactors,
   optimizations, renames, formatting changes, dependency upgrades, or configuration changes.
   Preserve unrelated `dev/1.3` behavior and keep formatter changes limited to the affected code.
   Do not silently cherry-pick prerequisite PRs or import newer `master` features to make the
   patch compile.
5. If a source change has no target counterpart, a prerequisite is missing, or equivalence cannot
   be established, identify the exact gap and the minimum work needed to resolve it. Obtain
   direction before expanding scope or dropping part of the source change; do not substitute
   a weaker fix or describe an incomplete backport as equivalent.
6. Port the relevant regression tests with equivalent scenarios and assertions, adapting only
   to the target test framework. Verify the original failure case and affected boundary/error
   paths on `dev/1.3`; do not weaken assertions or skip failing cases to make the backport pass.
7. Review the complete diff against the target base before finishing: account for every source
   change (including any already present on the target), and map every target hunk to a source
   change or a necessary compatibility adaptation. Report the source PR/commit(s), target base,
   adaptations and why they preserve semantics, and actual validation results/limitations.

## Build Commands

```bash
# Full build (skip tests)
mvn clean package -pl distribution -am -DskipTests

# Build a specific module (e.g., datanode)
mvn clean package -pl iotdb-core/datanode -am -DskipTests

# Run unit tests for a specific module
mvn clean test -pl iotdb-core/datanode

# Run a single test class
mvn clean test -pl iotdb-core/datanode -Dtest=ClassName

# Run a single test method
mvn clean test -pl iotdb-core/datanode -Dtest=ClassName#methodName

# Build with Chinese log & error messages
mvn clean package -pl distribution -am -DskipTests -P with-zh-locale

# Format code (requires JDK 17+; auto-skipped on JDK <17)
mvn spotless:apply

# Check formatting without applying
mvn spotless:check
```

## Module-specific Instructions

Before editing files or running build/test commands for the areas below, read and follow the
corresponding instructions. For changes spanning multiple areas, read each applicable file,
including when the task starts from the repository root.

| Area | Instructions | Module-specific guidance |
| --- | --- | --- |
| AINode | [iotdb-core/ainode/AGENTS.md](iotdb-core/ainode/AGENTS.md) | Python environment, generated sources, formatting, tests, and binary packaging |
| Tree-model SQL grammar | [iotdb-core/antlr/AGENTS.md](iotdb-core/antlr/AGENTS.md) | Lexer keywords, identifier compatibility, and grammar verification |
| Table-model SQL grammar | [iotdb-core/relational-grammar/AGENTS.md](iotdb-core/relational-grammar/AGENTS.md) | Reserved/non-reserved keywords, identifier compatibility, and grammar verification |
| Shared node models and infrastructure | [iotdb-core/node-commons/AGENTS.md](iotdb-core/node-commons/AGENTS.md) | Shared paths, schema, partition/auth/configuration models, and RPC/service abstractions |
| Reusable query computation | [iotdb-core/calc-commons/AGENTS.md](iotdb-core/calc-commons/AGENTS.md) | Reusable operators, column transformations, aggregation, joins, and sorting |
| Distribution and assembly descriptors | [distribution/AGENTS.md](distribution/AGENTS.md) | Artifact identities, package contents, Edge configuration, and packaging verification |
| Launchers, environment scripts, and operational tools | [scripts/AGENTS.md](scripts/AGENTS.md) | Platform compatibility, process ownership, cleanup behavior, and script verification |
| Integration tests | [integration-test/AGENTS.md](integration-test/AGENTS.md) | IT authoring, test profiles, targeted verification, and Edge/IDE runs |

Read both commons guides before adding or moving shared code between these modules. Logic tied to
a concrete node's storage, lifecycle, or execution scheduling belongs in that node's module.

For changes to `EdgeNode` or Edge configuration defaults outside these directories, also read the
distribution and scripts instructions. For SQL parsing or keyword changes in DataNode code, also
read the relevant grammar instructions. Keep shared rules in this file and specialized rules in
the corresponding module file; link to existing guidance instead of copying it.

## Code Style

- **Apache License header**: Every new file committed to the repository MUST include the Apache License 2.0 header. Use the comment syntax appropriate for the file type (e.g., `<!-- -->` for `.md`, `/* */` for `.java`, `#` for `.properties`/`.py`). JSON files are exempt (no comment syntax). Refer to existing files in the repo for the exact format.
- **Spotless** with Google Java Format (GOOGLE style). Import order: `org.apache.iotdb`, blank, `javax`, `java`, static.
- **Checkstyle** is also configured (see `checkstyle.xml` at project root).
- Java source/target level is 17 (compiled with `maven.compiler.release=17` on JDK 17+).

## Architecture

### Node Types

- **ConfigNode** (`iotdb-core/confignode`): Manages cluster metadata, schema regions, data regions, partition tables. Coordinates via Ratis consensus.
- **DataNode** (`iotdb-core/datanode`): Handles data storage, query execution, and client connections. The main server component.
- **AINode** (`iotdb-core/ainode`): Python-based node for AI/ML inference tasks.

### Edge Distribution

`EdgeNode` (`iotdb-core/confignode/src/main/java/org/apache/iotdb/edge/EdgeNode.java`) starts the
ConfigNode and DataNode in one JVM for resource-constrained single-node deployments. Its package
layout and operational requirements are maintained in the distribution and scripts instructions
listed above.

### Dual Data Model

IoTDB supports two data models operating on the same storage:
- **Tree model**: Traditional IoT hierarchy (e.g., `root.ln.wf01.wt01.temperature`). SQL uses path-based addressing.
- **Table model** (relational): SQL table semantics. Grammar lives in `iotdb-core/relational-grammar/`. Query plan code under `queryengine/plan/relational/`.

### Key DataNode Subsystems (`iotdb-core/datanode`)

- **queryengine**: SQL parsing, planning, optimization, and execution.
    - `plan/parser/` - ANTLR-based SQL parser
    - `plan/statement/` - AST statement nodes
    - `plan/planner/` - Logical and physical planning (tree model: `TreeModelPlanner`, table model: under `plan/relational/`)
    - `plan/optimization/` - Query optimization rules
    - `execution/operator/` - Physical operators (volcano-style iterator model)
    - `execution/exchange/` - Inter-node data exchange
    - `execution/fragment/` - Distributed query fragment management
- **storageengine**: Write path, memtable, flush, WAL, compaction, TsFile management.
    - `dataregion/` - DataRegion lifecycle, memtable, flush, compaction
    - `dataregion/wal/` - Write-ahead log
    - `buffer/` - Memory buffer management
- **schemaengine**: Schema (timeseries metadata) management.
- **pipe**: Data sync/replication framework (source -> processor -> sink pipeline).
- **consensus**: DataNode-side consensus integration.
- **subscription**: Client subscription service for streaming data changes.

### Consensus (`iotdb-core/consensus`)

Pluggable consensus protocols: Simple (single-node), Ratis (Raft-based), IoT Consensus (optimized for IoT writes). Factory pattern via `ConsensusFactory`.

### Protocol Layer (`iotdb-protocol/`)

Thrift IDL definitions for RPC between nodes. Generated sources are produced automatically during build. Sub-modules: `thrift-commons`, `thrift-confignode`, `thrift-datanode`, `thrift-consensus`, `thrift-ainode`.

### Client Libraries (`iotdb-client/`)

- `session/` - Java Session API (primary client interface)
- `jdbc/` - JDBC driver
- `cli/` - Command-line client
- `client-cpp/`, `client-go/`, `client-py/` - Multi-language clients
- `service-rpc/` - Shared Thrift service definitions

### API Layer (`iotdb-api/`)

Extension point interfaces: `udf-api` (user-defined functions), `trigger-api` (event triggers), `pipe-api` (data sync plugins), `external-api`, `external-service-api`.

## IDE Setup

After `mvn package`, right-click the root project in IntelliJ and choose "Maven -> Reload Project" to add generated source roots (Thrift and ANTLR).

Generated source directories that need to be on the source path:
- `**/target/generated-sources/thrift`
- `**/target/generated-sources/antlr4`

## Common Pitfalls

### Build

- **Missing Thrift compiler**: The local machine may not have the `thrift` binary installed. Running `mvn clean package -pl <module> -am -DskipTests` will fail at the `iotdb-thrift` module. **Workaround**: To verify your changes compile, use `mvn compile -pl <module>` (without `-am` or `clean`) to leverage existing target caches.
- **Pre-existing compilation errors in unrelated modules**: The datanode module may have pre-existing compile errors in other subsystems (e.g., pipe, copyto) that cause `mvn clean test -pl iotdb-core/datanode -Dtest=XxxTest` to fail during compilation. **Workaround**: First run `mvn compile -pl iotdb-core/datanode` to confirm your changed files compile successfully. If the errors are in files you did not modify, they are pre-existing and do not affect your changes.
- **Single-module / no-`clean` compiles hide cross-module and i18n errors**: `mvn compile -pl <module>` (the thrift workaround above) builds only that module against the **installed** jars in `~/.m2` and the existing `target/` classes; an incremental `mvn test-compile` without `clean` reuses stale `target/*.class`. If you add/rename a `*Messages` constant (or any cross-module API) in module B but `~/.m2`/`target/` still holds B's old artifact, module A referencing the new symbol compiles fine against the stale class — the real `cannot find symbol` only surfaces in CI or a clean build. **Workaround**: for i18n / cross-module edits, verify with a full-reactor compile (no `-pl`, no `clean`): `mvn test-compile -DskipTests` and `mvn test-compile -DskipTests -P with-zh-locale`. Capture the maven exit code directly (`mvn ... ; echo $?`), not via `mvn ... && echo OK` — `set -e` does NOT fail on a command inside a `&&` list, so a `&&` chain can print a false success.

### i18n (Chinese Messages)

The project uses compile-time i18n via the `build-helper-maven-plugin`. The property `i18n.locale` (default: `en`) controls which source directory is added: `src/main/i18n/${i18n.locale}`. Activating `-P with-zh-locale` sets `i18n.locale=zh`, swapping English message constant classes for Chinese ones. Each module that participates has both `src/main/i18n/en/` and `src/main/i18n/zh/` directories containing Java classes with identical structure but different string literals.

**Rule — never inline a user-facing English string literal.** Every string a user, operator, or log reader will see MUST be a `public static final String` constant in the module's `*Messages` class (e.g. `DataNodeQueryMessages`), referenced as `MessagesClass.CONSTANT` — never a `"raw literal"`. This applies to: `LOGGER/log.info|warn|error|debug|trace(...)`, `throw new …Exception(…)`, `super(…)` in exception constructors, `resp.setMessage(…)`, the message arg of `requireNonNull/checkArgument/checkState/checkNotNull`, and the template of `String.format(…)`. A raw literal is invisible to the zh build and renders English under `-P with-zh-locale` — that is the bug this rule prevents. (`toString()` debug output is exempt.) Reviewers and AI agents must reject/fix any new code that introduces such a literal.

**Naming a new constant** (deterministic, matches existing files): `<PREFIX>_<NORMALIZED>_<HASH>` where `PREFIX` is `LOG_` for `LOGGER`/log calls, `MESSAGE_` for `setMessage(...)`, and `EXCEPTION_` for `throw new …(…)`, `super(…)`, and the message arg of `requireNonNull/checkArgument/checkState/checkNotNull` (the three prefixes the codebase already uses — ~`LOG_`/`MESSAGE_`/`EXCEPTION_`); `NORMALIZED` is the English value uppercased with non-alphanumeric runs collapsed to `_` and `%s`/`%d`/`{}` replaced by `ARG`; `HASH` is the first 8 hex chars of `md5(englishValue)`, uppercased — compute with `python3 -c "import hashlib;print(hashlib.md5(b'the english').hexdigest()[:8].upper())"`. Add the constant to **both** the `en` and `zh` files under the **same name** (zh gets a natural Chinese translation; preserve every `%s`/`%d`/`{}` exactly; keep config keys, SQL keywords, class/method names, and product terms like TsFile/DataRegion/Consensus/ConfigNode in English inside the Chinese string). Reuse an existing same-named constant instead of duplicating. **Pure-punctuation fragments** (`. `, `, `, `: `, `.`, etc.) are **exempt** — inline them, do not create a constant (punctuation does not localize, and a hashed `. ` constant hurts readability). A single message that is built from several pieces should be ONE constant holding the full template, not several fragments concatenated at the call site.

**Verify before commit**:
- `mvn spotless:apply -pl <module>` (the PostToolUse formatter does not add the i18n import; spotless does).
- Compile **both** locales: `mvn test-compile -DskipTests` AND `mvn test-compile -P with-zh-locale -DskipTests` (the `.github/workflows/zh-locale-compile.yml` CI enforces the zh build).
- The `en` and `zh` files must stay in key parity (same constant names) with identical format-specifier counts; a raw-literal scan of main java must return nothing.

### Code Style

- **Always run `mvn spotless:apply` after editing Java files**: Spotless runs `spotless:check` automatically during the `compile` phase. Format violations cause an immediate BUILD FAILURE. Make it a habit to run `mvn spotless:apply -pl <module>` right after editing, not at the end.
- **Gson version compatibility**: `JsonObject.isEmpty()` / `JsonArray.isEmpty()` may not be available in the Gson version used by this project. Use `size() > 0` instead and add a comment explaining why.

## Git Commit

- Do NOT add `Co-Authored-By` trailer to commit messages.

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` and the
threat model it links before reporting issues.
