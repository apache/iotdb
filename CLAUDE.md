# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache IoTDB is a time series database for IoT data. It uses a distributed architecture with ConfigNodes (metadata/coordination) and DataNodes (storage/query). Data is stored in TsFile columnar format (separate repo: https://github.com/apache/tsfile). Current version is 2.0.7-SNAPSHOT.

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

# Format code (requires JDK 17+; auto-skipped on JDK <17)
mvn spotless:apply

# Format code in integration-test module
mvn spotless:apply -P with-integration-tests

# Check formatting without applying
mvn spotless:check
```

## Integration Tests

Integration tests live in `integration-test/` (not included in default build). They require the `with-integration-tests` profile:

```bash
# Build template-node first (needed once, or after code changes)
mvn clean package -DskipTests -pl integration-test -am -P with-integration-tests

# Run tree-model ITs (simple: 1 ConfigNode + 1 DataNode)
mvn clean verify -DskipUTs -pl integration-test -am -P with-integration-tests

# Run tree-model ITs (cluster: 1 ConfigNode + 3 DataNodes)
mvn clean verify -DskipUTs -pl integration-test -am -PClusterIT -P with-integration-tests

# Run table-model ITs (simple)
mvn clean verify -DskipUTs -pl integration-test -am -PTableSimpleIT -P with-integration-tests

# Run table-model ITs (cluster)
mvn clean verify -DskipUTs -pl integration-test -am -PTableClusterIT -P with-integration-tests

# Run a single IT class (tree-model simple, 1C1D)
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -P with-integration-tests

# Run a single IT class (tree-model cluster, 1C3D)
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -PClusterIT -P with-integration-tests

# Run a single IT class (table-model simple, 1C1D)
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -PTableSimpleIT -P with-integration-tests

# Run a single IT class (table-model cluster, 1C3D)
mvn clean verify -DskipUTs -Dit.test=ClassName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -PTableClusterIT -P with-integration-tests

# Run a single test method within an IT class (use ClassName#methodName)
mvn clean verify -DskipUTs -Dit.test=ClassName#methodName -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -PTableSimpleIT -P with-integration-tests
```

When verifying a new feature, only run the specific IT classes/methods that were added or modified in the current branch — do not run all ITs.

To run integration tests from IntelliJ: enable the `with-integration-tests` profile in Maven sidebar, then run test cases directly.

## Code Style

- **Spotless** with Google Java Format (GOOGLE style). Import order: `org.apache.iotdb`, blank, `javax`, `java`, static.
- **Checkstyle** is also configured (see `checkstyle.xml` at project root).
- Java source/target level is 1.8 (compiled with `maven.compiler.release=8` on JDK 9+).

## Architecture

### Node Types

- **ConfigNode** (`iotdb-core/confignode`): Manages cluster metadata, schema regions, data regions, partition tables. Coordinates via Ratis consensus.
- **DataNode** (`iotdb-core/datanode`): Handles data storage, query execution, and client connections. The main server component.
- **AINode** (`iotdb-core/ainode`): Python-based node for AI/ML inference tasks.

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

### Code Style

- **Always run `mvn spotless:apply` after editing Java files**: Spotless runs `spotless:check` automatically during the `compile` phase. Format violations cause an immediate BUILD FAILURE. Make it a habit to run `mvn spotless:apply -pl <module>` right after editing, not at the end. For files under `integration-test/`, add `-P with-integration-tests`.
- **Gson version compatibility**: `JsonObject.isEmpty()` / `JsonArray.isEmpty()` may not be available in the Gson version used by this project. Use `size() > 0` instead and add a comment explaining why.

## Git Commit

- Do NOT add `Co-Authored-By` trailer to commit messages.