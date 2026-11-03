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

# Integration Test Guidelines

These instructions apply to `integration-test/` and supplement the repository-level instructions.

## Test discovery and categories

- Name concrete IT classes to match Failsafe's default includes: `IT*`, `*IT`, or `*ITCase`.
  Prefer the existing `IoTDB...IT` convention; put variant numbers before the suffix, such as
  `IoTDBActiveRegionScan2IT`, not `IoTDBActiveRegionScanIT2`. An `IoTDB` prefix or a `Test` suffix
  alone does not make a class discoverable.
- Every test method must have an effective JUnit `@Category`, declared on the method, its class,
  or a superclass. All test profiles filter by category; `@Test` and `@RunWith` alone are not
  sufficient. In a class using method-level categories without a class/superclass category,
  annotate every new `@Test` method, even when neighboring methods already have categories.
- Prefer a class-level category when all cases share the same requirements. Preserve method-level
  categories when cases need different profiles; adding a broad class category can enable cases
  in unintended environments. Inherited categories are valid and need not be copied to subclasses.
- Select categories by the actual data model and topology, using the profile mapping below and
  `pom.xml`. Shared packages such as `session/it` contain both tree and table tests; the package
  name alone does not determine the category. Check the corresponding CI workflow for specialized
  categories such as `DailyIT` and multi-cluster tests.
- Follow the neighboring tests' runner and environment lifecycle conventions. Ordinary ITs use
  `@RunWith(IoTDBTestRunner.class)`; retain inherited or specialized runners for parameterized
  fixtures such as `AbstractSchemaIT` instead of replacing them.

## Credentials and deployment defaults

- Use `EnvFactory.getEnv()` connection helpers or client builders with their default credentials
  when a test does not need a specific user.
- When constructing clients or helpers that require explicit default credentials, use
  `SessionConfig.DEFAULT_USER` and `SessionConfig.DEFAULT_PASSWORD`. This includes
  `NodesSupplier`, pools, and direct RPC helpers; do not hardcode `"root"` as the default password.
  Default credentials can differ between IoTDB and downstream editions.
- Keep a client's business connections and discovery/recovery helpers on the same credentials.
  Check helper methods as well as the main test body: a builder may use the correct defaults
  while a manually constructed discovery client silently uses different ones.
- Explicit credentials are appropriate for users created by the test and for authentication or
  invalid-password scenarios. Make that intent clear, and clean up users created by the test.
- When a test creates users or roles with explicit credentials, make each password 12-32
  characters long with no spaces, drawn from the supported credential charset (letters,
  digits, `_ - ! @ # $ % ^ & * ( ) + =`), containing at least one lowercase letter, one
  uppercase letter, one digit, and one special character, and different from the username;
  usernames use the same charset and a 4-32 character length (for example `newuser` with
  `IoTDB!Test2024`). Trivial placeholders such as `123456` or `password` pass on a default
  local cluster yet are exactly what stricter password policies reject, and they encourage
  weak secrets to spread into real configurations through copied test code. Reserve weak or
  illegal credentials for scenarios that deliberately test them.
- Obtain node addresses and allocated ports from the test environment or node wrappers. Configure
  required consensus protocols, replication factors, and other non-default settings before
  initializing the cluster.

## Asynchronous behavior and failure recovery

- Use bounded condition waits, such as Awaitility or environment status helpers, for startup,
  discovery, leader election, and recovery. Prefer observable conditions over fixed sleeps.
- When no read-only condition exposes the state being awaited, use a finite wait derived from the
  running server's effective configuration and the required propagation rounds, with a documented
  safety margin. Reuse a feature-specific wait helper; do not guess a fixed duration or assume
  the test JVM's configuration singleton reflects a separately launched node's configuration.
- Wait for each relevant layer separately. A node status reported by ConfigNode does not prove
  that an existing client has refreshed its endpoint snapshot or recovered its connection.
- For locally stopped nodes, use `EnvUtils.getNodeStatusAfterLocalStop()` where applicable;
  graceful process termination behaves differently on Unix and Windows. Use the appropriate
  status expectation for forced termination.
- Keep connection/socket timeouts distinct from query timeouts and the overall assertion deadline.
  Allow for startup and election costs on CI; use deliberately short timeouts only when needed by
  the scenario. Do not retry non-idempotent setup statements just to hide a short timeout.
- For tests that require a stale discovery snapshot or a particular failover order, control
  background refresh and assert the preconditions before triggering recovery.
- Investigate authentication, configuration, and transport failures before increasing a wait
  timeout. A retained client snapshot can mean that discovery never succeeded. Include useful
  endpoint/status context and preserve underlying failures when adding diagnostics.
- Do not label a failure as environmental or intermittent based only on one failed run.

## Isolation and assertions

- Reuse existing test classes and fixtures, and share cluster startup and teardown across
  compatible cases within a class or suite whenever possible. Avoid starting and stopping a
  cluster for each test method or creating an extra cluster when an existing test cluster can
  cover the scenario. Use a separate cluster or restart only when required by the tested lifecycle,
  topology, configuration, or isolation.
- Keep tests that share a cluster independent of execution order: isolate their data and restore
  changed state or configuration using the repository's test helpers. Wait for asynchronous
  cleanup effects, such as quota accounting after schema deletion, before the next method's setup;
  successful cleanup SQL alone does not prove that dependent cluster state has converged.
- Close sessions, pools, and result sets; shut down test-owned executors and clean the cluster
  even when assertions fail. Use try-with-resources and `finally` as appropriate.
- Assert the behavior under test and its recovery result. Only tolerate expected cleanup errors
  in narrowly scoped cleanup code; do not swallow failures from the operation being verified.
- When enabling an old test or changing its expected result, verify the current intended
  client/server contract and preserve the original regression input and execution path. For an
  expected rejection, assert the specific exception and status/message so that an unrelated
  transport or setup failure cannot satisfy the test.
- Do not weaken assertions, remove cluster categories, or add `@Ignore` merely to make a failing
  test pass CI. If a known product defect must be quarantined, retain the reproducer and document
  the defect, affected topology, and tracking reference; report skipped cases separately.
- Account for platform semantics in process-exit assertions: POSIX exposes only the low 8 bits
  of an exit status. When checking a portable process status derived from a `TSStatusCode`,
  compare the low 8 bits of both values (`& 0xFF`); keep full status-code checks for RPC responses.

### Result ordering across DataNodes

- When row order is not part of the query's contract and may differ across DataNodes, use
  `TestUtils.treeResultSetEqualTestUnordered(sql, expectedHeader, expectedRetArray)` for tree-model
  SQL cases, or
  `TestUtils.tableResultSetEqualTestUnordered(sql, expectedHeader, expectedRetArray, database)`
  for table-model SQL cases (`database` may be `null` when no `USE` is needed).
  When the test already owns a result set, use
  `TestUtils.assertResultSetEqualUnordered(resultSet, expectedHeader, expectedRows)` and close
  the original result set after the assertion. Reuse these helpers instead of writing per-node
  comparison loops in individual tests.
- These helpers check every node independently, including headers, values, and duplicate counts.
  They use `ClusterTestResultSet.getNodeResultSets` to read each DataNode's underlying result set.
  The default `ClusterTestResultSet` compares nodes at the same cursor position; collecting or
  sorting rows after reading through that wrapper cannot avoid row-order mismatches. Do not
  replace the helpers with set equality or single-node checks that weaken these assertions.
- Keep ordered assertions when SQL guarantees the final result order. For `LIMIT`/`OFFSET`,
  ensure the selected rows are deterministic before applying unordered equality; sorting cannot
  repair a nondeterministic subset. Add sufficient `ORDER BY` tie-breakers when needed.

## Verification

This module is excluded from the default build. Run the commands below from the repository root,
always enabling `-P with-integration-tests` when building or testing this module.

Format Java changes with `mvn spotless:apply -pl integration-test -P with-integration-tests`.

### Selection check

After adding or renaming tests, changing categories or inheritance, or editing CI test selection,
run this source-level check from the repository root before the Maven build:

```bash
python3 .github/scripts/check-it-selection.py
```

- The check detects non-matching class names, missing effective categories, and missing or
  duplicated shard candidates. Also inspect the intended profile's `groups`/`excludedGroups`,
  POM excludes, and `@Ignore`; the check does not prove that a particular CI job selects a test.
  Running a test in an IDE or explicitly with `-Dit.test` does not prove default CI discovery.
- When editing CI sharding, reuse `.github/scripts/build-it-shard-list.sh`: shard all candidate
  classes by fully qualified name and let Failsafe apply category filters. Do not grep source
  files for category names; that misses inherited categories. Preserve POM excludes by using
  `failsafe.includesFile`, and reject empty lists because Failsafe falls back to default includes.
  Verify that shards cover each candidate exactly once and that their selected-test union matches
  the unsharded run for the same profile.

### Targeted test runs

Run only the affected IT classes or methods for a focused change. Select the matching test profile:

| Data model | Default topology | JUnit category | Test profile |
| --- | --- | --- | --- |
| Tree | 1 ConfigNode + 1 DataNode | `LocalStandaloneIT` | `SimpleIT` (default) |
| Tree | 1 ConfigNode + 3 DataNodes | `ClusterIT` | `ClusterIT` |
| Table | 1 ConfigNode + 1 DataNode | `TableLocalStandaloneIT` | `TableSimpleIT` |
| Table | 1 ConfigNode + 3 DataNodes | `TableClusterIT` | `TableClusterIT` |

For example, run one tree-model cluster test method:

```bash
mvn clean verify -DskipUTs \
  '-Dit.test=ClassName#methodName' \
  -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false \
  -pl integration-test -am -PClusterIT -P with-integration-tests
```

Use `-Dit.test=ClassName` to run a whole class. Replace `-PClusterIT` with the profile needed by the
test; it may be omitted for the default `SimpleIT` profile.

- Check `integration-test/target/failsafe-reports` to confirm that the intended classes and
  methods actually ran, including expected parameterized variants, and inspect skipped counts.
  A successful Maven exit is insufficient: the no-tests flags allow upstream reactor modules
  to have no matching tests and can also hide an incorrectly selected IT.
- For changes involving distributed state in tests enabled for both single- and multi-DataNode
  profiles, verify the affected cases in both profiles. A 1C1D pass does not establish 1C3D behavior.
- Report the tested revision, edition, profile, and result accurately. Passing an upstream build
  does not establish that the corresponding downstream edition was tested.

### Node templates and IDE runs

Build the node templates before an IDE run or a direct Failsafe invocation, and rebuild them after
code changes:

```bash
mvn clean package -DskipTests -pl integration-test -am -P with-integration-tests
```

The `clean verify` command above already includes this packaging step. In IntelliJ, enable the
`with-integration-tests` profile in the Maven sidebar before running test cases directly.

### Edge tests

Build the Edge distribution before running its dedicated Tree/Table read-write IT:

```bash
mvn clean package -DskipTests -pl distribution -am
mvn verify -DskipUTs -Dit.test=IoTDBEdgeBasicIT \
  -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false \
  -pl integration-test -am -P EdgeIT -P with-integration-tests
```
