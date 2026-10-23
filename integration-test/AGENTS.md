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
- Obtain node addresses and allocated ports from the test environment or node wrappers. Configure
  required consensus protocols, replication factors, and other non-default settings before
  initializing the cluster.

## Asynchronous behavior and failure recovery

- Use bounded condition waits, such as Awaitility or environment status helpers, for startup,
  discovery, leader election, and recovery. Prefer observable conditions over fixed sleeps.
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

- Follow the neighboring tests' runner, category, and environment lifecycle conventions. Select
  the category matching the data model and topology required by the scenario.
- Close sessions, pools, and result sets; shut down test-owned executors and clean the cluster
  even when assertions fail. Use try-with-resources and `finally` as appropriate.
- Assert the behavior under test and its recovery result. Only tolerate expected cleanup errors
  in narrowly scoped cleanup code; do not swallow failures from the operation being verified.

## Verification

This module is excluded from the default build. Run the commands below from the repository root,
always enabling `-P with-integration-tests` when building or testing this module.

Format Java changes with `mvn spotless:apply -pl integration-test -P with-integration-tests`.

### Targeted test runs

Run only the affected IT classes or methods for a focused change. Select the matching test profile:

| Data model | Default topology | Test profile |
| --- | --- | --- |
| Tree | 1 ConfigNode + 1 DataNode | `SimpleIT` (default) |
| Tree | 1 ConfigNode + 3 DataNodes | `ClusterIT` |
| Table | 1 ConfigNode + 1 DataNode | `TableSimpleIT` |
| Table | 1 ConfigNode + 3 DataNodes | `TableClusterIT` |

For example, run one tree-model cluster test method:

```bash
mvn clean verify -DskipUTs \
  '-Dit.test=ClassName#methodName' \
  -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false \
  -pl integration-test -am -PClusterIT -P with-integration-tests
```

Use `-Dit.test=ClassName` to run a whole class. Replace `-PClusterIT` with the profile needed by the
test; it may be omitted for the default `SimpleIT` profile.

- Check the Failsafe report to confirm that the selected test actually ran; the no-tests flags
  allow upstream reactor modules to have no matching tests.
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
