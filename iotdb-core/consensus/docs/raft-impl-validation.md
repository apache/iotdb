<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->


# Raft extraction validation

Functional source: `374f166686f5ce8b7ee161289b31d08f1b09323e`.
Upstream base: `6de79acdbe7f0576eaf57bf218e7162ea5e37031`.
Validation host: macOS arm64, OpenJDK 21, Maven 3.9.6.

| Check | Result |
| --- | --- |
| Raft unit tests | 27 suites, 133 tests; no failures, errors, or skips |
| Research DataNode request, application, snapshot tests | 21 tests; no failures, errors, or skips |
| Consensus protocol classification | 2 tests; no failures, errors, or skips |
| DataNode and ConfigNode reactor package/verify | Passed |
| Checkstyle, Spotless, dependency analysis, Apache RAT in that reactor | Passed |
| Three-DataNode publication smoke | Passed: one executed test, 272 single-point values, readback and forced restart |
| Sibling implementation references in main code | None |

The first DataNode test attempt failed because the host had approximately 42 GiB
free, less than the configured five-percent volume reserve. Scoped JUnit rules now
isolate the tiny snapshot fixtures from that host policy and restore it afterward.
The assertions and production disk policy were retained. No protocol failure was
hidden by retrying the same unmodified test. Reactor profiles may execute the same
unit suite more than once; counts above describe unique test cases, not repetitions.

Commands are documented in `research-raft-usage.md`. The full unit selector used
`org.apache.iotdb.consensus.raft.**.*Test`. DataNode verification additionally selected
`ResearchRequestNormalizationTest`, `PortableDataRegionSnapshotTest`,
`ResearchSnapshotConcurrentCloseTest`, `ResearchStateMachineWriteBatchTest`,
`SnapshotTakerConcurrentCloseTest`, and `ConsensusProtocolUtilsTest`.

This evidence covers fresh directories, process crashes, and protocol simulations.
It does not certify power-loss hardware behavior, mixed-version upgrades, all IoTDB
features, or production deployment. Performance matrices were not rerun.
