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


# TRaft publication validation

Functional source: `374f166686f5ce8b7ee161289b31d08f1b09323e`.
Apache base: `6de79acdbe7f0576eaf57bf218e7162ea5e37031`.
Host: macOS arm64, OpenJDK 21, Maven 3.9.6.

| Check | Result |
| --- | --- |
| Core: map, codec, engine, randomized fault/compaction tests | 148 tests passed, including 141 engine cases |
| Journal and real-RPC runtime tests | 83 tests passed after the fixture correction below |
| DataNode request semantics | 3 tests passed |
| Protocol classification | 2 tests passed |
| Clean 35-module integration reactor | Passed, including generated Thrift bindings |
| Three DataNodes, ordered and reverse-timestamp writes, readback, forced restart | 1 executed test, 272 single-point values, no failures/errors/skips |
| Checkstyle, Spotless, Apache RAT and dependency analysis | Passed in the integration reactor |
| Sibling implementation packages and imports | No ParallelRaft or LSM-Raft implementation dependency |

The original RPC fixture shortened election timeouts to 400-800 ms but inherited
the production 1000 ms group-force window. One of 83 runtime cases failed during
the sparse-history setup with a leadership change, before its recovery assertions.
The fixture now explicitly uses a 10 ms force window; no production default,
protocol implementation, or assertion was changed. All 20 cases in that affected
suite passed on both executions of the rerun. The other 63 runtime cases passed
unchanged. Counts describe unique cases rather than reactor repetitions.

Core tests include the full default randomized ranges: 1000 fault seeds, 500
compaction seeds and 50 negative-control seeds. Simulation and process-restart
tests do not establish hardware power-loss behavior or production readiness.

The clean integration command was:

```sh
mvn -pl integration-test -am -Pwith-integration-tests,ClusterIT \
  -Dtest=TRaftRequestSemanticsTest,ConsensusProtocolUtilsTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=IoTDBProtocolPublicationIT -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dresearch.consensus.run=true \
  -Dresearch.protocol.class=org.apache.iotdb.consensus.traft.TRaftConsensus \
  -Dtraft.journal.flush.interval.ms=10 clean verify
```

Core and runtime tests are selected with
`org.apache.iotdb.consensus.traft.paper.core.*Test` and
`org.apache.iotdb.consensus.traft.paper.runtime.*Test` respectively, using
`-pl iotdb-core/consensus -am -Dsurefire.failIfNoSpecifiedTests=false test`.
See `research-dependency-build.md` for the independently verified bootstrap of
the expired upstream TsFile dependency. No experiment repository or sibling branch
is needed. This publication assumes fresh data directories and does not certify
upgrades from the removed upstream implementation or mixed-version clusters.
