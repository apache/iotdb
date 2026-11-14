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


# Building and verifying the research Raft baseline

The registered DataRegion implementation is
`org.apache.iotdb.consensus.raft.RaftConsensus`. Keep ConfigNode and SchemaRegion
on Ratis unless validating another explicitly supported configuration. Ratis and
IoTConsensus remain available under their existing class names and defaults.
Use a fresh data directory on every node. A research consensus protocol is selected
with `data_region_consensus_protocol_class` in `conf/iotdb-system.properties`;
all replicas of a region must use the same implementation and configuration.

## Configuration and acknowledgment

Configuration is read from IoTDB's consensus settings, then `raft_*` entries in
`iotdb-system.properties` and `raft.properties`, then Java `-Draft.<key>=...`
overrides. `RaftConfig` and `RaftConfigLoader` define the authoritative defaults
and accepted keys. This extraction preserves the source defaults; prior performance
experiments used overrides that are not made production defaults here.

| Setting | Source default | Meaning |
| --- | --- | --- |
| `raft_max_entries_per_append` | 256 | Entry limit per replication message |
| `raft_max_inflight_batches` | 8 | Outstanding replication batches |
| `raft_log_fsync_max_batch_entries` | 1024 | Journal group-force entry limit |
| `raft_log_fsync_max_delay_ms` | 2 | Maximum journal force batching delay |
| `raft_log_cache_bytes` | min(max JVM heap / 16, 4 GiB) | Per-group log payload cache |
| `raft_apply_batch` | true | Apply consecutive committed data through writeBatch |
| `raft_pipeline_rows` | true | Admit normalized rows before waiting for each result |
| `raft_read_consistency` | READ_INDEX | Read confirmation policy |

Log append and durable completion are distinct events. Replication acknowledgment,
majority commitment, and successful state-machine application are distinct stages;
client success waits for the required application result. A timed-out request may
still commit. Retrying with a new request identity is not an exactly-once guarantee.

Snapshots and log replay restore the state machine. Research DataRegions use the
consensus log for recovery instead of duplicating recovery through the ordinary
DataRegion WAL. Existing upstream TRaft journals and snapshots are not supported
as migration inputs. No rolling mixed-version upgrade has been validated.

## Repeatable checks

On a fresh Maven cache, first run `bash tools/build-research-dependencies.sh`
from the repository root. See [the pinned dependency build](research-dependency-build.md)
for the expired upstream snapshot and verification details.

Use a JDK supported by this IoTDB checkout and Maven. Generate Thrift classes
through the reactor; do not copy generated files from another checkout.

```sh
mvn -pl iotdb-core/datanode,iotdb-core/confignode -am -DskipTests package
mvn -pl iotdb-core/consensus -am \
  '-Dtest=org.apache.iotdb.consensus.raft.**.*Test' \
  -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl integration-test -am -Pwith-integration-tests,ClusterIT \
  -Dtest=NoSuchPublicationUnitTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=IoTDBProtocolPublicationIT -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dresearch.consensus.run=true verify
```

The last command deliberately selects only the publication integration test. Check
that the Failsafe report contains one executed test, with no skips. It launches one
ConfigNode and three DataNodes, writes 272 single-point records across ordered,
reverse-timestamp, and post-restart phases, compares timestamp/value maps through
each endpoint, and forcibly restarts the DataNodes. Endpoint reads use the normal
query path and are not a physical replica-file audit. Performance is not an
acceptance criterion. Do not point this test at existing user data.
