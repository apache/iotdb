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


# TRaft partition protocol

This branch independently extends the published Raft common baseline. Its functional
source is `374f166686f5ce8b7ee161289b31d08f1b09323e`; later coalescing experiments
are excluded. It replaces, rather than runs alongside, the older TRaft on Apache's
`research/traft` branch. The Raft dependency commits are included in this branch and
in its upstream pull request. ParallelRaft and LSM-Raft are not prerequisites.

## Data path and safety boundaries

`TRaftConsensus` normalizes IoTDB requests and routes each group through
`TRaftGroup`. `TRaftEngine` tracks partition boundaries, entry identities,
proposals, acknowledgment evidence, commitment, leader changes, and recovery.
`TRaftCodec` encodes those events; `traftpaper.thrift` carries the transport.
Entries can arrive out of order. A proposal names its required identities;
missing identities are fetched before the corresponding protocol condition can
be satisfied. The configured order scope determines when timestamps create a
new partition. SERIES uses the request's series/device key; GLOBAL uses the
group-wide timestamp order. The configured capacity and timer also close partitions.

With the default SYNC journal, durable completion gates acknowledgment. Existing
journal pipelining can send data before the leader's local force, but the leader
counts remote acknowledgment evidence only after its own required force. Thus
replication overlaps local disk work without weakening durable acknowledgment.
Client success also waits for the state-machine result. Asynchronous application
moves work to a separate executor; it does not turn commit into client success
before application. Timeouts do not prove cancellation of an admitted write.

Replay restores journal state and reconstructs protocol/application progress.
Compaction retains the cluster safety cap needed by lagging replicas. Snapshot
installation and leadership recovery must preserve identity and partition
completeness. Unit tests cover interrupted records, missing entries, delayed
messages, election changes, and compaction rollback; randomized simulation is
additional evidence, not a formal proof of the implementation.

## Configuration

Select `org.apache.iotdb.consensus.traft.TRaftConsensus` as the DataRegion consensus
class. Keep ConfigNode and SchemaRegion on Ratis in the supported smoke setup.
Shared transport settings use `raft_*`; protocol settings use `traft_*` in
`iotdb-system.properties` or `raft.properties`. Java property overrides use dots.

| Setting | Fixed-source default |
| --- | --- |
| `traft_partition_max_entries` | 10000; 0 means unlimited, otherwise 2..1000000 |
| `traft_partition_max_ms` | 1000 |
| `traft_order_scope` | SERIES |
| `traft_journal_mode` | SYNC |
| `traft_journal_pipeline` | true |
| `traft_replicate_before_force` | true |
| `traft_journal_flush_interval_ms` | 1000 |
| `traft_journal_flush_entries` | 1000 |
| `traft_fetch_grace_ms` | 5 |
| `traft_pipeline_rows` | true |
| `traft_apply_batch` | true |
| `traft_apply_async` | false |
| `traft_compaction` | true |

The 10 ms group-force setting used by experiments and the publication smoke test
is an explicit override, not a change to the 1000 ms source default. ASYNC journal
mode does not promise power-loss durability and is not the default acceptance
configuration. `TRaftConsensus` validates partition and in-flight manifest budgets.

## Compatibility and scope

Start with new data directories. Compatibility with the removed upstream TRaft's
journal, snapshot, or wire format has not been established. Mixed-version clusters
and in-place upgrades are not supported by this publication. Existing checks on
journal capacity are preserved; they are not a migration guarantee. Do not copy
old Raft journals into TRaft directories. This branch contains all build inputs,
including Thrift definitions, and requires no local experiment repository.

See `research-raft-usage.md` for reactor commands. Select this protocol in the
publication smoke with `-Dresearch.protocol.class=org.apache.iotdb.consensus.traft.TRaftConsensus`
and `-Dtraft.journal.flush.interval.ms=10`. It includes ordered and reverse-timestamp
single-point writes, readback, forced restart, and post-restart writes. The test
uses individual application with asynchronous application disabled. These checks
make no throughput or production-readiness claim.
