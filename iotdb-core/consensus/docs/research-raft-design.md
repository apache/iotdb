# Research Raft baseline

This implementation is extracted from source commit
`374f166686f5ce8b7ee161289b31d08f1b09323e` onto Apache IoTDB's
`research/traft` base `6de79acdbe7f0576eaf57bf218e7162ea5e37031`.
The previous experimental TRaft implementation is removed before introducing
independently buildable Raft, TRaft, ParallelRaft, and LSM-Raft branches.

## Behavior and layering

The Raft kernel consumes events and produces explicit persistence, replication,
application, and reply actions. Durable completion notifications gate safety
transitions; queued writes are not durable acknowledgments. The runtime owns
per-group serialization, a segmented log, transport, timers, state-machine
application, and snapshots. A client write succeeds after the protocol commits
and the leader state machine successfully applies it. A timeout does not prove
that an already admitted write was cancelled.

Protocol profiles supply replication and entry metadata policies without importing
concrete extension protocols into Raft. Optional runtime hooks support specialized
entry application and transport encodings. Ordinary Raft has no file batching
policy enabled. Ratis and IoTConsensus remain separate implementations.

## Compatibility and scope

Use fresh data directories. Compatibility with the removed upstream TRaft journal,
RPC implementation, or mixed-version clusters is not established. This extraction
does not add dynamic membership guarantees or claim a production readiness level
beyond the source implementation. Existing configuration defaults and synchronous
journal acknowledgment semantics must be preserved. Later experiment-only
coalescing patches are outside this publication.

## Validation contract

Each published branch must compile without sibling branches. Validate election,
majority commitment, persisted log recovery, snapshot installation, request
normalization, and state-machine completion before publication. Record actual
commands and outcomes; an unexecuted check is not a pass. Performance matrices
are not part of this extraction's acceptance criteria.
