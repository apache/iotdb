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

# LOAD TsFile over the Consensus Protocol and 2PC-based Staging Directory Lifecycle Management

[中文版](load-tsfile-consensus-2pc-design_ZH.md)

| Item | Content |
| --- | --- |
| Status | Design Doc |
| Applies to | 2.0.11-SNAPSHOT |
| Reference implementation | Commit `45946c352c` (LOAD TsFile: replicate pieces through consensus and clean up staged directories) |
| Modules | `iotdb-core/datanode` (`queryengine/plan/scheduler/load`, `storageengine/load`), `iotdb-core/consensus` |
| Key classes | `LoadTsFileScheduler`, `TwoPhaseConsensusLoadStrategy`, `LoadTsFileConsensusNode`, `LoadTsFileManager`, `TsFileWriterManager`, `LoadTsFileProgress`, `LoadTaskRetention`, `LoadTsFileCleaner`, `LoadTsFileSnapshot` |

---

## 1. Background and Problems to Solve

The previous LOAD TsFile implementation had three structural defects:

1. **LOAD bypassed the consensus protocol, so consistency was at risk.**
   The coordinator fanned the serialized `LoadTsFilePieceNode` out to every replica over a dedicated RPC (`sendTsFilePieceNode`), and then told each node to persist its own copy through `EXECUTE`/`ROLLBACK` commands. That path bypassed the existing consensus state machine, so consistency across replicas rested entirely on the coordinator. A crash or a restart could easily leave an inconsistent intermediate state in which some replicas lagged behind while others ran ahead.

2. **The staging data lifecycle was a guess rather than a decision.**
   There was no reliable basis for deciding when a staged file could be deleted. Deleting too early left a catching-up replica unable to read the bytes back; deleting too late kept the files of finished tasks on disk (a disk leak).

**Goals of this design:**

- Fold the LOAD write path into the existing consensus write path;
- Replace the old `EXECUTE`/`ROLLBACK` command protocol with **two-phase commit (2PC)**;
- Manage the staging directories through a **deterministic reclamation driven by the consensus watermark**.

---

## 2. Overall Design

### 2.1 Layered Architecture

The coordinator submits once, to the write peer of the partition; replicas receive the command through ordinary consensus log replication, exactly like a normal write. The old mode, in which the coordinator sent one RPC per replica, is gone.

```text
[Coordinator DataNode]  LoadTsFileScheduler
        │ (choose the strategy by needDecodeTsFile)
        ├── LocalLoadStrategy          (no decode needed: hand the whole file to the local region)
        └── TwoPhaseConsensusLoadStrategy(decode needed: split -> stream -> two-phase commit)
                 │
                 │ LoadConsensusSubmitter (resolve the write peer as the normal write path does:
                 │                          Ratis leader / IoTConsensus write node)
                 ▼
[Consensus]  LoadTsFileConsensusNode (5 phase commands: BEGIN / PIECE / PREPARE / COMMIT / ABORT)
             (via DataRegionConsensusImpl.write -> WAL replication -> replica state machines)
                 ▼
[Storage: DataRegion]  DataExecutionVisitor
        └── writeLoadTsFile{Begin,Piece,Prepare,Commit,Abort}
                 └── LoadTsFileManager (one per DataRegion)
                          └── TsFileWriterManager (one per task: staging dir + staged TsFile + progress)
```

### 2.2 Two-Phase Commit (2PC) Protocol

#### 2.2.1 Roles and Transaction Boundary

| Concept | Design |
| --- | --- |
| Coordinator | The DataNode that receives the LOAD statement (`LoadTsFileScheduler` to `TwoPhaseConsensusLoadStrategy`). |
| Participant | Every DataRegion touched by the TsFile (the write peer executes; replicas take part through consensus log replication). |
| Transaction id | One `loadId` **per region** (`regionLoadIds.computeIfAbsent(regionId, UUID)`), not a single global id. |
| Transaction boundary | One source TsFile x one DataRegion. A TsFile spanning several regions is several independent 2PC transactions, which avoids a global transaction coordinator across consensus groups. |
| Submission channel | `LoadConsensusSubmitter` -> write peer of the partition -> `DataRegionConsensusImpl.write` -> WAL replication -> `DataExecutionVisitor`. |

#### 2.2.2 Phase Protocol and Server-side Semantics

The server keeps **no in-memory transaction state per load**: phase ordering and idempotency are the client's responsibility. The server only locates the staging directory by `loadId` and applies the command.

| Command | Server behaviour | Replay / duplicate delivery |
| --- | --- | --- |
| `BEGIN` | `DataExecutionVisitor` returns `OK` directly (a no-op). The staged writer is created lazily by the first `PIECE`. The phase is kept only for cross-version compatibility. | Idempotent by nature. |
| `PIECE` | `writePiece`: locate or create the staged TsFile for `(device, time partition)`, write the chunks and deletions, return `PieceRef`, then write the node (with references) to the WAL. | Idempotent by physical offset (see 2.2.4). |
| `PREPARE` | `prepare`: close the modification file, verify that the staged file has no hole, write the metadata zone to seal it, update the TimeIndex, record the `ProgressIndex`. | Skipped when the writer `isSealed()` (idempotent). |
| `COMMIT` | `loadAll`: evaluate `mustRetain` first, import the TsFile, finish the task, then write the WAL entry and the separator. | If the `loadId` already finished and its writer was removed, a no-op success. |
| `ABORT` | `deleteAll`, finish the task, then write the WAL entry and the separator (discard, or hand the directory to the retention mechanism). | Same as above, a no-op success. |
| `PULL` | Only the command definition is kept, for compatibility with older nodes; the logic is no longer supported. | - |

> `PREPARE` and `COMMIT` carry the accumulated `pieceCount`, `totalBytes` and `checksum`, plus the serialized `ProgressIndex` of every time partition, so the pipe/subscription progress semantics are preserved instead of degrading to `MinimumProgressIndex`.

#### 2.2.3 Protocol Timeline

```text
Coordinator                                  Region Write Peer (and its replicas)
    │                                                │
    │  round 1: stream the pieces                      │
    │---- PIECE(0, chunks, chunkLayout) ────────────>│ create the staged writer lazily, append by offset
    │---- PIECE(1, chunks, chunkLayout) ────────────>│ append chunks (out-of-order arrival is fine)
    │---- ...                                        │
    │                                                │
    │  round 2, step 1: every region votes PREPARE     │
    │---- PREPARE(pieceCount, bytes, progress) ─────>│ region 1: verify no hole, seal the staged TsFile
    │---- PREPARE(pieceCount, bytes, progress) ─────>│ region 2: verify no hole, seal the staged TsFile
    │                                                │
    │  step 2 runs only when every PREPARE succeeded   │
    │---- COMMIT ───────────────────────────────────>│ region 1: import the staged TsFile (mustRetain first)
    │---- COMMIT ───────────────────────────────────>│ region 2: import the staged TsFile
    │                                                │
 any PREPARE failed (the transaction is undecided):   │
    │---- ABORT ────────────────────────────────────>│ every touched region drops its staged data
    │                                                │  (retried on every failure; nothing was imported)
```

#### 2.2.4 The Three Mechanisms that Make 2PC Work

**Mechanism 1: idempotency by offset (instead of a server-side dedup table)**

While splitting, the coordinator computes the physical layout of every chunk up front with `ChunkOffsetCalculator` (`ChunkLayout`: chunk-group-header offset, chunk offset, whether the chunk is the first of its group) and sends it along with the `PIECE`. Before writing, the server asks `progress.hasChunkAt(offset)`:

- **already recorded**: skip the write and reuse the original reference (with a payload present, build a `ChunkPayloadRef` from where the chunk already sits; without one, keep the reference that arrived);
- **not recorded and no payload**: refuse with `..._ARRIVED_WITHOUT_ITS_CHUNK_PAYLOAD`, so data can never be dropped silently; a mismatch between `incomingRefs.size()` and `chunks.size()` is refused as well.

**Why this matters:** out-of-order pieces, network retries and WAL replays all converge, and the server never has to maintain a dedup table. `pieceIndex` is only an identity and a log-correlation aid; it carries no ordering constraint.

**Mechanism 2: decide before acting (must retain before import)**

`loadAll` evaluates `retention.mustRetain(searchIndex)` **before** it imports, and passes the outcome into the import. The reason is that the WAL PIECE entries hold references only, so moving the staged file away would leave a catching-up replica unable to read those bytes; therefore a retained task is imported from a **copy**. Decision and import are both driven by the same `searchIndex`, so "the decision allowed deletion while the import moved the file" cannot happen.

**Mechanism 3: a terminal record that makes the transaction recoverable**

`PREPARE`, `COMMIT` and `ABORT` are all written to the WAL (`logLoadNodeToWAL` plus `insertSeparatorToWAL`). Before the progress files are dropped, the terminal marker (terminal op plus its consensus index) is appended to their tail.

- **Crash and restart**: a scan that sees the marker deletes the directory instead of mistaking a finished task for one that must be resumed;
- **Command replay**: no `loadId` in memory (the writer was removed) means a safe no-op success.

#### 2.2.5 Commit Point and Failure-Handling Matrix

Core rule: **roll everything back before the commit point, and nothing after it** (presumed commit).

| Failure | Behaviour | Test assertion |
| --- | --- | --- |
| Phase 1 (splitting failure, or any piece failed to submit) | Every touched region gets a full ABORT; no region has voted yet. | all ABORT |
| `PREPARE` failed on any region | The transaction is **undecided**, so every touched region is aborted; a region that already prepared successfully does **not** commit. | `PREPARE@1, PREPARE@2, ABORT@1, ABORT@2` |
| `PREPARE` failed on the first region | Same, every region is aborted. | `PREPARE@1, ABORT@1, ABORT@2` |
| Every `PREPARE` succeeded | The commit round runs: `COMMIT` is sent to every region in turn (the commit point has passed, it cannot be rolled back). | `PREPARE@1, PREPARE@2, COMMIT@1, COMMIT@2` |
| `COMMIT` failed on one region | **No region is rolled back**: it may have imported already, and the other regions voted yes so they must still commit (presumed commit). The failure is reported and the file goes to the tablet fallback. | `PREPARE@1, PREPARE@2, COMMIT@1, COMMIT@2` |
| `COMMIT` outcome unknown (the answer was lost) | The command is re-sent, which is what makes the outcome decidable: a region that imported the task answers **success**, and a region that did not answers with its failure. Without that, a retry that follows a lost answer would report a load that committed everywhere as failed. | `testRepeatedCommitOfTheSameTaskSucceeds` |
| `ABORT` failed | Retried for **every** kind of failure (3 attempts with backoff): an ABORT of a task this region no longer holds is acknowledged as success, so a repetition is harmless, while giving up would leave the staged directory to a scan. A retry that fails as well is logged, and the leftover directory is reclaimed by the cleaner. | `testAbortIsRetriedOnFailure` |

> The **commit point** is the moment every region has prepared successfully: a failure before it rolls the whole transaction back, a failure after it never rolls anything back and is reported instead.
>
> Any failure before the commit point must reach an explicit ABORT: staging directories are named by `loadId` and never expire, so a task nobody finishes would hold disk space forever, and replaying the same pieces later would run into a half-filled staged file.

#### 2.2.6 Retry Policy and Its Interaction with Consensus

- **Bounded retry**: at most 3 attempts (`LOAD_CONSENSUS_SUBMIT_MAX_RETRIES`) with a `100ms x attempt` backoff. What may be repeated follows from what a repetition does at the region:
  - `PIECE`: transient failures only (`DISPATCH_ERROR`, `INTERNAL_SERVER_ERROR`, `NO_AVAILABLE_REGION_GROUP`, `EXECUTE_STATEMENT_ERROR`); repeating one is free because of offset idempotency;
  - `PREPARE`: transient failures only; a staged file that is already sealed is left as it is, so a replay cannot append a second metadata zone;
  - `COMMIT`: transient failures only; a task the region already imported answers success, which is what settles an outcome whose answer was lost, while a genuine failure still answers with a failure;
  - `ABORT`: every kind of failure, because an ABORT of a task the region no longer holds is acknowledged as success;
  - a permanent rejection of `PIECE` / `PREPARE` is returned immediately so the coordinator aborts.
- **Route of a transaction**: the replica set the splitter resolved is pinned per region, and every command of the task - the pieces, `PREPARE`, `COMMIT` and `ABORT` - is sent to it, so a migration between two pieces cannot leave the pieces staged under one route and the terminal commands under another. A transient failure is the signal that the route may no longer be the one that holds the region: the route is then looked up **again with the local partition cache dropped first** (a cache hit answers with the route it already has), with the same bounded number of attempts, and a route that **changed** is adopted by the transaction, which every following command of that region then uses. A route that cannot be resolved at all is no route either: the command is repeated on the pinned one and its answer is what the coordinator decides from, so a stale route is never silently replaced by an empty one.
- **Ratis**: the complete command, chunk data included, travels through the Ratis log, and every replica writes its own staging directory.
- **IoTConsensus**: after the write peer applies the piece, followers receive a marker-only entry (`pieceMarker`: index, checksum and byte size, a few dozen bytes). The bytes themselves are read back on demand from the staged file through `ChunkPayloadRef` (deferred materialization).

### 2.3 Splitting and Streaming Dispatch (Phase 1)

- **Pipeline**: `TsFileSplitter` emits `TsFileData` (CHUNK / DELETION) -> `TsFileSplitConsumer` buffers and routes it through `DataPartitionRouter` -> `PieceDispatcher` dispatches it.
- **Memory budget**: `MemoryBoundedBuffer` (budget = `thriftMaxFrameSize >> 2`) stays in sync with the cluster-wide LOAD data cache; when the budget is exceeded, `PieceDispatcher` evicts the largest buffered piece first (largest-first).
- **DELETION semantics**: a deletion is copied into every buffered piece, and chunks are routed before the deletion is written, so a deletion never overtakes its data.
- **Progress index**: while splitting, a `ProgressIndex` is produced per time partition and later carried by `PREPARE` / `COMMIT`.

### 2.4 Staging Directory Layout and Hole-Tolerant Resume

- **Layout**: `<load dir>/<database-region>/<load id>/` (`LoadStagingDirs`), deliberately independent of the DataRegion `sequence`/`unsequence` data layout.
- **Progress bitmap**: `LoadTsFileProgress` is an append-only progress file that records chunk offsets, the chunk header and statistics metadata, the exact physical data range, and the consensus index.
- **Resume and verification**: after a restart the `TsFilePrecalculatedChunkWriter` metadata is rebuilt from those records, and the file is continued after the last recorded chunk. `isReady(fileLength)` is the one completeness check the sealing (`prepare`), the cleaner (`isComplete`) and the resume path share: the recorded ranges have to cover the file from the TsFile header on **without a hole**, and the file has to hold at least the bytes they claim. A hole is what a piece that never arrived leaves, and sealing such a file would put zeros in its place while the cleaner would delete bytes a replica still has to read back.
- **A piece that arrives late**: a chunk is written at the absolute offset its own content defines, so a piece that reaches this node after a piece the layout puts behind it is written into its own range instead of at the end of the file. Without that, the planned range would stay a hole forever and a hole would no longer be a reliable sign of a missing piece.
- **A progress file that ends in the middle of an entry**: that is what a snapshot of the staging directory holds when it was taken while a piece was still being appended, because the copy is not serialized with the appends. The reader drops the fragment and resumes the entries before it, instead of treating the whole staged file as unattributable.
- **A staged file that cannot be resumed**: a piece of such a file fails instead of being dropped, because a replica that loses a piece without reporting anything imports a file that is missing it, and nothing downstream could tell.

### 2.5 Deterministic Staging Directory Reclamation (Watermark-Driven)

Timeouts are gone; the lifetime is driven by what the system actually did:

- **Chain**: a finished task writes its terminal marker -> `LoadTaskRetention` decides when the directory may be released, based on the WAL safe-deletion watermark -> `LoadTsFileCleaner` deletes it. The release is triggered by the watermark callback, not by polling.
- **Reclamation condition**: every replica must have passed that index (`replicasReached`). A `COMMIT` additionally requires `isComplete` (no hole, progress covering the end of the file), because otherwise a replica could not read the payloads back. An `ABORT`ed task is plain garbage once the watermark passed: it was never imported.
- **Degradation**: consensus V2 and every protocol without a WAL watermark report `ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX` (`Long.MIN_VALUE`, meaning "never reported"). That sentinel value is what marks "no follower to wait for", so a `COMMIT` or `ABORT` may be released as soon as it was applied.
- **Restart fallback**: on startup the staged directories are scanned, their terminal records are read, and directories that a previous run did not get to delete are reclaimed.

### 2.6 Zero-Copy and Snapshot Isolation

- **Zero-copy references in the WAL**: there is exactly one copy of every payload, and it lives in the staged file. The WAL, the replication queues and the memory accounting all hold `ChunkPayloadRef` references. The reservation accounting of `IndexedConsensusRequest`, `LogDispatcher`, `SubscriptionQueueRegistry` and `IoTConsensusMemoryManager` was adjusted accordingly (the reserved amount is snapshotted when the entry is queued and returned unchanged when it is released), so accounting cannot drift.
- **References are recorded relative to the staging root**: a `PieceRef` and a `ChunkPayloadRef` name their file by the path it has **inside** the staging root it was staged in, and the reader resolves that path under its own roots. A DataNode may stage its tasks in several roots, the roots of two nodes need not have the same names, and a restored task may live on a different root than the one it was staged on; an absolute path would stop describing the file in all three cases. An absolute path is still accepted, so references written by an older build keep working.
- **Snapshot isolation**: `LoadTsFileSnapshot` owns the snapshot and restore of the LOAD staging tree. The generic `SnapshotTaker` / `SnapshotLoader` explicitly skip the LOAD directories, so a `.progress` file is never mistaken for a data file; a recovering replica inherits the partial physical state plus the progress bitmaps and can continue an unfinished 2PC.
  - **Which protocols carry it**: a replica of IoTConsensus rebuilds the staged files from the entries it applies as a member of the region, so a replica that joins in the middle of a task has to inherit the staged state through the snapshot the migration transfers; the snapshot of the staging directory is therefore taken for IoTConsensus V1 and V2, and skipped for Ratis, which replicates the payload of every piece to every replica itself.
  - **A consistent copy without a lock**: the progress logs are copied **before** the staged files they describe, and a staged file only ever grows at its end, so a copied log never refers to bytes that the copy of its file does not hold; the one fragment a copy can still catch - the trailing entry of a log - is dropped by the reader of the copy. Holding the writer back for the whole copy would stop the region from applying its pieces for as long as a large staged file takes to be copied.
  - **What the snapshot holds**: the manifest `load/roots` records the staging root of every task, and a restore puts a task back on the root it was staged on when this node has it and on a root it does have otherwise. A copied file is published under its final name only once it is complete (`.copying.` plus an atomic move), and a staged file that cannot be enumerated aborts the snapshot instead of shortening it quietly.

### 2.7 Configuration and Failure Fallback

- **Configuration**: `setLoadTsFileDirs` was added (it refreshes the canonical paths as well), and the lower bound of `loadTsFileSpiltPartitionMaxSize` is now explicitly `>= 1`, since a value of 0 would fail every LOAD whose source file spans at least one time partition.
- **Failure fallback**: `LoadFallbackHandler` converts a failed TsFile into tablets and retries. The source file is deleted physically only after the whole LOAD batch has ended, which keeps the `deleteAfterLoad` semantics safe.

---

## 3. Core Design Philosophy

1. **Reuse the existing consensus stack instead of building another one.** The LOAD write path is the ordinary write path, so consistency, crash recovery and catch-up semantics come for free.
2. **Self-describing commands, stateless server.** A phase command carries everything it needs (totals, checksum, physical layout). The server keeps no volatile state machine, which is what makes duplicate delivery and out-of-order restarts robust.
3. **Physical facts before in-memory state.** Idempotency by offset replaces a dedup table: writing the same piece twice is harmless by construction. The progress file is append-only, and both resume and reclamation rest on what is durably on disk.
4. **Lifetime driven by the real watermark.** Liveness is decided by the consensus watermark, not by a timeout or a reference count.
5. **Delete conservatively, recover aggressively.** Deletion requires no hole plus an advanced watermark; recovery reads as much as it can and supports completing a file that still has a hole.
6. **Rollback must know when to stop.** The commit point is the moment every participant agreed to prepare: before it the transaction rolls back as a whole, after it (presumed commit) no participant is ever rolled back, because a fake rollback would only create an illusion of consistency. A cross-region failure that cannot be made atomic is degraded to the tablet retry path instead.
7. **Decide before acting.** `mustRetain` is evaluated before the import and passed into it, so the decision and the action can never disagree.
8. **Single responsibility and separated concerns.** Splitting, routing, buffering, dispatching, submitting, rolling back and falling back are separate, individually testable classes; the cleaner is a single DataNode-level service; the LOAD staging snapshot logic does not pollute the generic DataRegion snapshot; all LOAD classes live in `scheduler.load` and `storageengine.load`.

---

## 4. Compatibility, Degradation and Feature Overview

### 4.1 Failure Degradation and Fallback

- **Tablet fallback**: whenever the 2PC path leaves a cross-region inconsistency or fails overall, `LoadFallbackHandler` converts the failed TsFile into tablets and retries (table model through `convertForTableModel`, tree model through `convertForTreeModel`). If the fallback succeeds the state machine ends in `FINISHED`, otherwise in `FAILED`. The source file is deleted physically only after the whole batch ends.
- **Not strictly atomic**: this is an **optimistic two-phase commit** whose channel is the consensus log, whose idempotency is a physical offset, and whose liveness is a consensus watermark. A cross-region intermediate state in the commit round (A imported, B did not) is covered by the tablet retry described above, while nothing is ever imported partially before the commit point, because a single failed `PREPARE` rolls the whole transaction back.

### 4.2 Compatibility and Degradation

- **Protocol degradation**: consensus V2 and regions without replication degrade to immediate release.
- **Version compatibility**: the old `PULL` protocol is no longer supported; an old node calling `sendLoadCommand` receives an explicit "protocol removed" error; slice-carrying piece dispatch remains compatible (the #18627 path was kept and adapted).
- **Local loading**: a single-node or local load goes through `LocalLoadStrategy` and never crosses the network.

### 4.3 Monitoring and Configuration

- **Configuration validation**: `setLoadTsFileDirs` was added, and `loadTsFileSpiltPartitionMaxSize >= 1` is enforced, so a value of 0 cannot fail every LOAD.
- **Metrics**: the point-count metric `LoadPointCountMetrics`, plus per-phase cost metrics (`LoadTsFileCostMetricsSet`: `FIRST_PHASE`, `SECOND_PHASE`, `SCHEDULER_CAST_TABLETS`, and so on).

### 4.4 Feature List

**Scheduling and splitting**

- Two strategies chosen by `needDecodeTsFile`: local direct load (no decode) and two-phase consensus load (decode);
- splitting into CHUNK / DELETION, batched partition lookup and region routing (including the table-model and pipe database hint);
- memory budget with largest-first eviction, and a flush of the remaining pieces at end of file;
- region migration detection (`RegionReplicaSetChangedException` when the replica set changes);
- failure fallback: TsFile to tablet retry, with the state machine ending in `FINISHED` / `FAILED`.

**2PC protocol**

- Phase commands BEGIN / PIECE / PREPARE / COMMIT / ABORT applied through the consensus state machine; BEGIN is a no-op and the writer is created lazily by the first PIECE; the old PULL is no longer supported;
- one `loadId` per region; `PREPARE` / `COMMIT` carrying `pieceCount` / `totalBytes` / `checksum` and the per-time-partition ProgressIndex;
- submission to the write peer of the partition: Ratis leader, IoTConsensus write node; `RegionWriteExecutor` locally, internal RPC (`sendBatchPlanNode`) remotely;
- offset idempotency, the no-hole completeness check on PREPARE with `isSealed` replay skip, and no-op COMMIT / ABORT for a finished `loadId` (a region remembers the tasks it committed and answers a repeated COMMIT of one of them with success);
- two commit rounds: `PREPARE` is sent to every touched region first, and `COMMIT` is sent to all of them only once every one of them agreed (the commit point is "every PREPARE succeeded");
- the failure matrix (full ABORT on phase-1 failure, full ABORT on any PREPARE failure, no rollback of the regions that voted yes when a COMMIT fails, ABORT retried for every kind of failure);
- the decide-before-import ordering of `mustRetain` (a retained task is imported from a copy);
- bounded retry per command (3 attempts, 100ms backoff): transient failures for PIECE / PREPARE / COMMIT, every failure for ABORT;
- the route of a transaction pinned per region, and re-resolved with the partition cache dropped first when a command fails transiently; a route that changed is adopted by the transaction, one that cannot be resolved is never adopted.

**Staging writes**

- one directory per task; one staged TsFile per time partition; `TsFilePrecalculatedChunkWriter` writing directly with pre-calculated metadata;
- PIECE writes recording the physical landing point of every chunk and returning a `PieceRef`; `prepare` sealing, `loadAll` importing, `close` discarding;
- a piece that arrives after a later one written at its own planned offset, and a stuck file (a staged file with no resumable writer) failing its piece loudly instead of dropping it;
- references recorded relative to the staging root (`PieceRef`, `ChunkPayloadRef`), resolved under the roots of the reading node, with absolute paths still accepted;
- chunk references (`ChunkPayloadRef`) and the "payload unavailable" exception (`ChunkPayloadUnavailableException`).

**Progress and resume**

- an append-only progress file (magic, uuid, the physical range and metadata of every chunk, the consensus index);
- restart recovery: writer metadata rebuilt from the records, completion of a file that still has a hole, the gap-free `isReady` completeness check shared by `prepare` and the cleaner, a torn trailing entry dropped before the entries before it are resumed, and a terminal marker that prevents a finished task from being resumed.

**Reclamation and retention**

- the terminal record in the progress tail (op plus index); `LoadTaskRetention` keeping a directory until the WAL watermark passes; `LoadTsFileCleaner` deleting through both its registry and a directory scan;
- `ABORT` deleted as soon as the watermark passed, `COMMIT` additionally requiring "no hole"; immediate deletion on V2 or on regions without replication; missed directories reclaimed after a restart.

**WAL / consensus / memory**

- the new WAL entry type `LOAD_TSFILE_CONSENSUS_NODE(13)` (isUserData classification, serialization and deserialization, `IWALNode.log(memTableId, node)`);
- the WAL holding references only: downstream V1 sync forwards a `LoadPieceConsensusRequest` that reads the payloads back on demand;
- the watermark callback chain `IWALNode.setSafeDeletedSearchIndexListener` and `getSafelyDeletedSearchIndex()`;
- the queue-accounting fix for deferred-serialization requests (`getQueueReservedMemorySize`, `hasDeferredRequests`).

**Snapshot**

- snapshot / restore / clear of the LOAD staging tree; the generic snapshot path skipping the LOAD directories; the staged files and progress bitmaps of in-flight tasks restored together;
- the snapshot taken for IoTConsensus V1 / V2 only, the `load/roots` manifest with a restore that puts every task back on its staging root, the progress logs copied before the staged files, files published atomically, and an enumeration failure aborting the snapshot.

### 4.5 Test Coverage

- **Unit tests**: scheduler, the two-phase strategy and its failure matrix plus the route it follows (`TwoPhaseConsensusLoadStrategyAbortTest`, eight cases), the write peer and the route of a transaction (`LoadConsensusSubmitterTest`: the Ratis leader matched by node id, a leader outside the route, the pinned route not refreshed, the cache dropped before a route lookup, a route that cannot be resolved), consensus node serialization round-trip (`LoadTsFileConsensusNodeTest`), manager (`LoadTsFileManagerTest`: an out-of-order hole failing PREPARE and succeeding once it is filled, a torn progress entry repaired on recovery, a staged file without a resumable writer failing its piece, repeated PREPARE / ABORT / COMMIT, payload-reference bounds, the staging path boundary, the ignored lost-metadata case of a resumed task), resume (`LoadTsFileResumeTest`), progress (`LoadTsFileProgressTest`), split direct-write and offset idempotency (`LoadTsFileManagerSplitCoverageTest#testPieceAppliedTwiceIsWrittenOnce`, `#testIdenticalPieceWithPayloadIsNotWrittenTwice`), the staging snapshot (`LoadTsFileSnapshotTest`: the files of a task copied, a snapshot taken while pieces are applied staying restorable, multi-root restore including a node with fewer roots, the protocol guard), dispatcher, slice assembler, consensus requests (`IndexedConsensusRequestTest`), subscription queues (`SubscriptionQueueRegistryTest`).
- **Integration test**: `IoTDBLoadTsFileClusterIT` (load in a cluster, and load after one DataNode is stopped). To run it: `mvn clean package -pl distribution -am -DskipTests`, then `mvn verify -DskipUTs -Dit.test=IoTDBLoadTsFileClusterIT -pl integration-test -am -PClusterIT -P with-integration-tests`.
