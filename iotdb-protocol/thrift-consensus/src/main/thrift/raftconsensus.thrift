/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Raft / TRaft / ParallelRaft shared consensus RPC. Protocol-specific fields are optional and
 * left unset by the protocols that do not use them (Raft and TRaft never set the ParallelRaft
 * fields, so their wire format is unchanged).
 * Snapshot-fragment structs mirror iotconsensus.thrift (TSendSnapshotFragmentReq,
 * TTriggerSnapshotLoadReq/Res, TCleanupTransferredSnapshotReq/Res) so the iot transfer code can be copied.
 */
include "common.thrift"
namespace java org.apache.iotdb.consensus.raft.thrift

enum TEntryType { DATA = 1, NOOP = 2, CONFIG = 3, BARRIER = 4 }

// ACK: durable and covered by the follower's contiguous prefix (Raft: batch fully appended)
// RECEIVED: durable beyond a hole (TRaft only)
// NACK: TRaft partition gate rejection (reportedPartition = follower working partition; unset = stale-term)
// REJECT: Raft prefix mismatch / fence mismatch (hintIndex = follower contiguousEnd+1)
// NEED_FENCE: TRaft follower in partition mode saw a non-fence AE from a term it has not fenced with
enum TAckStatus { ACK = 1, RECEIVED = 2, NACK = 3, REJECT = 4, NEED_FENCE = 5 }

struct TRaftLogEntry {
  1: required i64 index
  2: required i64 term
  3: required TEntryType type
  4: required binary payload                 // DATA: IConsensusRequest.serializeToByteBuffer(); CONFIG: membership; NOOP/BARRIER: empty
  // ---- TRaft extension ----
  5: optional i64 timestamp                  // tau (unset for BARRIER)
  6: optional i64 timestampMax               // tablet max timestamp (== timestamp for single points)
  7: optional i64 partition                  // rho
  8: optional i32 prevSize                   // theta_{rho-1}
  9: optional i8  flags                      // bit0 BARRIER
  // ---- ParallelRaft extension ----
  10: optional binary profileMeta            // non-partitioned entry meta, encoded by the profile's EntryMetaCodec
}

struct TAppendEntriesReq {
  1: required common.TConsensusGroupId groupId
  2: required i32 leaderId
  3: required i64 term
  4: required i64 prevLogIndex               // Raft prefix check; TRaft partition mode: -1; TRaft fence probe: probe point
  5: required i64 prevLogTerm
  6: required list<TRaftLogEntry> entries    // empty = heartbeat / CommitNotify
  7: required i64 leaderCommit
  8: required i64 leaderCommitTerm           // identity check for commit learning (PY CN.entry_term)
  9: optional bool speculative               // TRaft: e.rho > v.rho (per request; batch shares one value)
  10: optional bool fence                    // TRaft: leader-term fence probe
  11: optional i64 msgId                     // correlates RpcFailed / debugging
  12: optional i64 schedRound                // TRaft debugging
  13: optional common.TEndPoint leaderEndPoint  // lets an empty-peers learner learn the leader endpoint before its first CONFIG
  // ---- ParallelRaft extension (AppendExt) ----
  14: optional i64 commitBase                // commitBits covers (commitBase, commitBase + B]
  15: optional binary commitBits             // bit i set = commitBase + 1 + i is committed
  16: optional i64 truncateFrom              // recovery push: remove every index >= truncateFrom first
  17: optional i64 syncTerm                  // recovery push: the sync term the pushed entries belong to
  18: optional bool recovery                 // recovery / catch-up push (harness kind PUSH)
  19: optional binary logFile                // LSM: immutable compressed contiguous log file; entries must be empty
}

struct TEntryAck {
  1: required i64 index                      // Raft ACK: last index of the batch; REJECT: prevLogIndex; TRaft: the entry index
  2: required i64 entryTerm                  // identity (PY RESP.entry_term); leader books only if slots[index].term == entryTerm
  3: required TAckStatus status
  4: optional i64 reportedPartition          // NACK: follower working partition wp
  5: optional i64 hintIndex                  // REJECT/NEED_FENCE: follower contiguousEnd+1 (nextIndex hint)
  6: optional i64 hintTerm                   // REJECT: term at follower contiguousEnd (conflict-term skip)
  7: optional i64 workingPartition           // fence ACK: follower wp (initial v.rho)
}

struct TAppendEntriesRes {
  1: required i64 term
  2: required list<TEntryAck> acks           // Raft: exactly one; TRaft: >= one per entry (Ack burst may exceed entries)
  3: optional i64 appliedIndex               // APPLIED piggyback (PY separate APPLIED RESP)
  4: optional i64 contiguousEnd              // follower frontier (catch-up hint / getGroupStatus parity)
  5: optional i64 workingPartition           // TRaft follower wp
  6: optional i64 truncatedFrom              // smallest index removed by suffix repair / fence truncation in this call
  7: optional i64 receiverMemSize            // backpressure feedback
  // ---- ParallelRaft extension ----
  8: optional i64 syncNumber                 // follower sync number
  9: optional i64 logFirstIndex              // follower first log index (after compaction)
}

struct TRequestVoteReq {
  1: required common.TConsensusGroupId groupId
  2: required i32 candidateId
  3: required i64 term
  4: required i64 lastLogIndex               // Raft: contiguousEnd; TRaft: fe
  5: required i64 lastLogTerm                // t_hat
  6: optional i64 rhoHat                     // TRaft TRIPLE
  7: optional i64 sigmaHat
  8: optional i64 committedTermHat           // TRaft PAIR (paper HEAD): latest fully committed partition
  9: optional i64 committedRhoHat
  10: optional bool preVote
  11: optional i64 syncNumber                // ParallelRaft: candidate sync number
}
struct TRequestVoteRes { 1: required i64 term  2: required bool granted  3: optional bool preVote }

struct TTimeoutNowReq { 1: required common.TConsensusGroupId groupId  2: required i32 leaderId  3: required i64 term }
struct TTimeoutNowRes { 1: required common.TSStatus status }

struct TForwardWriteReq {
  1: required common.TConsensusGroupId groupId
  2: required binary payload
  3: optional i64 timestamp
  4: optional i64 timestampMax
  5: optional bool barrier   // RequestMeta extracted by the forwarding node
  6: optional binary requestExt              // RequestMeta.ext (ParallelRaft conflict keys)
}
struct TForwardWriteRes { 1: required common.TSStatus status  2: optional i32 leaderId  3: optional common.TEndPoint leaderEndPoint }

enum TAdminOp { ADD_PEER = 1, REMOVE_PEER = 2, TRANSFER_LEADER = 3, RESET_PEERS = 4 }
struct TPeerInfo { 1: required i32 nodeId  2: required common.TEndPoint endPoint }
struct TAdminReq { 1: required common.TConsensusGroupId groupId  2: required TAdminOp op  3: required list<TPeerInfo> peers }
struct TAdminRes { 1: required common.TSStatus status  2: optional i32 leaderId  3: optional common.TEndPoint leaderEndPoint }

struct TInstallSnapshotMetaReq {
  1: required common.TConsensusGroupId groupId
  2: required i32 leaderId
  3: required i64 term
  4: required string snapshotId
  5: required i64 lastIncludedIndex
  6: required i64 lastIncludedTerm
  7: required list<TPeerInfo> voters
  8: required list<TPeerInfo> learners
  9: required i64 configIndex
  10: optional binary profileExt             // TRaft PartitionSummary
}
struct TInstallSnapshotMetaRes { 1: required i64 term  2: required common.TSStatus status }

struct TSendSnapshotFragmentReq {
  1: required common.TConsensusGroupId groupId
  2: required string snapshotId
  3: required string filePath
  4: required i64 offset
  5: required i64 chunkLength
  6: required binary fileChunk
  7: required i64 term
}
struct TSendSnapshotFragmentRes { 1: required common.TSStatus status  2: required i64 term }
struct TTriggerSnapshotLoadReq { 1: required common.TConsensusGroupId groupId  2: required string snapshotId  3: required i64 term }
struct TTriggerSnapshotLoadRes { 1: required common.TSStatus status }
struct TCleanupTransferredSnapshotReq { 1: required common.TConsensusGroupId groupId  2: required string snapshotId }
struct TCleanupTransferredSnapshotRes { 1: required common.TSStatus status }

struct TGroupStatusReq {
  1: required common.TConsensusGroupId groupId
  2: optional bool readBarrier = false
}
struct TGroupStatusRes {
  1: required i64 term
  2: required i32 leaderId
  3: required i32 role
  4: required i64 commitIndex
  5: required i64 lastApplied
  6: required i64 firstIndex
  7: required i64 lastLogIndex
  8: required i64 contiguousEnd
  9: optional i64 workingPartition
  10: optional i64 rhoMax
  11: optional i64 pendingSize
  // ---- ParallelRaft extension ----
  12: optional i64 syncNumber
  13: optional i64 maxCommitted              // largest committed index (read index under out-of-order commit)
  14: optional i64 pendingApply
}

// ---- ParallelRaft term recovery (RecoverTerm) and sync-number upgrade (UpdateSync) ----
struct TRecoverTermReq {
  1: required common.TConsensusGroupId groupId
  2: required i32 leaderId
  3: required i64 term
  4: required i64 syncTerm                   // the candidate's sync number s
  5: required i64 fromIndex                  // first index requested (paging)
  6: required i32 maxEntries
  7: required i64 maxBytes
}
struct TRecoverTermRes {
  1: required i64 term
  2: required bool granted
  3: required i64 syncNumber
  4: required i64 contiguousEnd
  5: optional i64 logFirstIndex
  6: optional i64 promiseBallot              // end-point promise (pBallot, pEnd) for syncTerm, if any
  7: optional i64 promiseEnd
  8: optional list<TRaftLogEntry> entries    // held entries of term syncTerm, ascending
  9: optional list<i64> committedIndexes     // the subset of entries this node knows to be committed
  10: optional bool more                     // another page follows
  11: optional i64 nextIndex                 // fromIndex of the next page
}
struct TUpdateSyncReq {
  1: required common.TConsensusGroupId groupId
  2: required i32 leaderId
  3: required i64 term
  4: required i64 newSync
}
struct TUpdateSyncRes {
  1: required i64 term
  2: required bool granted
  3: required i64 syncNumber
  4: optional i64 contiguousEnd
}

service RaftConsensusIService {
  TAppendEntriesRes appendEntries(TAppendEntriesReq req)
  TRequestVoteRes requestVote(TRequestVoteReq req)
  TTimeoutNowRes timeoutNow(TTimeoutNowReq req)
  TForwardWriteRes forwardWrite(TForwardWriteReq req)
  TAdminRes admin(TAdminReq req)
  TInstallSnapshotMetaRes installSnapshotMeta(TInstallSnapshotMetaReq req)
  TSendSnapshotFragmentRes sendSnapshotFragment(TSendSnapshotFragmentReq req)
  TTriggerSnapshotLoadRes triggerSnapshotLoad(TTriggerSnapshotLoadReq req)
  TCleanupTransferredSnapshotRes cleanupTransferredSnapshot(TCleanupTransferredSnapshotReq req)
  TGroupStatusRes getGroupStatus(TGroupStatusReq req)
  TRecoverTermRes recoverTerm(1: TRecoverTermReq req)
  TUpdateSyncRes updateSync(1: TUpdateSyncReq req)
}
