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

include "common.thrift"
namespace java org.apache.iotdb.consensus.iot.thrift

// If this TLogEntry comes from the queue in memory, the list size of the data will only be 1.
// If this TLogEntry comes from wal, the list size of the data may be greater than 1.
struct TLogEntry {
  1: required list<binary> data
  2: required i64 searchIndex
  3: required bool fromWAL
}

struct TSyncLogEntriesReq {
  # source peer where the TSyncLogEntriesReq is generated
  1: required i32 peerId
  2: required common.TConsensusGroupId consensusGroupId
  3: required list<TLogEntry> logEntries
}

struct TSyncLogEntriesRes {
  1: required list<common.TSStatus> statuses
}

struct TInactivatePeerReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: optional bool forDeletionPurpose
}

struct TInactivatePeerRes {
  1: required common.TSStatus status
}

struct TActivatePeerReq {
  1: required common.TConsensusGroupId consensusGroupId
}

struct TActivatePeerRes {
  1: required common.TSStatus status
}

struct TBuildSyncLogChannelReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: required common.TEndPoint endPoint
  3: required i32 nodeId
}

struct TBuildSyncLogChannelRes {
  1: required common.TSStatus status
}

struct TRemoveSyncLogChannelReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: required common.TEndPoint endPoint
  3: required i32 nodeId
}

struct TRemoveSyncLogChannelRes {
  1: required common.TSStatus status
}

struct TSendSnapshotFragmentReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: required string snapshotId
  3: required string filePath
  4: required i64 chunkLength
  5: required binary fileChunk
}

struct TWaitSyncLogCompleteReq {
  1: required common.TConsensusGroupId consensusGroupId
}

struct TWaitSyncLogCompleteRes {
  1: required bool complete
  2: required i64 searchIndex
  3: required i64 safeIndex
}

struct TWaitReleaseAllRegionRelatedResourceReq {
  1: required common.TConsensusGroupId consensusGroupId
}

struct TWaitReleaseAllRegionRelatedResourceRes {
  1: required bool releaseAllResource
}

struct TSendSnapshotFragmentRes {
  1: required common.TSStatus status
}

struct TTriggerSnapshotLoadReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: required string snapshotId
}

struct TTriggerSnapshotLoadRes {
  1: required common.TSStatus status
}

struct TCleanupTransferredSnapshotReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: required string snapshotId
}

struct TCleanupTransferredSnapshotRes {
  1: required common.TSStatus status
}

service IoTConsensusIService {
  TSyncLogEntriesRes syncLogEntries(TSyncLogEntriesReq req)
  TInactivatePeerRes inactivatePeer(TInactivatePeerReq req)
  TActivatePeerRes activatePeer(TActivatePeerReq req)
  TBuildSyncLogChannelRes buildSyncLogChannel(TBuildSyncLogChannelReq req)
  TRemoveSyncLogChannelRes removeSyncLogChannel(TRemoveSyncLogChannelReq req)
  TWaitSyncLogCompleteRes waitSyncLogComplete(TWaitSyncLogCompleteReq req)
  TWaitReleaseAllRegionRelatedResourceRes waitReleaseAllRegionRelatedResource(TWaitReleaseAllRegionRelatedResourceReq req)
  TSendSnapshotFragmentRes sendSnapshotFragment(TSendSnapshotFragmentReq req)
  TTriggerSnapshotLoadRes triggerSnapshotLoad(TTriggerSnapshotLoadReq req)
  TCleanupTransferredSnapshotRes cleanupTransferredSnapshot(TCleanupTransferredSnapshotReq req)
}