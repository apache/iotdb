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
namespace java org.apache.iotdb.consensus.iotconsensusv2.thrift

struct TCommitId {
  1:required i64 replicateIndex
  2:required i32 pipeTaskRestartTimes
  3:required i32 dataNodeRebootTimes
}

struct TIoTConsensusV2TransferReq {
  1:required i8 version
  2:required i16 type
  3:required TCommitId commitId
  4:required common.TConsensusGroupId consensusGroupId
  5:required i32 dataNodeId
  6:required binary body
  7:optional binary progressIndex
}

struct TIoTConsensusV2TransferResp {
  1:required common.TSStatus status
  2:optional binary body
}

struct TIoTConsensusV2BatchTransferReq {
  1:required list<TIoTConsensusV2TransferReq> batchReqs
}

struct TIoTConsensusV2BatchTransferResp {
  1:required list<TIoTConsensusV2TransferResp> batchResps
}

struct TSetActiveReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: required bool isActive
  3: required bool isForDeletionPurpose
}

struct TSetActiveResp {
  1: required common.TSStatus status
}

struct TNotifyPeerToCreateConsensusPipeReq {
  1: required common.TConsensusGroupId targetPeerConsensusGroupId
  2: required common.TEndPoint targetPeerEndPoint
  3: required i32 targetPeerNodeId
}

struct TNotifyPeerToCreateConsensusPipeResp {
  1: required common.TSStatus status
}

struct TNotifyPeerToDropConsensusPipeReq {
  1: required common.TConsensusGroupId targetPeerConsensusGroupId
  2: required common.TEndPoint targetPeerEndPoint
  3: required i32 targetPeerNodeId
}

struct TNotifyPeerToDropConsensusPipeResp {
  1: required common.TSStatus status
}

struct TCheckConsensusPipeCompletedReq {
  1: required common.TConsensusGroupId consensusGroupId
  2: required list<string> consensusPipeNames;
  3: required bool refreshCachedProgressIndex
}

struct TCheckConsensusPipeCompletedResp {
  1: required common.TSStatus status
  2: required bool isCompleted
}

struct TWaitReleaseAllRegionRelatedResourceReq {
  1: required common.TConsensusGroupId consensusGroupId
}

struct TWaitReleaseAllRegionRelatedResourceResp {
  1: required bool releaseAllResource
}

service IoTConsensusV2IService {
  /**
  * Transfer stream data in a given ConsensusGroup, used by IoTConsensusV2
  **/
  TIoTConsensusV2TransferResp iotConsensusV2Transfer(TIoTConsensusV2TransferReq req)

  /**
  * Transfer batch data in a given ConsensusGroup, used by IoTConsensusV2
  **/
  TIoTConsensusV2BatchTransferResp iotConsensusV2BatchTransfer(TIoTConsensusV2BatchTransferReq req)

  TSetActiveResp setActive(TSetActiveReq req)

  TNotifyPeerToCreateConsensusPipeResp notifyPeerToCreateConsensusPipe(TNotifyPeerToCreateConsensusPipeReq req)

  TNotifyPeerToDropConsensusPipeResp notifyPeerToDropConsensusPipe(TNotifyPeerToDropConsensusPipeReq req)

  TCheckConsensusPipeCompletedResp checkConsensusPipeCompleted(TCheckConsensusPipeCompletedReq req)

  TWaitReleaseAllRegionRelatedResourceResp waitReleaseAllRegionRelatedResource(TWaitReleaseAllRegionRelatedResourceReq req)
}