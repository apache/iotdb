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

package org.apache.iotdb.consensus.iot.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusInactivatePeerKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.BatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.thrift.IoTConsensusIService;
import org.apache.iotdb.consensus.iot.thrift.TActivatePeerReq;
import org.apache.iotdb.consensus.iot.thrift.TActivatePeerRes;
import org.apache.iotdb.consensus.iot.thrift.TBuildSyncLogChannelReq;
import org.apache.iotdb.consensus.iot.thrift.TBuildSyncLogChannelRes;
import org.apache.iotdb.consensus.iot.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.iot.thrift.TCleanupTransferredSnapshotRes;
import org.apache.iotdb.consensus.iot.thrift.TInactivatePeerReq;
import org.apache.iotdb.consensus.iot.thrift.TInactivatePeerRes;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.thrift.TRemoveSyncLogChannelReq;
import org.apache.iotdb.consensus.iot.thrift.TRemoveSyncLogChannelRes;
import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesReq;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesRes;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.consensus.iot.thrift.TWaitReleaseAllRegionRelatedResourceReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitReleaseAllRegionRelatedResourceRes;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteRes;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.stream.Collectors;

public class IoTConsensusRPCServiceProcessor implements IoTConsensusIService.Iface {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTConsensusRPCServiceProcessor.class);

  private final IoTConsensus consensus;

  public IoTConsensusRPCServiceProcessor(IoTConsensus consensus) {
    this.consensus = consensus;
  }

  @Override
  public TSyncLogEntriesRes syncLogEntries(TSyncLogEntriesReq req) {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for TSyncLogEntriesReq which size is %s",
              groupId, req.getLogEntries().size());
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TSyncLogEntriesRes(Collections.singletonList(status));
    }
    if (impl.isReadOnly()) {
      String message = "fail to sync logEntries because system is read-only.";
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode());
      status.setMessage(message);
      return new TSyncLogEntriesRes(Collections.singletonList(status));
    }
    if (!impl.isActive()) {
      TSStatus status = new TSStatus(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode());
      status.setMessage("peer is inactive and not ready to receive sync log request");
      return new TSyncLogEntriesRes(Collections.singletonList(status));
    }
    BatchIndexedConsensusRequest logEntriesInThisBatch =
        new BatchIndexedConsensusRequest(req.peerId);
    // We use synchronized to ensure atomicity of executing multiple logs
    for (TLogEntry entry : req.getLogEntries()) {
      logEntriesInThisBatch.add(
          impl.buildIndexedConsensusRequestForRemoteRequest(
              entry.getSearchIndex(),
              entry.getData().stream()
                  .map(
                      entry.isFromWAL()
                          ? IoTConsensusRequest::new
                          : ByteBufferConsensusRequest::new)
                  .collect(Collectors.toList())));
    }
    long buildRequestTime = System.nanoTime();
    IConsensusRequest deserializedRequest =
        impl.getStateMachine().deserializeRequest(logEntriesInThisBatch);
    impl.getIoTConsensusServerMetrics().recordDeserializeCost(System.nanoTime() - buildRequestTime);
    TSStatus writeStatus =
        impl.syncLog(logEntriesInThisBatch.getSourcePeerId(), deserializedRequest);
    LOGGER.debug(
        "execute TSyncLogEntriesReq for {} with result {}",
        req.consensusGroupId,
        writeStatus.subStatus);
    return new TSyncLogEntriesRes(writeStatus.subStatus);
  }

  @Override
  public TInactivatePeerRes inactivatePeer(TInactivatePeerReq req) throws TException {
    if (req.isForDeletionPurpose()) {
      KillPoint.setKillPoint(IoTConsensusInactivatePeerKillPoints.BEFORE_INACTIVATE);
    }
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);

    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for inactivatePeer request", groupId);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TInactivatePeerRes(status);
    }
    impl.setActive(false);
    if (req.isForDeletionPurpose()) {
      KillPoint.setKillPoint(IoTConsensusInactivatePeerKillPoints.AFTER_INACTIVATE);
    }
    return new TInactivatePeerRes(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  @Override
  public TActivatePeerRes activatePeer(TActivatePeerReq req) throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for inactivatePeer request", groupId);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TActivatePeerRes(status);
    }
    KillPoint.setKillPoint(DataNodeKillPoints.DESTINATION_ADD_PEER_DONE);
    impl.setActive(true);
    return new TActivatePeerRes(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  @Override
  public TBuildSyncLogChannelRes buildSyncLogChannel(TBuildSyncLogChannelReq req)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TBuildSyncLogChannelRes(status);
    }
    TSStatus responseStatus;
    try {
      impl.buildSyncLogChannel(new Peer(groupId, req.nodeId, req.endPoint));
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
    }
    return new TBuildSyncLogChannelRes(responseStatus);
  }

  @Override
  public TRemoveSyncLogChannelRes removeSyncLogChannel(TRemoveSyncLogChannelReq req)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TRemoveSyncLogChannelRes(status);
    }
    TSStatus responseStatus;
    if (impl.removeSyncLogChannel(new Peer(groupId, req.nodeId, req.endPoint))) {
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage("remove sync log channel failed");
    }
    return new TRemoveSyncLogChannelRes(responseStatus);
  }

  @Override
  public TWaitSyncLogCompleteRes waitSyncLogComplete(TWaitSyncLogCompleteReq req)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for waitSyncLogComplete request", groupId);
      LOGGER.error(message);
      return new TWaitSyncLogCompleteRes(true, 0, 0);
    }
    long searchIndex = impl.getSearchIndex();
    long safeIndex = impl.getMinSyncIndex();
    return new TWaitSyncLogCompleteRes(searchIndex == safeIndex, searchIndex, safeIndex);
  }

  @Override
  public TWaitReleaseAllRegionRelatedResourceRes waitReleaseAllRegionRelatedResource(
      TWaitReleaseAllRegionRelatedResourceReq req) throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for TWaitReleaseAllRegionRelatedResourceRes request",
              groupId);
      LOGGER.error(message);
      return new TWaitReleaseAllRegionRelatedResourceRes(true);
    }
    return new TWaitReleaseAllRegionRelatedResourceRes(
        impl.hasReleaseAllRegionRelatedResource(groupId));
  }

  @Override
  public TSendSnapshotFragmentRes sendSnapshotFragment(TSendSnapshotFragmentReq req)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TSendSnapshotFragmentRes(status);
    }
    TSStatus responseStatus;
    try {
      impl.receiveSnapshotFragment(req.snapshotId, req.filePath, req.fileChunk, req.offset);
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
    }
    return new TSendSnapshotFragmentRes(responseStatus);
  }

  @Override
  public TTriggerSnapshotLoadRes triggerSnapshotLoad(TTriggerSnapshotLoadReq req)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TTriggerSnapshotLoadRes(status);
    }
    impl.loadSnapshot(req.snapshotId);
    KillPoint.setKillPoint(DataNodeKillPoints.DESTINATION_ADD_PEER_TRANSITION);
    return new TTriggerSnapshotLoadRes(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  @Override
  public TCleanupTransferredSnapshotRes cleanupTransferredSnapshot(
      TCleanupTransferredSnapshotReq req) throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TCleanupTransferredSnapshotRes(status);
    }
    TSStatus responseStatus;
    try {
      impl.cleanupSnapshot(req.snapshotId);
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      LOGGER.error("failed to cleanup transferred snapshot {}", req.snapshotId, e);
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
    }
    return new TCleanupTransferredSnapshotRes(responseStatus);
  }

  public void handleClientExit() {}
}
