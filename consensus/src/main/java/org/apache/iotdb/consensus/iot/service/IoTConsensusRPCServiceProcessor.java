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
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.BatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
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
import org.apache.iotdb.consensus.iot.thrift.TLogBatch;
import org.apache.iotdb.consensus.iot.thrift.TRemoveSyncLogChannelReq;
import org.apache.iotdb.consensus.iot.thrift.TRemoveSyncLogChannelRes;
import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogReq;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogRes;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteRes;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.stream.Collectors;

public class IoTConsensusRPCServiceProcessor implements IoTConsensusIService.AsyncIface {

  private final Logger logger = LoggerFactory.getLogger(IoTConsensusRPCServiceProcessor.class);

  private final IoTConsensus consensus;

  public IoTConsensusRPCServiceProcessor(IoTConsensus consensus) {
    this.consensus = consensus;
  }

  @Override
  public void syncLog(TSyncLogReq req, AsyncMethodCallback<TSyncLogRes> resultHandler) {
    try {
      ConsensusGroupId groupId =
          ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
      IoTConsensusServerImpl impl = consensus.getImpl(groupId);
      if (impl == null) {
        String message =
            String.format(
                "unexpected consensusGroupId %s for TSyncLogReq which size is %s",
                groupId, req.getBatches().size());
        logger.error(message);
        TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        status.setMessage(message);
        resultHandler.onComplete(new TSyncLogRes(Collections.singletonList(status)));
        return;
      }
      if (impl.isReadOnly()) {
        String message = "fail to sync log because system is read-only.";
        logger.error(message);
        TSStatus status = new TSStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode());
        status.setMessage(message);
        resultHandler.onComplete(new TSyncLogRes(Collections.singletonList(status)));
        return;
      }
      if (!impl.isActive()) {
        TSStatus status = new TSStatus(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode());
        status.setMessage("peer is inactive and not ready to receive sync log request");
        resultHandler.onComplete(new TSyncLogRes(Collections.singletonList(status)));
        return;
      }
      BatchIndexedConsensusRequest requestsInThisBatch =
          new BatchIndexedConsensusRequest(req.peerId);
      // We use synchronized to ensure atomicity of executing multiple logs
      for (TLogBatch batch : req.getBatches()) {
        requestsInThisBatch.add(
            impl.buildIndexedConsensusRequestForRemoteRequest(
                batch.getSearchIndex(),
                batch.getData().stream()
                    .map(
                        batch.isFromWAL()
                            ? IoTConsensusRequest::new
                            : ByteBufferConsensusRequest::new)
                    .collect(Collectors.toList())));
      }
      TSStatus writeStatus = impl.getStateMachine().write(requestsInThisBatch);
      logger.debug(
          "execute TSyncLogReq for {} with result {}", req.consensusGroupId, writeStatus.subStatus);
      resultHandler.onComplete(new TSyncLogRes(writeStatus.subStatus));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void inactivatePeer(
      TInactivatePeerReq req, AsyncMethodCallback<TInactivatePeerRes> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for inactivatePeer request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TInactivatePeerRes(status));
      return;
    }
    impl.setActive(false);
    resultHandler.onComplete(
        new TInactivatePeerRes(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
  }

  @Override
  public void activatePeer(
      TActivatePeerReq req, AsyncMethodCallback<TActivatePeerRes> resultHandler) throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for inactivatePeer request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TActivatePeerRes(status));
      return;
    }
    impl.setActive(true);
    resultHandler.onComplete(
        new TActivatePeerRes(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
  }

  @Override
  public void buildSyncLogChannel(
      TBuildSyncLogChannelReq req, AsyncMethodCallback<TBuildSyncLogChannelRes> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TBuildSyncLogChannelRes(status));
      return;
    }
    TSStatus responseStatus;
    try {
      impl.buildSyncLogChannel(new Peer(groupId, req.nodeId, req.endPoint));
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
    }
    resultHandler.onComplete(new TBuildSyncLogChannelRes(responseStatus));
  }

  @Override
  public void removeSyncLogChannel(
      TRemoveSyncLogChannelReq req, AsyncMethodCallback<TRemoveSyncLogChannelRes> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TRemoveSyncLogChannelRes(status));
      return;
    }
    TSStatus responseStatus;
    try {
      impl.removeSyncLogChannel(new Peer(groupId, req.nodeId, req.endPoint));
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
    }
    resultHandler.onComplete(new TRemoveSyncLogChannelRes(responseStatus));
  }

  @Override
  public void waitSyncLogComplete(
      TWaitSyncLogCompleteReq req, AsyncMethodCallback<TWaitSyncLogCompleteRes> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for waitSyncLogComplete request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TWaitSyncLogCompleteRes(true, 0, 0));
      return;
    }
    long searchIndex = impl.getIndex();
    long safeIndex = impl.getCurrentSafelyDeletedSearchIndex();
    resultHandler.onComplete(
        new TWaitSyncLogCompleteRes(searchIndex == safeIndex, searchIndex, safeIndex));
  }

  @Override
  public void sendSnapshotFragment(
      TSendSnapshotFragmentReq req, AsyncMethodCallback<TSendSnapshotFragmentRes> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TSendSnapshotFragmentRes(status));
      return;
    }
    TSStatus responseStatus;
    try {
      impl.receiveSnapshotFragment(req.snapshotId, req.filePath, req.fileChunk);
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
    }
    resultHandler.onComplete(new TSendSnapshotFragmentRes(responseStatus));
  }

  @Override
  public void triggerSnapshotLoad(
      TTriggerSnapshotLoadReq req, AsyncMethodCallback<TTriggerSnapshotLoadRes> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TTriggerSnapshotLoadRes(status));
      return;
    }
    impl.loadSnapshot(req.snapshotId);
    resultHandler.onComplete(
        new TTriggerSnapshotLoadRes(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
  }

  @Override
  public void cleanupTransferredSnapshot(
      TCleanupTransferredSnapshotReq req,
      AsyncMethodCallback<TCleanupTransferredSnapshotRes> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    IoTConsensusServerImpl impl = consensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for buildSyncLogChannel request", groupId);
      logger.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TCleanupTransferredSnapshotRes(status));
      return;
    }
    TSStatus responseStatus;
    try {
      impl.cleanupTransferredSnapshot(req.snapshotId);
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      logger.error(String.format("failed to cleanup transferred snapshot %s", req.snapshotId), e);
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
    }
    resultHandler.onComplete(new TCleanupTransferredSnapshotRes(responseStatus));
  }

  public void handleClientExit() {}
}
