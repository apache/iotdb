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

package org.apache.iotdb.consensus.pipe.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.PipeConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.consensus.pipe.PipeConsensusServerImpl;
import org.apache.iotdb.consensus.pipe.thrift.PipeConsensusIService;
import org.apache.iotdb.consensus.pipe.thrift.TCheckConsensusPipeCompletedReq;
import org.apache.iotdb.consensus.pipe.thrift.TCheckConsensusPipeCompletedResp;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToCreateConsensusPipeReq;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToCreateConsensusPipeResp;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToDropConsensusPipeReq;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToDropConsensusPipeResp;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferResp;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.consensus.pipe.thrift.TSetActiveReq;
import org.apache.iotdb.consensus.pipe.thrift.TSetActiveResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConsensusRPCServiceProcessor implements PipeConsensusIService.AsyncIface {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusRPCServiceProcessor.class);
  private final PipeConsensus pipeConsensus;

  private final PipeConsensusConfig.Pipe config;

  public PipeConsensusRPCServiceProcessor(
      PipeConsensus pipeConsensus, PipeConsensusConfig.Pipe config) {
    this.pipeConsensus = pipeConsensus;
    this.config = config;
  }

  @Override
  public void pipeConsensusTransfer(
      TPipeConsensusTransferReq req,
      AsyncMethodCallback<TPipeConsensusTransferResp> resultHandler) {
    try {
      TPipeConsensusTransferResp resp = config.getConsensusPipeReceiver().receive(req);
      // we need to call onComplete by hand
      resultHandler.onComplete(resp);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  // TODO: consider batch transfer
  @Override
  public void pipeConsensusBatchTransfer(
      TPipeConsensusBatchTransferReq req,
      AsyncMethodCallback<TPipeConsensusBatchTransferResp> resultHandler)
      throws TException {}

  @Override
  public void setActive(TSetActiveReq req, AsyncMethodCallback<TSetActiveResp> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    PipeConsensusServerImpl impl = pipeConsensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for set active request %s", groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TSetActiveResp(status));
      return;
    }
    impl.setActive(req.isActive);
    resultHandler.onComplete(new TSetActiveResp(RpcUtils.SUCCESS_STATUS));
  }

  @Override
  public void notifyPeerToCreateConsensusPipe(
      TNotifyPeerToCreateConsensusPipeReq req,
      AsyncMethodCallback<TNotifyPeerToCreateConsensusPipeResp> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.targetPeerConsensusGroupId);
    PipeConsensusServerImpl impl = pipeConsensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for create consensus pipe request %s", groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TNotifyPeerToCreateConsensusPipeResp(status));
      return;
    }
    TSStatus responseStatus;
    try {
      impl.createConsensusPipeToTargetPeer(
          new Peer(
              ConsensusGroupId.Factory.createFromTConsensusGroupId(req.targetPeerConsensusGroupId),
              req.targetPeerNodeId,
              req.targetPeerEndPoint));
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
      LOGGER.warn("Failed to create consensus pipe to target peer with req {}", req, e);
    }
    resultHandler.onComplete(new TNotifyPeerToCreateConsensusPipeResp(responseStatus));
  }

  @Override
  public void notifyPeerToDropConsensusPipe(
      TNotifyPeerToDropConsensusPipeReq req,
      AsyncMethodCallback<TNotifyPeerToDropConsensusPipeResp> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.targetPeerConsensusGroupId);
    PipeConsensusServerImpl impl = pipeConsensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for drop consensus pipe request %s", groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TNotifyPeerToDropConsensusPipeResp(status));
      return;
    }
    TSStatus responseStatus;
    try {
      impl.dropConsensusPipeToTargetPeer(
          new Peer(
              ConsensusGroupId.Factory.createFromTConsensusGroupId(req.targetPeerConsensusGroupId),
              req.targetPeerNodeId,
              req.targetPeerEndPoint));
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
      LOGGER.warn("Failed to drop consensus pipe to target peer with req {}", req, e);
    }
    resultHandler.onComplete(new TNotifyPeerToDropConsensusPipeResp(responseStatus));
  }

  @Override
  public void checkConsensusPipeCompleted(
      TCheckConsensusPipeCompletedReq req,
      AsyncMethodCallback<TCheckConsensusPipeCompletedResp> resultHandler)
      throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    PipeConsensusServerImpl impl = pipeConsensus.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for check transfer completed request %s",
              groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      resultHandler.onComplete(new TCheckConsensusPipeCompletedResp(status, true));
      return;
    }
    TSStatus responseStatus;
    boolean isCompleted;
    try {
      isCompleted =
          impl.isConsensusPipesTransmissionCompleted(
              req.consensusPipeNames, req.refreshCachedProgressIndex);
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
      isCompleted = true;
      LOGGER.warn(
          "Failed to check consensus pipe completed with req {}, set is completed to {}",
          req,
          true,
          e);
    }
    resultHandler.onComplete(new TCheckConsensusPipeCompletedResp(responseStatus, isCompleted));
  }

  public void handleExit() {}
}
