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

package org.apache.iotdb.consensus.air.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusInactivatePeerKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.AirReplicationConfig;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.air.AirReplication;
import org.apache.iotdb.consensus.air.AirReplicationServerImpl;
import org.apache.iotdb.consensus.air.thrift.AirReplicationIService;
import org.apache.iotdb.consensus.air.thrift.TCheckAirReplicationCompletedReq;
import org.apache.iotdb.consensus.air.thrift.TCheckAirReplicationCompletedResp;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToCreateAirReplicationReq;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToCreateAirReplicationResp;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToDropAirReplicationReq;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToDropAirReplicationResp;
import org.apache.iotdb.consensus.air.thrift.TAirReplicationBatchTransferReq;
import org.apache.iotdb.consensus.air.thrift.TAirReplicationBatchTransferResp;
import org.apache.iotdb.consensus.air.thrift.TAirReplicationTransferReq;
import org.apache.iotdb.consensus.air.thrift.TAirReplicationTransferResp;
import org.apache.iotdb.consensus.pipe.thrift.TSetActiveReq;
import org.apache.iotdb.consensus.pipe.thrift.TSetActiveResp;
import org.apache.iotdb.consensus.pipe.thrift.TWaitReleaseAllRegionRelatedResourceReq;
import org.apache.iotdb.consensus.pipe.thrift.TWaitReleaseAllRegionRelatedResourceResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AirReplicationRPCServiceProcessor implements AirReplicationIService.Iface {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AirReplicationRPCServiceProcessor.class);
  private final AirReplication airReplication;

  private final AirReplicationConfig.Air config;

  public AirReplicationRPCServiceProcessor(
      AirReplication airReplication, AirReplicationConfig.Air config) {
    this.airReplication = airReplication;
    this.config = config;
  }

  @Override
  public TAirReplicationTransferResp airReplicationTransfer(TAirReplicationTransferReq req) {
    return config.getAirReplicationReceiver().receive(req);
  }

  // TODO: consider batch transfer
  @Override
  public TAirReplicationBatchTransferResp airReplicationBatchTransfer(
      TAirReplicationBatchTransferReq req) throws TException {
    return new TAirReplicationBatchTransferResp();
  }

  @Override
  public TSetActiveResp setActive(TSetActiveReq req) throws TException {
    if (req.isForDeletionPurpose && !req.isActive) {
      KillPoint.setKillPoint(IoTConsensusInactivatePeerKillPoints.BEFORE_INACTIVATE);
    }
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    AirReplicationServerImpl impl = airReplication.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format("unexpected consensusGroupId %s for set active request %s", groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TSetActiveResp(status);
    }
    impl.setActive(req.isActive);
    if (req.isActive) {
      KillPoint.setKillPoint(DataNodeKillPoints.DESTINATION_ADD_PEER_DONE);
    }
    if (req.isForDeletionPurpose && !req.isActive) {
      KillPoint.setKillPoint(IoTConsensusInactivatePeerKillPoints.AFTER_INACTIVATE);
    }
    return new TSetActiveResp(RpcUtils.SUCCESS_STATUS);
  }

  @Override
  public TNotifyPeerToCreateConsensusPipeResp notifyPeerToCreateConsensusPipe(
      TNotifyPeerToCreateConsensusPipeReq req) throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.targetPeerConsensusGroupId);
    AirReplicationServerImpl impl = airReplication.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for create consensus pipe request %s", groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TNotifyPeerToCreateConsensusPipeResp(status);
    }
    TSStatus responseStatus;
    try {
      // Other peers which don't act as coordinator will only transfer data(may contain both
      // historical and realtime data) after the snapshot progress.
      impl.createConsensusPipeToTargetPeer(
          new Peer(
              ConsensusGroupId.Factory.createFromTConsensusGroupId(req.targetPeerConsensusGroupId),
              req.targetPeerNodeId,
              req.targetPeerEndPoint),
          false);
      responseStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (ConsensusGroupModifyPeerException e) {
      responseStatus = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      responseStatus.setMessage(e.getMessage());
      LOGGER.warn("Failed to create consensus pipe to target peer with req {}", req, e);
    }
    return new TNotifyPeerToCreateConsensusPipeResp(responseStatus);
  }

  @Override
  public TNotifyPeerToDropConsensusPipeResp notifyPeerToDropConsensusPipe(
      TNotifyPeerToDropConsensusPipeReq req) throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.targetPeerConsensusGroupId);
    AirReplicationServerImpl impl = airReplication.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for drop consensus pipe request %s", groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TNotifyPeerToDropConsensusPipeResp(status);
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
    return new TNotifyPeerToDropConsensusPipeResp(responseStatus);
  }

  @Override
  public TCheckConsensusPipeCompletedResp checkConsensusPipeCompleted(
      TCheckConsensusPipeCompletedReq req) throws TException {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    AirReplicationServerImpl impl = airReplication.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for check transfer completed request %s",
              groupId, req);
      LOGGER.error(message);
      TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      status.setMessage(message);
      return new TCheckConsensusPipeCompletedResp(status, true);
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
    return new TCheckConsensusPipeCompletedResp(responseStatus, isCompleted);
  }

  @Override
  public TWaitReleaseAllRegionRelatedResourceResp waitReleaseAllRegionRelatedResource(
      TWaitReleaseAllRegionRelatedResourceReq req) {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
    AirReplicationServerImpl impl = airReplication.getImpl(groupId);
    if (impl == null) {
      String message =
          String.format(
              "unexpected consensusGroupId %s for TWaitReleaseAllRegionRelatedResourceRes request",
              groupId);
      LOGGER.error(message);
      return new TWaitReleaseAllRegionRelatedResourceResp(true);
    }
    return new TWaitReleaseAllRegionRelatedResourceResp(
        impl.hasReleaseAllRegionRelatedResource(groupId));
  }

  public void handleExit() {}
}
