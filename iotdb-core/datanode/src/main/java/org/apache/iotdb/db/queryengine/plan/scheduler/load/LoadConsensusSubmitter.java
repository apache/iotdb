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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.execution.executor.RegionExecutionResult;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Submits LOAD consensus lifecycle commands (PIECE, PREPARE, COMMIT, ABORT) to target DataRegions
 * via local execution or internal sync Thrift RPC.
 */
public class LoadConsensusSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadConsensusSubmitter.class);

  private static final long ROUTE_RESOLVE_BASE_BACKOFF_MS = 100L;

  private final TEndPoint localEndPoint;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  public LoadConsensusSubmitter(
      final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager) {
    this.clientManager =
        Objects.requireNonNull(
            clientManager, DataNodeQueryMessages.EXCEPTION_CLIENTMANAGER_CANNOT_BE_NULL_FAF70317);
    final String localIp = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    final int localPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
    this.localEndPoint = new TEndPoint(localIp, localPort);
  }

  // -------------------------------------------------------------------------
  // Route Resolution
  // -------------------------------------------------------------------------

  /** Re-resolves target region replica route with exponential backoff on stale topology. */
  public TRegionReplicaSet resolveRoute(final TConsensusGroupId regionId, final int maxAttempts) {
    if (regionId == null || maxAttempts <= 0) {
      return null;
    }

    final ClusterPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        partitionFetcher.invalidAllCache();
        final List<TRegionReplicaSet> replicaSets =
            partitionFetcher.getRegionReplicaSet(Collections.singletonList(regionId));
        if (replicaSets != null && !replicaSets.isEmpty()) {
          return replicaSets.get(0);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            StorageEngineMessages
                .LOG_FAILED_TO_LOOK_THE_ROUTE_OF_REGION_ARG_UP_AGAIN_ATTEMPT_ARG_OF_ARG_ARG_4A94EC21,
            regionId,
            attempt,
            maxAttempts,
            e.getMessage());
      }

      if (attempt < maxAttempts) {
        try {
          Thread.sleep(ROUTE_RESOLVE_BASE_BACKOFF_MS * attempt);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn(
              StorageEngineMessages.LOG_ROUTE_RESOLUTION_INTERRUPTED_FOR_REGION_ARG_F8799966,
              regionId);
          return null;
        }
      }
    }
    return null;
  }

  // -------------------------------------------------------------------------
  // Command Submission Pipeline
  // -------------------------------------------------------------------------

  /** Submits a consensus command node to the resolved active write peer of the replica set. */
  public TSStatus submit(final TRegionReplicaSet replicaSet, final LoadTsFileConsensusNode node) {
    Objects.requireNonNull(
        replicaSet, StorageEngineMessages.EXCEPTION_REPLICASET_CANNOT_BE_NULL_A7340AC3);
    Objects.requireNonNull(node, StorageEngineMessages.EXCEPTION_NODE_CANNOT_BE_NULL_BC7D5BB9);

    if (replicaSet.getRegionId() == null) {
      return RpcUtils.getStatus(
          TSStatusCode.DISPATCH_ERROR,
          StorageEngineMessages.MESSAGE_TREGIONREPLICASET_HAS_NULL_TCONSENSUSGROUPID_B7172B9E);
    }

    final ConsensusGroupId regionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(replicaSet.getRegionId());
    node.setRegionReplicaSet(replicaSet);

    final String consensusProtocol =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    LOGGER.info(
        StorageEngineMessages.LOG_LOAD_CONSENSUS_WRITE_TO_REGION_ARG_VIA_PROTOCOL_ARG_EBB55042,
        regionId,
        consensusProtocol);

    final TDataNodeLocation writePeer = resolveWritePeer(replicaSet, regionId, consensusProtocol);
    if (writePeer == null || writePeer.getInternalEndPoint() == null) {
      return RpcUtils.getStatus(
          TSStatusCode.DISPATCH_ERROR,
          String.format(
              StorageEngineMessages
                  .MESSAGE_UNABLE_TO_RESOLVE_VALID_WRITE_PEER_FOR_REGION_ARG_76190FB5,
              regionId));
    }

    final TEndPoint targetEndPoint = writePeer.getInternalEndPoint();
    return isLocal(targetEndPoint)
        ? writeLocal(regionId, node)
        : writeRemote(targetEndPoint, regionId, node);
  }

  /**
   * Resolves the primary write peer: Ratis matches active elected leader, IoTConsensus routes to
   * index 0.
   */
  TDataNodeLocation resolveWritePeer(
      final TRegionReplicaSet replicaSet, final ConsensusGroupId regionId, final String protocol) {
    final List<TDataNodeLocation> locations = replicaSet.getDataNodeLocations();
    if (locations == null || locations.isEmpty()) {
      return null;
    }

    if (ConsensusFactory.RATIS_CONSENSUS.equals(protocol)) {
      final Peer leader = DataRegionConsensusImpl.getInstance().getLeader(regionId);
      if (leader != null) {
        for (final TDataNodeLocation location : locations) {
          if (location.getDataNodeId() == leader.getNodeId()) {
            return location;
          }
        }
        LOGGER.warn(
            StorageEngineMessages
                .LOG_LOAD_CONSENSUS_ROUTE_OF_REGION_ARG_IS_STALE_WRITE_NODE_ARG_IS_NOT_IN_REPLICA_SET_ARG_E7F1DDD2,
            regionId,
            leader.getNodeId(),
            replicaSet);
        return null;
      }
    }
    return locations.get(0);
  }

  // -------------------------------------------------------------------------
  // Local & Remote Invocations
  // -------------------------------------------------------------------------

  private TSStatus writeLocal(final ConsensusGroupId regionId, final LoadTsFileConsensusNode node) {
    try {
      final RegionWriteExecutor executor = new RegionWriteExecutor();
      final RegionExecutionResult result = executor.execute(regionId, node);
      return result != null && result.getStatus() != null
          ? result.getStatus()
          : RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR,
              StorageEngineMessages.MESSAGE_NULL_LOCAL_EXECUTION_RESULT_E58C362F);
    } catch (final Exception e) {
      LOGGER.error(
          StorageEngineMessages.LOG_FAILED_TO_EXECUTE_CONSENSUS_NODE_LOCALLY_ON_REGION_ARG_3B675D32,
          regionId,
          e);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
  }

  private TSStatus writeRemote(
      final TEndPoint endPoint,
      final ConsensusGroupId regionId,
      final LoadTsFileConsensusNode node) {
    try (final SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      final TSendSinglePlanNodeReq singleReq =
          new TSendSinglePlanNodeReq(
              new TPlanNode(node.serializeToByteBuffer()), regionId.convertToTConsensusGroupId());

      final TSendBatchPlanNodeReq batchReq =
          new TSendBatchPlanNodeReq(Collections.singletonList(singleReq));

      final TSendBatchPlanNodeResp batchResp = client.sendBatchPlanNode(batchReq);
      if (batchResp == null
          || batchResp.getResponses() == null
          || batchResp.getResponses().isEmpty()) {
        return RpcUtils.getStatus(
            TSStatusCode.DISPATCH_ERROR,
            String.format(
                StorageEngineMessages.MESSAGE_EMPTY_BATCH_RESPONSE_FROM_ARG_770449BB, endPoint));
      }

      final TSendSinglePlanNodeResp singleResp = batchResp.getResponses().get(0);
      if (singleResp.isAccepted()) {
        return RpcUtils.SUCCESS_STATUS;
      }

      return singleResp.getStatus() != null
          ? singleResp.getStatus()
          : RpcUtils.getStatus(
              TSStatusCode.DISPATCH_ERROR,
              StorageEngineMessages.MESSAGE_TARGET_NODE_REJECTED_COMMAND_WITHOUT_STATUS_12009B10);
    } catch (final Exception e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_FAILED_TO_DISPATCH_LOAD_COMMAND_TO_REMOTE_ENDPOINT_ARG_142A712A,
          endPoint,
          e);
      return RpcUtils.getStatus(
          TSStatusCode.DISPATCH_ERROR,
          String.format(
              StorageEngineMessages.MESSAGE_RPC_COMMUNICATION_FAILURE_TO_ARG_ARG_1B429913,
              endPoint,
              e.getMessage()));
    }
  }

  private boolean isLocal(final TEndPoint endPoint) {
    return localEndPoint.equals(endPoint);
  }
}
