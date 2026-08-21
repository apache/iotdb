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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.execution.executor.RegionExecutionResult;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Transport for LOAD consensus commands (BEGIN / PIECE / PREPARE / COMMIT / ABORT). One instance is
 * shared by all files of a scheduler; it is stateless besides the local endpoint and the client
 * manager.
 *
 * <p>{@link #submit(TRegionReplicaSet, LoadTsFileConsensusNode)}:
 *
 * <ol>
 *   <li>stamps the target {@code regionReplicaSet} onto the node for correlation only - no follower
 *       endpoints are carried, replicas receive the command through consensus log replication
 *       exactly like ordinary writes;
 *   <li>resolves the single write peer of the partition: the current Ratis leader when the protocol
 *       is Ratis, otherwise the first replica-set location (the IoTConsensus write node, the same
 *       target the normal write path dispatches to);
 *   <li>writes the command through {@link RegionWriteExecutor} (local) or the internal RPC ({@code
 *       sendBatchPlanNode}) on that peer, which applies it via {@code
 *       DataRegionConsensusImpl.write} like any other write plan.
 * </ol>
 *
 * <p>IoTConsensus replicates the WAL entries (marker-only for LOAD pieces) to the followers, whose
 * own {@code TsFileWriterManager} rebuilds the staged files; the chunk bytes are pulled back from
 * the write node on demand. Ratis replicates the full command through its own log, so every replica
 * applies the chunk data directly and keeps its own writer.
 */
public class LoadConsensusSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadConsensusSubmitter.class);

  private final String localhostIp;
  private final int localhostPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  public LoadConsensusSubmitter(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager) {
    this.clientManager = clientManager;
    this.localhostIp = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  public TSStatus submit(TRegionReplicaSet replicaSet, LoadTsFileConsensusNode node) {
    final ConsensusGroupId regionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(replicaSet.getRegionId());
    // Follow the freshest partition route: after a write-node switch (leader change / region
    // migration) the replica set captured at split time may be stale, so re-resolve it from the
    // local partition table (a cache miss fetches the latest route map from the ConfigNode) and
    // submit to the current write node, exactly like normal writes.
    final TRegionReplicaSet currentReplicaSet = refreshReplicaSet(replicaSet, regionId);
    node.setRegionReplicaSet(currentReplicaSet);

    final String protocol =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    LOGGER.info(
        StorageEngineMessages.LOG_LOAD_CONSENSUS_WRITE_TO_REGION_ARG_VIA_PROTOCOL_ARG_EBB55042,
        regionId,
        protocol);

    final TDataNodeLocation writePeer = resolveWritePeer(currentReplicaSet, regionId, protocol);
    if (writePeer == null) {
      return new TSStatus(TSStatusCode.DISPATCH_ERROR.getStatusCode())
          .setMessage(String.valueOf(replicaSet));
    }
    return isLocal(writePeer.getInternalEndPoint())
        ? writeLocal(regionId, node)
        : writeRemote(writePeer.getInternalEndPoint(), regionId, node);
  }

  /**
   * Re-resolves the partition replica set from the local partition table (falling back to the
   * passed set when the lookup fails). IoTConsensus V1 has no leader election, so after a write
   * node switch the coordinator must route by the refreshed route map instead of the stale set.
   */
  private TRegionReplicaSet refreshReplicaSet(
      TRegionReplicaSet replicaSet, ConsensusGroupId regionId) {
    try {
      final List<TRegionReplicaSet> replicaSets =
          ClusterPartitionFetcher.getInstance()
              .getRegionReplicaSet(
                  Collections.singletonList(regionId.convertToTConsensusGroupId()));
      if (!replicaSets.isEmpty()) {
        return replicaSets.get(0);
      }
    } catch (Exception e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_REFRESH_REPLICA_SET_FAILED_7C244C63,
          regionId,
          e.getMessage());
    }
    return replicaSet;
  }

  /**
   * Resolves the single write peer of the partition. Ratis writes must land on the current leader,
   * so the leader endpoint is matched against the replica-set locations first (falling back to the
   * first location while the leader is not known yet); IoTConsensus routes every write to the first
   * replica-set location, which is the partition's write node - the same target normal writes use.
   */
  private TDataNodeLocation resolveWritePeer(
      TRegionReplicaSet replicaSet, ConsensusGroupId regionId, String protocol) {
    final List<TDataNodeLocation> locations = replicaSet.getDataNodeLocations();
    if (locations == null || locations.isEmpty()) {
      return null;
    }
    if (ConsensusFactory.RATIS_CONSENSUS.equals(protocol)) {
      final Peer leader = DataRegionConsensusImpl.getInstance().getLeader(regionId);
      if (leader != null) {
        for (TDataNodeLocation location : locations) {
          final TEndPoint endPoint = location.getInternalEndPoint();
          if (endPoint != null && endPoint.getIp().equals(leader.getEndpoint().getIp())) {
            return location;
          }
        }
      }
    }
    return locations.get(0);
  }

  private TSStatus writeLocal(ConsensusGroupId regionId, LoadTsFileConsensusNode node) {
    final RegionWriteExecutor executor = new RegionWriteExecutor();
    final RegionExecutionResult result = executor.execute(regionId, node);
    return result.getStatus();
  }

  private TSStatus writeRemote(
      TEndPoint endPoint, ConsensusGroupId regionId, LoadTsFileConsensusNode node) {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      final TSendSinglePlanNodeReq singleReq =
          new TSendSinglePlanNodeReq(
              new TPlanNode(node.serializeToByteBuffer()), regionId.convertToTConsensusGroupId());
      final TSendBatchPlanNodeReq batchReq =
          new TSendBatchPlanNodeReq(Collections.singletonList(singleReq));
      final List<TSendSinglePlanNodeResp> responses =
          client.sendBatchPlanNode(batchReq).getResponses();
      if (responses == null || responses.isEmpty()) {
        return new TSStatus(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      }
      final TSendSinglePlanNodeResp resp = responses.get(0);
      if (resp.isAccepted()) {
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      return resp.getStatus() != null
          ? resp.getStatus()
          : new TSStatus(TSStatusCode.DISPATCH_ERROR.getStatusCode());
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.DISPATCH_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
  }

  private boolean isLocal(TEndPoint endPoint) {
    return endPoint != null
        && localhostIp.equals(endPoint.getIp())
        && localhostPort == endPoint.getPort();
  }
}
