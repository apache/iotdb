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
 *   <li>resolves the single write peer of the partition within that set: the current Ratis leader
 *       when the protocol is Ratis, otherwise the first replica-set location (the IoTConsensus
 *       write node, the same target the normal write path dispatches to);
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

  /** How long a route lookup that failed is waited out before it is tried again. */
  private static final long LOAD_CONSENSUS_ROUTE_RESOLVE_BACKOFF_MS = 100L;

  private final String localhostIp;
  private final int localhostPort;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  public LoadConsensusSubmitter(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager) {
    this.clientManager = clientManager;
    this.localhostIp = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  /**
   * Looks the route of a region up again and returns the freshest replica set, or null when it
   * cannot be resolved within the attempts.
   *
   * <p>What this DataNode has cached is dropped first: a lookup answers with the route the cache
   * holds, and the route that has to be looked up again is exactly the one that may have changed
   * under a write-node switch or a region migration. That is what the query path does before it
   * re-analyzes a statement it was redirected for. A route that resolves to nothing within the
   * attempts is reported as no route at all, so the caller repeats its command on the route it had
   * instead of silently replacing it with one that could not be read.
   */
  public TRegionReplicaSet resolveRoute(final TConsensusGroupId regionId, final int attempts) {
    for (int attempt = 1; attempt <= attempts; attempt++) {
      try {
        ClusterPartitionFetcher.getInstance().invalidAllCache();
        final List<TRegionReplicaSet> replicaSets =
            ClusterPartitionFetcher.getInstance()
                .getRegionReplicaSet(Collections.singletonList(regionId));
        if (!replicaSets.isEmpty()) {
          return replicaSets.get(0);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            StorageEngineMessages
                .LOG_FAILED_TO_LOOK_THE_ROUTE_OF_REGION_ARG_UP_AGAIN_ATTEMPT_ARG_OF_ARG_ARG_4A94EC21,
            regionId,
            attempt,
            attempts,
            e.getMessage());
      }
      try {
        Thread.sleep(LOAD_CONSENSUS_ROUTE_RESOLVE_BACKOFF_MS * attempt);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  public TSStatus submit(TRegionReplicaSet replicaSet, LoadTsFileConsensusNode node) {
    final ConsensusGroupId regionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(replicaSet.getRegionId());
    // The route is pinned for the whole transaction: the pieces of a task are already staged under
    // the replica set the splitter resolved, and the PREPARE, COMMIT and ABORT of that task are
    // sent
    // to the same one. Re-resolving it per command would let a migration between two pieces move
    // the
    // rest of the task to a route whose regions hold a different plan, and a route that is merely
    // refreshed while the pinned one is still the one holding the staged bytes tells nothing about
    // whether those bytes may be committed. A pinned route that has become obsolete fails the
    // dispatch instead, and the load is retried from the start, which is what the migration check
    // of
    // the scheduler asks for as well.
    node.setRegionReplicaSet(replicaSet);

    final String protocol =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    LOGGER.info(
        StorageEngineMessages.LOG_LOAD_CONSENSUS_WRITE_TO_REGION_ARG_VIA_PROTOCOL_ARG_EBB55042,
        regionId,
        protocol);

    final TDataNodeLocation writePeer = resolveWritePeer(replicaSet, regionId, protocol);
    if (writePeer == null) {
      return new TSStatus(TSStatusCode.DISPATCH_ERROR.getStatusCode())
          .setMessage(String.valueOf(replicaSet));
    }
    return isLocal(writePeer.getInternalEndPoint())
        ? writeLocal(regionId, node)
        : writeRemote(writePeer.getInternalEndPoint(), regionId, node);
  }

  /**
   * Resolves the single write peer of the partition. Ratis writes must land on the current leader,
   * so the leader is matched against the replica-set locations by its DataNode id first: a Ratis
   * leader is reported as a node id, without an endpoint to compare, so the node id is the only
   * thing that identifies it. When the leader is known but the route does not hold it, the route is
   * stale and no peer is returned at all - sending the command to another replica would leave the
   * pieces staged on a node that cannot commit them. When the leader is not known (the coordinator
   * does not host the partition, or Ratis has not elected one yet), IoTConsensus-like routing to
   * the first location is used: that is the partition's write node, the same target normal writes
   * use.
   *
   * <p>Package-private so that the resolution can be pinned by a test without a cluster around it.
   */
  TDataNodeLocation resolveWritePeer(
      TRegionReplicaSet replicaSet, ConsensusGroupId regionId, String protocol) {
    final List<TDataNodeLocation> locations = replicaSet.getDataNodeLocations();
    if (locations == null || locations.isEmpty()) {
      return null;
    }
    if (ConsensusFactory.RATIS_CONSENSUS.equals(protocol)) {
      final Peer leader = DataRegionConsensusImpl.getInstance().getLeader(regionId);
      if (leader != null) {
        for (TDataNodeLocation location : locations) {
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
