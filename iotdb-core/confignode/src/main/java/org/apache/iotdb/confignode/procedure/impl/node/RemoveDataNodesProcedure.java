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

package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeManager;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.confignode.procedure.state.RemoveDataNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;

/** remove data node procedure */
public class RemoveDataNodesProcedure extends AbstractNodeProcedure<RemoveDataNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveDataNodesProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private List<TDataNodeLocation> removedDataNodes;

  private List<RegionMigrationPlan> regionMigrationPlans = new ArrayList<>();

  public RemoveDataNodesProcedure() {
    super();
  }

  public RemoveDataNodesProcedure(List<TDataNodeLocation> removedDataNodes) {
    super();
    this.removedDataNodes = removedDataNodes;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveDataNodeState state) {
    if (removedDataNodes.isEmpty()) {
      return Flow.NO_MORE_STATE;
    }

    RemoveDataNodeManager manager = env.getRemoveDataNodeManager();
    try {
      switch (state) {
        case REGION_REPLICA_CHECK:
          if (env.checkEnoughDataNodeAfterRemoving(removedDataNodes)) {
            setNextState(RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
          } else {
            LOG.error(
                "{}, Can not remove DataNode {} "
                    + "because the number of DataNodes is less or equal than region replica number",
                REMOVE_DATANODE_PROCESS,
                removedDataNodes);
            return Flow.NO_MORE_STATE;
          }
        case REMOVE_DATA_NODE_PREPARE:
          // mark the datanode as removing status and broadcast region route map
          env.markDataNodesAsRemovingAndBroadcast(removedDataNodes);
          regionMigrationPlans = manager.getRegionMigrationPlans(removedDataNodes);
          LOG.info(
              "{}, DataNode regions to be removed is {}",
              REMOVE_DATANODE_PROCESS,
              regionMigrationPlans);
          setNextState(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE);
          break;
        case BROADCAST_DISABLE_DATA_NODE:
          manager.broadcastDisableDataNodes(removedDataNodes);
          setNextState(RemoveDataNodeState.SUBMIT_REGION_MIGRATE);
          break;
        case SUBMIT_REGION_MIGRATE:
          submitChildRegionMigrate(env);
          setNextState(RemoveDataNodeState.STOP_DATA_NODE);
          break;
        case STOP_DATA_NODE:
          if (isAllRegionMigratedSuccessfully(env)) {
            LOG.info("{}, Begin to stop DataNode: {}", REMOVE_DATANODE_PROCESS, removedDataNodes);
            manager.removeDataNodePersistence(removedDataNodes);
            manager.stopDataNodes(removedDataNodes);
          }
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Remove Data Node failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to remove data node {}, state {}",
            removedDataNodes,
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void submitChildRegionMigrate(ConfigNodeProcedureEnv env) {
    regionMigrationPlans.forEach(
        regionMigrationPlan -> {
          TConsensusGroupId regionId = regionMigrationPlan.getRegionId();
          TDataNodeLocation removedDataNode = regionMigrationPlan.getFromDataNode();
          TDataNodeLocation destDataNode =
              env.getRegionMaintainHandler().findDestDataNode(regionId);
          // TODO: need to improve the coordinator selection method here, maybe through load
          // balancing and other means.
          final TDataNodeLocation coordinatorForAddPeer =
              env.getRegionMaintainHandler()
                  .filterDataNodeWithOtherRegionReplica(regionId, destDataNode)
                  .orElse(removedDataNode);
          final TDataNodeLocation coordinatorForRemovePeer = destDataNode;
          if (destDataNode != null) {
            RegionMigrateProcedure regionMigrateProcedure =
                new RegionMigrateProcedure(
                    regionId,
                    removedDataNode,
                    destDataNode,
                    coordinatorForAddPeer,
                    coordinatorForRemovePeer);
            addChildProcedure(regionMigrateProcedure);
            LOG.info("Submit child procedure {} for regionId {}", regionMigrateProcedure, regionId);
          } else {
            LOG.error(
                "{}, Cannot find target DataNode to migrate the region: {}",
                REMOVE_DATANODE_PROCESS,
                regionId);
            // TODO terminate all the uncompleted remove datanode process
          }
        });
  }

  private boolean isAllRegionMigratedSuccessfully(ConfigNodeProcedureEnv env) {
    List<TRegionReplicaSet> replicaSets =
        env.getConfigManager().getPartitionManager().getAllReplicaSets();
    List<TConsensusGroupId> migratedFailedRegions =
        replicaSets.stream()
            .filter(
                replica ->
                    removedDataNodes.stream()
                        .anyMatch(
                            removedDataNode ->
                                replica.getDataNodeLocations().contains(removedDataNode)))
            .map(TRegionReplicaSet::getRegionId)
            .collect(Collectors.toList());
    if (!migratedFailedRegions.isEmpty()) {
      LOG.warn(
          "{}, Some regions are migrated failed, the StopDataNode process should not be executed, migratedFailedRegions: {}",
          REMOVE_DATANODE_PROCESS,
          migratedFailedRegions);
      return false;
    }

    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RemoveDataNodeState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean isRollbackSupported(RemoveDataNodeState state) {
    return false;
  }

  /**
   * Used to keep procedure lock even when the procedure is yielded or suspended.
   *
   * @param env env
   * @return true if hold the lock
   */
  protected boolean holdLock(ConfigNodeProcedureEnv env) {
    return true;
  }

  @Override
  protected RemoveDataNodeState getState(int stateId) {
    return RemoveDataNodeState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveDataNodeState removeDataNodeState) {
    return removeDataNodeState.ordinal();
  }

  @Override
  protected RemoveDataNodeState getInitialState() {
    return RemoveDataNodeState.REGION_REPLICA_CHECK;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_DATA_NODE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    stream.writeInt(removedDataNodes.size());
    removedDataNodes.forEach(
        dataNode -> ThriftCommonsSerDeUtils.serializeTDataNodeLocation(dataNode, stream));
    stream.writeInt(regionMigrationPlans.size());
    regionMigrationPlans.forEach(
        regionMigrationPlan -> {
          ThriftCommonsSerDeUtils.serializeTConsensusGroupId(
              regionMigrationPlan.getRegionId(), stream);
          ThriftCommonsSerDeUtils.serializeTDataNodeLocation(
              regionMigrationPlan.getFromDataNode(), stream);
          ThriftCommonsSerDeUtils.serializeTDataNodeLocation(
              regionMigrationPlan.getToDataNode(), stream);
        });
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      int removedDataNodeSize = byteBuffer.getInt();
      removedDataNodes = new ArrayList<>(removedDataNodeSize);
      for (int i = 0; i < removedDataNodeSize; i++) {
        removedDataNodes.add(ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer));
      }
      int regionMigrationPlanSize = byteBuffer.getInt();
      regionMigrationPlans = new ArrayList<>(regionMigrationPlanSize);
      for (int i = 0; i < regionMigrationPlanSize; i++) {
        TConsensusGroupId regionId =
            ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
        TDataNodeLocation fromDataNode =
            ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
        RegionMigrationPlan regionMigrationPlan =
            RegionMigrationPlan.create(regionId, fromDataNode);
        regionMigrationPlan.setToDataNode(
            ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer));
        regionMigrationPlans.add(regionMigrationPlan);
      }
    } catch (ThriftSerDeException e) {
      LOG.error("Error in deserialize RemoveConfigNodeProcedure", e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RemoveDataNodesProcedure) {
      RemoveDataNodesProcedure thatProc = (RemoveDataNodesProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.removedDataNodes.equals(this.removedDataNodes)
          && thatProc.regionMigrationPlans.equals(this.regionMigrationPlans);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.removedDataNodes, this.regionMigrationPlans);
  }
}
