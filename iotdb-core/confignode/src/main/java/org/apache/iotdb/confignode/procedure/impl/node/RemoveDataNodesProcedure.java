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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.RemoveDataNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;

/** remove data node procedure */
public class RemoveDataNodesProcedure extends AbstractNodeProcedure<RemoveDataNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveDataNodesProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private List<TDataNodeLocation> removedDataNodes;

  private List<RegionMigrationPlan> regionMigrationPlans = new ArrayList<>();

  private Map<Integer, NodeStatus> nodeStatusMap;

  public RemoveDataNodesProcedure() {
    super();
  }

  public RemoveDataNodesProcedure(
      List<TDataNodeLocation> removedDataNodes, Map<Integer, NodeStatus> nodeStatusMap) {
    super();
    this.removedDataNodes = removedDataNodes;
    this.nodeStatusMap = nodeStatusMap;
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOG.info(
          ProcedureMessages
              .LOG_PROCEDUREID_ARG_REMOVEDATANODES_SKIPS_ACQUIRING_LOCK_SINCE_UPPER_LAYER_ENSURES_C7546FF8,
          getProcId());
      return ProcedureLockState.LOCK_ACQUIRED;
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOG.info(
          ProcedureMessages
              .LOG_PROCEDUREID_ARG_REMOVEDATANODES_SKIPS_RELEASING_LOCK_SINCE_IT_HASN_T_AED8A3DA,
          getProcId());
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveDataNodeState state) {
    if (removedDataNodes.isEmpty()) {
      return Flow.NO_MORE_STATE;
    }

    RemoveDataNodeHandler removeDataNodeHandler = env.getRemoveDataNodeHandler();
    try {
      switch (state) {
        case REGION_REPLICA_CHECK:
          if (removeDataNodeHandler.checkEnoughDataNodeAfterRemoving(removedDataNodes)) {
            setNextState(RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
          } else {
            LOG.error(
                ProcedureMessages.LOG_ARG_CAN_NOT_REMOVE_DATANODE_ARG_495F9F85
                    + ProcedureMessages
                        .LOG_BECAUSE_NUMBER_DATANODES_LESS_EQUAL_THAN_REGION_REPLICA_NUMBER_DEC0CB38,
                REMOVE_DATANODE_PROCESS,
                removedDataNodes);
            return Flow.NO_MORE_STATE;
          }
        case REMOVE_DATA_NODE_PREPARE:
          Map<Integer, NodeStatus> removedNodeStatusMap = new HashMap<>();
          removedDataNodes.forEach(
              dataNode -> removedNodeStatusMap.put(dataNode.getDataNodeId(), NodeStatus.Removing));
          removeDataNodeHandler.changeDataNodeStatus(removedDataNodes, removedNodeStatusMap);
          regionMigrationPlans =
              removeDataNodeHandler.selectedRegionMigrationPlans(removedDataNodes);
          LOG.info(
              ProcedureMessages.LOG_ARG_DATANODE_REGIONS_REMOVED_ARG_216A7DC7,
              REMOVE_DATANODE_PROCESS,
              regionMigrationPlans);
          setNextState(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE);
          break;
        case BROADCAST_DISABLE_DATA_NODE:
          removeDataNodeHandler.broadcastDataNodeStatusChange(removedDataNodes);
          setNextState(RemoveDataNodeState.SUBMIT_REGION_MIGRATE);
          break;
        case SUBMIT_REGION_MIGRATE:
          // Avoid re-submit region-migration when leader change or ConfigNode reboot
          if (!isStateDeserialized()) {
            submitChildRegionMigrate(env);
          }
          setNextState(RemoveDataNodeState.STOP_DATA_NODE);
          break;
        case STOP_DATA_NODE:
          checkRegionStatusAndStopDataNode(env);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException(ProcedureMessages.REMOVE_DATA_NODE_FAILED + state));
      } else {
        LOG.error(
            ProcedureMessages.LOG_RETRIEVABLE_ERROR_TRYING_REMOVE_DATA_NODE_ARG_STATE_ARG_4EFEB850,
            removedDataNodes,
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException(ProcedureMessages.STATE_STUCK_AT + state));
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
          TDataNodeLocation destDataNode = regionMigrationPlan.getToDataNode();
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
            LOG.info(
                ProcedureMessages
                    .LOG_SUBMIT_REGIONMIGRATEPROCEDURE_REGIONID_ARG_REMOVEDDATANODE_ARG_DESTDATANODE_ARG_COORDINATORFORADDPEER_ARG_,
                regionId,
                simplifyTDataNodeLocation(removedDataNode),
                simplifyTDataNodeLocation(destDataNode),
                simplifyTDataNodeLocation(coordinatorForAddPeer),
                simplifyTDataNodeLocation(coordinatorForRemovePeer));
          } else {
            LOG.error(
                ProcedureMessages.LOG_ARG_CANNOT_FIND_TARGET_DATANODE_MIGRATE_REGION_ARG_81A78E06,
                REMOVE_DATANODE_PROCESS,
                regionId);
            // TODO terminate all the uncompleted remove datanode process
          }
        });
  }

  private String simplifyTDataNodeLocation(TDataNodeLocation dataNodeLocation) {
    return String.format(
        "DataNode(id:%d, address:%s)",
        dataNodeLocation.getDataNodeId(), dataNodeLocation.getInternalEndPoint().getIp());
  }

  private void checkRegionStatusAndStopDataNode(ConfigNodeProcedureEnv env) {
    List<TRegionReplicaSet> replicaSets =
        env.getConfigManager().getPartitionManager().getAllReplicaSets();
    List<TDataNodeLocation> rollBackDataNodes = new ArrayList<>();
    List<TDataNodeLocation> successDataNodes = new ArrayList<>();
    for (TDataNodeLocation dataNode : removedDataNodes) {
      List<TConsensusGroupId> migratedFailedRegions =
          replicaSets.stream()
              .filter(
                  replica ->
                      replica.getDataNodeLocations().stream()
                          .anyMatch(loc -> loc.getDataNodeId() == dataNode.dataNodeId))
              .map(TRegionReplicaSet::getRegionId)
              .collect(Collectors.toList());
      if (!migratedFailedRegions.isEmpty()) {
        LOG.warn(
            ProcedureMessages
                    .LOG_ARG_SOME_REGIONS_MIGRATED_FAILED_DATANODE_ARG_MIGRATEDFAILEDREGIONS_ARG_11644841
                + ProcedureMessages
                    .LOG_REGIONS_HAVE_BEEN_SUCCESSFULLY_MIGRATED_WILL_NOT_ROLL_BACK_YOU_AE904563,
            REMOVE_DATANODE_PROCESS,
            dataNode,
            migratedFailedRegions);
        rollBackDataNodes.add(dataNode);
      } else {
        successDataNodes.add(dataNode);
      }
    }
    if (!successDataNodes.isEmpty()) {
      LOG.info(
          ProcedureMessages
              .LOG_ARG_DATANODES_ARG_ALL_REGIONS_MIGRATED_SUCCESSFULLY_START_STOP_THEM_32D56F28,
          REMOVE_DATANODE_PROCESS,
          successDataNodes);
      env.getRemoveDataNodeHandler().removeDataNodePersistence(successDataNodes);
      env.getRemoveDataNodeHandler().stopDataNodes(successDataNodes);
    }
    if (!rollBackDataNodes.isEmpty()) {
      LOG.info(
          ProcedureMessages.LOG_ARG_START_ROLL_BACK_DATANODES_STATUS_ARG_05C67270,
          REMOVE_DATANODE_PROCESS,
          rollBackDataNodes);
      env.getRemoveDataNodeHandler().changeDataNodeStatus(rollBackDataNodes, nodeStatusMap);
      env.getRemoveDataNodeHandler().broadcastDataNodeStatusChange(rollBackDataNodes);
      LOG.info(
          ProcedureMessages.LOG_ARG_ROLL_BACK_DATANODES_STATUS_SUCCESSFULLY_ARG_6773A2DF,
          REMOVE_DATANODE_PROCESS,
          rollBackDataNodes);
    }
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
    for (RegionMigrationPlan regionMigrationPlan : regionMigrationPlans) {
      regionMigrationPlan.serialize(stream);
    }
    stream.writeInt(nodeStatusMap.size());
    for (Map.Entry<Integer, NodeStatus> entry : nodeStatusMap.entrySet()) {
      stream.writeInt(entry.getKey());
      stream.writeByte(entry.getValue().ordinal());
    }
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
        regionMigrationPlans.add(RegionMigrationPlan.deserialize(byteBuffer));
      }
      int nodeStatusMapSize = byteBuffer.getInt();
      nodeStatusMap = new HashMap<>(nodeStatusMapSize);
      for (int i = 0; i < nodeStatusMapSize; i++) {
        int dataNodeId = byteBuffer.getInt();
        NodeStatus nodeStatus = NodeStatus.values()[byteBuffer.get()];
        nodeStatusMap.put(dataNodeId, nodeStatus);
      }
    } catch (ThriftSerDeException e) {
      LOG.error(ProcedureMessages.ERROR_IN_DESERIALIZE_REMOVECONFIGNODEPROCEDURE, e);
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

  public List<TDataNodeLocation> getRemovedDataNodes() {
    return removedDataNodes;
  }
}
