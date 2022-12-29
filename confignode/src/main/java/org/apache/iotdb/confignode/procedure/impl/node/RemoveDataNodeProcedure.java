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
import org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.state.RemoveDataNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;

/** remove data node procedure */
public class RemoveDataNodeProcedure extends AbstractNodeProcedure<RemoveDataNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveDataNodeProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TDataNodeLocation removedDataNode;

  private List<TConsensusGroupId> migratedDataNodeRegions = new ArrayList<>();

  public RemoveDataNodeProcedure() {
    super();
  }

  public RemoveDataNodeProcedure(TDataNodeLocation removedDataNode) {
    super();
    this.removedDataNode = removedDataNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveDataNodeState state) {
    if (removedDataNode == null) {
      return Flow.NO_MORE_STATE;
    }

    DataNodeRemoveHandler handler = env.getDataNodeRemoveHandler();
    try {
      switch (state) {
        case REGION_REPLICA_CHECK:
          if (env.doubleCheckReplica(removedDataNode)) {
            setNextState(RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
          } else {
            LOG.error(
                "{}, Can not remove DataNode {} "
                    + "because the number of DataNodes is less or equal than region replica number",
                REMOVE_DATANODE_PROCESS,
                removedDataNode);
            return Flow.NO_MORE_STATE;
          }
        case REMOVE_DATA_NODE_PREPARE:
          // mark the datanode as removing status and broadcast region route map
          env.markDataNodeAsRemovingAndBroadcast(removedDataNode);
          migratedDataNodeRegions = handler.getMigratedDataNodeRegions(removedDataNode);
          LOG.info(
              "{}, DataNode regions to be removed is {}",
              REMOVE_DATANODE_PROCESS,
              migratedDataNodeRegions);
          setNextState(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE);
          break;
        case BROADCAST_DISABLE_DATA_NODE:
          handler.broadcastDisableDataNode(removedDataNode);
          setNextState(RemoveDataNodeState.SUBMIT_REGION_MIGRATE);
          break;
        case SUBMIT_REGION_MIGRATE:
          submitChildRegionMigrate(env);
          setNextState(RemoveDataNodeState.STOP_DATA_NODE);
          break;
        case STOP_DATA_NODE:
          if (isAllRegionMigratedSuccessfully(env)) {
            LOG.info("{}, Begin to stop DataNode: {}", REMOVE_DATANODE_PROCESS, removedDataNode);
            handler.removeDataNodePersistence(removedDataNode);
            handler.stopDataNode(removedDataNode);
          }
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Remove Data Node failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to remove data node {}, state {}", removedDataNode, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void submitChildRegionMigrate(ConfigNodeProcedureEnv env) {
    migratedDataNodeRegions.forEach(
        regionId -> {
          TDataNodeLocation destDataNode =
              env.getDataNodeRemoveHandler().findDestDataNode(regionId);
          if (destDataNode != null) {
            RegionMigrateProcedure regionMigrateProcedure =
                new RegionMigrateProcedure(regionId, removedDataNode, destDataNode);
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
            .filter(replica -> replica.getDataNodeLocations().contains(removedDataNode))
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
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(removedDataNode, stream);
    stream.writeInt(migratedDataNodeRegions.size());
    migratedDataNodeRegions.forEach(
        tid -> ThriftCommonsSerDeUtils.serializeTConsensusGroupId(tid, stream));
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      removedDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      int regionSize = byteBuffer.getInt();
      migratedDataNodeRegions = new ArrayList<>(regionSize);
      for (int i = 0; i < regionSize; i++) {
        migratedDataNodeRegions.add(
            ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer));
      }
    } catch (ThriftSerDeException e) {
      LOG.error("Error in deserialize RemoveConfigNodeProcedure", e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RemoveDataNodeProcedure) {
      RemoveDataNodeProcedure thatProc = (RemoveDataNodeProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.removedDataNode.equals(this.removedDataNode)
          && thatProc.migratedDataNodeRegions.equals(this.migratedDataNodeRegions);
    }
    return false;
  }
}
