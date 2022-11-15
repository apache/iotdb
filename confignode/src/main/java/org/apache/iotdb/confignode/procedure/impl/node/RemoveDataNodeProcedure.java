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
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
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

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;

/** remove data node procedure */
public class RemoveDataNodeProcedure extends AbstractNodeProcedure<RemoveDataNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveDataNodeProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TDataNodeLocation disableDataNodeLocation;

  private List<TConsensusGroupId> execDataNodeRegionIds = new ArrayList<>();

  public RemoveDataNodeProcedure() {
    super();
  }

  public RemoveDataNodeProcedure(TDataNodeLocation disableDataNodeLocation) {
    super();
    this.disableDataNodeLocation = disableDataNodeLocation;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveDataNodeState state) {
    if (disableDataNodeLocation == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case REGION_REPLICA_CHECK:
          if (env.doubleCheckReplica()) {
            setNextState(RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
          } else {
            LOG.error(
                "{}, Can not remove DataNode {} because the number of DataNodes is less or equal than region replica number",
                REMOVE_DATANODE_PROCESS,
                disableDataNodeLocation);
            return Flow.NO_MORE_STATE;
          }
        case REMOVE_DATA_NODE_PREPARE:
          // mark the datanode as removing status and broadcast region route map
          env.markDataNodeAsRemovingAndBroadcast(disableDataNodeLocation);
          execDataNodeRegionIds =
              env.getDataNodeRemoveHandler().getDataNodeRegionIds(disableDataNodeLocation);
          LOG.info(
              "{}, DataNode regions to be removed is {}",
              REMOVE_DATANODE_PROCESS,
              execDataNodeRegionIds);
          setNextState(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE);
          break;
        case BROADCAST_DISABLE_DATA_NODE:
          env.getDataNodeRemoveHandler().broadcastDisableDataNode(disableDataNodeLocation);
          setNextState(RemoveDataNodeState.SUBMIT_REGION_MIGRATE);
          break;
        case SUBMIT_REGION_MIGRATE:
          submitChildRegionMigrate(env);
          setNextState(RemoveDataNodeState.STOP_DATA_NODE);
          break;
        case STOP_DATA_NODE:
          // TODO if region migrate is failed, don't execute STOP_DATA_NODE
          LOG.info(
              "{}, Begin to stop DataNode: {}", REMOVE_DATANODE_PROCESS, disableDataNodeLocation);
          env.getDataNodeRemoveHandler().removeDataNodePersistence(disableDataNodeLocation);
          env.getDataNodeRemoveHandler().stopDataNode(disableDataNodeLocation);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Remove Data Node failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to remove data node {}, state {}",
            disableDataNodeLocation,
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
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
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(disableDataNodeLocation, stream);
    stream.writeInt(execDataNodeRegionIds.size());
    execDataNodeRegionIds.forEach(
        tid -> ThriftCommonsSerDeUtils.serializeTConsensusGroupId(tid, stream));
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      disableDataNodeLocation = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      int regionSize = byteBuffer.getInt();
      execDataNodeRegionIds = new ArrayList<>(regionSize);
      for (int i = 0; i < regionSize; i++) {
        execDataNodeRegionIds.add(ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer));
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
          && thatProc.disableDataNodeLocation.equals(this.disableDataNodeLocation);
    }
    return false;
  }

  private void submitChildRegionMigrate(ConfigNodeProcedureEnv env) {
    execDataNodeRegionIds.forEach(
        regionId -> {
          TDataNodeLocation destDataNode =
              env.getDataNodeRemoveHandler().findDestDataNode(regionId);
          if (destDataNode != null) {
            RegionMigrateProcedure regionMigrateProcedure =
                new RegionMigrateProcedure(regionId, disableDataNodeLocation, destDataNode);
            addChildProcedure(regionMigrateProcedure);
            LOG.info("Submit child procedure {} for regionId {}", regionMigrateProcedure, regionId);
          } else {
            LOG.error(
                "{}, Cannot find target DataNode to remove the region: {}",
                REMOVE_DATANODE_PROCESS,
                regionId);
            // TODO terminate all the uncompleted remove datanode process
          }
        });
  }
}
