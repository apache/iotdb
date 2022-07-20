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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.RemoveDataNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** remove config node procedure */
public class RemoveDataNodeProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RemoveDataNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveDataNodeProcedure.class);
  private static final int retryThreshold = 5;

  private TDataNodeLocation tDataNodeLocation;

  private List<TConsensusGroupId> execDataNodeRegionIds = new ArrayList<>();

  public RemoveDataNodeProcedure() {
    super();
  }

  public RemoveDataNodeProcedure(TDataNodeLocation tDataNodeLocation) {
    super();
    this.tDataNodeLocation = tDataNodeLocation;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveDataNodeState state) {
    if (tDataNodeLocation == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case REMOVE_DATA_NODE_PREPARE:
          execDataNodeRegionIds =
              env.getDataNodeRemoveManager().getDataNodeRegionIds(tDataNodeLocation);
          LOG.info("DataNode region id is {}", execDataNodeRegionIds);
          setNextState(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE);
          break;
        case BROADCAST_DISABLE_DATA_NODE:
          env.getDataNodeRemoveManager().broadcastDisableDataNode(tDataNodeLocation);
          setNextState(RemoveDataNodeState.SUBMIT_REGION_MIGRATE);
          break;
        case SUBMIT_REGION_MIGRATE:
          submitChildRegionMigrate(env);
          setNextState(RemoveDataNodeState.STOP_DATA_NODE);
          break;
        case STOP_DATA_NODE:
          env.getDataNodeRemoveManager().stopDataNode(tDataNodeLocation);
          env.getDataNodeRemoveManager().removeDataNodePersistence(tDataNodeLocation);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Remove Config Node failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to remove config node {}, state {}",
            tDataNodeLocation,
            state,
            e);
        if (getCycles() > retryThreshold) {
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

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      if (configNodeProcedureEnv.getNodeLock().tryLock(this)) {
        LOG.info("{} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      configNodeProcedureEnv.getNodeLock().waitProcedure(this);
      LOG.info("{} wait for lock.", getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOG.info("{} release lock.", getProcId());
      if (configNodeProcedureEnv.getNodeLock().releaseLock(this)) {
        configNodeProcedureEnv
            .getNodeLock()
            .wakeWaitingProcedures(configNodeProcedureEnv.getScheduler());
      }
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
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
  protected int getStateId(RemoveDataNodeState deleteStorageGroupState) {
    return deleteStorageGroupState.ordinal();
  }

  @Override
  protected RemoveDataNodeState getInitialState() {
    return RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.REMOVE_DATA_NODE_PROCEDURE.ordinal());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(tDataNodeLocation, stream);
    stream.writeInt(execDataNodeRegionIds.size());
    execDataNodeRegionIds.forEach(
        tid -> ThriftCommonsSerDeUtils.serializeTConsensusGroupId(tid, stream));
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      tDataNodeLocation = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
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
          && thatProc.tDataNodeLocation.equals(this.tDataNodeLocation);
    }
    return false;
  }

  private void submitChildRegionMigrate(ConfigNodeProcedureEnv env) {
    execDataNodeRegionIds.forEach(
        regionId -> {
          TDataNodeLocation destDataNode =
              env.getDataNodeRemoveManager().findDestDataNode(tDataNodeLocation, regionId);
          if (destDataNode != null) {
            RegionMigrateProcedure regionMigrateProcedure =
                new RegionMigrateProcedure(regionId, tDataNodeLocation, destDataNode);
            addChildProcedure(regionMigrateProcedure);
            LOG.info("Submit child procedure, {}", regionMigrateProcedure);
          }
        });
  }
}
