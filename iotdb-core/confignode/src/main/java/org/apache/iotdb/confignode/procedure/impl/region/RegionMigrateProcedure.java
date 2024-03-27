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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Region migrate procedure */
public class RegionMigrateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RegionTransitionState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionMigrateProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  /** Wait region migrate finished */
  private TConsensusGroupId consensusGroupId;

  private TDataNodeLocation originalDataNode;
  private TDataNodeLocation destDataNode;
  private TDataNodeLocation coordinatorForAddPeer;
  private TDataNodeLocation coordinatorForRemovePeer;

  private String migrateResult = "";

  public RegionMigrateProcedure() {
    super();
  }

  public RegionMigrateProcedure(
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TDataNodeLocation coordinatorForAddPeer,
      TDataNodeLocation coordinatorForRemovePeer) {
    super();
    this.consensusGroupId = consensusGroupId;
    this.originalDataNode = originalDataNode;
    this.destDataNode = destDataNode;
    this.coordinatorForAddPeer = coordinatorForAddPeer;
    this.coordinatorForRemovePeer = coordinatorForRemovePeer;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RegionTransitionState state) {
    if (consensusGroupId == null) {
      return Flow.NO_MORE_STATE;
    }
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      switch (state) {
        case REGION_MIGRATE_PREPARE:
          setNextState(RegionTransitionState.ADD_REGION_PEER);
          break;
        case ADD_REGION_PEER:
          addChildProcedure(
              new AddRegionPeerProcedure(consensusGroupId, coordinatorForAddPeer, destDataNode));
          setNextState(RegionTransitionState.CHECK_ADD_REGION_PEER);
          break;
        case CHECK_ADD_REGION_PEER:
          if (!env.getConfigManager()
              .getPartitionManager()
              .isDataNodeContainsRegion(destDataNode.getDataNodeId(), consensusGroupId)) {
            LOGGER.warn(
                "sub-procedure AddRegionPeerProcedure fail, RegionMigrateProcedure will not continue");
            return Flow.NO_MORE_STATE;
          }
          LOGGER.info("sub-procedure AddRegionPeerProcedure success");
          setNextState(RegionTransitionState.CHANGE_REGION_LEADER);
          break;
        case CHANGE_REGION_LEADER:
          handler.changeRegionLeader(consensusGroupId, originalDataNode, destDataNode);
          setNextState(RegionTransitionState.REMOVE_REGION_PEER);
          break;
        case REMOVE_REGION_PEER:
          addChildProcedure(
              new RemoveRegionPeerProcedure(
                  consensusGroupId, coordinatorForRemovePeer, originalDataNode));
          setNextState(RegionTransitionState.CHECK_REMOVE_REGION_PEER);
          break;
        case CHECK_REMOVE_REGION_PEER:
          if (env.getConfigManager()
              .getPartitionManager()
              .isDataNodeContainsRegion(originalDataNode.getDataNodeId(), consensusGroupId)) {
            LOGGER.warn(
                "RegionMigrateProcedure success, but you may need to manually clean the old region to make everything works fine");
          } else {
            LOGGER.info(
                "RegionMigrateProcedure success, region {} has been migrated from DataNode {} to {}",
                consensusGroupId.getId(),
                originalDataNode.getDataNodeId(),
                destDataNode.getDataNodeId());
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      LOGGER.error("RegionMigrateProcedure state {} fail", state, e);
      // meets exception in region migrate process terminate the process
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info("RegionMigrateProcedure state {} complete", state);
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RegionTransitionState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean isRollbackSupported(RegionTransitionState state) {
    return false;
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      if (configNodeProcedureEnv.getRegionMigrateLock().tryLock(this)) {
        LOGGER.info("procedureId {} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      configNodeProcedureEnv.getRegionMigrateLock().waitProcedure(this);

      LOGGER.info("procedureId {} wait for lock.", getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOGGER.info("procedureId {} release lock.", getProcId());
      if (configNodeProcedureEnv.getRegionMigrateLock().releaseLock(this)) {
        configNodeProcedureEnv
            .getRegionMigrateLock()
            .wakeWaitingProcedures(configNodeProcedureEnv.getScheduler());
      }
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected RegionTransitionState getState(int stateId) {
    return RegionTransitionState.values()[stateId];
  }

  @Override
  protected int getStateId(RegionTransitionState regionTransitionState) {
    return regionTransitionState.ordinal();
  }

  @Override
  protected RegionTransitionState getInitialState() {
    return RegionTransitionState.REGION_MIGRATE_PREPARE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REGION_MIGRATE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(originalDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(destDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinatorForAddPeer, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinatorForRemovePeer, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      originalDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      destDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      consensusGroupId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      coordinatorForAddPeer = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      coordinatorForRemovePeer = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOGGER.error(
          "Error in deserialize {} (procID {}), this procedure may belong to old version and already cannot be used.",
          this.getClass(),
          this.getProcId(),
          e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RegionMigrateProcedure) {
      RegionMigrateProcedure thatProc = (RegionMigrateProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.originalDataNode.equals(this.originalDataNode)
          && thatProc.destDataNode.equals(this.destDataNode)
          && thatProc.consensusGroupId.equals(this.consensusGroupId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.originalDataNode, this.destDataNode, this.consensusGroupId);
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }
}
