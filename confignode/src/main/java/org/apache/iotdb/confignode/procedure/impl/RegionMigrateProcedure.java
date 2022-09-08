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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** region migrate procedure */
public class RegionMigrateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RegionTransitionState> {
  private static final Logger LOG = LoggerFactory.getLogger(RegionMigrateProcedure.class);
  private static final int retryThreshold = 5;

  /** Wait region migrate finished */
  private final Object regionMigrateLock = new Object();

  private TConsensusGroupId consensusGroupId;

  private TDataNodeLocation originalDataNode;

  private TDataNodeLocation destDataNode;

  public RegionMigrateProcedure() {
    super();
  }

  public RegionMigrateProcedure(
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode) {
    super();
    this.consensusGroupId = consensusGroupId;
    this.originalDataNode = originalDataNode;
    this.destDataNode = destDataNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RegionTransitionState state) {
    if (consensusGroupId == null) {
      return Flow.NO_MORE_STATE;
    }
    TSStatus tsStatus;
    try {
      switch (state) {
        case REGION_MIGRATE_PREPARE:
          setNextState(RegionTransitionState.CREATE_NEW_REGION_PEER);
          break;
        case CREATE_NEW_REGION_PEER:
          env.getDataNodeRemoveHandler().createNewRegionPeer(consensusGroupId, destDataNode);
          setNextState(RegionTransitionState.ADD_REGION_PEER);
          break;
        case ADD_REGION_PEER:
          tsStatus = env.getDataNodeRemoveHandler().addRegionPeer(destDataNode, consensusGroupId);
          if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            waitForOneMigrationStepFinished(consensusGroupId);
            LOG.info("Wait for region {}  add peer finished", consensusGroupId);
          } else {
            throw new ProcedureException("Failed to add region peer");
          }
          setNextState(RegionTransitionState.CHANGE_REGION_LEADER);
          break;
        case CHANGE_REGION_LEADER:
          env.getDataNodeRemoveHandler().changeRegionLeader(consensusGroupId, originalDataNode);
          setNextState(RegionTransitionState.REMOVE_REGION_PEER);
          break;
        case REMOVE_REGION_PEER:
          tsStatus =
              env.getDataNodeRemoveHandler().removeRegionPeer(originalDataNode, consensusGroupId);
          if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            waitForOneMigrationStepFinished(consensusGroupId);
            LOG.info("Wait for region {} remove peer finished", consensusGroupId);
          } else {
            throw new ProcedureException("Failed to remove region peer");
          }
          setNextState(RegionTransitionState.DELETE_OLD_REGION_PEER);
          break;
        case DELETE_OLD_REGION_PEER:
          tsStatus =
              env.getDataNodeRemoveHandler()
                  .deleteOldRegionPeer(originalDataNode, consensusGroupId);
          if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            waitForOneMigrationStepFinished(consensusGroupId);
            LOG.info("Wait for region {}  remove consensus group finished", consensusGroupId);
          }
          // remove consensus group after a node stop, which will be failed, but we will
          // continuously execute.
          setNextState(RegionTransitionState.UPDATE_REGION_LOCATION_CACHE);
          break;
        case UPDATE_REGION_LOCATION_CACHE:
          env.getDataNodeRemoveHandler()
              .updateRegionLocationCache(consensusGroupId, originalDataNode, destDataNode);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Region migrate failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to region migrate {}, state {}", originalDataNode, state, e);
        if (getCycles() > retryThreshold) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
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
        LOG.info("{} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      configNodeProcedureEnv.getRegionMigrateLock().waitProcedure(this);

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
    stream.writeInt(ProcedureFactory.ProcedureType.REGION_MIGRATE_PROCEDURE.ordinal());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(originalDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(destDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      originalDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      destDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      consensusGroupId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOG.error("Error in deserialize RemoveConfigNodeProcedure", e);
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

  public TSStatus waitForOneMigrationStepFinished(TConsensusGroupId consensusGroupId) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    synchronized (regionMigrateLock) {
      try {
        // TODO set timeOut?
        regionMigrateLock.wait();
      } catch (InterruptedException e) {
        LOG.error("region migrate {} interrupt", consensusGroupId, e);
        Thread.currentThread().interrupt();
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage("wait region migrate interrupt," + e.getMessage());
      }
    }
    return status;
  }

  /** DataNode report region migrate result to ConfigNode, and continue */
  public void notifyTheRegionMigrateFinished(TRegionMigrateResultReportReq req) {
    // TODO the req is used in roll back
    synchronized (regionMigrateLock) {
      regionMigrateLock.notify();
    }
    LOG.info(
        "notified after DataNode reported region {} migrate result:{} ", req.getRegionId(), req);
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }
}
