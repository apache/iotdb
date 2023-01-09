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

package org.apache.iotdb.confignode.procedure.impl.statemachine;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REGION_MIGRATE_PROCESS;
import static org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler.getIdWithRpcEndpoint;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

/** Region migrate procedure */
public class RegionMigrateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RegionTransitionState> {
  private static final Logger LOG = LoggerFactory.getLogger(RegionMigrateProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  /** Wait region migrate finished */
  private final Object regionMigrateLock = new Object();

  private TConsensusGroupId consensusGroupId;

  private TDataNodeLocation originalDataNode;

  private TDataNodeLocation destDataNode;

  private boolean migrateSuccess = true;

  private String migrateResult = "";

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
    DataNodeRemoveHandler handler = env.getDataNodeRemoveHandler();
    try {
      switch (state) {
        case REGION_MIGRATE_PREPARE:
          setNextState(RegionTransitionState.CREATE_NEW_REGION_PEER);
          break;
        case CREATE_NEW_REGION_PEER:
          handler.createNewRegionPeer(consensusGroupId, destDataNode);
          setNextState(RegionTransitionState.ADD_REGION_PEER);
          break;
        case ADD_REGION_PEER:
          tsStatus = handler.addRegionPeer(destDataNode, consensusGroupId);
          if (tsStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
            waitForOneMigrationStepFinished(consensusGroupId, state);
          } else {
            throw new ProcedureException("ADD_REGION_PEER executed failed in DataNode");
          }
          setNextState(RegionTransitionState.CHANGE_REGION_LEADER);
          break;
        case CHANGE_REGION_LEADER:
          handler.changeRegionLeader(consensusGroupId, originalDataNode, destDataNode);
          setNextState(RegionTransitionState.REMOVE_REGION_PEER);
          break;
        case REMOVE_REGION_PEER:
          tsStatus = handler.removeRegionPeer(originalDataNode, destDataNode, consensusGroupId);
          if (tsStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
            waitForOneMigrationStepFinished(consensusGroupId, state);
          } else {
            throw new ProcedureException("REMOVE_REGION_PEER executed failed in DataNode");
          }
          setNextState(RegionTransitionState.DELETE_OLD_REGION_PEER);
          break;
        case DELETE_OLD_REGION_PEER:
          tsStatus = handler.deleteOldRegionPeer(originalDataNode, consensusGroupId);
          if (tsStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
            waitForOneMigrationStepFinished(consensusGroupId, state);
          }
          // Remove consensus group after a node stop, which will be failed, but we will
          // continuously execute.
          setNextState(RegionTransitionState.UPDATE_REGION_LOCATION_CACHE);
          break;
        case UPDATE_REGION_LOCATION_CACHE:
          handler.updateRegionLocationCache(consensusGroupId, originalDataNode, destDataNode);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      LOG.error(
          "{}, Meets error in region migrate state, "
              + "please do the rollback operation yourself manually according to the error message!!! "
              + "error state: {}, migrateResult: {}",
          REGION_MIGRATE_PROCESS,
          state,
          migrateResult);
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Region migrate failed at state: " + state));
      } else {
        LOG.error(
            "{}, Failed state [{}] is not support rollback, originalDataNode: {}",
            REGION_MIGRATE_PROCESS,
            state,
            getIdWithRpcEndpoint(originalDataNode));
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  "Procedure retried failed exceed 5 times, state stuck at " + state));
        }

        // meets exception in region migrate process terminate the process
        return Flow.NO_MORE_STATE;
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
        LOG.info("procedureId {} acquire lock.", getProcId());
        return ProcedureLockState.LOCK_ACQUIRED;
      }
      configNodeProcedureEnv.getRegionMigrateLock().waitProcedure(this);

      LOG.info("procedureId {} wait for lock.", getProcId());
      return ProcedureLockState.LOCK_EVENT_WAIT;
    } finally {
      configNodeProcedureEnv.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    configNodeProcedureEnv.getSchedulerLock().lock();
    try {
      LOG.info("procedureId {} release lock.", getProcId());
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

  public TSStatus waitForOneMigrationStepFinished(
      TConsensusGroupId consensusGroupId, RegionTransitionState state) throws Exception {

    LOG.info(
        "{}, Wait for state {} finished, regionId: {}",
        REGION_MIGRATE_PROCESS,
        state,
        consensusGroupId);

    TSStatus status = new TSStatus(SUCCESS_STATUS.getStatusCode());
    synchronized (regionMigrateLock) {
      try {
        // TODO set timeOut?
        regionMigrateLock.wait();

        if (!migrateSuccess) {
          throw new ProcedureException(
              String.format("Region migration failed, regionId: %s", consensusGroupId));
        }
      } catch (InterruptedException e) {
        LOG.error("{}, region migration {} interrupt", REGION_MIGRATE_PROCESS, consensusGroupId, e);
        Thread.currentThread().interrupt();
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage("Waiting for region migration interruption," + e.getMessage());
      }
    }
    return status;
  }

  /** DataNode report region migrate result to ConfigNode, and continue */
  public void notifyTheRegionMigrateFinished(TRegionMigrateResultReportReq req) {

    LOG.info(
        "{}, ConfigNode received region migration result reported by DataNode: {}",
        REGION_MIGRATE_PROCESS,
        req);

    // TODO the req is used in roll back
    synchronized (regionMigrateLock) {
      TSStatus migrateStatus = req.getMigrateResult();
      // Migration failed
      if (migrateStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
        LOG.info(
            "{}, Region migration failed in DataNode, migrateStatus: {}",
            REGION_MIGRATE_PROCESS,
            migrateStatus);
        migrateSuccess = false;
        migrateResult = migrateStatus.toString();
      }
      regionMigrateLock.notify();
    }
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }
}
