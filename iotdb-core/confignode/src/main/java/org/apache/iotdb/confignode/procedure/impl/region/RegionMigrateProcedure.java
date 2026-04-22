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
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Region migrate procedure */
public class RegionMigrateProcedure extends RegionOperationProcedure<RegionTransitionState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionMigrateProcedure.class);

  /** Wait region migrate finished */
  private TDataNodeLocation originalDataNode;

  private TDataNodeLocation destDataNode;
  private TDataNodeLocation coordinatorForAddPeer;
  private TDataNodeLocation coordinatorForRemovePeer;

  /**
   * Cancel flag for graceful cancellation. When set to true, the procedure will stop at the
   * earliest safe point. This flag is NOT persisted — after ConfigNode restart, recovered
   * procedures resume normally and the user can re-issue CANCEL ALL MIGRATIONS if needed.
   */
  private volatile boolean cancelled = false;

  public RegionMigrateProcedure() {
    super();
  }

  public RegionMigrateProcedure(
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TDataNodeLocation coordinatorForAddPeer,
      TDataNodeLocation coordinatorForRemovePeer) {
    super(consensusGroupId);
    this.originalDataNode = originalDataNode;
    this.destDataNode = destDataNode;
    this.coordinatorForAddPeer = coordinatorForAddPeer;
    this.coordinatorForRemovePeer = coordinatorForRemovePeer;
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv env) {
    if (env.getRegionMigrateSemaphore().tryLock(this)) {
      LOGGER.info("[pid{}][MigrateRegion] acquired migration lock.", getProcId());
      return ProcedureLockState.LOCK_ACQUIRED;
    }
    LOGGER.info(
        "[pid{}][MigrateRegion] migration concurrency limit reached, will wait.", getProcId());
    return ProcedureLockState.LOCK_EVENT_WAIT;
  }

  @Override
  protected boolean holdLock(ConfigNodeProcedureEnv env) {
    // Hold the lock during the critical phases (PREPARE → ADD_PEER → CHECK_ADD_PEER → REMOVE_PEER)
    // to enforce the concurrency limit. After REMOVE_PEER completes and we enter
    // CHECK_REMOVE_REGION_PEER, the migration is essentially done (new replica is up,
    // old replica removal is submitted), so we release the lock early to allow other
    // migration procedures to start.
    RegionTransitionState state = getCurrentState();
    return state != null && state != RegionTransitionState.CHECK_REMOVE_REGION_PEER;
  }

  @Override
  protected void releaseLock(ConfigNodeProcedureEnv env) {
    env.getSchedulerLock().lock();
    try {
      LOGGER.info("[pid{}][MigrateRegion] released migration lock.", getProcId());
      if (env.getRegionMigrateSemaphore().releaseLock(this)) {
        env.getRegionMigrateSemaphore().wakeWaitingProcedures(env.getScheduler());
      }
    } finally {
      env.getSchedulerLock().unlock();
    }
  }

  @Override
  protected void onLockEventWait(ConfigNodeProcedureEnv env) {
    env.getRegionMigrateSemaphore().waitProcedure(this);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RegionTransitionState state) {
    if (regionId == null) {
      return Flow.NO_MORE_STATE;
    }

    // Check cancel flag at the entry of each state transition.
    //
    // Cancellation semantics by state:
    //   REGION_MIGRATE_PREPARE — nothing has happened, clean abort.
    //   ADD_REGION_PEER — addChildProcedure() has NOT been called yet, clean abort.
    //   CHECK_ADD_REGION_PEER — no special cancel handling needed here. The cancel
    //       signal has already been propagated into AddRegionPeerProcedure's
    //       waitTaskFinish() polling loop, so either:
    //       (a) AddPeer was cancelled mid-transfer and rolled back by itself
    //           → isDataNodeContainsRegion returns false → normal "AddPeer failed" path.
    //       (b) AddPeer completed before cancel took effect → normal success path
    //           continues to REMOVE_REGION_PEER (migration nearly done, let it finish).
    //   REMOVE_REGION_PEER / CHECK_REMOVE_REGION_PEER — migration is nearly done,
    //       ignore the cancel and let it finish.
    if (cancelled) {
      switch (state) {
        case REGION_MIGRATE_PREPARE:
        case ADD_REGION_PEER:
          if (state == RegionTransitionState.ADD_REGION_PEER) {
            addChildProcedure(new NotifyRegionMigrationProcedure(regionId, false));
          }
          LOGGER.info(
              "[pid{}][MigrateRegion] cancelled at state {} before any irreversible change. "
                  + "{} migration from DataNode {} to {} is aborted.",
              getProcId(),
              state,
              regionId,
              RegionMaintainHandler.simplifiedLocation(originalDataNode),
              RegionMaintainHandler.simplifiedLocation(destDataNode));
          return Flow.NO_MORE_STATE;
        case CHECK_ADD_REGION_PEER:
          // No special cancel handling needed. The cancel signal has already been
          // propagated into AddRegionPeerProcedure's waitTaskFinish() polling loop.
          // Just fall through to the normal logic which checks whether AddPeer
          // succeeded or failed.
          break;
        case REMOVE_REGION_PEER:
        case CHECK_REMOVE_REGION_PEER:
          // Migration is nearly or fully complete — no point in cancelling.
          // Ignore the cancel flag and let it finish.
          LOGGER.info(
              "[pid{}][MigrateRegion] cancel request ignored at state {} — "
                  + "migration is nearly complete and will continue to finish.",
              getProcId(),
              state);
          break;
        default:
          break;
      }
    }

    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      switch (state) {
        case REGION_MIGRATE_PREPARE:
          LOGGER.info(
              "[pid{}][MigrateRegion] started, {} will be migrated from DataNode {} to {}.",
              getProcId(),
              regionId,
              RegionMaintainHandler.simplifiedLocation(originalDataNode),
              RegionMaintainHandler.simplifiedLocation(destDataNode));
          addChildProcedure(new NotifyRegionMigrationProcedure(regionId, true));
          setNextState(RegionTransitionState.ADD_REGION_PEER);
          break;
        case ADD_REGION_PEER:
          addChildProcedure(
              new AddRegionPeerProcedure(regionId, coordinatorForAddPeer, destDataNode));
          setNextState(RegionTransitionState.CHECK_ADD_REGION_PEER);
          break;
        case CHECK_ADD_REGION_PEER:
          if (!env.getConfigManager()
              .getPartitionManager()
              .isDataNodeContainsRegion(destDataNode.getDataNodeId(), regionId)) {
            LOGGER.warn(
                "[pid{}][MigrateRegion] sub-procedure AddRegionPeerProcedure failed, RegionMigrateProcedure will not continue",
                getProcId());
            return Flow.NO_MORE_STATE;
          }
          setNextState(RegionTransitionState.REMOVE_REGION_PEER);
          break;
        case REMOVE_REGION_PEER:
          addChildProcedure(
              new RemoveRegionPeerProcedure(regionId, coordinatorForRemovePeer, originalDataNode));
          setNextState(RegionTransitionState.CHECK_REMOVE_REGION_PEER);
          break;
        case CHECK_REMOVE_REGION_PEER:
          String cleanHint = "";
          if (env.getConfigManager()
              .getPartitionManager()
              .isDataNodeContainsRegion(originalDataNode.getDataNodeId(), regionId)) {
            cleanHint =
                "but you may need to restart the related DataNode to make sure everything is cleaned up. ";
          }
          LOGGER.info(
              "[pid{}][MigrateRegion] success,{} {} has been migrated from DataNode {} to {}. Procedure took {} (started at {}).",
              getProcId(),
              cleanHint,
              regionId,
              RegionMaintainHandler.simplifiedLocation(originalDataNode),
              RegionMaintainHandler.simplifiedLocation(destDataNode),
              CommonDateTimeUtils.convertMillisecondToDurationStr(
                  System.currentTimeMillis() - getSubmittedTime()),
              DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
          addChildProcedure(new NotifyRegionMigrationProcedure(regionId, false));
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      LOGGER.error("[pid{}][MigrateRegion] state {} fail", getProcId(), state, e);
      // meets exception in region migrate process terminate the process
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info("[pid{}][MigrateRegion] state {} complete", getProcId(), state);
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RegionTransitionState state)
      throws IOException, InterruptedException, ProcedureException {}

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
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinatorForAddPeer, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinatorForRemovePeer, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      originalDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      destDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      coordinatorForAddPeer = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      coordinatorForRemovePeer = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOGGER.warn(
          "Error in deserialize {} (procID {}). This procedure will be ignored. It may belong to old version and cannot be used now.",
          this.getClass(),
          this.getProcId(),
          e);
      throw e;
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
          && thatProc.regionId.equals(this.regionId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.originalDataNode, this.destDataNode, this.regionId);
  }

  /**
   * Request cancellation of this migration procedure. The cancellation is cooperative — the
   * procedure will stop at the next safe point in its state machine. If the procedure has already
   * passed the REMOVE_REGION_PEER state, the cancel request is ignored and the migration completes.
   */
  public void cancel() {
    this.cancelled = true;
  }

  /** Returns true if cancellation has been requested for this procedure. */
  public boolean isCancelled() {
    return cancelled;
  }

  public TDataNodeLocation getDestDataNode() {
    return destDataNode;
  }

  public TDataNodeLocation getOriginalDataNode() {
    return originalDataNode;
  }

  public TDataNodeLocation getCoordinatorForAddPeer() {
    return coordinatorForAddPeer;
  }

  /**
   * Get the current RegionTransitionState of this migration procedure
   *
   * @return the current RegionTransitionState, or null if not available
   */
  public RegionTransitionState getCurrentRegionTransitionState() {
    return getCurrentState();
  }
}
