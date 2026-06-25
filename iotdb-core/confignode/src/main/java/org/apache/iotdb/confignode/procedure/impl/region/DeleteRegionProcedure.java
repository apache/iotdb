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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.DeleteRegionState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler.simplifiedLocation;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

/**
 * Delete a single region replica (the consensus peer and all of its data) from one DataNode.
 *
 * <p>This replaces the old queue-based DELETE path in {@code
 * PartitionManager.maintainRegionReplicas}. The DataNode runs the deletion asynchronously (it can
 * remove a large amount of TsFile data and outlive a single RPC timeout) and this procedure polls
 * for the result, so a slow deletion can never be wrongly reported as finished — fixing the "ghost
 * task" problem where the coordinator forgot a task that was still running.
 */
public class DeleteRegionProcedure extends RegionOperationProcedure<DeleteRegionState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRegionProcedure.class);

  private TDataNodeLocation targetDataNode;

  public DeleteRegionProcedure() {
    super();
  }

  public DeleteRegionProcedure(TConsensusGroupId regionId, TDataNodeLocation targetDataNode) {
    super(regionId);
    this.targetDataNode = targetDataNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteRegionState state)
      throws InterruptedException {
    if (regionId == null || targetDataNode == null) {
      return Flow.NO_MORE_STATE;
    }
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      switch (state) {
        case DELETE_REGION:
          LOGGER.info(
              ProcedureMessages.PID_DELETEREGION_STARTED_WILL_BE_DELETED_FROM_DATANODE,
              getProcId(),
              regionId,
              simplifiedLocation(targetDataNode));
          // Only submit the delete task on the very first entry of this state. We must NOT
          // re-submit
          // when:
          //   - the state was restored from disk after a leader change / ConfigNode reboot
          //     (isStateDeserialized()), or
          //   - this state is being re-entered in place because a previous attempt parked here on
          //     PROCESSING (getCycles() > 0).
          // The DataNode also dedups by taskId (= procId), so a duplicate submit would be a no-op,
          // but skipping it here avoids the useless RPC and keeps the re-poll cheap.
          if (!this.isStateDeserialized() && getCycles() == 0) {
            TSStatus submitStatus =
                handler.submitDeleteRegionTask(this.getProcId(), targetDataNode, regionId);
            setKillPoint(state);
            if (submitStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
              // Submit failed (e.g. the DataNode is unreachable). End the procedure; the parent
              // will
              // detect via the partition table that the region is still present and may retry.
              LOGGER.warn(
                  ProcedureMessages.PID_DELETEREGION_SUBMIT_TASK_FAILED,
                  getProcId(),
                  regionId,
                  simplifiedLocation(targetDataNode),
                  submitStatus);
              return Flow.NO_MORE_STATE;
            }
          }
          TRegionMigrateResult result = handler.waitTaskFinish(this.getProcId(), targetDataNode);
          switch (result.getTaskStatus()) {
            case SUCCESS:
              // Requirement: every successfully completed maintain task must be logged.
              LOGGER.info(
                  ProcedureMessages
                      .PID_DELETEREGION_SUCCESS_REGION_HAS_BEEN_DELETED_FROM_DATANODE_PROCEDURE,
                  getProcId(),
                  regionId,
                  simplifiedLocation(targetDataNode),
                  CommonDateTimeUtils.convertMillisecondToDurationStr(
                      System.currentTimeMillis() - getSubmittedTime()),
                  DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
              return Flow.NO_MORE_STATE;
            case PROCESSING:
              // waitTaskFinish() only returns PROCESSING when its polling loop was interrupted,
              // i.e.
              // this ConfigNode is shutting down / losing leadership. The delete task is still
              // running on the DataNode, so we must persist and re-poll after recovery rather than
              // declare the region deleted. Stay in DELETE_REGION; the isStateDeserialized() guard
              // above prevents re-submitting.
              setNextState(DeleteRegionState.DELETE_REGION);
              return Flow.HAS_MORE_STATE;
            case TASK_NOT_EXIST:
            // The DataNode has no record of this task (it never received the submit, or it
            // restarted
            // and lost its in-memory task table). Treat it as a terminal failure: the parent's
            // CHECK
            // state inspects the partition table and will spawn a fresh DeleteRegionProcedure (with
            // a
            // new procId) if the region is still present, which retries cleanly.
            case FAIL:
            default:
              LOGGER.warn(
                  ProcedureMessages.PID_DELETEREGION_EXECUTED_FAILED,
                  getProcId(),
                  regionId,
                  simplifiedLocation(targetDataNode),
                  result.getTaskStatus());
              return Flow.NO_MORE_STATE;
          }
        default:
          throw new ProcedureException(ProcedureMessages.UNSUPPORTED_STATE + state.name());
      }
    } catch (Exception e) {
      LOGGER.error(ProcedureMessages.PID_DELETEREGION_STATE_FAILED, getProcId(), state, e);
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DeleteRegionState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected DeleteRegionState getState(int stateId) {
    return DeleteRegionState.values()[stateId];
  }

  @Override
  protected int getStateId(DeleteRegionState deleteRegionState) {
    return deleteRegionState.ordinal();
  }

  @Override
  protected DeleteRegionState getInitialState() {
    return DeleteRegionState.DELETE_REGION;
  }

  public TDataNodeLocation getTargetDataNode() {
    return targetDataNode;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DELETE_REGION_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(targetDataNode, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      targetDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOGGER.error(ProcedureMessages.ERROR_IN_DESERIALIZE, this.getClass(), e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DeleteRegionProcedure)) {
      return false;
    }
    DeleteRegionProcedure procedure = (DeleteRegionProcedure) obj;
    return this.regionId.equals(procedure.regionId)
        && this.targetDataNode.equals(procedure.targetDataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionId, targetDataNode);
  }

  @Override
  public String toString() {
    return "DeleteRegionProcedure{"
        + "regionId="
        + regionId
        + ", targetDataNode="
        + simplifiedLocation(targetDataNode)
        + '}';
  }
}
