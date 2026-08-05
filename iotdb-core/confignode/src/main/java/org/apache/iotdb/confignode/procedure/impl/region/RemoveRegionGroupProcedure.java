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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionGroupState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler.simplifiedLocation;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

/**
 * Delete a whole region group: every replica's consensus peer and all of its data on every DataNode
 * that hosts it.
 *
 * <p>Each replica is removed with a local {@code deleteLocalPeer} (the {@code
 * submitDeleteOldRegionPeerTask} path), which needs no consensus quorum and tolerates an
 * already-absent peer, so it works for a group of any size — including a sub-quorum group that
 * never finished forming. The DataNode runs the deletion asynchronously and this procedure polls
 * for the result, so a slow deletion is never wrongly reported as finished.
 */
public class RemoveRegionGroupProcedure extends RegionOperationProcedure<RemoveRegionGroupState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveRegionGroupProcedure.class);

  private static final int MAX_DELETE_REPLICA_RETRY = 3;
  private static final long DELETE_REPLICA_RETRY_INTERVAL_MS = 5_000;

  private TRegionReplicaSet regionReplicaSet;

  // The index of the replica currently being deleted. Persisted and advanced only after that
  // replica
  // is deleted, so after a ConfigNode leader change the procedure resumes on the first replica it
  // has
  // not finished deleting.
  private int currentReplicaIndex;

  // Number of failed attempts on the replica at currentReplicaIndex. Transient: a leader change
  // restarts the retry budget for the current replica.
  private transient int attemptedForCurrentReplica;

  public RemoveRegionGroupProcedure() {
    super();
  }

  public RemoveRegionGroupProcedure(TRegionReplicaSet regionReplicaSet) {
    super(regionReplicaSet.getRegionId());
    this.regionReplicaSet = regionReplicaSet;
  }

  @TestOnly
  void setCurrentReplicaIndex(int currentReplicaIndex) {
    this.currentReplicaIndex = currentReplicaIndex;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveRegionGroupState state)
      throws InterruptedException {
    final List<TDataNodeLocation> dataNodeLocations =
        regionReplicaSet == null ? null : regionReplicaSet.getDataNodeLocations();
    if (dataNodeLocations == null) {
      // A null replica set means deserialization failed; fail loudly instead of silently reporting
      // the group as deleted, otherwise the parent would drop the partition table while the region
      // data is still on disk.
      setFailure(
          new ProcedureException(ProcedureMessages.UNSUPPORTED_STATE + "missing regionReplicaSet"));
      return Flow.NO_MORE_STATE;
    }
    final RegionMaintainHandler handler = env.getRegionMaintainHandler();
    switch (state) {
      case DELETE_REGION_REPLICAS:
        if (currentReplicaIndex == 0 && attemptedForCurrentReplica == 0) {
          LOGGER.info(
              ProcedureMessages.PID_REMOVEREGIONGROUP_STARTED_WILL_BE_DELETED,
              getProcId(),
              regionId,
              dataNodeLocations.stream()
                  .map(RegionMaintainHandler::simplifiedLocation)
                  .collect(Collectors.toList()));
        }
        if (currentReplicaIndex >= dataNodeLocations.size()) {
          // Requirement: every successfully completed maintain task must be logged.
          LOGGER.info(
              ProcedureMessages.PID_REMOVEREGIONGROUP_SUCCESS_PROCEDURE_TOOK,
              getProcId(),
              regionId,
              CommonDateTimeUtils.convertMillisecondToDurationStr(
                  System.currentTimeMillis() - getSubmittedTime()),
              DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
          return Flow.NO_MORE_STATE;
        }

        final TDataNodeLocation targetDataNode = dataNodeLocations.get(currentReplicaIndex);
        LOGGER.info(
            ProcedureMessages.PID_REMOVEREGIONGROUP_STARTED_REPLICA_WILL_BE_DELETED_FROM_DATANODE,
            getProcId(),
            regionId,
            simplifiedLocation(targetDataNode));

        // deleteLocalPeer is idempotent (it tolerates an already-absent peer) and the DataNode
        // dedups by taskId, so re-submitting after a leader change or a retry is safe.
        final TSStatus submitStatus;
        final TRegionMigrateResult result;
        try {
          submitStatus =
              handler.submitDeleteOldRegionPeerTask(getProcId(), targetDataNode, regionId);
          setKillPoint(state);
          if (submitStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
            return retryCurrentReplicaOrFail(
                String.format(
                    "submit delete task for region %s to DataNode %s failed: %s",
                    regionId, simplifiedLocation(targetDataNode), submitStatus));
          }
          result = handler.waitTaskFinish(getProcId(), targetDataNode);
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          LOGGER.error(ProcedureMessages.PID_REMOVEREGIONGROUP_STATE_FAILED, getProcId(), state, e);
          return retryCurrentReplicaOrFail(
              String.format(
                  "delete region %s from DataNode %s threw %s",
                  regionId, simplifiedLocation(targetDataNode), e));
        }

        switch (result.getTaskStatus()) {
          case SUCCESS:
            // Advance to the next replica with a fresh retry budget.
            currentReplicaIndex++;
            attemptedForCurrentReplica = 0;
            setNextState(RemoveRegionGroupState.DELETE_REGION_REPLICAS);
            return Flow.HAS_MORE_STATE;
          case PROCESSING:
            // waitTaskFinish() only returns PROCESSING when its polling loop was interrupted, i.e.
            // this ConfigNode is shutting down / losing leadership. The delete task is still
            // running
            // on the DataNode, so persist and re-poll after recovery: stay on this replica without
            // advancing it and without consuming a retry attempt.
            setNextState(RemoveRegionGroupState.DELETE_REGION_REPLICAS);
            return Flow.HAS_MORE_STATE;
          case TASK_NOT_EXIST:
          case FAIL:
          default:
            return retryCurrentReplicaOrFail(
                String.format(
                    "delete region %s from DataNode %s, task status is %s",
                    regionId, simplifiedLocation(targetDataNode), result.getTaskStatus()));
        }
      default:
        setFailure(new ProcedureException(ProcedureMessages.UNSUPPORTED_STATE + state.name()));
        return Flow.NO_MORE_STATE;
    }
  }

  /**
   * Retry the replica at {@link #currentReplicaIndex} after a backoff, or fail the whole procedure
   * once the per-replica retry budget is exhausted. Failing (rather than skipping the replica)
   * keeps the parent from dropping the partition table while a region's peer/data is still on disk.
   */
  private Flow retryCurrentReplicaOrFail(String reason) throws InterruptedException {
    attemptedForCurrentReplica++;
    if (attemptedForCurrentReplica <= MAX_DELETE_REPLICA_RETRY) {
      LOGGER.warn(
          ProcedureMessages.PID_REMOVEREGIONGROUP_DELETE_REPLICA_FAILED,
          getProcId(),
          regionId,
          attemptedForCurrentReplica,
          MAX_DELETE_REPLICA_RETRY + 1,
          reason);
      Thread.sleep(DELETE_REPLICA_RETRY_INTERVAL_MS);
      setNextState(RemoveRegionGroupState.DELETE_REGION_REPLICAS);
      return Flow.HAS_MORE_STATE;
    }
    setFailure(
        new ProcedureException(
            String.format(
                "[pid%d][RemoveRegionGroup] gave up after %d attempts: %s",
                getProcId(), attemptedForCurrentReplica, reason)));
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RemoveRegionGroupState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected RemoveRegionGroupState getState(int stateId) {
    return RemoveRegionGroupState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveRegionGroupState removeRegionGroupState) {
    return removeRegionGroupState.ordinal();
  }

  @Override
  protected RemoveRegionGroupState getInitialState() {
    return RemoveRegionGroupState.DELETE_REGION_REPLICAS;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_REGION_GROUP_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, stream);
    ReadWriteIOUtils.write(currentReplicaIndex, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      regionReplicaSet = ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(byteBuffer);
      regionId = regionReplicaSet.getRegionId();
      currentReplicaIndex = ReadWriteIOUtils.readInt(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOGGER.error(ProcedureMessages.ERROR_IN_DESERIALIZE, this.getClass(), e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RemoveRegionGroupProcedure)) {
      return false;
    }
    RemoveRegionGroupProcedure procedure = (RemoveRegionGroupProcedure) obj;
    return this.currentReplicaIndex == procedure.currentReplicaIndex
        && Objects.equals(this.regionReplicaSet, procedure.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionReplicaSet, currentReplicaIndex);
  }

  @Override
  public String toString() {
    return "RemoveRegionGroupProcedure{"
        + "regionReplicaSet="
        + regionReplicaSet
        + ", currentReplicaIndex="
        + currentReplicaIndex
        + '}';
  }
}
