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
 *
 * <p>This procedure is submitted as an independent root procedure (not a child) by its callers,
 * which only enqueue the deletion and return immediately. It therefore owns the deletion end to
 * end: on any failure it retries the current replica forever (backing off between attempts) instead
 * of giving up, because there is no parent left to fall back to and the region's peer/data must not
 * be left on disk. Each genuine re-attempt uses a FRESH DataNode-side taskId (the DataNode dedups
 * by taskId and caches a terminal result forever, so reusing one taskId would make every retry a
 * no-op that never re-runs the delete); the in-flight taskId is persisted so a leader change
 * re-polls the same task rather than double-submitting. It carries its own {@link
 * TRegionReplicaSet} copy, so it can finish even after the caller has dropped the partition table,
 * and it survives ConfigNode leader change / restart.
 */
public class RemoveRegionGroupProcedure extends RegionOperationProcedure<RemoveRegionGroupState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveRegionGroupProcedure.class);

  private static final long DELETE_REPLICA_RETRY_INTERVAL_MS = 5_000;

  private TRegionReplicaSet regionReplicaSet;

  // The index of the replica currently being deleted. Persisted and advanced only after that
  // replica
  // is deleted, so after a ConfigNode leader change the procedure resumes on the first replica it
  // has
  // not finished deleting.
  private int currentReplicaIndex;

  // Number of failed attempts on the replica at currentReplicaIndex, used only for logging. Retries
  // are unbounded, so this is not a budget. Transient: a leader change restarts the counter for the
  // current replica.
  private transient int attemptedForCurrentReplica;

  // Monotonic count of delete tasks this procedure has submitted, across all replicas. Persisted
  // and
  // only ever incremented. It is the low half of the DataNode-side taskId (see deleteTaskId): a
  // fresh value per genuine re-attempt makes the DataNode re-run the delete instead of replaying a
  // cached terminal result for a reused taskId (the DataNode dedups by taskId and never clears the
  // cache), which is the bug this fixes. It never resets, so every taskId this procedure emits is
  // distinct even across replicas and retries.
  private long deleteTaskSeq;

  // Whether a delete task for the replica at currentReplicaIndex has already been submitted (and
  // thus
  // deleteTaskSeq already identifies an in-flight task to re-poll) rather than needing a fresh one.
  // Persisted so a leader change mid-attempt re-polls the SAME in-flight task instead of submitting
  // a
  // duplicate; cleared on success or when a terminal failure forces a fresh re-attempt.
  private boolean deleteTaskSubmitted;

  // Bit budget for deleteTaskId(): sign bit (=> negative) + PROC_ID_BITS + SEQ_BITS must be <= 64.
  private static final int SEQ_BITS = 20;
  private static final int PROC_ID_BITS = 43;

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

  @TestOnly
  void setDeleteTaskState(long deleteTaskSeq, boolean deleteTaskSubmitted) {
    this.deleteTaskSeq = deleteTaskSeq;
    this.deleteTaskSubmitted = deleteTaskSubmitted;
  }

  @TestOnly
  long deleteTaskIdForTest() {
    return deleteTaskId();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveRegionGroupState state)
      throws InterruptedException {
    final List<TDataNodeLocation> dataNodeLocations =
        regionReplicaSet == null ? null : regionReplicaSet.getDataNodeLocations();
    if (dataNodeLocations == null) {
      // A null replica set means deserialization failed. Retrying cannot recover the lost
      // locations,
      // so fail loudly instead of silently reporting the group as deleted (which would leave the
      // region's peer/data on disk with no record of where it lives).
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

        // Start a fresh attempt (fresh taskId) unless we are resuming an already-submitted one
        // after
        // a leader change, in which case we re-poll the SAME task rather than submitting a
        // duplicate.
        if (!deleteTaskSubmitted) {
          deleteTaskSeq++;
          deleteTaskSubmitted = true;
        }
        final long deleteTaskId = deleteTaskId();

        // deleteLocalPeer is idempotent (it tolerates an already-absent peer), and re-submitting
        // the
        // same taskId re-polls the same DataNode task, so resuming after a leader change is safe.
        final TSStatus submitStatus;
        final TRegionMigrateResult result;
        try {
          submitStatus =
              handler.submitDeleteOldRegionPeerTask(deleteTaskId, targetDataNode, regionId);
          setKillPoint(state);
          if (submitStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
            return retryCurrentReplica(
                String.format(
                    "submit delete task for region %s to DataNode %s failed: %s",
                    regionId, simplifiedLocation(targetDataNode), submitStatus));
          }
          result = handler.waitTaskFinish(deleteTaskId, targetDataNode);
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          LOGGER.error(ProcedureMessages.PID_REMOVEREGIONGROUP_STATE_FAILED, getProcId(), state, e);
          return retryCurrentReplica(
              String.format(
                  "delete region %s from DataNode %s threw %s",
                  regionId, simplifiedLocation(targetDataNode), e));
        }

        switch (result.getTaskStatus()) {
          case SUCCESS:
            // Advance to the next replica with a fresh retry counter and a fresh delete task.
            currentReplicaIndex++;
            attemptedForCurrentReplica = 0;
            deleteTaskSubmitted = false;
            setNextState(RemoveRegionGroupState.DELETE_REGION_REPLICAS);
            return Flow.HAS_MORE_STATE;
          case PROCESSING:
            // waitTaskFinish() only returns PROCESSING when its polling loop was interrupted, i.e.
            // this ConfigNode is shutting down / losing leadership. The delete task is still
            // running on the DataNode, so persist and re-poll after recovery: stay on this replica
            // without advancing it, without consuming a retry attempt, and keeping deleteTaskSeq /
            // deleteTaskSubmitted so the re-poll targets the same in-flight task.
            setNextState(RemoveRegionGroupState.DELETE_REGION_REPLICAS);
            return Flow.HAS_MORE_STATE;
          case TASK_NOT_EXIST:
          case FAIL:
          default:
            return retryCurrentReplica(
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
   * Retry the replica at {@link #currentReplicaIndex} after a backoff. This procedure never gives
   * up on a replica: because it is submitted as an independent root procedure, there is no parent
   * to fall back to, and skipping or failing would leave the region's peer/data on disk. So it
   * backs off and re-runs the same state until the replica is deleted, which eventually succeeds
   * once the target DataNode is reachable: the delete is idempotent, and clearing {@link
   * #deleteTaskSubmitted} here makes the next attempt use a FRESH DataNode-side taskId (a new
   * {@link #deleteTaskSeq}), so the DataNode actually re-executes the delete instead of returning a
   * cached terminal result for the previous taskId.
   */
  private Flow retryCurrentReplica(String reason) throws InterruptedException {
    attemptedForCurrentReplica++;
    LOGGER.warn(
        ProcedureMessages.PID_REMOVEREGIONGROUP_DELETE_REPLICA_FAILED,
        getProcId(),
        regionId,
        attemptedForCurrentReplica,
        reason);
    // Force a fresh delete task on the next attempt so the DataNode re-runs the delete rather than
    // replaying a cached FAIL/SUCCESS for this taskId.
    deleteTaskSubmitted = false;
    Thread.sleep(DELETE_REPLICA_RETRY_INTERVAL_MS);
    setNextState(RemoveRegionGroupState.DELETE_REGION_REPLICAS);
    return Flow.HAS_MORE_STATE;
  }

  /**
   * The DataNode-side taskId for the current attempt, derived from this procedure's (globally
   * unique, consensus-replicated) procId and its monotonic {@link #deleteTaskSeq}. It is packed
   * into the NEGATIVE i64 space, which is disjoint from every real procId (all {@code >= 0}); other
   * region-maintain procedures (add/remove peer) use {@code getProcId()} directly as the taskId
   * against the same DataNode task map, so a negative id can never collide with theirs. Unlike
   * minting from the procedure-store id allocator, this needs nothing extra replicated: procId is
   * already replicated and deleteTaskSeq is persisted with this procedure, so the taskId is stable
   * across a leader change and never regresses.
   *
   * <p>Layout: sign bit set (=> negative) | {@value PROC_ID_BITS} bits of procId | {@value
   * SEQ_BITS} bits of deleteTaskSeq. The bounds are astronomically beyond any real cluster (a
   * procId needs 2^43 procedures; a single group delete needs 2^20 retries), and are asserted
   * rather than silently wrapped so a violation fails the procedure loudly instead of emitting a
   * colliding id.
   */
  private long deleteTaskId() {
    final long procId = getProcId();
    if (procId < 0 || procId >= (1L << PROC_ID_BITS) || deleteTaskSeq >= (1L << SEQ_BITS)) {
      throw new IllegalStateException(
          String.format(
              "cannot derive a collision-free delete taskId: procId=%d, deleteTaskSeq=%d exceed the "
                  + "%d/%d-bit budget",
              procId, deleteTaskSeq, PROC_ID_BITS, SEQ_BITS));
    }
    return Long.MIN_VALUE | (procId << SEQ_BITS) | deleteTaskSeq;
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
    // Persist the delete-task cursor so a leader change re-derives the SAME in-flight taskId and
    // re-polls it (deleteTaskSubmitted == true) instead of submitting a duplicate, and so the
    // monotonic deleteTaskSeq never regresses.
    ReadWriteIOUtils.write(deleteTaskSeq, stream);
    ReadWriteIOUtils.write(deleteTaskSubmitted, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      regionReplicaSet = ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(byteBuffer);
      regionId = regionReplicaSet.getRegionId();
      currentReplicaIndex = ReadWriteIOUtils.readInt(byteBuffer);
      // deleteTaskSeq/deleteTaskSubmitted were appended after the first version of this procedure.
      // That first version only ever existed on the unreleased branch that added this procedure
      // (never in a release), but a dev/CI cluster could persist a blob without these trailing
      // fields; tolerate it by defaulting to "no in-flight task" instead of reading past the end.
      if (byteBuffer.hasRemaining()) {
        deleteTaskSeq = ReadWriteIOUtils.readLong(byteBuffer);
        deleteTaskSubmitted = ReadWriteIOUtils.readBool(byteBuffer);
      }
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
        && this.deleteTaskSeq == procedure.deleteTaskSeq
        && this.deleteTaskSubmitted == procedure.deleteTaskSubmitted
        && Objects.equals(this.regionReplicaSet, procedure.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionReplicaSet, currentReplicaIndex, deleteTaskSeq, deleteTaskSubmitted);
  }

  @Override
  public String toString() {
    return "RemoveRegionGroupProcedure{"
        + "regionReplicaSet="
        + regionReplicaSet
        + ", currentReplicaIndex="
        + currentReplicaIndex
        + ", deleteTaskSeq="
        + deleteTaskSeq
        + ", deleteTaskSubmitted="
        + deleteTaskSubmitted
        + '}';
  }
}
