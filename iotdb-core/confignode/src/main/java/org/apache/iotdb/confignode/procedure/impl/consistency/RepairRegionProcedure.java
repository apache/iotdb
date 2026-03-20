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

package org.apache.iotdb.confignode.procedure.impl.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.consistency.RepairState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RepairRegionProcedure orchestrates partition-scoped replica repair for a single consensus group.
 *
 * <p>The current repair path is logical-snapshot driven: ConfigNode first identifies the exact
 * partition/leaf mismatch scope, then the procedure executes the corresponding logical repair
 * operations and verifies the partition after repair.
 */
public class RepairRegionProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RepairState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RepairRegionProcedure.class);

  private static final ConcurrentHashMap<String, RepairExecutionContext> EXECUTION_CONTEXTS =
      new ConcurrentHashMap<>();

  private TConsensusGroupId consensusGroupId;
  private long tSafe;
  private List<Long> pendingPartitions;
  private int currentPartitionIndex;
  private boolean hashMatched;
  private String executionContextId;
  private String lastFailureReason;

  private transient RepairExecutionContext executionContext;
  private transient RepairProgressTable repairProgressTable;
  private transient PartitionRepairContext currentPartitionContext;
  private transient List<String> repairOperationIds;
  private transient Set<String> executedRepairOperations;

  /** Required for deserialization. */
  public RepairRegionProcedure() {
    this(null, (String) null);
  }

  public RepairRegionProcedure(TConsensusGroupId consensusGroupId) {
    this(consensusGroupId, (String) null);
  }

  public RepairRegionProcedure(
      TConsensusGroupId consensusGroupId, RepairExecutionContext executionContext) {
    this(consensusGroupId, registerExecutionContext(executionContext));
    this.executionContext = executionContext;
  }

  public RepairRegionProcedure(TConsensusGroupId consensusGroupId, String executionContextId) {
    this.consensusGroupId = consensusGroupId;
    this.executionContextId = executionContextId;
    this.pendingPartitions = new ArrayList<>();
    this.currentPartitionIndex = 0;
    this.hashMatched = false;
    initializeTransientState();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RepairState state)
      throws InterruptedException {
    try {
      switch (state) {
        case INIT:
          LOGGER.info("RepairRegionProcedure: INIT for group {}", consensusGroupId);
          setNextState(RepairState.CHECK_SYNC_LAG);
          break;

        case CHECK_SYNC_LAG:
          LOGGER.info("RepairRegionProcedure: CHECK_SYNC_LAG for group {}", consensusGroupId);
          if (!requireExecutionContext().isReplicationComplete()) {
            LOGGER.info(
                "RepairRegionProcedure: skipping group {} because replication is not complete",
                consensusGroupId);
            finishWithoutRepair();
            return Flow.NO_MORE_STATE;
          }
          setNextState(RepairState.COMPUTE_WATERMARK);
          break;

        case COMPUTE_WATERMARK:
          LOGGER.info("RepairRegionProcedure: COMPUTE_WATERMARK for group {}", consensusGroupId);
          computeWatermarkAndPartitions();
          if (pendingPartitions.isEmpty()) {
            LOGGER.info("No pending partitions to repair for group {}", consensusGroupId);
            finishWithoutRepair();
            return Flow.NO_MORE_STATE;
          }
          setNextState(RepairState.PREPARE_LOGICAL_SNAPSHOT);
          break;

        case PREPARE_LOGICAL_SNAPSHOT:
          LOGGER.info(
              "RepairRegionProcedure: PREPARE_LOGICAL_SNAPSHOT for partition {} of group {}",
              getCurrentPartitionId(),
              consensusGroupId);
          buildPartitionContext();
          setNextState(RepairState.COMPARE_ROOT_HASH);
          break;

        case COMPARE_ROOT_HASH:
          hashMatched = requireCurrentPartitionContext().isRootHashMatched();
          if (hashMatched) {
            LOGGER.info("Partition {} is already matched", getCurrentPartitionId());
            setNextState(RepairState.COMMIT_PARTITION);
          } else {
            LOGGER.info("Partition {} is mismatched", getCurrentPartitionId());
            setNextState(RepairState.DRILL_DOWN);
          }
          break;

        case DRILL_DOWN:
          prepareRepairOperations();
          setNextState(RepairState.EXECUTE_REPAIR_OPERATIONS);
          break;

        case EXECUTE_REPAIR_OPERATIONS:
          executeRepairOperations();
          setNextState(RepairState.VERIFY_REPAIR);
          break;

        case VERIFY_REPAIR:
          if (hashMatched || requireCurrentPartitionContext().verify()) {
            setNextState(RepairState.COMMIT_PARTITION);
          } else {
            lastFailureReason =
                "Verification failed for partition "
                    + getCurrentPartitionId()
                    + " in group "
                    + consensusGroupId;
            LOGGER.warn(lastFailureReason);
            setNextState(RepairState.ROLLBACK);
          }
          break;

        case COMMIT_PARTITION:
          commitPartition();
          currentPartitionIndex++;
          if (currentPartitionIndex < pendingPartitions.size()) {
            setNextState(RepairState.PREPARE_LOGICAL_SNAPSHOT);
          } else {
            setNextState(RepairState.ADVANCE_WATERMARK);
          }
          break;

        case ADVANCE_WATERMARK:
          advanceWatermark();
          return Flow.NO_MORE_STATE;

        case ROLLBACK:
          LOGGER.warn("RepairRegionProcedure: ROLLBACK for group {}", consensusGroupId);
          rollback();
          return Flow.NO_MORE_STATE;

        case DONE:
          finishWithoutRepair();
          return Flow.NO_MORE_STATE;

        default:
          LOGGER.error("Unknown state: {}", state);
          finishWithoutRepair();
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      lastFailureReason = e.getMessage();
      LOGGER.error(
          "Error in RepairRegionProcedure state {} for group {}: {}",
          state,
          consensusGroupId,
          e.getMessage(),
          e);
      setNextState(RepairState.ROLLBACK);
    }
    return Flow.HAS_MORE_STATE;
  }

  private void computeWatermarkAndPartitions() {
    RepairExecutionContext context = requireExecutionContext();
    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    tSafe = context.computeSafeWatermark();

    pendingPartitions.clear();
    if (tSafe == Long.MIN_VALUE) {
      return;
    }

    List<Long> candidatePartitions =
        safeList(context.collectPendingPartitions(tSafe, progressTable));
    candidatePartitions.stream()
        .filter(Objects::nonNull)
        .filter(partitionId -> partitionId <= tSafe)
        .distinct()
        .sorted(Comparator.naturalOrder())
        .forEach(
            partitionId -> {
              pendingPartitions.add(partitionId);
              progressTable.getOrCreatePartition(partitionId);
            });
    persistRepairProgressTable(progressTable);
  }

  private void buildPartitionContext() {
    resetCurrentPartitionState();
    long partitionId = getCurrentPartitionId();
    currentPartitionContext = requireExecutionContext().getPartitionContext(partitionId);
    if (currentPartitionContext == null) {
      throw new IllegalStateException(
          "Missing partition context for partition "
              + partitionId
              + " in group "
              + consensusGroupId);
    }
    if (currentPartitionContext.getPartitionId() != partitionId) {
      throw new IllegalStateException(
          "Partition context mismatch: expected "
              + partitionId
              + ", actual "
              + currentPartitionContext.getPartitionId());
    }
  }

  private void prepareRepairOperations() {
    PartitionRepairContext partitionContext = requireCurrentPartitionContext();
    LOGGER.info(
        "Partition {} has {} logical repair operations in the cached mismatch scope",
        partitionContext.getPartitionId(),
        safeList(partitionContext.getRepairOperationIds()).size());

    List<String> operationIds = safeList(partitionContext.getRepairOperationIds());
    if (operationIds.isEmpty()) {
      String blockingReason = partitionContext.getBlockingReason();
      throw new IllegalStateException(
          blockingReason != null
              ? blockingReason
              : ("Partition "
                  + partitionContext.getPartitionId()
                  + " is mismatched but no repair operations were provided"));
    }

    for (String operationId : operationIds) {
      repairOperationIds.add(operationId);
    }

    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    String repairEpoch =
        partitionContext.getRepairEpoch() != null
            ? partitionContext.getRepairEpoch()
            : progressTable
                .getOrCreatePartition(partitionContext.getPartitionId())
                .getRepairEpoch();
    progressTable.markRepairRunning(partitionContext.getPartitionId(), repairEpoch);
    persistRepairProgressTable(progressTable);
  }

  private void executeRepairOperations() throws Exception {
    for (String operationId : repairOperationIds) {
      if (!executedRepairOperations.add(operationId)) {
        continue;
      }
      requireExecutionContext().executeRepairOperation(operationId);
    }
  }

  private void commitPartition() {
    long partitionId = getCurrentPartitionId();
    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    long checkedAt = System.currentTimeMillis();
    requireExecutionContext().onPartitionCommitted(partitionId, checkedAt, progressTable);
    RepairProgressTable.PartitionProgress progress =
        progressTable.getOrCreatePartition(partitionId);
    if (progress.getCheckState() != RepairProgressTable.CheckState.VERIFIED) {
      progressTable.markRepairSucceeded(
          partitionId,
          checkedAt,
          tSafe,
          progress.getPartitionMutationEpoch(),
          progress.getSnapshotEpoch(),
          progress.getSnapshotState(),
          progress.getRepairEpoch());
    }
    persistRepairProgressTable(progressTable);
    LOGGER.info("Committed partition {} for group {}", partitionId, consensusGroupId);
    resetCurrentPartitionState();
  }

  private void advanceWatermark() {
    persistRepairProgressTable(getOrCreateRepairProgressTable());
    LOGGER.info(
        "Finished repair procedure for group {} at safe watermark {}", consensusGroupId, tSafe);
    cleanupExecutionContext();
  }

  protected void rollback() {
    long partitionId = getCurrentPartitionId();
    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    if (partitionId >= 0) {
      RepairProgressTable.PartitionProgress partitionProgress =
          progressTable.getOrCreatePartition(partitionId);
      progressTable.markRepairFailed(
          partitionId,
          partitionProgress.getRepairEpoch(),
          "REPAIR_FAILED",
          lastFailureReason == null ? "Unknown repair failure" : lastFailureReason);
      progressTable.markCheckFailed(
          partitionId,
          System.currentTimeMillis(),
          tSafe,
          partitionProgress.getPartitionMutationEpoch(),
          partitionProgress.getSnapshotEpoch(),
          partitionProgress.getSnapshotState(),
          "REPAIR_FAILED",
          lastFailureReason == null ? "Unknown repair failure" : lastFailureReason);
    }
    RepairExecutionContext context = getExecutionContextIfPresent();
    if (context != null && partitionId >= 0) {
      context.rollbackPartition(partitionId, progressTable);
    }
    persistRepairProgressTable(progressTable);
    LOGGER.warn("Rolled back repair for group {}", consensusGroupId);
    cleanupExecutionContext();
  }

  private long getCurrentPartitionId() {
    if (pendingPartitions.isEmpty() || currentPartitionIndex >= pendingPartitions.size()) {
      return -1;
    }
    return pendingPartitions.get(currentPartitionIndex);
  }

  @Override
  protected RepairState getState(int stateId) {
    return RepairState.values()[stateId];
  }

  @Override
  protected int getStateId(RepairState repairState) {
    return repairState.ordinal();
  }

  @Override
  protected RepairState getInitialState() {
    return RepairState.INIT;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RepairState state) {
    LOGGER.warn("Rollback requested for state {} in group {}", state, consensusGroupId);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REPAIR_REGION_PROCEDURE.getTypeCode());
    super.serialize(stream);

    stream.writeBoolean(consensusGroupId != null);
    if (consensusGroupId != null) {
      stream.writeInt(consensusGroupId.getId());
      stream.writeInt(consensusGroupId.getType().getValue());
    }
    stream.writeLong(tSafe);
    stream.writeInt(currentPartitionIndex);
    stream.writeBoolean(hashMatched);
    writeString(stream, executionContextId);
    writeString(stream, lastFailureReason);

    stream.writeInt(pendingPartitions.size());
    for (long partitionId : pendingPartitions) {
      stream.writeLong(partitionId);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    if (byteBuffer.get() != 0) {
      int groupId = byteBuffer.getInt();
      int groupType = byteBuffer.getInt();
      this.consensusGroupId = new TConsensusGroupId();
      this.consensusGroupId.setId(groupId);
      this.consensusGroupId.setType(
          org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.findByValue(groupType));
    } else {
      this.consensusGroupId = null;
    }

    this.tSafe = byteBuffer.getLong();
    this.currentPartitionIndex = byteBuffer.getInt();
    this.hashMatched = byteBuffer.get() != 0;
    this.executionContextId = readString(byteBuffer);
    this.lastFailureReason = readString(byteBuffer);

    int partitionCount = byteBuffer.getInt();
    this.pendingPartitions = new ArrayList<>(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      pendingPartitions.add(byteBuffer.getLong());
    }
    initializeTransientState();
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public String getExecutionContextId() {
    return executionContextId;
  }

  public static String registerExecutionContext(RepairExecutionContext executionContext) {
    if (executionContext == null) {
      return null;
    }
    String contextId = UUID.randomUUID().toString();
    EXECUTION_CONTEXTS.put(contextId, executionContext);
    return contextId;
  }

  public static void unregisterExecutionContext(String contextId) {
    if (contextId == null) {
      return;
    }
    EXECUTION_CONTEXTS.remove(contextId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RepairRegionProcedure)) {
      return false;
    }
    RepairRegionProcedure that = (RepairRegionProcedure) o;
    return tSafe == that.tSafe
        && currentPartitionIndex == that.currentPartitionIndex
        && hashMatched == that.hashMatched
        && Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(pendingPartitions, that.pendingPartitions)
        && Objects.equals(executionContextId, that.executionContextId)
        && Objects.equals(lastFailureReason, that.lastFailureReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        consensusGroupId,
        tSafe,
        pendingPartitions,
        currentPartitionIndex,
        hashMatched,
        executionContextId,
        lastFailureReason);
  }

  private void initializeTransientState() {
    this.executionContext = getExecutionContextIfPresent();
    this.currentPartitionContext = null;
    this.repairProgressTable = null;
    this.repairOperationIds = new ArrayList<>();
    this.executedRepairOperations = new LinkedHashSet<>();
  }

  private void resetCurrentPartitionState() {
    this.currentPartitionContext = null;
    this.repairOperationIds.clear();
    this.executedRepairOperations.clear();
    this.hashMatched = false;
    this.lastFailureReason = null;
  }

  private RepairExecutionContext requireExecutionContext() {
    RepairExecutionContext context = getExecutionContextIfPresent();
    if (context == null) {
      throw new IllegalStateException(
          "No execution context registered for repair procedure of group "
              + consensusGroupId
              + ". Expected context id: "
              + executionContextId);
    }
    return context;
  }

  private RepairExecutionContext getExecutionContextIfPresent() {
    if (executionContext == null && executionContextId != null) {
      executionContext = EXECUTION_CONTEXTS.get(executionContextId);
    }
    return executionContext;
  }

  private PartitionRepairContext requireCurrentPartitionContext() {
    if (currentPartitionContext == null) {
      throw new IllegalStateException(
          "Partition context has not been prepared for partition " + getCurrentPartitionId());
    }
    return currentPartitionContext;
  }

  private RepairProgressTable getOrCreateRepairProgressTable() {
    if (repairProgressTable == null) {
      RepairExecutionContext context = getExecutionContextIfPresent();
      if (context != null) {
        repairProgressTable =
            context.loadRepairProgressTable(toConsensusGroupKey(consensusGroupId));
      }
      if (repairProgressTable == null) {
        repairProgressTable = new RepairProgressTable(toConsensusGroupKey(consensusGroupId));
      }
    }
    return repairProgressTable;
  }

  private void finishWithoutRepair() {
    if (repairProgressTable != null) {
      persistRepairProgressTable(repairProgressTable);
    }
    cleanupExecutionContext();
  }

  private void persistRepairProgressTable(RepairProgressTable progressTable) {
    RepairExecutionContext context = getExecutionContextIfPresent();
    if (context != null && progressTable != null) {
      context.persistRepairProgressTable(progressTable);
    }
  }

  private void cleanupExecutionContext() {
    RepairExecutionContext context = getExecutionContextIfPresent();
    if (context != null) {
      try {
        context.close();
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to close repair execution context {}: {}",
            executionContextId,
            e.getMessage(),
            e);
      }
    }
    unregisterExecutionContext(executionContextId);
    executionContext = null;
  }

  private static <T> List<T> safeList(List<T> list) {
    return list == null ? Collections.emptyList() : list;
  }

  private static String toConsensusGroupKey(TConsensusGroupId consensusGroupId) {
    if (consensusGroupId == null) {
      return "unknown";
    }
    return consensusGroupId.getType() + "-" + consensusGroupId.getId();
  }

  private static void writeString(DataOutputStream stream, String value) throws IOException {
    if (value == null) {
      stream.writeInt(-1);
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    stream.writeInt(bytes.length);
    stream.write(bytes);
  }

  private static String readString(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    if (length < 0) {
      return null;
    }
    byte[] bytes = new byte[length];
    byteBuffer.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Bridge between the state machine and the transport/runtime-specific implementation that
   * provides logical-snapshot mismatch repair operations.
   */
  public interface RepairExecutionContext extends AutoCloseable {

    boolean isReplicationComplete();

    long computeSafeWatermark();

    List<Long> collectPendingPartitions(
        long safeWatermark, RepairProgressTable repairProgressTable);

    PartitionRepairContext getPartitionContext(long partitionId);

    void executeRepairOperation(String operationId) throws Exception;

    default RepairProgressTable loadRepairProgressTable(String consensusGroupKey) {
      return null;
    }

    default void persistRepairProgressTable(RepairProgressTable repairProgressTable) {}

    default void onPartitionCommitted(
        long partitionId, long committedAt, RepairProgressTable repairProgressTable) {}

    default void rollbackPartition(long partitionId, RepairProgressTable repairProgressTable) {}

    @Override
    default void close() {}
  }

  /** Immutable repair inputs for a single time partition. */
  public interface PartitionRepairContext {

    long getPartitionId();

    boolean isRootHashMatched();

    List<String> getRepairOperationIds();

    default String getRepairEpoch() {
      return null;
    }

    default String getBlockingReason() {
      return null;
    }

    default boolean verify() {
      return true;
    }
  }
}
