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
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable.RegionRepairStatus;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DiffEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.RowRefIndex;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.DiffAttribution;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.ModEntrySummary;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairConflictResolver;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairCostModel;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairPlan;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairRecord;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairSession;
import org.apache.iotdb.commons.consensus.iotv2.consistency.repair.RepairStrategy;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RepairRegionProcedure orchestrates the consistency check and repair lifecycle for a single
 * consensus group. The procedure is intentionally isolated from transport details: all external
 * interactions are supplied by a {@link RepairExecutionContext}, while the procedure itself
 * persists the state-machine and applies the repair policy.
 */
public class RepairRegionProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RepairState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RepairRegionProcedure.class);

  private static final RepairCostModel REPAIR_COST_MODEL = new RepairCostModel();
  private static final ConcurrentHashMap<String, RepairExecutionContext> EXECUTION_CONTEXTS =
      new ConcurrentHashMap<>();

  private TConsensusGroupId consensusGroupId;
  private long tSafe;
  private long globalRepairedWatermark;
  private List<Long> pendingPartitions;
  private int currentPartitionIndex;
  private boolean hashMatched;
  private String executionContextId;
  private String lastFailureReason;

  private transient RepairExecutionContext executionContext;
  private transient RepairProgressTable repairProgressTable;
  private transient PartitionRepairContext currentPartitionContext;
  private transient DiffAttribution diffAttribution;
  private transient RowRefIndex currentRowRefIndex;
  private transient Map<String, List<DiffEntry>> attributedDiffs;
  private transient Map<String, RepairPlan> repairPlans;
  private transient Set<String> executedTsFileTransfers;
  private transient Set<String> executedPointStreamingPlans;
  private transient List<DiffEntry> decodedDiffs;
  private transient RepairSession repairSession;
  private transient long estimatedDiffCount;

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
          if (!checkSyncLagCompleted(env)) {
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
          computeWatermarkAndPartitions(env);
          if (pendingPartitions.isEmpty()) {
            LOGGER.info("No pending partitions to check for group {}", consensusGroupId);
            finishWithoutRepair();
            return Flow.NO_MORE_STATE;
          }
          setNextState(RepairState.BUILD_MERKLE_VIEW);
          break;

        case BUILD_MERKLE_VIEW:
          LOGGER.info(
              "RepairRegionProcedure: BUILD_MERKLE_VIEW for partition {} of group {}",
              getCurrentPartitionId(),
              consensusGroupId);
          buildMerkleView(env);
          setNextState(RepairState.COMPARE_ROOT_HASH);
          break;

        case COMPARE_ROOT_HASH:
          hashMatched = compareRootHash(env);
          if (hashMatched) {
            LOGGER.info("Root hash matched for partition {}", getCurrentPartitionId());
            setNextState(RepairState.COMMIT_PARTITION);
          } else {
            LOGGER.info("Root hash mismatched for partition {}", getCurrentPartitionId());
            setNextState(RepairState.DRILL_DOWN);
          }
          break;

        case DRILL_DOWN:
          LOGGER.info(
              "RepairRegionProcedure: DRILL_DOWN for partition {}", getCurrentPartitionId());
          drillDown(env);
          setNextState(RepairState.SMALL_TSFILE_SHORT_CIRCUIT);
          break;

        case SMALL_TSFILE_SHORT_CIRCUIT:
          if (prepareDirectTransferOnlyPlans()) {
            setNextState(RepairState.EXECUTE_TSFILE_TRANSFER);
          } else {
            boolean hasSmallFiles = handleSmallTsFileShortCircuit(env);
            if (hasLargeFilesNeedingIBF()) {
              setNextState(RepairState.NEGOTIATE_KEY_MAPPING);
            } else if (hasSmallFiles) {
              setNextState(RepairState.EXECUTE_TSFILE_TRANSFER);
            } else {
              setNextState(RepairState.COMMIT_PARTITION);
            }
          }
          break;

        case NEGOTIATE_KEY_MAPPING:
          LOGGER.info("RepairRegionProcedure: NEGOTIATE_KEY_MAPPING");
          negotiateKeyMapping(env);
          setNextState(RepairState.ESTIMATE_DIFF);
          break;

        case ESTIMATE_DIFF:
          estimateDiff(env);
          setNextState(RepairState.EXCHANGE_IBF);
          break;

        case EXCHANGE_IBF:
          exchangeIBF(env);
          setNextState(RepairState.DECODE_DIFF);
          break;

        case DECODE_DIFF:
          boolean decodeSuccess = decodeDiff(env);
          if (decodeSuccess) {
            setNextState(RepairState.ATTRIBUTE_DIFFS);
          } else {
            LOGGER.warn(
                "IBF decode failed for partition {}, falling back to direct transfer",
                getCurrentPartitionId());
            setNextState(RepairState.EXECUTE_TSFILE_TRANSFER);
          }
          break;

        case ATTRIBUTE_DIFFS:
          attributeDiffs(env);
          setNextState(RepairState.SELECT_REPAIR_STRATEGY);
          break;

        case SELECT_REPAIR_STRATEGY:
          selectRepairStrategy(env);
          if (hasPendingTsFileTransfers()) {
            setNextState(RepairState.EXECUTE_TSFILE_TRANSFER);
          } else if (hasPendingPointStreaming()) {
            setNextState(RepairState.EXECUTE_POINT_STREAMING);
          } else {
            setNextState(RepairState.VERIFY_REPAIR);
          }
          break;

        case EXECUTE_TSFILE_TRANSFER:
          LOGGER.info("RepairRegionProcedure: EXECUTE_TSFILE_TRANSFER");
          executeTsFileTransfer(env);
          if (hasPendingPointStreaming()) {
            setNextState(RepairState.EXECUTE_POINT_STREAMING);
          } else {
            setNextState(RepairState.VERIFY_REPAIR);
          }
          break;

        case EXECUTE_POINT_STREAMING:
          LOGGER.info("RepairRegionProcedure: EXECUTE_POINT_STREAMING");
          executePointStreaming(env);
          setNextState(RepairState.VERIFY_REPAIR);
          break;

        case VERIFY_REPAIR:
          boolean verified = verifyRepair(env);
          if (verified) {
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
          commitPartition(env);
          currentPartitionIndex++;
          if (currentPartitionIndex < pendingPartitions.size()) {
            setNextState(RepairState.BUILD_MERKLE_VIEW);
          } else {
            setNextState(RepairState.ADVANCE_WATERMARK);
          }
          break;

        case ADVANCE_WATERMARK:
          advanceWatermark(env);
          return Flow.NO_MORE_STATE;

        case ROLLBACK:
          LOGGER.warn("RepairRegionProcedure: ROLLBACK for group {}", consensusGroupId);
          rollback(env);
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

  private boolean checkSyncLagCompleted(ConfigNodeProcedureEnv env) {
    return requireExecutionContext().isReplicationComplete();
  }

  private void computeWatermarkAndPartitions(ConfigNodeProcedureEnv env) {
    RepairExecutionContext context = requireExecutionContext();
    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    progressTable.setRegionStatus(RegionRepairStatus.RUNNING);
    globalRepairedWatermark =
        Math.max(globalRepairedWatermark, progressTable.getGlobalRepairedWatermark());
    tSafe = Math.max(globalRepairedWatermark, context.computeSafeWatermark());

    pendingPartitions.clear();
    if (tSafe <= globalRepairedWatermark) {
      return;
    }

    List<Long> candidatePartitions =
        safeList(context.collectPendingPartitions(globalRepairedWatermark, tSafe, progressTable));
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

  private void buildMerkleView(ConfigNodeProcedureEnv env) {
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

  private boolean compareRootHash(ConfigNodeProcedureEnv env) {
    return requireCurrentPartitionContext().isRootHashMatched();
  }

  private void drillDown(ConfigNodeProcedureEnv env) {
    PartitionRepairContext partitionContext = requireCurrentPartitionContext();
    List<MerkleFileContent> mismatchedFiles =
        safeList(partitionContext.getMismatchedLeaderMerkleFiles());
    LOGGER.info(
        "Partition {} has {} mismatched TsFiles after Merkle drill-down",
        partitionContext.getPartitionId(),
        mismatchedFiles.size());
    if (mismatchedFiles.isEmpty()) {
      LOGGER.warn(
          "Partition {} reported root mismatch but returned no mismatched TsFiles",
          partitionContext.getPartitionId());
    }
  }

  private boolean prepareDirectTransferOnlyPlans() {
    PartitionRepairContext partitionContext = requireCurrentPartitionContext();
    if (!partitionContext.shouldForceDirectTsFileTransfer()) {
      return false;
    }

    List<String> fallbackTsFiles = safeList(partitionContext.getFallbackTsFiles());
    if (fallbackTsFiles.isEmpty()) {
      throw new IllegalStateException(
          "Partition "
              + partitionContext.getPartitionId()
              + " requested direct TsFile transfer but provided no fallback files");
    }

    for (String tsFilePath : fallbackTsFiles) {
      repairPlans.putIfAbsent(
          tsFilePath,
          RepairPlan.directTransfer(tsFilePath, partitionContext.getTsFileSize(tsFilePath)));
    }
    return true;
  }

  private boolean handleSmallTsFileShortCircuit(ConfigNodeProcedureEnv env) {
    boolean hasSmallFiles = false;
    for (MerkleFileContent content :
        safeList(requireCurrentPartitionContext().getMismatchedLeaderMerkleFiles())) {
      String tsFilePath = content.getSourceTsFilePath();
      long tsFileSize = requireCurrentPartitionContext().getTsFileSize(tsFilePath);
      if (REPAIR_COST_MODEL.shouldBypassIBF(tsFileSize)) {
        hasSmallFiles = true;
        repairPlans.put(tsFilePath, RepairPlan.directTransfer(tsFilePath, tsFileSize));
      }
    }
    return hasSmallFiles;
  }

  private boolean hasLargeFilesNeedingIBF() {
    for (MerkleFileContent content :
        safeList(requireCurrentPartitionContext().getMismatchedLeaderMerkleFiles())) {
      String tsFilePath = content.getSourceTsFilePath();
      if (repairPlans.containsKey(tsFilePath)) {
        continue;
      }
      long tsFileSize = requireCurrentPartitionContext().getTsFileSize(tsFilePath);
      if (!REPAIR_COST_MODEL.shouldBypassIBF(tsFileSize)) {
        return true;
      }
    }
    return false;
  }

  private void negotiateKeyMapping(ConfigNodeProcedureEnv env) {
    currentRowRefIndex = requireCurrentPartitionContext().getRowRefIndex();
    if (currentRowRefIndex == null) {
      throw new IllegalStateException(
          "Large-file diff localization requires a RowRefIndex for partition "
              + getCurrentPartitionId());
    }
  }

  private void estimateDiff(ConfigNodeProcedureEnv env) {
    estimatedDiffCount = requireCurrentPartitionContext().estimateDiffCount();
    if (estimatedDiffCount <= 0 && requireCurrentPartitionContext().isDiffDecodeSuccessful()) {
      estimatedDiffCount = safeList(requireCurrentPartitionContext().decodeDiffs()).size();
    }
    LOGGER.info(
        "Estimated diff count for partition {} is {}", getCurrentPartitionId(), estimatedDiffCount);
  }

  private void exchangeIBF(ConfigNodeProcedureEnv env) {
    decodedDiffs.clear();
    decodedDiffs.addAll(safeList(requireCurrentPartitionContext().decodeDiffs()));
  }

  private boolean decodeDiff(ConfigNodeProcedureEnv env) {
    PartitionRepairContext partitionContext = requireCurrentPartitionContext();
    if (!partitionContext.isDiffDecodeSuccessful()) {
      for (String tsFilePath : safeList(partitionContext.getFallbackTsFiles())) {
        repairPlans.put(
            tsFilePath,
            RepairPlan.directTransfer(tsFilePath, partitionContext.getTsFileSize(tsFilePath)));
      }
      return false;
    }
    if (decodedDiffs.isEmpty()) {
      decodedDiffs.addAll(safeList(partitionContext.decodeDiffs()));
    }
    return true;
  }

  private void attributeDiffs(ConfigNodeProcedureEnv env) {
    attributedDiffs =
        diffAttribution.attributeToSourceTsFiles(
            decodedDiffs,
            currentRowRefIndex,
            safeList(requireCurrentPartitionContext().getLeaderMerkleFiles()));
  }

  private void selectRepairStrategy(ConfigNodeProcedureEnv env) {
    PartitionRepairContext partitionContext = requireCurrentPartitionContext();
    for (Map.Entry<String, List<DiffEntry>> entry : attributedDiffs.entrySet()) {
      String tsFilePath = entry.getKey();
      if (repairPlans.containsKey(tsFilePath)) {
        continue;
      }
      List<DiffEntry> diffs = entry.getValue();
      long tsFileSize = partitionContext.getTsFileSize(tsFilePath);
      int totalPointCount = partitionContext.getTotalPointCount(tsFilePath);
      RepairStrategy strategy =
          REPAIR_COST_MODEL.selectStrategy(tsFileSize, totalPointCount, diffs.size());
      if (strategy == RepairStrategy.DIRECT_TSFILE_TRANSFER) {
        repairPlans.put(tsFilePath, RepairPlan.directTransfer(tsFilePath, tsFileSize));
      } else {
        repairPlans.put(tsFilePath, RepairPlan.pointStreaming(tsFilePath, tsFileSize, diffs));
      }
    }
  }

  private boolean hasPendingTsFileTransfers() {
    return repairPlans.values().stream()
        .anyMatch(
            plan ->
                plan.getStrategy() == RepairStrategy.DIRECT_TSFILE_TRANSFER
                    && !executedTsFileTransfers.contains(plan.getTsFilePath()));
  }

  private boolean hasPendingPointStreaming() {
    return repairPlans.values().stream()
        .anyMatch(
            plan ->
                plan.getStrategy() == RepairStrategy.POINT_STREAMING
                    && !executedPointStreamingPlans.contains(plan.getTsFilePath()));
  }

  private void executeTsFileTransfer(ConfigNodeProcedureEnv env) {
    for (RepairPlan plan : repairPlans.values()) {
      if (plan.getStrategy() != RepairStrategy.DIRECT_TSFILE_TRANSFER
          || executedTsFileTransfers.contains(plan.getTsFilePath())) {
        continue;
      }
      try {
        requireExecutionContext().transferTsFile(plan.getTsFilePath());
        executedTsFileTransfers.add(plan.getTsFilePath());
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to transfer TsFile " + plan.getTsFilePath() + ": " + e.getMessage(), e);
      }
    }
  }

  private void executePointStreaming(ConfigNodeProcedureEnv env) {
    RepairConflictResolver conflictResolver =
        new RepairConflictResolver(
            safeList(requireCurrentPartitionContext().getLeaderDeletions()),
            safeList(requireCurrentPartitionContext().getFollowerDeletions()));
    repairSession = requireExecutionContext().createRepairSession(getCurrentPartitionId());

    for (RepairPlan plan : repairPlans.values()) {
      if (plan.getStrategy() != RepairStrategy.POINT_STREAMING
          || executedPointStreamingPlans.contains(plan.getTsFilePath())) {
        continue;
      }
      for (DiffEntry diff : plan.getDiffs()) {
        RepairRecord record =
            requireExecutionContext()
                .buildRepairRecord(
                    requireCurrentPartitionContext(), diff, currentRowRefIndex, conflictResolver);
        if (record != null) {
          repairSession.stage(record);
        }
      }
      executedPointStreamingPlans.add(plan.getTsFilePath());
    }

    if (repairSession.getStagedCount() > 0 && !repairSession.promoteAtomically()) {
      throw new IllegalStateException(
          "Failed to atomically promote repair session " + repairSession.getSessionId());
    }
  }

  private boolean verifyRepair(ConfigNodeProcedureEnv env) {
    return hashMatched || requireCurrentPartitionContext().verify(repairPlans, hashMatched);
  }

  private void commitPartition(ConfigNodeProcedureEnv env) {
    long partitionId = getCurrentPartitionId();
    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    long repairedTo =
        Math.max(tSafe, progressTable.getOrCreatePartition(partitionId).getRepairedTo());
    progressTable.commitPartition(partitionId, repairedTo);
    requireExecutionContext().onPartitionCommitted(partitionId, repairedTo, progressTable);
    if (repairSession != null) {
      repairSession.cleanup();
    }
    persistRepairProgressTable(progressTable);
    LOGGER.info("Committed partition {} for group {}", partitionId, consensusGroupId);
    resetCurrentPartitionState();
  }

  private void advanceWatermark(ConfigNodeProcedureEnv env) {
    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    globalRepairedWatermark = progressTable.advanceGlobalWatermark();
    progressTable.setRegionStatus(RegionRepairStatus.IDLE);
    requireExecutionContext().onWatermarkAdvanced(globalRepairedWatermark, progressTable);
    persistRepairProgressTable(progressTable);
    LOGGER.info(
        "Advanced repair watermark for group {} to {}", consensusGroupId, globalRepairedWatermark);
    cleanupExecutionContext();
  }

  protected void rollback(ConfigNodeProcedureEnv env) {
    long partitionId = getCurrentPartitionId();
    RepairProgressTable progressTable = getOrCreateRepairProgressTable();
    if (partitionId >= 0) {
      progressTable.failPartition(
          partitionId, lastFailureReason == null ? "Unknown repair failure" : lastFailureReason);
    }
    progressTable.setRegionStatus(RegionRepairStatus.FAILED);
    if (repairSession != null) {
      repairSession.abort();
    }
    RepairExecutionContext context = getExecutionContextIfPresent();
    if (context != null && partitionId >= 0) {
      context.rollbackPartition(partitionId, repairSession, progressTable);
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
    stream.writeLong(globalRepairedWatermark);
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
    this.globalRepairedWatermark = byteBuffer.getLong();
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

  public long getGlobalRepairedWatermark() {
    return globalRepairedWatermark;
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
        && globalRepairedWatermark == that.globalRepairedWatermark
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
        globalRepairedWatermark,
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
    this.diffAttribution = new DiffAttribution();
    this.currentRowRefIndex = null;
    this.attributedDiffs = new LinkedHashMap<>();
    this.repairPlans = new LinkedHashMap<>();
    this.executedTsFileTransfers = new HashSet<>();
    this.executedPointStreamingPlans = new HashSet<>();
    this.decodedDiffs = new ArrayList<>();
    this.repairSession = null;
    this.estimatedDiffCount = 0L;
  }

  private void resetCurrentPartitionState() {
    this.currentPartitionContext = null;
    this.currentRowRefIndex = null;
    this.attributedDiffs.clear();
    this.repairPlans.clear();
    this.executedTsFileTransfers.clear();
    this.executedPointStreamingPlans.clear();
    this.decodedDiffs.clear();
    this.repairSession = null;
    this.hashMatched = false;
    this.estimatedDiffCount = 0L;
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
      repairProgressTable.setRegionStatus(RegionRepairStatus.RUNNING);
      for (int i = 0; i < pendingPartitions.size(); i++) {
        long partitionId = pendingPartitions.get(i);
        RepairProgressTable.PartitionProgress progress =
            repairProgressTable.getOrCreatePartition(partitionId);
        if (i < currentPartitionIndex) {
          progress.markVerified(tSafe);
        }
      }
      repairProgressTable.advanceGlobalWatermark();
    }
    return repairProgressTable;
  }

  private void finishWithoutRepair() {
    if (repairProgressTable != null) {
      repairProgressTable.setRegionStatus(RegionRepairStatus.IDLE);
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
   * provides Merkle snapshots, decoded diffs and repair primitives.
   */
  public interface RepairExecutionContext extends AutoCloseable {

    boolean isReplicationComplete();

    long computeSafeWatermark();

    List<Long> collectPendingPartitions(
        long globalRepairedWatermark, long safeWatermark, RepairProgressTable repairProgressTable);

    PartitionRepairContext getPartitionContext(long partitionId);

    RepairRecord buildRepairRecord(
        PartitionRepairContext partitionContext,
        DiffEntry diffEntry,
        RowRefIndex rowRefIndex,
        RepairConflictResolver conflictResolver);

    void transferTsFile(String tsFilePath) throws Exception;

    default RepairSession createRepairSession(long partitionId) {
      return new RepairSession(partitionId);
    }

    default RepairProgressTable loadRepairProgressTable(String consensusGroupKey) {
      return null;
    }

    default void persistRepairProgressTable(RepairProgressTable repairProgressTable) {}

    default void onPartitionCommitted(
        long partitionId, long repairedTo, RepairProgressTable repairProgressTable) {}

    default void onWatermarkAdvanced(
        long globalWatermark, RepairProgressTable repairProgressTable) {}

    default void rollbackPartition(
        long partitionId, RepairSession repairSession, RepairProgressTable repairProgressTable) {}

    @Override
    default void close() {}
  }

  /** Immutable repair inputs for a single time partition. */
  public interface PartitionRepairContext {

    long getPartitionId();

    boolean isRootHashMatched();

    List<MerkleFileContent> getLeaderMerkleFiles();

    List<MerkleFileContent> getMismatchedLeaderMerkleFiles();

    RowRefIndex getRowRefIndex();

    List<DiffEntry> decodeDiffs();

    boolean isDiffDecodeSuccessful();

    long estimateDiffCount();

    List<String> getFallbackTsFiles();

    long getTsFileSize(String tsFilePath);

    int getTotalPointCount(String tsFilePath);

    default List<ModEntrySummary> getLeaderDeletions() {
      return Collections.emptyList();
    }

    default List<ModEntrySummary> getFollowerDeletions() {
      return Collections.emptyList();
    }

    default boolean shouldForceDirectTsFileTransfer() {
      return false;
    }

    default boolean verify(Map<String, RepairPlan> repairPlans, boolean rootHashMatched) {
      return true;
    }
  }
}
