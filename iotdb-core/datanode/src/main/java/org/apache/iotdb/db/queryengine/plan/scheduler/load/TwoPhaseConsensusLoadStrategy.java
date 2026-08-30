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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.load.LoadTsFileChecksumUtils;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Two-phase consensus LOAD strategy for files that need decoding.
 *
 * <p><b>Phase 1 (split &amp; stream).</b> {@link #execute(LoadSingleTsFileNode)} resets the
 * per-file state, assigns a fresh uuid to {@link LoadTsFileDispatcherImpl} (executor naming / log
 * correlation) and feeds the source TsFile through {@link TsFileSplitter} into {@link
 * TsFileSplitConsumer}. Every dispatched piece goes through {@code dispatchConsensusPiece}: the
 * first piece of a region sends BEGIN with a fresh per-region load id, then PIECE commands with a
 * monotonically increasing {@code pieceIndex}; {@link RegionConsensusContext#accumulate(long,
 * long)} records piece count, total bytes and the XOR checksum. Submission goes through {@link
 * LoadConsensusSubmitter} with bounded retries for transient failures only.
 *
 * <p><b>Phase 2 (commit or abort).</b> If every region received all its pieces, each touched region
 * gets PREPARE (with the accumulated count/bytes/checksum) followed by COMMIT; otherwise every
 * touched region gets ABORT so the staged data is dropped.
 *
 * <p>Per-file state: {@code allReplicaSets} (the regions to prepare/commit/abort), {@code
 * consensusContexts} (per-region two-phase state) and {@code timePartitionSlotToProgressIndex}
 * (pipe progress index per time partition, collected while splitting for the upcoming
 * progress-index sync).
 */
public class TwoPhaseConsensusLoadStrategy implements TsFileLoadStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoPhaseConsensusLoadStrategy.class);

  private static final LoadTsFileCostMetricsSet LOAD_TSFILE_COST_METRICS_SET =
      LoadTsFileCostMetricsSet.getInstance();

  /**
   * Bounded retry for transient LOAD consensus submission failures (network errors, region
   * migration, transient server errors). The write node deduplicates pieces by (loadId, pieceIndex,
   * checksum), so a retried request whose first attempt actually applied is acknowledged as success
   * instead of being applied twice.
   */
  private static final int LOAD_CONSENSUS_SUBMIT_MAX_RETRIES = 3;

  private static final long LOAD_CONSENSUS_SUBMIT_RETRY_BACKOFF_MS = 100L;

  private final LoadTsFileDispatcherImpl dispatcher;
  private final DataPartitionBatchFetcher partitionFetcher;
  private final LoadTsFileDataCacheMemoryBlock block;
  private final LoadConsensusSubmitter consensusSubmitter;
  private final String userName;
  private final boolean isGeneratedByPipe;

  /** The source file being loaded, kept for the phase-two commands and BEGIN metadata. */
  private LoadSingleTsFileNode currentNode;

  /** Regions touched by the current file; used to send ABORT/PREPARE+COMMIT in phase two. */
  private final Set<TRegionReplicaSet> allReplicaSets = new HashSet<>();

  /** Per-region two-phase state of the current file; replaces the old five parallel maps. */
  private final Map<TConsensusGroupId, RegionConsensusContext> consensusContexts =
      new ConcurrentHashMap<>();

  /**
   * Progress index per time partition, assigned while the file is being split. Kept for the
   * upcoming progress-index sync with the consensus prepare phase.
   */
  private final Map<TTimePartitionSlot, ProgressIndex> timePartitionSlotToProgressIndex =
      new HashMap<>();

  public TwoPhaseConsensusLoadStrategy(
      LoadTsFileDispatcherImpl dispatcher,
      DataPartitionBatchFetcher partitionFetcher,
      LoadTsFileDataCacheMemoryBlock block,
      LoadConsensusSubmitter consensusSubmitter,
      String userName,
      boolean isGeneratedByPipe) {
    this.dispatcher = dispatcher;
    this.partitionFetcher = partitionFetcher;
    this.block = block;
    this.consensusSubmitter = consensusSubmitter;
    this.userName = userName;
    this.isGeneratedByPipe = isGeneratedByPipe;
  }

  @Override
  public boolean execute(LoadSingleTsFileNode node) {
    this.currentNode = node;
    dispatcher.setUuid(UUID.randomUUID().toString());
    allReplicaSets.clear();
    consensusContexts.clear();
    timePartitionSlotToProgressIndex.clear();

    long startTime = System.nanoTime();
    final boolean isFirstPhaseSuccess;
    try {
      isFirstPhaseSuccess = firstPhase(node);
    } finally {
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.FIRST_PHASE, System.nanoTime() - startTime);
    }

    startTime = System.nanoTime();
    final boolean isSecondPhaseSuccess;
    try {
      isSecondPhaseSuccess = secondPhase(node, isFirstPhaseSuccess);
    } finally {
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.SECOND_PHASE, System.nanoTime() - startTime);
    }

    return isFirstPhaseSuccess && isSecondPhaseSuccess;
  }

  private boolean firstPhase(LoadSingleTsFileNode node) {
    final TsFileSplitConsumer pipeline =
        new TsFileSplitConsumer(
            node,
            block,
            partitionFetcher,
            userName,
            this::computeTimePartitionSlotToProgressIndexIfAbsent,
            this::dispatchOnePieceNode);
    try {
      new TsFileSplitter(node.getTsFileResource().getTsFile(), pipeline)
          .splitTsFileByDataPartition();
      return pipeline.sendAllTsFileData();
    } catch (IllegalStateException e) {
      LOGGER.warn(
          String.format(
              DataNodeQueryMessages.DISPATCH_TSFILEDATA_ERROR_WHEN_PARSING_TSFILE_S,
              node.getTsFileResource().getTsFile()),
          e);
      return false;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              DataNodeQueryMessages.PARSE_OR_SEND_TSFILE_S_ERROR,
              node.getTsFileResource().getTsFile()),
          e);
      return false;
    } finally {
      pipeline.clear();
    }
  }

  private boolean dispatchOnePieceNode(
      LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    allReplicaSets.add(replicaSet);
    return dispatchConsensusPiece(pieceNode, replicaSet);
  }

  /**
   * Submits a LOAD consensus request with a bounded number of attempts. Only transient failures are
   * retried; permanent rejections (checksum mismatch, missing staged writer) are returned to the
   * caller immediately so the scheduler can abort.
   */
  private TSStatus submitConsensusWithRetry(
      TRegionReplicaSet replicaSet, LoadTsFileConsensusNode node) {
    TSStatus status = null;
    for (int attempt = 1; attempt <= LOAD_CONSENSUS_SUBMIT_MAX_RETRIES; attempt++) {
      status = consensusSubmitter.submit(replicaSet, node);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || !isTransientConsensusFailure(status)
          || attempt == LOAD_CONSENSUS_SUBMIT_MAX_RETRIES) {
        break;
      }
      LOGGER.warn(
          DataNodeQueryMessages.LOG_LOAD_CONSENSUS_SUBMIT_TRANSIENT_FAILURE_RETRY_D7E1D9A6,
          node.getOp(),
          node.getLoadId(),
          replicaSet,
          attempt,
          LOAD_CONSENSUS_SUBMIT_MAX_RETRIES,
          status.getMessage());
      try {
        Thread.sleep(LOAD_CONSENSUS_SUBMIT_RETRY_BACKOFF_MS * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return status;
  }

  private boolean isTransientConsensusFailure(TSStatus status) {
    switch (TSStatusCode.representOf(status.getCode())) {
      case DISPATCH_ERROR:
      case INTERNAL_SERVER_ERROR:
      case NO_AVAILABLE_REGION_GROUP:
      case EXECUTE_STATEMENT_ERROR:
        return true;
      default:
        return false;
    }
  }

  private boolean dispatchConsensusPiece(
      LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    final TConsensusGroupId regionId = replicaSet.getRegionId();
    final RegionConsensusContext context =
        consensusContexts.computeIfAbsent(regionId, o -> new RegionConsensusContext());
    final String loadId = context.getLoadId();

    if (!context.isBegun()) {
      context.markBegun();
      final LoadTsFileConsensusNode begin =
          LoadTsFileConsensusNode.begin(
              new PlanNodeId("load-begin-" + loadId),
              loadId,
              pieceNode.getTsFile() == null ? null : pieceNode.getTsFile().getName(),
              currentNode.isTableModel(),
              currentNode.getDatabase(),
              // The total piece count is only known after phase one; PREPARE carries the real
              // count, BEGIN keeps the "unknown" sentinel so the two never disagree.
              -1);
      final TSStatus beginStatus = submitConsensusWithRetry(replicaSet, begin);
      if (beginStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            DataNodeQueryMessages.DISPATCH_ONE_PIECE_TO_REPLICASET_ARG_ERROR_RESULT_STATUS_CODE_ARG
                + DataNodeQueryMessages
                    .RESULT_STATUS_MESSAGE_ARG_DISPATCH_PIECE_NODE_ERROR_PERCENT_NARG,
            replicaSet,
            TSStatusCode.representOf(beginStatus.getCode()).name(),
            beginStatus.getMessage(),
            pieceNode);
        return false;
      }
    }

    final long pieceIndex = context.getPieceCount();
    // The checksum is part of the consensus contract: both the write node and every follower
    // verify it, and the per-piece digest includes the piece index so reordered payloads are
    // detected. Sending a constant here would silently disable checksum validation.
    final long checksum =
        LoadTsFileChecksumUtils.checksum(pieceIndex, pieceNode.getAllTsFileData());
    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.piece(
            new PlanNodeId("load-piece-" + loadId + "-" + pieceIndex),
            loadId,
            pieceNode.getTsFile() == null ? null : pieceNode.getTsFile().getName(),
            pieceIndex,
            0L,
            pieceNode.getAllTsFileData(),
            checksum);
    final TSStatus pieceStatus = submitConsensusWithRetry(replicaSet, piece);
    if (pieceStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          DataNodeQueryMessages.DISPATCH_ONE_PIECE_TO_REPLICASET_ARG_ERROR_RESULT_STATUS_CODE_ARG
              + DataNodeQueryMessages
                  .RESULT_STATUS_MESSAGE_ARG_DISPATCH_PIECE_NODE_ERROR_PERCENT_NARG,
          replicaSet,
          TSStatusCode.representOf(pieceStatus.getCode()).name(),
          pieceStatus.getMessage(),
          pieceNode);
      return false;
    }
    context.accumulate(piece.getDataSize(), piece.getChecksum());
    return true;
  }

  private boolean secondPhase(LoadSingleTsFileNode node, boolean isFirstPhaseSuccess) {
    if (!isFirstPhaseSuccess) {
      return abortAllRegions();
    }
    return prepareAndCommitAllRegions(node);
  }

  private boolean abortAllRegions() {
    for (TRegionReplicaSet replicaSet : allReplicaSets) {
      final String loadId = consensusContexts.get(replicaSet.getRegionId()).getLoadId();
      final LoadTsFileConsensusNode abort =
          LoadTsFileConsensusNode.abort(
              new PlanNodeId("load-abort-" + loadId), loadId, null, isGeneratedByPipe);
      final TSStatus status = consensusSubmitter.submit(replicaSet, abort);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            DataNodeQueryMessages
                    .DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR
                + DataNodeQueryMessages.RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG,
            abort,
            loadId,
            allReplicaSets,
            TSStatusCode.representOf(status.getCode()).name(),
            status.getMessage());
        return false;
      }
    }
    return true;
  }

  private boolean prepareAndCommitAllRegions(LoadSingleTsFileNode node) {
    final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex =
        serializeTimePartitionProgressIndexes();
    for (TRegionReplicaSet replicaSet : allReplicaSets) {
      final RegionConsensusContext context = consensusContexts.get(replicaSet.getRegionId());
      final String loadId = context.getLoadId();
      final LoadTsFileConsensusNode prepare =
          LoadTsFileConsensusNode.prepare(
              new PlanNodeId("load-prepare-" + loadId),
              loadId,
              null,
              (int) context.getPieceCount(),
              context.getTotalBytes(),
              context.getChecksum(),
              timePartition2ProgressIndex);
      final TSStatus prepareStatus = consensusSubmitter.submit(replicaSet, prepare);
      if (prepareStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            DataNodeQueryMessages
                    .DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR
                + DataNodeQueryMessages.RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG,
            prepare,
            loadId,
            allReplicaSets,
            TSStatusCode.representOf(prepareStatus.getCode()).name(),
            prepareStatus.getMessage());
        return false;
      }

      final LoadTsFileConsensusNode commit =
          LoadTsFileConsensusNode.commit(
              new PlanNodeId("load-commit-" + loadId),
              loadId,
              null,
              isGeneratedByPipe,
              node.isDeleteAfterLoad(),
              timePartition2ProgressIndex);
      final TSStatus commitStatus = consensusSubmitter.submit(replicaSet, commit);
      if (commitStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            DataNodeQueryMessages
                    .DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR
                + DataNodeQueryMessages.RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG,
            commit,
            loadId,
            allReplicaSets,
            TSStatusCode.representOf(commitStatus.getCode()).name(),
            commitStatus.getMessage());
        return false;
      }
    }
    return true;
  }

  /**
   * Serializes the per-time-partition {@link ProgressIndex} collected during splitting into the
   * byte form carried by PREPARE/COMMIT, so the receiving side can restore the real progress
   * instead of degrading it to {@code MinimumProgressIndex}.
   */
  private Map<TTimePartitionSlot, byte[]> serializeTimePartitionProgressIndexes() {
    final Map<TTimePartitionSlot, byte[]> result = new HashMap<>();
    for (Map.Entry<TTimePartitionSlot, ProgressIndex> entry :
        timePartitionSlotToProgressIndex.entrySet()) {
      try {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        entry.getValue().serialize(baos);
        result.put(entry.getKey(), baos.toByteArray());
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format(
                StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_PROGRESS_SERIALIZE_FAILED_28EFD091,
                entry.getKey().getStartTime()),
            e);
      }
    }
    return result;
  }

  private void computeTimePartitionSlotToProgressIndexIfAbsent(
      final TTimePartitionSlot timePartitionSlot) {
    timePartitionSlotToProgressIndex.putIfAbsent(
        timePartitionSlot, PipeDataNodeAgent.runtime().getNextProgressIndexForTsFileLoad());
  }
}
