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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
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
 * TsFileSplitConsumer}. Every dispatched piece goes through {@code dispatchConsensusPiece}: each
 * piece is submitted as PIECE directly with a fresh per-region load id and a monotonically
 * increasing {@code pieceIndex}. Submission goes through {@link LoadConsensusSubmitter} with
 * bounded retries for transient failures only.
 *
 * <p><b>Phase 2 (commit or abort).</b> If every region received all its pieces, the two rounds of
 * the commit protocol run: every touched region is asked to PREPARE first (with the accumulated
 * count/bytes), and only once every region agreed is COMMIT sent to all of them. A region that
 * refuses to prepare, or a first phase that did not deliver every piece, turns the transaction into
 * an ABORT for every touched region, so no region keeps an imported file of a transaction another
 * region refused.
 *
 * <p>Per-file state tracks the touched regions, their load ids, piece counters and pipe progress
 * indexes.
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

  /** The source file being loaded, kept for the phase-two commands. */
  private LoadSingleTsFileNode currentNode;

  /** Regions touched by the current file; used to send ABORT/PREPARE+COMMIT in phase two. */
  private final Set<TRegionReplicaSet> allReplicaSets = new HashSet<>();

  private final Map<TConsensusGroupId, String> regionLoadIds = new ConcurrentHashMap<>();
  private final Map<TConsensusGroupId, Long> regionPieceCounts = new ConcurrentHashMap<>();
  private final Map<TConsensusGroupId, Long> regionTotalBytes = new ConcurrentHashMap<>();

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
    regionLoadIds.clear();
    regionPieceCounts.clear();
    regionTotalBytes.clear();
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
   * Submits a LOAD piece with a bounded number of attempts. Only transient failures are retried;
   * permanent rejections are returned to the caller immediately so the scheduler can abort.
   */
  private TSStatus submitConsensusWithRetry(
      TRegionReplicaSet replicaSet, LoadTsFileConsensusNode node) {
    return submitWithRetry(replicaSet, node, false);
  }

  /**
   * Submits a terminal command of the commit protocol (COMMIT or ABORT) with the same bounded
   * number of attempts.
   *
   * <p>A terminal command is the last thing the coordinator sends about a task, so a submission
   * that failed leaves the region with staged data that only a scan of the staging directories
   * could reclaim: it is repeated instead of being given up after one attempt. An ABORT is repeated
   * for every kind of failure, because dropping staged data is idempotent - an ABORT of a task this
   * region no longer holds is acknowledged as success. A COMMIT is repeated for transient failures
   * only: a permanent rejection means either that the task was already imported, in which case a
   * repetition is refused again, or that the import failed half way, and repeating that one could
   * import a partition of the staged file twice.
   */
  private TSStatus submitTerminalCommandWithRetry(
      final TRegionReplicaSet replicaSet, final LoadTsFileConsensusNode node) {
    return submitWithRetry(replicaSet, node, node.getOp() == LoadTsFileConsensusOp.ABORT);
  }

  private TSStatus submitWithRetry(
      final TRegionReplicaSet replicaSet,
      final LoadTsFileConsensusNode node,
      final boolean retryEveryFailure) {
    TSStatus status = null;
    for (int attempt = 1; attempt <= LOAD_CONSENSUS_SUBMIT_MAX_RETRIES; attempt++) {
      status = consensusSubmitter.submit(replicaSet, node);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || (!retryEveryFailure && !isTransientConsensusFailure(status))
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
    final String loadId =
        regionLoadIds.computeIfAbsent(regionId, ignored -> UUID.randomUUID().toString());

    final long pieceIndex = pieceNode.getPieceIndex();
    LOGGER.info(
        DataNodeQueryMessages
            .LOG_DISPATCH_LOAD_PIECE_LOADID_ARG_REGIONID_ARG_PIECEINDEX_ARG_TSFILE_ARG_DATASIZE_ARG_REPLICASET_ARG_D9C87EB7,
        loadId,
        regionId,
        pieceIndex,
        pieceNode.getTsFile() == null ? null : pieceNode.getTsFile().getName(),
        pieceNode.getDataSize(),
        replicaSet);
    final LoadTsFileConsensusNode piece =
        LoadTsFileConsensusNode.piece(
            new PlanNodeId("load-piece-" + loadId + "-" + pieceIndex),
            loadId,
            pieceNode.getTsFile() == null ? null : pieceNode.getTsFile().getName(),
            pieceIndex,
            pieceNode.getAllTsFileData());
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
    LOGGER.info(
        DataNodeQueryMessages
            .LOG_DISPATCH_LOAD_PIECE_SUCCESS_LOADID_ARG_REGIONID_ARG_PIECEINDEX_ARG_DATASIZE_ARG_75F7AEFE,
        loadId,
        regionId,
        pieceIndex,
        piece.getDataSize());
    regionPieceCounts.merge(regionId, 1L, Long::sum);
    regionTotalBytes.merge(regionId, piece.getDataSize(), Long::sum);
    return true;
  }

  private boolean secondPhase(LoadSingleTsFileNode node, boolean isFirstPhaseSuccess) {
    if (!isFirstPhaseSuccess) {
      return abortAllRegions();
    }
    return prepareAndCommitAllRegions(node);
  }

  private boolean abortAllRegions() {
    return abortRegions(allReplicaSets);
  }

  /**
   * Drops the staged data of the given regions. Every failure that happens before the commit
   * decision goes through here, so a load that cannot be committed leaves no staged pieces behind:
   * they would otherwise keep disk space of a task nobody ever finishes, and a later replay of the
   * same pieces would find a half-filled file.
   */
  private boolean abortRegions(final Set<TRegionReplicaSet> replicaSets) {
    boolean allAborted = true;
    for (TRegionReplicaSet replicaSet : replicaSets) {
      final String loadId = regionLoadIds.get(replicaSet.getRegionId());
      final LoadTsFileConsensusNode abort =
          LoadTsFileConsensusNode.abort(
              new PlanNodeId("load-abort-" + loadId), loadId, null, isGeneratedByPipe);
      final TSStatus status = submitTerminalCommandWithRetry(replicaSet, abort);
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
        allAborted = false;
      }
    }
    return allAborted;
  }

  private boolean prepareAndCommitAllRegions(LoadSingleTsFileNode node) {
    final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex =
        serializeTimePartitionProgressIndexes();
    // The two rounds of the commit protocol: every touched region is asked to prepare first, and
    // the commit is sent only once every one of them agreed to it. No region may end up having
    // imported a file of a transaction that another region refused.
    if (!prepareAllRegions(timePartition2ProgressIndex)) {
      // Nothing was committed anywhere yet, so every touched region drops what it staged.
      abortAllRegions();
      return false;
    }
    return commitAllRegions(node, timePartition2ProgressIndex);
  }

  /** The prepare round: every touched region seals its staged data without importing anything. */
  private boolean prepareAllRegions(
      final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    for (TRegionReplicaSet replicaSet : allReplicaSets) {
      final TConsensusGroupId regionId = replicaSet.getRegionId();
      final String loadId = regionLoadIds.get(regionId);
      final LoadTsFileConsensusNode prepare =
          LoadTsFileConsensusNode.prepare(
              new PlanNodeId("load-prepare-" + loadId),
              loadId,
              null,
              Math.toIntExact(regionPieceCounts.getOrDefault(regionId, 0L)),
              regionTotalBytes.getOrDefault(regionId, 0L),
              0L,
              isGeneratedByPipe,
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
    }
    return true;
  }

  /**
   * The commit round: every touched region agreed to commit, so the decision can no longer be taken
   * back. A region whose commit failed is therefore not rolled back either - it may have imported
   * its files before the failure was reported - and the regions behind it are still committed, so
   * the transaction reaches as many of its participants as it can.
   *
   * <p>A transient failure is retried, and the retry settles what the first attempt left open: the
   * region answers a COMMIT of a task it already imported with success, see {@code
   * LoadTsFileManager#loadAll}, so a command whose answer was lost is not reported as a failure
   * while a command that truly failed still is. A region that fails for another reason keeps its
   * answer: it is left as it is, which the load reports.
   */
  private boolean commitAllRegions(
      LoadSingleTsFileNode node,
      final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    boolean allCommitted = true;
    for (TRegionReplicaSet replicaSet : allReplicaSets) {
      final TConsensusGroupId regionId = replicaSet.getRegionId();
      final String loadId = regionLoadIds.get(regionId);
      final LoadTsFileConsensusNode commit =
          LoadTsFileConsensusNode.commit(
              new PlanNodeId("load-commit-" + loadId),
              loadId,
              null,
              isGeneratedByPipe,
              node.isDeleteAfterLoad(),
              timePartition2ProgressIndex);
      final TSStatus commitStatus = submitTerminalCommandWithRetry(replicaSet, commit);
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
        allCommitted = false;
      }
    }
    return allCommitted;
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
