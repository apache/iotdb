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
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;
import org.apache.iotdb.db.storageengine.load.splitter.WholeFileData;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

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

  /** Progress of one file is logged on the first piece, each further 10%, and at least every 5s. */
  static final long LOAD_DISPATCH_PROGRESS_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

  static final int LOAD_DISPATCH_PROGRESS_BUCKETS = 10;

  private static final String PREPARE_COMMIT_PHASE = "prepare-commit";

  private static final String ABORT_PHASE = "abort";

  private final LoadTsFileDispatcherImpl dispatcher;

  /** Resolves the data partitions of the file, reusing the ones the judgement already resolved. */
  private final DataPartitionResolver partitionResolver;

  private final LoadTsFileDataCacheMemoryBlock block;
  private final LoadConsensusSubmitter consensusSubmitter;
  private final boolean isGeneratedByPipe;

  /** When true the source file is copied as whole-file slices instead of being split by chunks. */
  private final boolean wholeFile;

  /** The source file being loaded, kept for the phase-two commands. */
  private LoadSingleTsFileNode currentNode;

  /**
   * The route of every touched region, replaced by a fresher one when a command fails and the route
   * can be looked up again, see {@link #submitWithRetry}. Plain maps: one coordinator thread.
   */
  private final Map<TConsensusGroupId, TRegionReplicaSet> regionRoutes = new LinkedHashMap<>();

  /** The attempts a route is looked up again with before a command gives up on it. */
  private static final int LOAD_CONSENSUS_ROUTE_RESOLVE_MAX_ATTEMPTS = 3;

  /** The load id of every touched region: a fresh one per region, so a task is per region. */
  private final Map<TConsensusGroupId, String> regionLoadIds = new LinkedHashMap<>();

  /** How many pieces were accepted by every touched region. */
  private final Map<TConsensusGroupId, Long> regionPieceCounts = new LinkedHashMap<>();

  /** How many payload bytes those pieces carried. */
  private final Map<TConsensusGroupId, Long> regionTotalBytes = new LinkedHashMap<>();

  /** Where each staged file has to end, over the pieces whose dispatch succeeded. */
  private final Map<TConsensusGroupId, Map<TTimePartitionSlot, Long>> regionExpectedDataEnd =
      new LinkedHashMap<>();

  /**
   * Progress index per time partition, assigned while the file is being split. Kept for the
   * upcoming progress-index sync with the consensus prepare phase.
   */
  private final Map<TTimePartitionSlot, ProgressIndex> timePartitionSlotToProgressIndex =
      new HashMap<>();

  private String sourceFilePath = "";

  private long sourceFileLength;

  private long dispatchedPayloadBytes;

  private long dispatchedPieceCount;

  private int lastLoggedProgressBucket = -1;

  private long lastProgressLogNanos;

  public TwoPhaseConsensusLoadStrategy(
      LoadTsFileDispatcherImpl dispatcher,
      DataPartitionResolver partitionResolver,
      LoadTsFileDataCacheMemoryBlock block,
      LoadConsensusSubmitter consensusSubmitter,
      boolean isGeneratedByPipe) {
    this(dispatcher, partitionResolver, block, consensusSubmitter, isGeneratedByPipe, false);
  }

  public TwoPhaseConsensusLoadStrategy(
      LoadTsFileDispatcherImpl dispatcher,
      DataPartitionResolver partitionResolver,
      LoadTsFileDataCacheMemoryBlock block,
      LoadConsensusSubmitter consensusSubmitter,
      boolean isGeneratedByPipe,
      boolean wholeFile) {
    this.dispatcher = dispatcher;
    this.partitionResolver = partitionResolver;
    this.block = block;
    this.consensusSubmitter = consensusSubmitter;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.wholeFile = wholeFile;
  }

  @Override
  public boolean execute(LoadSingleTsFileNode node) {
    this.currentNode = node;
    dispatcher.setUuid(UUID.randomUUID().toString());
    regionRoutes.clear();
    regionLoadIds.clear();
    regionPieceCounts.clear();
    regionTotalBytes.clear();
    regionExpectedDataEnd.clear();
    timePartitionSlotToProgressIndex.clear();
    dispatchedPayloadBytes = 0L;
    dispatchedPieceCount = 0L;
    lastLoggedProgressBucket = -1;
    lastProgressLogNanos = 0L;
    final File sourceFile = node.getTsFileResource().getTsFile();
    sourceFilePath = sourceFile.getAbsolutePath();
    sourceFileLength = sourceFile.length();

    logLoadProgress(
        DataNodeQueryMessages.LOG_LOAD_TSFILE_ARG_PHASE_DISPATCH_START_FILESIZE_ARG_D7E5D5F4,
        sourceFilePath,
        sourceFileLength);

    long startTime = System.nanoTime();
    final boolean isFirstPhaseSuccess;
    try {
      isFirstPhaseSuccess = firstPhase(node);
    } finally {
      final long elapsedNanos = System.nanoTime() - startTime;
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.FIRST_PHASE, elapsedNanos);
      logLoadProgress(
          DataNodeQueryMessages
              .LOG_LOAD_TSFILE_ARG_PHASE_DISPATCH_END_PIECES_ARG_BYTES_ARG_ELAPSEDMS_ARG_DB1D69B6,
          sourceFilePath,
          dispatchedPieceCount,
          dispatchedPayloadBytes,
          TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
    }

    final String secondPhaseName = isFirstPhaseSuccess ? PREPARE_COMMIT_PHASE : ABORT_PHASE;
    logLoadProgress(
        DataNodeQueryMessages.LOG_LOAD_TSFILE_ARG_PHASE_ARG_START_B225907D,
        sourceFilePath,
        secondPhaseName);
    startTime = System.nanoTime();
    boolean isSecondPhaseSuccess = false;
    try {
      isSecondPhaseSuccess = secondPhase(isFirstPhaseSuccess);
      return isFirstPhaseSuccess && isSecondPhaseSuccess;
    } finally {
      final long elapsedNanos = System.nanoTime() - startTime;
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.SECOND_PHASE, elapsedNanos);
      logLoadProgress(
          DataNodeQueryMessages
              .LOG_LOAD_TSFILE_ARG_PHASE_ARG_END_SUCCESS_ARG_ELAPSEDMS_ARG_6126B0D0,
          sourceFilePath,
          secondPhaseName,
          isSecondPhaseSuccess,
          TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
    }
  }

  private void logDispatchProgress(final long pieceBytes) {
    dispatchedPayloadBytes += pieceBytes;
    if (!isLoadProgressLogEnabled()) {
      return;
    }
    final long nowNanos = System.nanoTime();
    if (!shouldLogDispatchProgress(
        sourceFileLength,
        dispatchedPayloadBytes,
        lastLoggedProgressBucket,
        lastProgressLogNanos,
        nowNanos)) {
      return;
    }
    lastLoggedProgressBucket = progressBucket(sourceFileLength, dispatchedPayloadBytes);
    lastProgressLogNanos = nowNanos;
    logLoadProgress(
        DataNodeQueryMessages
            .LOG_LOAD_TSFILE_ARG_PHASE_DISPATCH_DISPATCHEDBYTES_ARG_FILESIZE_ARG_PROGRESS_ARG_668540F9,
        sourceFilePath,
        dispatchedPayloadBytes,
        sourceFileLength,
        progressPercent(dispatchedPayloadBytes, sourceFileLength));
  }

  private void logLoadProgress(final String message, final Object... args) {
    if (!LOGGER.isDebugEnabled()) {
      return;
    }
    LOGGER.debug(message, args);
  }

  private boolean isLoadProgressLogEnabled() {
    return LOGGER.isDebugEnabled();
  }

  static boolean shouldLogDispatchProgress(
      final long fileSize,
      final long dispatchedBytes,
      final int lastBucket,
      final long lastLogNanos,
      final long nowNanos) {
    if (lastLogNanos == 0L) {
      return true;
    }
    if (nowNanos - lastLogNanos >= LOAD_DISPATCH_PROGRESS_LOG_INTERVAL_NANOS) {
      return true;
    }
    return progressBucket(fileSize, dispatchedBytes) > lastBucket;
  }

  static int progressBucket(final long fileSize, final long dispatchedBytes) {
    if (fileSize <= 0L || dispatchedBytes <= 0L) {
      return 0;
    }
    if (dispatchedBytes >= fileSize) {
      return LOAD_DISPATCH_PROGRESS_BUCKETS;
    }
    return (int) (dispatchedBytes * LOAD_DISPATCH_PROGRESS_BUCKETS / fileSize);
  }

  private static String progressPercent(final long dispatchedBytes, final long fileSize) {
    final double percent = fileSize <= 0L ? 0.0 : dispatchedBytes * 100.0 / fileSize;
    return String.format("%.3f", percent);
  }

  private boolean firstPhase(LoadSingleTsFileNode node) {
    if (wholeFile) {
      return dispatchWholeFile(node);
    }
    final TsFileSplitConsumer pipeline =
        new TsFileSplitConsumer(
            node,
            block,
            partitionResolver,
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

  /** How many source bytes one whole-file PIECE carries. Kept well below the RPC frame limit. */
  private static final int WHOLE_FILE_SLICE_SIZE = 4 * 1024 * 1024;

  /**
   * Copies one source file through the consensus LOAD transaction: every slice is a PIECE, so the
   * consensus layer replicates it and PREPARE/COMMIT still decide the transaction atomically.
   */
  private boolean dispatchWholeFile(final LoadSingleTsFileNode node) {
    final TRegionReplicaSet replicaSet = node.getLocalRegionReplicaSet();
    final TTimePartitionSlot timePartitionSlot = node.getWholeFileTimePartitionSlot();
    final File sourceFile = node.getTsFileResource().getTsFile();
    if (replicaSet == null || timePartitionSlot == null || sourceFile == null) {
      LOGGER.warn(
          String.format(
              DataNodeQueryMessages.PARSE_OR_SEND_TSFILE_S_ERROR,
              sourceFile == null ? node.getTsFileResource().getTsFilePath() : sourceFile),
          new IllegalStateException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_WHOLE_FILE_LOAD_HAS_NO_SINGLE_RESOLVED_REGION_FOR_S_DAC09CDC,
                  node.getTsFileResource().getTsFilePath())));
      return false;
    }

    final TConsensusGroupId regionId = replicaSet.getRegionId();
    final String loadId =
        regionLoadIds.computeIfAbsent(regionId, ignored -> UUID.randomUUID().toString());
    regionRoutes.putIfAbsent(regionId, replicaSet);
    computeTimePartitionSlotToProgressIndexIfAbsent(timePartitionSlot);

    final long sourceLength = sourceFile.length();
    if (sourceLength <= 0L) {
      LOGGER.warn(
          String.format(
              DataNodeQueryMessages.PARSE_OR_SEND_TSFILE_S_ERROR, sourceFile.getAbsolutePath()));
      return false;
    }
    regionExpectedDataEnd
        .computeIfAbsent(regionId, ignored -> new HashMap<>())
        .put(timePartitionSlot, sourceLength);

    if (!dispatchWholeFileKind(
        sourceFile, WholeFileData.Kind.TSFILE, sourceLength, timePartitionSlot, replicaSet, node)) {
      return false;
    }
    return dispatchWholeFileMods(sourceFile, timePartitionSlot, replicaSet, node);
  }

  private boolean dispatchWholeFileMods(
      final File sourceFile,
      final TTimePartitionSlot timePartitionSlot,
      final TRegionReplicaSet replicaSet,
      final LoadSingleTsFileNode node) {
    File modsFile = ModificationFile.getExclusiveMods(sourceFile);
    WholeFileData.Kind kind = WholeFileData.Kind.MODS_V2;
    if (!modsFile.isFile()) {
      modsFile = ModificationFileV1.getNormalMods(sourceFile);
      kind = WholeFileData.Kind.MODS_V1;
    }
    if (!modsFile.isFile()) {
      return true;
    }
    return dispatchWholeFileKind(
        modsFile, kind, modsFile.length(), timePartitionSlot, replicaSet, node);
  }

  private boolean dispatchWholeFileKind(
      final File file,
      final WholeFileData.Kind kind,
      final long sourceLength,
      final TTimePartitionSlot timePartitionSlot,
      final TRegionReplicaSet replicaSet,
      final LoadSingleTsFileNode node) {
    long offset = 0L;
    long pieceIndex = regionPieceCounts.getOrDefault(replicaSet.getRegionId(), 0L);
    try (final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      while (offset < sourceLength) {
        final int expected = (int) Math.min(WHOLE_FILE_SLICE_SIZE, sourceLength - offset);
        final ByteBuffer buffer = ByteBuffer.allocate(expected);
        int read = 0;
        while (read < expected) {
          final int current = channel.read(buffer, offset + read);
          if (current < 0) {
            break;
          }
          read += current;
        }
        if (read <= 0) {
          LOGGER.warn(
              String.format(
                  DataNodeQueryMessages.PARSE_OR_SEND_TSFILE_S_ERROR, file.getAbsolutePath()));
          return false;
        }
        final byte[] payload = Arrays.copyOf(buffer.array(), read);
        final LoadTsFilePieceNode pieceNode =
            LoadTsFilePieceNode.wholeFilePiece(node.getPlanNodeId(), file, pieceIndex++);
        pieceNode.addTsFileData(
            new WholeFileData(
                kind,
                offset,
                sourceLength,
                timePartitionSlot.getStartTime(),
                isGeneratedByPipe,
                crc32c(payload),
                payload));
        if (!dispatchOnePieceNode(pieceNode, replicaSet)) {
          return false;
        }
        offset += read;
      }
    } catch (final IOException e) {
      LOGGER.warn(
          String.format(DataNodeQueryMessages.PARSE_OR_SEND_TSFILE_S_ERROR, file.getAbsolutePath()),
          e);
      return false;
    }
    return offset == sourceLength;
  }

  private static long crc32c(final byte[] payload) {
    final CRC32C crc32c = new CRC32C();
    crc32c.update(payload, 0, payload.length);
    return crc32c.getValue();
  }

  private boolean dispatchOnePieceNode(
      LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    // The first piece of a region pins the route the splitter resolved; a route that a re-resolve
    // replaced later is followed by every command of the transaction from then on.
    final TRegionReplicaSet pinned = regionRoutes.putIfAbsent(replicaSet.getRegionId(), replicaSet);
    return dispatchConsensusPiece(pieceNode, pinned == null ? replicaSet : pinned);
  }

  /**
   * Submits a LOAD piece with a bounded number of attempts. Only transient failures are retried;
   * permanent rejections are returned to the caller immediately so the scheduler can abort.
   */
  private TSStatus submitConsensusWithRetry(
      final TConsensusGroupId regionId, final LoadTsFileConsensusNode node) {
    return submitWithRetry(regionId, node, false);
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
      final TConsensusGroupId regionId, final LoadTsFileConsensusNode node) {
    return submitWithRetry(regionId, node, node.getOp() == LoadTsFileConsensusOp.ABORT);
  }

  /**
   * Submits a command to its region with a bounded number of attempts. Only a failure that may mean
   * the route is stale makes the route be looked up again, with the local cache dropped first.
   */
  private TSStatus submitWithRetry(
      final TConsensusGroupId regionId,
      final LoadTsFileConsensusNode node,
      final boolean retryEveryFailure) {
    TRegionReplicaSet replicaSet = currentRoute(regionId);
    TSStatus status = consensusSubmitter.submit(replicaSet, node);
    for (int attempt = 1; attempt < LOAD_CONSENSUS_SUBMIT_MAX_RETRIES; attempt++) {
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          || (!retryEveryFailure && !isTransientConsensusFailure(status))) {
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
      if (isRouteRelatedFailure(status)) {
        // The route may be why the command did not get through: look it up again, or end the
        // command.
        final TRegionReplicaSet resolved =
            consensusSubmitter.resolveRoute(regionId, LOAD_CONSENSUS_ROUTE_RESOLVE_MAX_ATTEMPTS);
        if (resolved == null) {
          LOGGER.warn(
              DataNodeQueryMessages
                  .LOG_THE_ROUTE_OF_REGION_ARG_CANNOT_BE_LOOKED_UP_AGAIN_THE_COMMAND_ARG_OF_THE_LOAD_TASK_ARG_IS_NOT_REPEATED_ON_THE_ROUTE_IT_FAILED_ON_02FFB468,
              regionId,
              node.getOp(),
              node.getLoadId());
          break;
        }
        if (!resolved.equals(replicaSet)) {
          LOGGER.info(
              DataNodeQueryMessages
                  .LOG_THE_ROUTE_OF_REGION_ARG_CHANGED_FROM_ARG_TO_ARG_WHILE_THE_TASK_IS_BEING_LOADED_THE_TASK_FOLLOWS_IT_6182715B,
              regionId,
              replicaSet,
              resolved);
          replicaSet = resolved;
          regionRoutes.put(regionId, resolved);
        }
      }
      try {
        Thread.sleep(LOAD_CONSENSUS_SUBMIT_RETRY_BACKOFF_MS * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      status = consensusSubmitter.submit(replicaSet, node);
    }
    return status;
  }

  /** Whether a failure says something about the route, and a fresh lookup is worth its cost. */
  private boolean isRouteRelatedFailure(final TSStatus status) {
    switch (TSStatusCode.representOf(status.getCode())) {
      case DISPATCH_ERROR:
      case NO_AVAILABLE_REGION_GROUP:
        return true;
      default:
        return false;
    }
  }

  /** The route of a region in this transaction, pinned to what the splitter resolved at first. */
  private TRegionReplicaSet currentRoute(final TConsensusGroupId regionId) {
    final TRegionReplicaSet route = regionRoutes.get(regionId);
    if (route != null) {
      return route;
    }
    // A command that has no route yet cannot be sent anywhere: the caller held no region for it.
    throw new IllegalStateException(
        String.format(
            DataNodeQueryMessages
                .EXCEPTION_NO_ROUTE_IS_KNOWN_FOR_REGION_ARG_OF_THE_LOAD_TASK_BC8F698F,
            regionId));
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
    // Debug: one line per piece, and a large TsFile has thousands of them.
    LOGGER.debug(
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
    final TSStatus pieceStatus = submitConsensusWithRetry(regionId, piece);
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
    LOGGER.debug(
        DataNodeQueryMessages
            .LOG_DISPATCH_LOAD_PIECE_SUCCESS_LOADID_ARG_REGIONID_ARG_PIECEINDEX_ARG_DATASIZE_ARG_75F7AEFE,
        loadId,
        regionId,
        pieceIndex,
        piece.getDataSize());
    regionPieceCounts.merge(regionId, 1L, Long::sum);
    regionTotalBytes.merge(regionId, piece.getDataSize(), Long::sum);
    dispatchedPieceCount++;
    logDispatchProgress(piece.getDataSize());
    final Map<TTimePartitionSlot, Long> expectedDataEnd =
        regionExpectedDataEnd.computeIfAbsent(regionId, id -> new HashMap<>());
    for (final TsFileData data : pieceNode.getAllTsFileData()) {
      if (data instanceof ChunkData chunkData && chunkData.getChunkLayout() != null) {
        // The chunk group occupies [offset, offset + length) of the staged file of its partition.
        expectedDataEnd.merge(
            chunkData.getTimePartitionSlot(),
            chunkData.getChunkLayout().offset() + chunkData.getChunkLayout().length(),
            Math::max);
      }
    }
    return true;
  }

  private boolean secondPhase(boolean isFirstPhaseSuccess) {
    if (!isFirstPhaseSuccess) {
      return abortAllRegions();
    }
    return prepareAndCommitAllRegions();
  }

  private boolean abortAllRegions() {
    return abortRegions();
  }

  /**
   * Drops the staged data of the given regions. Every failure that happens before the commit
   * decision goes through here, so a load that cannot be committed leaves no staged pieces behind:
   * they would otherwise keep disk space of a task nobody ever finishes, and a later replay of the
   * same pieces would find a half-filled file.
   */
  private boolean abortRegions() {
    boolean allAborted = true;
    for (TRegionReplicaSet replicaSet : regionRoutes.values()) {
      final String loadId = regionLoadIds.get(replicaSet.getRegionId());
      final LoadTsFileConsensusNode abort =
          LoadTsFileConsensusNode.abort(
              new PlanNodeId("load-abort-" + loadId),
              loadId,
              isGeneratedByPipe,
              regionExpectedDataEnd.getOrDefault(replicaSet.getRegionId(), Collections.emptyMap()));
      final TSStatus status = submitTerminalCommandWithRetry(replicaSet.getRegionId(), abort);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            DataNodeQueryMessages
                    .DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR
                + DataNodeQueryMessages.RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG,
            abort,
            loadId,
            regionRoutes.values(),
            TSStatusCode.representOf(status.getCode()).name(),
            status.getMessage());
        allAborted = false;
      }
    }
    return allAborted;
  }

  private boolean prepareAndCommitAllRegions() {
    final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex =
        serializeTimePartitionProgressIndexes();
    // Prepare first, then commit once all agreed: no region imports a file another refused.
    if (!prepareAllRegions(timePartition2ProgressIndex)) {
      // Nothing was committed anywhere yet, so every touched region drops what it staged.
      abortAllRegions();
      return false;
    }
    return commitAllRegions(timePartition2ProgressIndex);
  }

  /** The prepare round: every touched region seals its staged data without importing anything. */
  private boolean prepareAllRegions(
      final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    for (TRegionReplicaSet replicaSet : regionRoutes.values()) {
      final TConsensusGroupId regionId = replicaSet.getRegionId();
      final String loadId = regionLoadIds.get(regionId);
      final LoadTsFileConsensusNode prepare =
          LoadTsFileConsensusNode.prepare(
              new PlanNodeId("load-prepare-" + loadId),
              loadId,
              Math.toIntExact(regionPieceCounts.getOrDefault(regionId, 0L)),
              regionTotalBytes.getOrDefault(regionId, 0L),
              isGeneratedByPipe,
              timePartition2ProgressIndex,
              regionExpectedDataEnd.getOrDefault(regionId, Collections.emptyMap()));
      final TSStatus prepareStatus = submitConsensusWithRetry(regionId, prepare);
      if (prepareStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            DataNodeQueryMessages
                    .DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR
                + DataNodeQueryMessages.RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG,
            prepare,
            loadId,
            regionRoutes.values(),
            TSStatusCode.representOf(prepareStatus.getCode()).name(),
            prepareStatus.getMessage());
        return false;
      }
    }
    return true;
  }

  /**
   * The commit round: every touched region agreed to commit, so the decision can no longer be taken
   * back. A region whose commit failed is therefore not rolled back on the data it already
   * imported, and the regions behind it are still committed, so the transaction reaches as many of
   * its participants as it can.
   *
   * <p>A transient failure is retried, and the retry settles what the first attempt left open: the
   * region answers a COMMIT of a task it already imported with success, see {@code
   * LoadTsFileManager#loadAll}, so a command whose answer was lost is not reported as a failure
   * while a command that truly failed still is. A region that fails for another reason keeps its
   * answer: it is left as it is, which the load reports.
   *
   * <p>Whatever the region answered, its task must not be left half decided, because nothing else
   * ever revisits it: the caller reports the load as failed and moves on, and the coordinator has
   * no record to retry from. The region is therefore sent the terminal ABORT of the task as well.
   * An ABORT is the idempotent side of a decision that may or may not have been applied - it drops
   * the staged directory of a task whose COMMIT never landed, and it is acknowledged as success for
   * a task that already reached a terminal record, see {@code LoadTsFileManager#deleteAll} - so the
   * region ends the load in exactly one of the two states it can be in, instead of keeping staged
   * data that only a scan of the staging directories could reclaim.
   */
  private boolean commitAllRegions(
      final Map<TTimePartitionSlot, byte[]> timePartition2ProgressIndex) {
    boolean allCommitted = true;
    for (TRegionReplicaSet replicaSet : regionRoutes.values()) {
      final TConsensusGroupId regionId = replicaSet.getRegionId();
      final String loadId = regionLoadIds.get(regionId);
      final LoadTsFileConsensusNode commit =
          LoadTsFileConsensusNode.commit(
              new PlanNodeId("load-commit-" + loadId),
              loadId,
              isGeneratedByPipe,
              timePartition2ProgressIndex,
              regionExpectedDataEnd.getOrDefault(regionId, Collections.emptyMap()));
      final TSStatus commitStatus = submitTerminalCommandWithRetry(regionId, commit);
      if (commitStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            DataNodeQueryMessages
                    .DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR
                + DataNodeQueryMessages.RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG,
            commit,
            loadId,
            regionRoutes.values(),
            TSStatusCode.representOf(commitStatus.getCode()).name(),
            commitStatus.getMessage());
        allCommitted = false;
        resolveTheCommitOutcomeOf(regionId, loadId);
      }
    }
    return allCommitted;
  }

  /**
   * Ends a task whose COMMIT was not acknowledged, so the region does not keep a staged directory
   * that no later command refers to. The ABORT is idempotent with respect to the COMMIT: a region
   * that did commit answers it with success as well, see {@code LoadTsFileManager#deleteAll}.
   */
  private void resolveTheCommitOutcomeOf(final TConsensusGroupId regionId, final String loadId) {
    final LoadTsFileConsensusNode abort =
        LoadTsFileConsensusNode.abort(
            new PlanNodeId("load-abort-unresolved-commit-" + loadId),
            loadId,
            isGeneratedByPipe,
            regionExpectedDataEnd.getOrDefault(regionId, Collections.emptyMap()));
    final TSStatus abortStatus = submitTerminalCommandWithRetry(regionId, abort);
    if (abortStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          DataNodeQueryMessages
              .LOG_THE_COMMIT_OUTCOME_OF_LOAD_TASK_ARG_IN_REGION_ARG_COULD_NOT_BE_RESOLVED_ARG_89B6F261,
          loadId,
          regionId,
          abortStatus.getMessage());
    }
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
