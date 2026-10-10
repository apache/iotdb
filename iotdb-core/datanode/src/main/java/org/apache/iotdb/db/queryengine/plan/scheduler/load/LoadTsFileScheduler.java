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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.service.RegionMigrateService;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LOAD scheduler: {@link LoadTsFileScheduler} is the coordinator of a batch of {@link
 * LoadSingleTsFileNode} loads (one node per source TsFile). It owns only the lifecycle - concurrent
 * file lock, per-node guard clauses, state-machine transitions and the failure fallback - and
 * routes every file to a {@link TsFileLoadStrategy}. All per-region bookkeeping, memory management
 * and consensus submission live in the strategy components.
 *
 * <h2>Architecture overview</h2>
 *
 * <p>End-to-end picture of one LOAD batch: the coordinator slices every source TsFile and streams
 * the resulting pieces to the region write peers (phase 1), and only then decides the batch with
 * one PREPARE round followed by COMMIT or ABORT (phase 2). Both phases are detailed in the sections
 * below.
 *
 * <pre>{@code
 * +------------------------------------------------------------------------------------+
 * |                    COORDINATOR (the node that starts the LOAD)                     |
 * |                                                                                    |
 * |  +--------------------+ ----> +------------------+ ----> +----------------------+  |
 * |  |LoadSingleTsFileNode| ----> |  TsFileSplitter  | ----> | TsFileSplitConsumer  |  |
 * |  |  (source TsFile)   | ----> |(physical slicing)| ----> |   (memory budget /   |  |
 * |  |                    | ----> |                  | ----> |router / aggregation) |  |
 * |  +--------------------+ ----> +------------------+ ----> +----------------------+  |
 * |                                                                                    |
 * +------------------------------------------------------------------------------------+
 *                                           |
 *                     [ in-memory buffering & routing (DataPartitionRouter) ]
 *                                           v
 *                                +------------------------+
 *                                |    PieceDispatcher     |
 *                                |   (group chunks into   |
 *                                |   pieces per Region)   |
 *                                +------------------------+
 *                                           |
 * ==========================================|===========================================
 *   [ PHASE 1: STREAMING PIECE DISPATCH (PIECE RPCs) ]
 * ==========================================|===========================================
 *                       |                                       |
 *                       +---------------------------------------+---------+
 *                       | (RPC: PIECE + retry)                   | (RPC: PIECE + retry)
 *                       v                                       v
 *      +----------------------------------+    +----------------------------------+
 *      |       Target DataRegion 1        |    |       Target DataRegion 2        |
 *      |  +--------------------------+  |    |  +--------------------------+  |
 *      |  |   TsFileWriterManager    |  |    |  |   TsFileWriterManager    |  |
 *      |  |  - PrecalculatedWriter   |  |    |  |  - PrecalculatedWriter   |  |
 *      |  |   - LoadTsFileProgress   |  |    |  |   - LoadTsFileProgress   |  |
 *      |  | (.progress ledger marks) |  |    |  | (.progress ledger marks) |  |
 *      |  +--------------------------+  |    |  +--------------------------+  |
 *      +----------------------------------+    +----------------------------------+
 *                       |                                       |
 * ======================|=======================================|=======================
 *   [ PHASE 2: TWO-PHASE CONSENSUS DECISION: PREPARE -> COMMIT / ABORT ]
 * ======================|=======================================|=======================
 *                       |                                       |
 *   1. broadcast PREPARE: every touched region verifies that its piece
 *      sequence has no hole, then seals its staged TsFile
 *                       |                                       |
 *   2. [decide] every touched region reported SUCCESS?
 *         |
 *         +----------- YES -------------+----------- NO --------------+
 *         |                             |                             |
 *         v                             v                             v
 *    [ COMMIT ]                    [ ABORT ]                     [ ABORT ]
 *  (RPC: COMMIT: write        (RPC: ABORT: region 1         (RPC: ABORT: region 2
 *   the real partitions)      discards staged data)         discards staged data)
 * }</pre>
 *
 * <h2>Overall structure</h2>
 *
 * <p>All components below are part of the LOAD pipeline (LOAD TSFILE): the scheduler, the load
 * strategies and every routing/buffering/dispatching helper are LOAD-only classes under {@code
 * org.apache.iotdb.db.queryengine.plan.scheduler.load}.
 *
 * <pre>{@code
 * LoadTsFileScheduler.start()
 *     |
 *     +--> per LoadSingleTsFileNode
 *     |       lock file -> empty? -> strategy -> migration check
 *     |
 *     +--> needDecodeTsFile?
 *     |       |-- false -> LocalLoadStrategy
 *     |       |            `-- FragmentInstance -> local region (no decode)
 *     |       `-- true  -> TwoPhaseConsensusLoadStrategy
 *     |                     phase1: TsFileSplitConsumer
 *     |                       DataPartitionRouter -> MemoryBoundedBuffer
 *     |                       -> PieceDispatcher
 *     |                     phase2: BEGIN -> PIECE* -> PREPARE -> COMMIT
 *     |                       or ABORT, via per-region state +
 *     |                       LoadConsensusSubmitter
 *     |
 *     +--> success -> register pending deletion (the source file is kept
 *     |               until the whole LOAD batch ends) + log
 *     |       failure -> record failed index
 *     |
 *     `--> all success -> FINISHED
 *             else -> LoadFallbackHandler (convert to tablets, retry)
 *                      -> FINISHED / FAILED
 *
 *     finally -> deletePendingDeletionFiles()
 * }</pre>
 *
 * <h2>Consensus pipeline (phase 1)</h2>
 *
 * <pre>{@code
 * TsFileSplitter
 *      |
 *      | TsFileData (CHUNK / DELETION)
 *      v
 * TsFileSplitConsumer
 *      |
 *      +-- CHUNK:  buffer -> DataPartitionRouter -> per-region piece
 *      |           over budget? -> PieceDispatcher: dispatch largest first
 *      +-- end of file: flush remaining pieces
 *      |
 *      v
 * PieceDispatcher
 *      | dispatch callback
 *      v
 * TwoPhaseConsensusLoadStrategy.dispatchConsensusPiece
 *      |
 *      +-- first piece of a region: BEGIN(loadId) then PIECE(0)
 *      +-- later pieces:            PIECE(1), PIECE(2), ...
 *      |
 *      v
 * Accumulate per-region piece count and bytes
 *      |
 *      v
 * LoadConsensusSubmitter (submit to the partition write node; bounded retry)
 * }</pre>
 *
 * <h2>Two-phase protocol timeline</h2>
 *
 * <pre>{@code
 * coordinator                          region write peer
 *      |                                      |
 *      |---- PIECE(0, chunks) --------------->| create the staged writer, append chunks
 *      |---- PIECE(1, chunks) --------------->| append chunks
 *      |---- ...                              |
 *      |                                      |
 *      |   commit protocol, round 1: every touched region votes
 *      |---- PREPARE(count, bytes) ---------->| region 1: seal its staged TsFile
 *      |---- PREPARE(count, bytes) ---------->| region 2: seal its staged TsFile
 *      |                                      |
 *      |   round 2 runs only when every PREPARE succeeded
 *      |---- COMMIT ------------------------->| region 1: load its staged TsFile
 *      |---- COMMIT ------------------------->| region 2: load its staged TsFile
 *      |                                      |
 *   before the commit point, on failure:
 *      |---- ABORT -------------------------->| every touched region drops its staged data
 * }</pre>
 *
 * <h2>Result handling</h2>
 *
 * Successful files are only registered for deferred deletion and logged (debug for pipe-generated
 * loads, info otherwise). Failed indexes are collected; when all files are done the scheduler
 * either transitions to FINISHED or hands the failures to {@link LoadFallbackHandler}, which
 * converts the failed TsFiles into tablets, retries the insertion and finally transitions to
 * FINISHED or FAILED. Only then - in the finally block - are the registered source TsFiles
 * physically deleted, so a file with deleteAfterLoad is never removed before the whole LOAD batch
 * has ended.
 *
 * <h2>Component responsibilities</h2>
 *
 * <table>
 *   <caption>Components of the LOAD pipeline</caption>
 *   <tr>
 *     <th>Component</th>
 *     <th>Responsibility</th>
 *   </tr>
 *   <tr>
 *     <td>{@link DataPartitionBatchFetcher}</td>
 *     <td>LOAD partition fetcher: batches (device, time-partition) queries with the transmit limit
 *         and applies the table-model/pipe database hint</td>
 *   </tr>
 *   <tr>
 *     <td>{@link DataPartitionRouter}</td>
 *     <td>LOAD chunk router: deduplicates (device, slot) pairs and maps every chunk to its target
 *         {@code TRegionReplicaSet}</td>
 *   </tr>
 *   <tr>
 *     <td>{@link MemoryBoundedBuffer}</td>
 *     <td>LOAD memory budget: pure memory-pool accounting; emits the "over budget" signal that
 *         triggers eviction</td>
 *   </tr>
 *   <tr>
 *     <td>{@link PieceDispatcher}</td>
 *     <td>LOAD piece dispatcher: buffered per-region pieces, largest-first eviction heap and
 *         flushing</td>
 *   </tr>
 *   <tr>
 *     <td>{@link TsFileSplitConsumer}</td>
 *     <td>LOAD split consumer: the {@code TsFileDataConsumer} composing router, buffer and
 *         dispatcher into the route -&gt; buffer -&gt; dispatch pipeline</td>
 *   </tr>
 *   <tr>
 *     <td>Per-region maps in {@link TwoPhaseConsensusLoadStrategy}</td>
 *     <td>LOAD per-region two-phase state: one context per region with load id, piece count, total
 *         bytes and BEGIN state</td>
 *   </tr>
 *   <tr>
 *     <td>{@link LoadConsensusSubmitter}</td>
 *     <td>LOAD consensus transport for BEGIN/PIECE/PREPARE/COMMIT/ABORT: resolves the partition's
 *         write node and submits via local consensus write or internal RPC, like the normal write
 *         path</td>
 *   </tr>
 *   <tr>
 *     <td>{@link LoadTsFileDispatcherImpl}</td>
 *     <td>legacy LOAD local dispatcher (local-load path) and the per-file uuid holder for
 *         correlation</td>
 *   </tr>
 *   <tr>
 *     <td>{@link LoadFallbackHandler}</td>
 *     <td>LOAD failure fallback: converts failed TsFiles into tablets and retries, then resolves
 *         the final state-machine result</td>
 *   </tr>
 * </table>
 */
public class LoadTsFileScheduler implements IScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileScheduler.class);

  /** Global thread-safe set tracking file paths actively being loaded across all schedulers. */
  private static final Set<String> ACTIVE_LOADING_FILES = ConcurrentHashMap.newKeySet();

  private final MPPQueryContext queryContext;
  private final QueryStateMachine stateMachine;
  private final LoadTsFileDispatcherImpl dispatcher;
  private final DataPartitionBatchFetcher partitionFetcher;
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  private final List<Integer> failedTsFileNodeIndexes;
  private final PlanFragmentId fragmentId;
  private final boolean isGeneratedByPipe;
  private final LoadTsFileDataCacheMemoryBlock memoryBlock;
  private final LoadConsensusSubmitter consensusSubmitter;

  /**
   * Tracks files whose COMMIT has succeeded, postponing physical deletion until the entire batch
   * concludes.
   */
  private final Map<String, LoadSingleTsFileNode> pendingDeletionFiles = new HashMap<>();

  // -------------------------------------------------------------------------
  // Initialization & Construction
  // -------------------------------------------------------------------------

  /**
   * Constructs a scheduler instance, binding resources, dispatchers, and parsing the execution
   * plan.
   *
   * @param distributedQueryPlan query plan containing fragments for all TsFiles in the batch
   * @param queryContext MPP query runtime context
   * @param stateMachine query state machine managing FINISHED/FAILED transitions
   * @param clientManager RPC client manager for remote DataNode communication
   * @param partitionFetcher cluster partition fetcher
   * @param isGeneratedByPipe whether this load operation is initiated by the Pipe subsystem
   */
  public LoadTsFileScheduler(
      final DistributedQueryPlan distributedQueryPlan,
      final MPPQueryContext queryContext,
      final QueryStateMachine stateMachine,
      final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
      final IPartitionFetcher partitionFetcher,
      final boolean isGeneratedByPipe) {
    Objects.requireNonNull(distributedQueryPlan, "distributedQueryPlan cannot be null");
    this.queryContext = Objects.requireNonNull(queryContext, "queryContext cannot be null");
    this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine cannot be null");
    Objects.requireNonNull(clientManager, "clientManager cannot be null");
    Objects.requireNonNull(partitionFetcher, "partitionFetcher cannot be null");

    this.tsFileNodeList = new ArrayList<>();
    this.failedTsFileNodeIndexes = new ArrayList<>();
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.dispatcher = new LoadTsFileDispatcherImpl(clientManager, isGeneratedByPipe);
    this.partitionFetcher = new DataPartitionBatchFetcher(partitionFetcher);
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.memoryBlock = LoadTsFileMemoryManager.getInstance().allocateDataCacheMemoryBlock();
    this.consensusSubmitter = new LoadConsensusSubmitter(clientManager);

    for (final FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      this.tsFileNodeList.add(
          (LoadSingleTsFileNode) fragmentInstance.getFragment().getPlanNodeTree());
    }
  }

  // -------------------------------------------------------------------------
  // Main Execution Loop
  // -------------------------------------------------------------------------

  /** Starts the batch loading orchestration across all input TsFile nodes. */
  @Override
  public void start() {
    try {
      stateMachine.transitionToRunning();
      boolean allFilesSucceeded = true;

      final int totalFiles = tsFileNodeList.size();
      for (int i = 0; i < totalFiles; ++i) {
        final LoadSingleTsFileNode node = tsFileNodeList.get(i);
        final String filePath = node.getTsFileResource().getTsFilePath();
        final String userName = queryContext.getSession().getUserName();

        partitionFetcher.setDatabase(getPartitionQueryDatabase(node, isGeneratedByPipe));

        if (!processSingleNode(node, i, totalFiles, userName)) {
          allFilesSucceeded = false;
          failedTsFileNodeIndexes.add(i);
          continue;
        }

        registerPendingDeletion(node);
        logLoadSuccess(filePath, i + 1, totalFiles);
      }

      if (allFilesSucceeded) {
        stateMachine.transitionToFinished();
      } else {
        new LoadFallbackHandler(
                queryContext,
                isGeneratedByPipe,
                tsFileNodeList,
                failedTsFileNodeIndexes,
                stateMachine)
            .convertFailedTsFilesToTablets();
      }
    } finally {
      teardown();
    }
  }

  // -------------------------------------------------------------------------
  // Single-Node Processing & Strategy Execution
  // -------------------------------------------------------------------------

  /**
   * Processes a single TsFile node through locking, strategy execution, and migration checking.
   *
   * @param node the single TsFile node to process
   * @param index current index in the batch
   * @param totalFiles total number of files in the batch
   * @param userName user executing the command
   * @return true if loading succeeded without migration interference, false otherwise
   */
  private boolean processSingleNode(
      final LoadSingleTsFileNode node,
      final int index,
      final int totalFiles,
      final String userName) {
    final String filePath = node.getTsFileResource().getTsFilePath();
    final long startTimeMs = System.currentTimeMillis();
    boolean lockAcquired = false;

    try {
      if (!ACTIVE_LOADING_FILES.add(filePath)) {
        throw new LoadFileException(
            String.format(
                DataNodeQueryMessages
                    .QUERY_EXCEPTION_TSFILE_S_IS_LOADING_BY_ANOTHER_SCHEDULER_55077B82,
                filePath));
      }
      lockAcquired = true;

      if (node.isTsFileEmpty()) {
        LOGGER.info(DataNodeQueryMessages.LOAD_SKIP_TSFILE_BECAUSE_IT_HAS_NO_DATA, filePath);
        return true;
      }

      final TsFileLoadStrategy strategy = resolveStrategy(node, userName);
      final boolean loadSuccess = strategy.execute(node);

      if (isRegionMigrating(startTimeMs)) {
        LOGGER.warn(
            DataNodeQueryMessages
                .LOADTSFILESCHEDULER_REGION_MIGRATION_WAS_DETECTED_DURING_LOADING_TSFILE_ARG_WILL_CONVERT,
            filePath);
        logCannotLoad(node, index, totalFiles);
        return false;
      }

      if (!loadSuccess) {
        logCannotLoad(node, index, totalFiles);
        return false;
      }

      return true;
    } catch (final Exception e) {
      LOGGER.warn(DataNodeQueryMessages.LOADTSFILESCHEDULER_LOADS_TSFILE_ERROR, filePath, e);
      return false;
    } finally {
      if (lockAcquired) {
        ACTIVE_LOADING_FILES.remove(filePath);
      }
    }
  }

  /**
   * Resolves the load strategy: LocalLoadStrategy for local non-decoded loads, or
   * TwoPhaseConsensusLoadStrategy for distributed decoded partition loads.
   */
  private TsFileLoadStrategy resolveStrategy(final LoadSingleTsFileNode node, final String userName)
      throws Exception {
    final boolean needDecode =
        node.needDecodeTsFile(slotList -> partitionFetcher.queryDataPartition(slotList, userName));

    if (!needDecode) {
      return new LocalLoadStrategy(queryContext, fragmentId, dispatcher);
    } else {
      return new TwoPhaseConsensusLoadStrategy(
          dispatcher,
          partitionFetcher,
          memoryBlock,
          consensusSubmitter,
          userName,
          isGeneratedByPipe);
    }
  }

  private boolean isRegionMigrating(final long taskStartTimeMs) {
    final RegionMigrateService migrateService = RegionMigrateService.getInstance();
    return migrateService.getLastNotifyMigratingTime() > taskStartTimeMs
        || migrateService.mayHaveMigratingRegions();
  }

  // -------------------------------------------------------------------------
  // Deferred Physical Cleanup & Logging
  // -------------------------------------------------------------------------

  /** Registers a successfully loaded TsFile node for deferred physical deletion. */
  private void registerPendingDeletion(final LoadSingleTsFileNode node) {
    if (node.isDeleteAfterLoad()) {
      pendingDeletionFiles.put(node.getTsFileResource().getTsFilePath(), node);
    }
  }

  /**
   * Safely deletes all original source TsFiles registered for deletion after the entire batch
   * finishes.
   */
  private void deletePendingDeletionFiles() {
    if (pendingDeletionFiles.isEmpty()) {
      return;
    }

    LOGGER.info(
        DataNodeQueryMessages
            .LOG_LOAD_BATCH_FINISHED_DELETING_ARG_SOURCE_TSFILES_AFTER_LOAD_D5EE56E9,
        pendingDeletionFiles.size());

    for (final LoadSingleTsFileNode node : pendingDeletionFiles.values()) {
      try {
        node.clean();
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to clean up source TsFile after loading: {}",
            node.getTsFileResource().getTsFilePath(),
            e);
      }
    }
    pendingDeletionFiles.clear();
  }

  /**
   * Guaranteed teardown releasing physical files, RPC dispatchers, and allocated memory cache
   * blocks.
   */
  private void teardown() {
    try {
      deletePendingDeletionFiles();
    } catch (final Throwable t) {
      LOGGER.warn("Exception encountered during deferred source file deletion", t);
    }

    try {
      dispatcher.close();
    } catch (final Throwable t) {
      LOGGER.warn("Exception encountered closing dispatcher", t);
    }

    try {
      LoadTsFileMemoryManager.getInstance().releaseDataCacheMemoryBlock();
    } catch (final Throwable t) {
      LOGGER.warn("Exception encountered releasing data cache memory block", t);
    }
  }

  private void logLoadSuccess(final String filePath, final int current, final int total) {
    if (isGeneratedByPipe) {
      LOGGER.debug(
          DataNodeQueryMessages.LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG,
          filePath,
          current,
          total);
    } else {
      LOGGER.info(
          DataNodeQueryMessages.LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG,
          filePath,
          current,
          total);
    }
  }

  private void logCannotLoad(final LoadSingleTsFileNode node, final int current, final int total) {
    LOGGER.warn(
        DataNodeQueryMessages.CAN_NOT_LOAD_TSFILE_ARG_LOAD_PROCESS_ARG_ARG,
        node.getTsFileResource().getTsFilePath(),
        current,
        total);
  }

  /** Resolves the target database for partition querying according to model and pipe context. */
  static String getPartitionQueryDatabase(
      final LoadSingleTsFileNode node, final boolean isGeneratedByPipe) {
    return node.isTableModel() || isGeneratedByPipe ? node.getDatabase() : null;
  }

  // -------------------------------------------------------------------------
  // IScheduler Contract Implementation
  // -------------------------------------------------------------------------

  /** Aborts active dispatchers and transitions the query state machine to failed. */
  @Override
  public void stop(final Throwable t) {
    try {
      dispatcher.abort();
    } finally {
      if (t != null) {
        stateMachine.transitionToFailed(t);
      }
    }
  }

  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }

  public enum LoadCommand {
    EXECUTE,
    ROLLBACK
  }
}
