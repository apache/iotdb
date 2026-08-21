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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * LOAD scheduler: {@link LoadTsFileScheduler} is the coordinator of a batch of {@link
 * LoadSingleTsFileNode} loads (one node per source TsFile). It owns only the lifecycle - concurrent
 * file lock, per-node guard clauses, state-machine transitions and the failure fallback - and
 * routes every file to a {@link TsFileLoadStrategy}. All per-region bookkeeping, memory management
 * and consensus submission live in the strategy components.
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
 *     |                       or ABORT, via RegionConsensusContext +
 *     |                       LoadConsensusSubmitter
 *     |
 *     +--> success -> node.clean() + log
 *     |       failure -> record failed index
 *     |
 *     `--> all success -> FINISHED
 *             else -> LoadFallbackHandler (convert to tablets, retry)
 *                      -> FINISHED / FAILED
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
 * RegionConsensusContext.accumulate(bytes, checksum)
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
 *      |---- BEGIN(loadId) ------------------>| create staged writer
 *      |---- PIECE(0, chunks) --------------->| append chunks
 *      |---- PIECE(1, chunks) --------------->| append chunks
 *      |---- ...                              |
 *      |---- PREPARE(count, bytes, checksum)->| seal staged TsFile
 *      |---- COMMIT ------------------------->| load staged TsFile
 *      |                                      |
 *   on failure:
 *      |---- ABORT -------------------------->| drop staged data
 * }</pre>
 *
 * <h2>Result handling</h2>
 *
 * Successful files are cleaned up and logged (debug for pipe-generated loads, info otherwise).
 * Failed indexes are collected; when all files are done the scheduler either transitions to
 * FINISHED or hands the failures to {@link LoadFallbackHandler}, which converts the failed TsFiles
 * into tablets, retries the insertion and finally transitions to FINISHED or FAILED.
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
 *     <td>{@link RegionConsensusContext}</td>
 *     <td>LOAD per-region two-phase state: one context per region with load id, piece count, total
 *         bytes, XOR checksum and BEGIN state</td>
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

  private static final Set<String> LOADING_FILE_SET = new HashSet<>();

  private final MPPQueryContext queryContext;
  private final QueryStateMachine stateMachine;
  private final LoadTsFileDispatcherImpl dispatcher;
  private final DataPartitionBatchFetcher partitionFetcher;
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  private final List<Integer> failedTsFileNodeIndexes;
  private final PlanFragmentId fragmentId;
  private final boolean isGeneratedByPipe;
  private final LoadTsFileDataCacheMemoryBlock block;
  private final LoadConsensusSubmitter consensusSubmitter;

  public LoadTsFileScheduler(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      IPartitionFetcher partitionFetcher,
      boolean isGeneratedByPipe) {
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.tsFileNodeList = new ArrayList<>();
    this.failedTsFileNodeIndexes = new ArrayList<>();
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.dispatcher = new LoadTsFileDispatcherImpl(internalServiceClientManager, isGeneratedByPipe);
    this.partitionFetcher = new DataPartitionBatchFetcher(partitionFetcher);
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.block = LoadTsFileMemoryManager.getInstance().allocateDataCacheMemoryBlock();
    this.consensusSubmitter = new LoadConsensusSubmitter(internalServiceClientManager);

    for (FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      tsFileNodeList.add((LoadSingleTsFileNode) fragmentInstance.getFragment().getPlanNodeTree());
    }
  }

  @Override
  public void start() {
    try {
      stateMachine.transitionToRunning();
      boolean isLoadSuccess = true;

      for (int i = 0; i < tsFileNodeList.size(); ++i) {
        final LoadSingleTsFileNode node = tsFileNodeList.get(i);
        final String filePath = node.getTsFileResource().getTsFilePath();
        final String userName = queryContext.getSession().getUserName();

        partitionFetcher.setDatabase(getPartitionQueryDatabase(node, isGeneratedByPipe));

        if (!processSingleNode(node, i, tsFileNodeList.size(), userName)) {
          isLoadSuccess = false;
          failedTsFileNodeIndexes.add(i);
          continue;
        }

        node.clean();
        if (isGeneratedByPipe) {
          LOGGER.debug(
              DataNodeQueryMessages.LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG,
              filePath,
              i + 1,
              tsFileNodeList.size());
        } else {
          LOGGER.info(
              DataNodeQueryMessages.LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG,
              filePath,
              i + 1,
              tsFileNodeList.size());
        }
      }

      if (isLoadSuccess) {
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
      dispatcher.close();
      LoadTsFileMemoryManager.getInstance().releaseDataCacheMemoryBlock();
    }
  }

  /**
   * Loads one TsFile: guard clauses first (concurrent-load lock, empty file), then route to the
   * strategy and finally detect whether a region migration raced with the load.
   */
  private boolean processSingleNode(
      LoadSingleTsFileNode node, int index, int listSize, String userName) {
    final String filePath = node.getTsFileResource().getTsFilePath();
    final long startTimeMs = System.currentTimeMillis();
    boolean shouldRemoveFileFromLoadingSet = false;
    try {
      synchronized (LOADING_FILE_SET) {
        if (LOADING_FILE_SET.contains(filePath)) {
          throw new LoadFileException(
              String.format(
                  DataNodeQueryMessages
                      .QUERY_EXCEPTION_TSFILE_S_IS_LOADING_BY_ANOTHER_SCHEDULER_55077B82,
                  filePath));
        }
        LOADING_FILE_SET.add(filePath);
      }
      shouldRemoveFileFromLoadingSet = true;

      if (node.isTsFileEmpty()) {
        LOGGER.info(DataNodeQueryMessages.LOAD_SKIP_TSFILE_BECAUSE_IT_HAS_NO_DATA, filePath);
        return true;
      }

      final TsFileLoadStrategy strategy;
      if (!node.needDecodeTsFile(
          slotList -> partitionFetcher.queryDataPartition(slotList, userName))) {
        // do not decode, load locally
        strategy = new LocalLoadStrategy(queryContext, fragmentId, dispatcher);
      } else {
        // need decode, use the consensus two-phase pipeline
        strategy =
            new TwoPhaseConsensusLoadStrategy(
                dispatcher,
                partitionFetcher,
                block,
                consensusSubmitter,
                userName,
                isGeneratedByPipe);
      }
      final boolean isLoadSingleTsFileSuccess = strategy.execute(node);

      if (RegionMigrateService.getInstance().getLastNotifyMigratingTime() > startTimeMs
          || RegionMigrateService.getInstance().mayHaveMigratingRegions()) {
        LOGGER.warn(
            DataNodeQueryMessages
                .LOADTSFILESCHEDULER_REGION_MIGRATION_WAS_DETECTED_DURING_LOADING_TSFILE_ARG_WILL_CONVERT,
            filePath);
        logCannotLoad(node, index, listSize);
        return false;
      }
      if (!isLoadSingleTsFileSuccess) {
        logCannotLoad(node, index, listSize);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn(DataNodeQueryMessages.LOADTSFILESCHEDULER_LOADS_TSFILE_ERROR, filePath, e);
      return false;
    } finally {
      if (shouldRemoveFileFromLoadingSet) {
        synchronized (LOADING_FILE_SET) {
          LOADING_FILE_SET.remove(filePath);
        }
      }
    }
  }

  private void logCannotLoad(LoadSingleTsFileNode node, int index, int listSize) {
    LOGGER.warn(
        DataNodeQueryMessages.CAN_NOT_LOAD_TSFILE_ARG_LOAD_PROCESS_ARG_ARG,
        node.getTsFileResource().getTsFilePath(),
        index + 1,
        listSize);
  }

  static String getPartitionQueryDatabase(
      final LoadSingleTsFileNode node, final boolean isGeneratedByPipe) {
    return node.isTableModel() || isGeneratedByPipe ? node.getDatabase() : null;
  }

  @Override
  public void stop(Throwable t) {
    dispatcher.abort();
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
