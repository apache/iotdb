/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.AbstractResultListener;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadAgent;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.exception.write.PageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * {@link LoadTsFileManager} is the DataNode-side facade over the LOAD staging machinery. Every
 * consensus command of a LOAD task (BEGIN / PIECE / PREPARE / COMMIT / ABORT) enters through {@link
 * #applyConsensusRequest(DataRegion, LoadTsFileConsensusNode)}; the facade routes the work to the
 * per-task {@link TsFileWriterManager} and keeps the rest of the lifecycle in dedicated components.
 * The facade itself carries no parallel maps and no global write lock on the apply path.
 *
 * <p>All classes in this structure belong to the LOAD subsystem ({@code storageengine.load}): they
 * manage the staged files, WAL/ref bookkeeping, snapshots and cleanup of LOAD TSFILE only.
 *
 * <pre>{@code
 *         LOAD consensus commands (BEGIN / PIECE / PREPARE / COMMIT / ABORT)
 *                                     |
 *                                     v
 *                   +-----------------+-----------------+
 *                   |   LoadTsFileManager (facade)      |
 *                   +-----------------+-----------------+
 *                                     |
 *        +----------------+-----------+-----------+----------------+
 *        |                |                       |                |
 *        v                v                       v                v
 * LoadTaskRegistry  TsFileWriterManager     LoadSnapshotManager LoadCleanupScheduler
 * (uuid -> manager) | per-task lock         (include LOAD       (abandoned-task
 * lifecycle)        | applied/cached/        staging in         eviction,
 *                    | retained pieces       snapshots)          delayed cleanup)
 *                    v
 *              PartitionContext (one per data partition)
 *              [TsFileIOWriter + TsFileResource + mods]
 *              writeChunk/writeDeletion, syncedOffset, finalized
 * }</pre>
 *
 * <h2>Command flow</h2>
 *
 * <pre>{@code
 * BEGIN(loadId)             -> register the task in LoadTaskRegistry
 * PIECE(idx, checksum)      -> apply chunk/deletion to the PartitionContext writers,
 *                              retain the serialized bytes, write a marker-only WAL
 *                              entry; followers apply the marker (via consensus log
 *                              replication) and pull the retained bytes back on demand
 * PREPARE(count, bytes, cs) -> finalizeAll(): endChunkGroup + endFile (footer) +
 *                              captureRefs; write a WAL marker so followers seal their
 *                              own staged files at the same logical point
 * COMMIT                    -> write a WAL marker, load every staged file into the
 *                              DataRegion via loadNewTsFile(progress indexes), clean up
 * ABORT                     -> write a WAL marker, delete the staged files
 * }</pre>
 *
 * <h2>Startup recovery</h2>
 *
 * <pre>{@code
 * recover(): scan every configured load directory
 *   -> for each leftover task dir, rebuild a TsFileWriterManager (unsealed-file
 *      recovery) and register it with LoadCleanupScheduler for later eviction
 * }</pre>
 *
 * <p>Staged files live under the configured load directories and are resolved by {@link
 * #findLoadTsFile(String)}. The WAL keeps marker-only entries (dozens of bytes per piece); the
 * actual chunk bytes stay in the write node's retained-piece store and are pulled back by a
 * follower through a DataNode-to-DataNode client ({@code SYNC_DATANODE_CLIENT_MANAGER}) when its
 * marker arrives. Under Ratis the full command is replicated through the Ratis log, so every
 * replica applies the chunk data directly.
 */
public class LoadTsFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileManager.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  /** Snapshot sub-directory that carries the in-progress LOAD staging files of a DataRegion. */
  public static final String LOAD_SNAPSHOT_DIR_NAME = "load";

  private static final AtomicReference<String[]> LOAD_BASE_DIRS =
      new AtomicReference<>(CONFIG.getLoadTsFileDirs());
  private static final AtomicReference<FolderManager> FOLDER_MANAGER = new AtomicReference<>();

  public static final Cache<String, String> MEASUREMENT_ID_CACHE =
      Caffeine.newBuilder()
          .maximumWeight(CONFIG.getLoadMeasurementIdCacheSizeInBytes())
          .weigher((String k, String v) -> v.length())
          .build();

  private final LoadTaskRegistry taskRegistry = new LoadTaskRegistry();
  private final LoadCleanupScheduler cleanupScheduler =
      new LoadCleanupScheduler(
          CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds(), this::forceCloseWriterManager);
  private final LoadSnapshotManager snapshotManager =
      new LoadSnapshotManager(taskRegistry, cleanupScheduler, this::allocateTaskDir);
  private final ActiveLoadAgent activeLoadAgent = new ActiveLoadAgent();

  /** DataNode-to-DataNode client used for the LOAD piece pull-back. */
  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      SYNC_DATANODE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  private static final long PULL_WAIT_INTERVAL_MS = 100L;
  private static final int PULL_WAIT_RETRIES = 50;

  public LoadTsFileManager() {
    cleanupScheduler.start();
    recover();
  }

  /** Resolve a staging file by its relative path under any configured load directory. */
  public static Optional<File> findLoadTsFile(String relativePath) {
    if (relativePath == null || relativePath.isEmpty()) {
      return Optional.empty();
    }
    for (String baseDir : LOAD_BASE_DIRS.get()) {
      final File file = new File(baseDir, relativePath);
      if (file.isFile()) {
        return Optional.of(file);
      }
    }
    return Optional.empty();
  }

  public void start() {
    activeLoadAgent.start();
  }

  public void stop() {
    activeLoadAgent.stop();
    cleanupScheduler.shutdown();
    try {
      taskRegistry.snapshot(TsFileWriterManager::close);
    } catch (IOException e) {
      LOGGER.warn(StorageEngineMessages.LOAD_CLEANUP_TASK_ERROR, "all", e);
    }
  }

  private void recover() {
    if (CONFIG.getLoadTsFileDirs() != LOAD_BASE_DIRS.get()) {
      synchronized (FOLDER_MANAGER) {
        if (CONFIG.getLoadTsFileDirs() != LOAD_BASE_DIRS.get()) {
          LOAD_BASE_DIRS.set(CONFIG.getLoadTsFileDirs());
        }
      }
    }

    final File[] baseDirs = Arrays.stream(LOAD_BASE_DIRS.get()).map(File::new).toArray(File[]::new);
    final File[] files =
        Arrays.stream(baseDirs)
            .filter(File::exists)
            .flatMap(
                dir -> {
                  final File[] listedFiles = dir.listFiles();
                  return listedFiles != null ? Arrays.stream(listedFiles) : Stream.empty();
                })
            .toArray(File[]::new);

    Arrays.stream(files)
        .parallel()
        .forEach(
            taskDir -> {
              final TsFileWriterManager writerManager = new TsFileWriterManager(taskDir);
              writerManager.close();
              cleanupScheduler.registerOrRefresh(taskDir.getName());
            });
  }

  private TsFileWriterManager createWriterManager(String uuid) throws Exception {
    return getFolderManager()
        .getNextWithRetry(folder -> new TsFileWriterManager(new File(folder, uuid)));
  }

  private File allocateTaskDir(String uuid) throws Exception {
    return getFolderManager().getNextWithRetry(folder -> new File(folder, uuid));
  }

  public void writeToDataRegion(DataRegion dataRegion, LoadTsFilePieceNode pieceNode, String uuid)
      throws IOException, PageException, LoadFileException {
    cleanupScheduler.registerOrRefresh(uuid);
    cleanupScheduler.markRunning(uuid);
    try {
      final TsFileWriterManager writerManager = getOrCreateWriterManager(uuid);
      writerManager.writePieceNode(dataRegion, pieceNode);
    } finally {
      cleanupScheduler.markIdle(uuid);
    }
  }

  private TsFileWriterManager getOrCreateWriterManager(String uuid) throws IOException {
    return taskRegistry.getOrCreate(uuid, this::createWriterManager);
  }

  /** Whether pieces {@code 0..pieceIndex-1} were already applied contiguously on this node. */
  private boolean isContinuous(final String uuid, final long pieceIndex) {
    return taskRegistry
        .get(uuid)
        .map(writerManager -> writerManager.hasAppliedAllUpTo(pieceIndex - 1))
        .orElse(pieceIndex == 0);
  }

  /** Apply a consensus LOAD request on the DataRegion state machine path. */
  public TSStatus applyConsensusRequest(DataRegion dataRegion, LoadTsFileConsensusNode node)
      throws IOException, PageException, LoadFileException {
    switch (node.getOp()) {
      case BEGIN:
        return beginConsensus(node);
      case PIECE:
        return appendConsensusPiece(dataRegion, node);
      case PREPARE:
        return prepareConsensus(dataRegion, node);
      case COMMIT:
        return commitConsensus(dataRegion, node);
      case ABORT:
        return abortConsensus(dataRegion, node);
      default:
        return new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode())
            .setMessage(
                DataNodeQueryMessages.EXCEPTION_UNKNOWN_LOADTSFILECONSENSUSOP_ORDINAL_ARG_62848FC2
                    + node.getOp());
    }
  }

  private TSStatus beginConsensus(LoadTsFileConsensusNode node) {
    cleanupScheduler.registerOrRefresh(node.getLoadId());
    return StatusUtils.OK;
  }

  private TSStatus appendConsensusPiece(DataRegion dataRegion, LoadTsFileConsensusNode node)
      throws IOException, PageException, LoadFileException {
    final String uuid = node.getLoadId();
    cleanupScheduler.registerOrRefresh(uuid);
    cleanupScheduler.markRunning(uuid);
    try {
      if (!node.getPieceRefs().isEmpty()) {
        // Legacy raw-ref PIECE (previous format): the refs are contiguous from offset 0, so a
        // replica can rebuild the staged file from the WAL without a local writer. New entries no
        // longer use this form, but entries logged by an older leader must stay applicable during a
        // rolling upgrade.
        final TsFileWriterManager writerManager = getOrCreateWriterManager(uuid);
        writerManager.appendRawTsFilePieces(dataRegion, node.getPieceRefs());
        writerManager.applyDeletion(dataRegion, node.getTsFileDataList());
        return StatusUtils.OK;
      }

      if (!node.hasChunkData()) {
        // Marker-only PIECE replicated through the WAL. The marker is the ordering authority of the
        // load: a follower applies the chunk data (pulled back from the write node, or retained
        // locally) only when its marker arrives (and only after every previous marker was
        // applied), so consensus order and local apply order can never diverge.
        return applyPieceMarker(dataRegion, node);
      }

      // Chunk-data PIECE submitted by the coordinator to the write node (or to a caught-up new
      // leader after failover). Every node maintains its own applied-piece prefix, so the failover
      // fence is continuity: pieceIndex is accepted only when 0..pieceIndex-1 were applied locally,
      // which a follower-turned-leader satisfies automatically because it built its own writers
      // while applying the markers. A node without the prefix must fail instead of silently
      // rebuilding the file, which would fork the replicas.
      if (!isContinuous(uuid, node.getPieceIndex())) {
        return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    StorageEngineMessages
                        .MESSAGE_LOAD_CONSENSUS_PIECE_NOT_CONTINUOUS_AFTER_FAILOVER_D6FFAC6C,
                    node.getPieceIndex(),
                    uuid));
      }

      final TsFileWriterManager writerManager = getOrCreateWriterManager(uuid);
      // Idempotent apply guard: a scheduler retry after a lost response may re-deliver the same
      // piece. Without deduplication the chunk data would be appended twice and the staged file
      // would diverge from the followers.
      if (writerManager.isPieceAlreadyApplied(node.getPieceIndex(), node.getChecksum())) {
        return StatusUtils.OK;
      }
      if (writerManager.isPieceConflicting(node.getPieceIndex(), node.getChecksum())) {
        return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PIECE_CHECKSUM_MISMATCH_CF261675,
                    uuid,
                    node.getPieceIndex()));
      }
      if (node.getChecksum() != LoadTsFileChecksumUtils.checksum(node.getTsFileDataList())) {
        return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PIECE_CHECKSUM_MISMATCH_CF261675,
                    uuid,
                    node.getPieceIndex()));
      }

      writerManager.appendChunkPieceAndRecord(
          dataRegion, node.getTsFileDataList(), node.getPieceIndex(), node.getChecksum());
      // Retain the serialized piece until COMMIT/ABORT as the backfill source for a follower that
      // pulls it back on demand.
      writerManager.retainPiece(node.getPieceIndex(), serializeNode(node));
      // Only the write node logs the marker: the marker-only WAL entry is what IoTConsensus
      // replicates to the followers, which then pull the retained chunk bytes back. A follower
      // applying the same command through consensus log replication skips the local WAL write,
      // exactly like ordinary writes on a follower.
      if (!node.isGeneratedByRemoteConsensusLeader()) {
        logPieceMarkerToWal(dataRegion, node, writerManager);
      }
      return StatusUtils.OK;
    } finally {
      // An applied PIECE leaves the task idle again so an abandoned load (no COMMIT/ABORT ever
      // arrives) is eventually reclaimed by the sweeper after the configured delay.
      cleanupScheduler.markIdle(uuid);
    }
  }

  /**
   * Applies a marker-only PIECE replicated through the WAL. The chunk data (pulled back from the
   * write node, or retained locally when this node was the write node before a restart) is written
   * into this node's own partition writers exactly like the write node does; a still-missing piece
   * is pulled back from the current write node before failing.
   */
  private TSStatus applyPieceMarker(DataRegion dataRegion, LoadTsFileConsensusNode node)
      throws IOException, PageException, LoadFileException {
    final String uuid = node.getLoadId();
    final TsFileWriterManager writerManager = getOrCreateWriterManager(uuid);
    if (writerManager.isPieceAlreadyApplied(node.getPieceIndex(), node.getChecksum())) {
      return StatusUtils.OK;
    }
    if (writerManager.isPieceConflicting(node.getPieceIndex(), node.getChecksum())) {
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PIECE_CHECKSUM_MISMATCH_CF261675,
                  uuid,
                  node.getPieceIndex()));
    }
    if (!writerManager.hasAppliedAllUpTo(node.getPieceIndex() - 1)) {
      // The WAL delivers markers in order, so a hole here means the marker log is corrupt or the
      // task state was reset (e.g. restore without the applied-piece prefix). Failing loudly beats
      // skipping a piece and forking the staged file.
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  StorageEngineMessages
                      .MESSAGE_LOAD_CONSENSUS_PIECE_NOT_CONTINUOUS_AFTER_FAILOVER_D6FFAC6C,
                  node.getPieceIndex(),
                  uuid));
    }
    if (writerManager.hasCachedPiece(node.getPieceIndex(), node.getChecksum())) {
      writerManager.applyCachedPiece(dataRegion, node.getPieceIndex(), node.getChecksum());
      return StatusUtils.OK;
    }
    // The write node's own retained store may still hold the serialized piece (durable on disk),
    // e.g. this node was the write node before a restart and is now replaying its own markers.
    // Backfilling locally avoids a self-RPC round trip and works even when no peer is reachable.
    final Optional<byte[]> retained = writerManager.getRetainedPiece(node.getPieceIndex());
    if (retained.isPresent() && cacheLocalRetainedPiece(writerManager, node, retained.get())) {
      writerManager.applyCachedPiece(dataRegion, node.getPieceIndex(), node.getChecksum());
      return StatusUtils.OK;
    }
    // The piece was not delivered yet (there is no out-of-band push; the marker itself carries no
    // chunk bytes). Pull the piece back from the current write node, which retains the serialized
    // bytes until COMMIT/ABORT.
    if (pullPieceFromLeader(dataRegion, node, writerManager)) {
      writerManager.applyCachedPiece(dataRegion, node.getPieceIndex(), node.getChecksum());
      return StatusUtils.OK;
    }
    return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
        .setMessage(
            String.format(
                StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PIECE_DATA_MISSING_AFTER_PULL_8269CB0B,
                node.getPieceIndex(),
                uuid));
  }

  /** Caches a locally retained serialized PIECE if it matches the marker being applied. */
  private boolean cacheLocalRetainedPiece(
      TsFileWriterManager writerManager, LoadTsFileConsensusNode marker, byte[] serializedPiece) {
    final PlanNode planNode = PlanNodeType.deserialize(ByteBuffer.wrap(serializedPiece));
    if (!(planNode instanceof LoadTsFileConsensusNode)) {
      return false;
    }
    final LoadTsFileConsensusNode chunkPiece = (LoadTsFileConsensusNode) planNode;
    if (chunkPiece.getOp() != LoadTsFileConsensusOp.PIECE
        || !chunkPiece.hasChunkData()
        || !chunkPiece.getLoadId().equals(marker.getLoadId())
        || chunkPiece.getPieceIndex() != marker.getPieceIndex()
        || chunkPiece.getChecksum() != marker.getChecksum()) {
      return false;
    }
    try {
      return writerManager.cachePiece(
          chunkPiece.getPieceIndex(), chunkPiece.getChecksum(), chunkPiece.getTsFileDataList());
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Logs a marker-only PIECE (metadata without chunk bytes) to the WAL. The WAL is the ordering
   * authority replicated to the followers; the actual chunk data is retained by the write node and
   * pulled back by a follower when the marker arrives, so the WAL stays at dozens of bytes per
   * piece instead of the full LOAD bytes.
   */
  private void logPieceMarkerToWal(
      DataRegion dataRegion, LoadTsFileConsensusNode node, TsFileWriterManager writerManager)
      throws IOException {
    final Optional<IWALNode> walNodeOptional = dataRegion.getWALNode();
    if (!walNodeOptional.isPresent()) {
      return;
    }
    // The refs are only local bookkeeping (they advance the synced cursor used by snapshots); they
    // are no longer replicated, so drain and discard them.
    writerManager.drainPendingPieceRefs();
    final LoadTsFileConsensusNode marker =
        LoadTsFileConsensusNode.pieceMarker(
            new PlanNodeId("load-wal-marker-" + node.getLoadId() + "-" + node.getPieceIndex()),
            node.getLoadId(),
            node.getTsFileId(),
            node.getPieceIndex(),
            node.getChecksum(),
            node.getDataSize());
    final WALFlushListener listener =
        walNodeOptional.get().log(TsFileProcessor.MEMTABLE_NOT_EXIST, marker);
    if (listener.waitForResult() == AbstractResultListener.Status.FAILURE) {
      throw new IOException(
          StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_WAL_FLUSH_FAILED_8BE1375A,
          listener.getCause());
    }
  }

  /** Logs a PREPARE/COMMIT/ABORT op itself as the WAL marker so followers apply the same phase. */
  private void logOpMarkerToWal(DataRegion dataRegion, LoadTsFileConsensusNode node)
      throws IOException {
    final Optional<IWALNode> walNodeOptional = dataRegion.getWALNode();
    if (!walNodeOptional.isPresent()) {
      return;
    }
    final WALFlushListener listener =
        walNodeOptional.get().log(TsFileProcessor.MEMTABLE_NOT_EXIST, node);
    if (listener.waitForResult() == AbstractResultListener.Status.FAILURE) {
      throw new IOException(
          StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_WAL_FLUSH_FAILED_8BE1375A,
          listener.getCause());
    }
  }

  /**
   * Pulls one missing piece from the current write node. The write node receives a PULL request,
   * reads the retained serialized piece and pushes it back to {@code pullSourceEndPoint}; this side
   * waits (bounded) for the pushed piece to land in the cache.
   */
  private boolean pullPieceFromLeader(
      DataRegion dataRegion, LoadTsFileConsensusNode marker, TsFileWriterManager writerManager) {
    final ConsensusGroupId groupId =
        new DataRegionId(Integer.parseInt(dataRegion.getDataRegionIdString()));
    final TEndPoint leaderEndPoint = resolveWriteNodeEndPoint(dataRegion, groupId, marker);
    if (leaderEndPoint == null) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_PULL_PIECE_FAILED_AFB003D5,
          marker.getPieceIndex(),
          marker.getLoadId(),
          "unknown",
          "cannot resolve the current write node from the partition table");
      return false;
    }
    final String localEndPoint = CONFIG.getInternalAddress() + ":" + CONFIG.getInternalPort();
    final LoadTsFileConsensusNode pull =
        LoadTsFileConsensusNode.pull(
            new PlanNodeId("load-pull-" + marker.getLoadId() + "-" + marker.getPieceIndex()),
            marker.getLoadId(),
            marker.getTsFileId(),
            marker.getPieceIndex(),
            marker.getChecksum(),
            localEndPoint);
    try (final SyncDataNodeInternalServiceClient client =
        SYNC_DATANODE_CLIENT_MANAGER.borrowClient(leaderEndPoint)) {
      final TLoadResp resp =
          client.sendTsFilePieceNode(
              new TTsFilePieceReq(
                  pull.serializeToByteBuffer(),
                  marker.getLoadId(),
                  groupId.convertToTConsensusGroupId()));
      if (!resp.isAccepted()) {
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_PULL_PIECE_FAILED_AFB003D5,
            marker.getPieceIndex(),
            marker.getLoadId(),
            leaderEndPoint,
            resp.getMessage());
        return false;
      }
    } catch (Exception e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_PULL_PIECE_FAILED_AFB003D5,
          marker.getPieceIndex(),
          marker.getLoadId(),
          leaderEndPoint,
          e.getMessage());
      return false;
    }
    for (int i = 0; i < PULL_WAIT_RETRIES; i++) {
      if (writerManager.hasCachedPiece(marker.getPieceIndex(), marker.getChecksum())) {
        return true;
      }
      try {
        Thread.sleep(PULL_WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  /**
   * Resolves the internal endpoint of the partition's current write node from the local partition
   * table (the same replica-set routing the normal write path uses). This is the node that retains
   * the applied piece bytes, so a follower missing a delivery pulls them back from here.
   */
  private TEndPoint resolveWriteNodeEndPoint(
      DataRegion dataRegion, ConsensusGroupId groupId, LoadTsFileConsensusNode marker) {
    try {
      final List<TRegionReplicaSet> replicaSets =
          ClusterPartitionFetcher.getInstance()
              .getRegionReplicaSet(Collections.singletonList(groupId.convertToTConsensusGroupId()));
      if (!replicaSets.isEmpty()) {
        final List<TDataNodeLocation> locations = replicaSets.get(0).getDataNodeLocations();
        if (locations != null && !locations.isEmpty()) {
          final TEndPoint writeNodeEndPoint = locations.get(0).getInternalEndPoint();
          if (writeNodeEndPoint != null) {
            return writeNodeEndPoint;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_PULL_PIECE_FAILED_AFB003D5,
          marker.getPieceIndex(),
          marker.getLoadId(),
          "unknown",
          "failed to resolve the current write node from the partition table: " + e.getMessage());
    }
    // Fall back to the consensus leader lookup (meaningful for Ratis; IoTConsensus reports the
    // local node, which is still correct for the local retained-piece case).
    final Peer leader = DataRegionConsensusImpl.getInstance().getLeader(groupId);
    return leader == null
        ? null
        : new TEndPoint(leader.getEndpoint().getIp(), leader.getEndpoint().getPort());
  }

  /** The write node's side of a PULL: push the retained piece bytes back to the requester. */
  public TSStatus handlePullPiece(DataRegion dataRegion, LoadTsFileConsensusNode pullNode) {
    final String uuid = pullNode.getLoadId();
    final long pieceIndex = pullNode.getPieceIndex();
    final TEndPoint target = parseEndPoint(pullNode.getPullSourceEndPoint());
    if (target == null) {
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PULL_WITHOUT_SOURCE_ENDPOINT_3B20D9E9);
    }
    final Optional<byte[]> retained =
        taskRegistry.get(uuid).flatMap(writerManager -> writerManager.getRetainedPiece(pieceIndex));
    if (!retained.isPresent()) {
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PULL_WITHOUT_RETAINED_PIECE_AD3C9D4F,
                  pieceIndex,
                  uuid));
    }
    final byte[] serialized = retained.get();
    final PlanNode planNode = PlanNodeType.deserialize(ByteBuffer.wrap(serialized));
    if (!(planNode instanceof LoadTsFileConsensusNode)
        || ((LoadTsFileConsensusNode) planNode).getOp() != LoadTsFileConsensusOp.PIECE) {
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PULL_WITHOUT_RETAINED_PIECE_AD3C9D4F,
                  pieceIndex,
                  uuid));
    }
    final TConsensusGroupId groupId =
        new DataRegionId(Integer.parseInt(dataRegion.getDataRegionIdString()))
            .convertToTConsensusGroupId();
    try (final SyncDataNodeInternalServiceClient client =
        SYNC_DATANODE_CLIENT_MANAGER.borrowClient(target)) {
      final TLoadResp resp =
          client.sendTsFilePieceNode(
              new TTsFilePieceReq(ByteBuffer.wrap(serialized), uuid, groupId));
      return resp.isAccepted()
          ? StatusUtils.OK
          : new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
              .setMessage(
                  String.format(
                      StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PULL_PUSH_BACK_FAILED_1A90C2B9,
                      pieceIndex,
                      uuid,
                      target,
                      resp.getMessage()));
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PULL_PUSH_BACK_FAILED_1A90C2B9,
                  pieceIndex,
                  uuid,
                  target,
                  e.getMessage()));
    }
  }

  /**
   * Caches a chunk-data PIECE pushed back by the write node in response to a PULL (or delivered by
   * a legacy out-of-band push). The data is not applied until the corresponding WAL marker arrives,
   * because the marker (not the delivery order) decides the apply order on this node.
   */
  public TSStatus cacheConsensusPiece(DataRegion dataRegion, LoadTsFileConsensusNode node)
      throws IOException {
    final String uuid = node.getLoadId();
    cleanupScheduler.registerOrRefresh(uuid);
    cleanupScheduler.markRunning(uuid);
    try {
      if (!node.hasChunkData()) {
        return StatusUtils.OK;
      }
      final TsFileWriterManager writerManager = getOrCreateWriterManager(uuid);
      if (writerManager.isPieceAlreadyApplied(node.getPieceIndex(), node.getChecksum())) {
        // The marker already applied this piece; the redundant delivery is dropped.
        return StatusUtils.OK;
      }
      if (!writerManager.cachePiece(
          node.getPieceIndex(), node.getChecksum(), node.getTsFileDataList())) {
        return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PIECE_CHECKSUM_MISMATCH_CF261675,
                    uuid,
                    node.getPieceIndex()));
      }
      return StatusUtils.OK;
    } finally {
      cleanupScheduler.markIdle(uuid);
    }
  }

  private TEndPoint parseEndPoint(final String endPointString) {
    if (endPointString == null || endPointString.isEmpty()) {
      return null;
    }
    final int separatorIndex = endPointString.lastIndexOf(':');
    if (separatorIndex <= 0 || separatorIndex == endPointString.length() - 1) {
      return null;
    }
    try {
      return new TEndPoint(
          endPointString.substring(0, separatorIndex),
          Integer.parseInt(endPointString.substring(separatorIndex + 1)));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static byte[] serializeNode(LoadTsFileConsensusNode node) {
    final ByteBuffer buffer = node.serializeToByteBuffer();
    final byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private TSStatus prepareConsensus(DataRegion dataRegion, LoadTsFileConsensusNode node)
      throws IOException {
    if (!taskRegistry.contains(node.getLoadId())) {
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  StorageEngineMessages.MESSAGE_LOAD_CONSENSUS_PREPARE_WITHOUT_STAGED_DATA_FE8ADC37,
                  node.getLoadId()));
    }
    final String uuid = node.getLoadId();
    cleanupScheduler.markRunning(uuid);
    try {
      final TsFileWriterManager writerManager = getOrCreateWriterManager(uuid);
      // Reconcile before sealing: the staged file must contain exactly the pieces the coordinator
      // sent. A write-node switch mid-load can leave this node with a hole in its applied prefix
      // that the per-piece continuity fence cannot detect (no further PIECE arrives); sealing and
      // loading such a file would silently fork the replicas, so fail loudly instead.
      if (!writerManager.isLegacyRawRefTask()
          && !writerManager.verifyAppliedPieces(node.getPieceCount(), node.getChecksum())) {
        return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    StorageEngineMessages
                        .MESSAGE_LOAD_CONSENSUS_PREPARE_VERIFICATION_FAILED_B3865A82,
                    uuid,
                    node.getPieceCount(),
                    node.getChecksum(),
                    writerManager.getAppliedPieceCount(),
                    writerManager.getAppliedPiecesChecksum()));
      }
      writerManager.finalizeAll();
      if (!node.isGeneratedByRemoteConsensusLeader()) {
        // Replicate the PREPARE marker so every follower seals its own staged files at the same
        // logical point before COMMIT.
        logOpMarkerToWal(dataRegion, node);
      }
      return StatusUtils.OK;
    } finally {
      cleanupScheduler.markIdle(uuid);
    }
  }

  private TSStatus commitConsensus(DataRegion dataRegion, LoadTsFileConsensusNode node)
      throws IOException, LoadFileException {
    final Map<TTimePartitionSlot, ProgressIndex> progressIndexes = new HashMap<>();
    for (Map.Entry<TTimePartitionSlot, byte[]> entry :
        node.getTimePartition2ProgressIndex().entrySet()) {
      final ProgressIndex progressIndex = MinimumProgressIndex.INSTANCE;
      progressIndexes.put(entry.getKey(), progressIndex);
    }
    if (!node.isGeneratedByRemoteConsensusLeader()) {
      // Replicate the COMMIT marker before loading so followers import their own staged files too.
      logOpMarkerToWal(dataRegion, node);
    }
    if (!loadAll(node.getLoadId(), dataRegion, node.isGeneratedByPipe(), progressIndexes)) {
      return new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
          .setMessage(
              StorageEngineMessages
                      .MESSAGE_NO_LOAD_TSFILE_UUID_ARG_RECORDED_EXECUTE_LOAD_COMMAND_ARG_66722D80
                  + node.getLoadId());
    }
    return StatusUtils.OK;
  }

  private TSStatus abortConsensus(DataRegion dataRegion, LoadTsFileConsensusNode node) {
    if (!node.isGeneratedByRemoteConsensusLeader()) {
      try {
        // Replicate the ABORT marker so followers discard their staged files as well.
        logOpMarkerToWal(dataRegion, node);
      } catch (IOException e) {
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_ABORT_MARKER_FAILED_6A218023,
            node.getLoadId(),
            e.getMessage());
      }
    }
    deleteAll(node.getLoadId());
    return StatusUtils.OK;
  }

  private FolderManager getFolderManager() throws DiskSpaceInsufficientException {
    if (CONFIG.getLoadTsFileDirs() != LOAD_BASE_DIRS.get()) {
      synchronized (FOLDER_MANAGER) {
        if (CONFIG.getLoadTsFileDirs() != LOAD_BASE_DIRS.get()) {
          LOAD_BASE_DIRS.set(CONFIG.getLoadTsFileDirs());
          FOLDER_MANAGER.set(
              new FolderManager(
                  Arrays.asList(LOAD_BASE_DIRS.get()), DirectoryStrategyType.SEQUENCE_STRATEGY));
          return FOLDER_MANAGER.get();
        }
      }
    }

    if (FOLDER_MANAGER.get() == null) {
      synchronized (FOLDER_MANAGER) {
        if (FOLDER_MANAGER.get() == null) {
          FOLDER_MANAGER.set(
              new FolderManager(
                  Arrays.asList(LOAD_BASE_DIRS.get()), DirectoryStrategyType.SEQUENCE_STRATEGY));
          return FOLDER_MANAGER.get();
        }
      }
    }

    return FOLDER_MANAGER.get();
  }

  public boolean loadAll(
      String uuid,
      boolean isGeneratedByPipe,
      Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    return loadAll(uuid, null, isGeneratedByPipe, timePartitionProgressIndexMap);
  }

  /**
   * Loads the staged data of the given load into the DataRegion. The consensus COMMIT path passes
   * the current {@code dataRegion} so that staged files restored from a snapshot (which have no
   * in-memory writer) can be bound and loaded; the legacy direct-load path passes {@code null} and
   * only ever contains writer-managed files.
   */
  public boolean loadAll(
      String uuid,
      DataRegion dataRegion,
      boolean isGeneratedByPipe,
      Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    final Optional<TsFileWriterManager> writerManagerOptional = taskRegistry.get(uuid);
    if (!writerManagerOptional.isPresent()) {
      return false;
    }

    cleanupScheduler.registerOrRefresh(uuid);
    cleanupScheduler.markRunning(uuid);
    try {
      writerManagerOptional
          .get()
          .loadAll(dataRegion, isGeneratedByPipe, timePartitionProgressIndexMap);
    } finally {
      cleanupScheduler.markIdle(uuid);
    }

    clean(uuid);
    return true;
  }

  public boolean deleteAll(String uuid) {
    if (!taskRegistry.contains(uuid)) {
      return false;
    }
    clean(uuid);
    return true;
  }

  private void clean(String uuid) {
    cleanupScheduler.remove(uuid);
    forceCloseWriterManager(uuid);
  }

  private void forceCloseWriterManager(String uuid) {
    final TsFileWriterManager writerManager = taskRegistry.remove(uuid);
    if (writerManager != null) {
      writerManager.close();
    }
  }

  public void snapshotLoadTasksForRegion(DataRegion dataRegion, File snapshotDir)
      throws IOException {
    snapshotManager.snapshotLoadTasksForRegion(dataRegion, snapshotDir);
  }

  public void restoreLoadTasksFromSnapshot(File loadSnapshotDir) throws IOException {
    snapshotManager.restoreLoadTasksFromSnapshot(loadSnapshotDir);
  }

  public static void updateWritePointCountMetrics(
      final DataRegion dataRegion,
      final String databaseName,
      final long writePointCount,
      final boolean isGeneratedByIoTConsensusV2Leader) {
    MemTableFlushTask.recordFlushPointsMetricInternal(
        writePointCount, databaseName, dataRegion.getDataRegionIdString());
    MetricService.getInstance()
        .count(
            writePointCount,
            Metric.QUANTITY.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            Metric.POINTS_IN.toString(),
            Tag.DATABASE.toString(),
            databaseName,
            Tag.REGION.toString(),
            dataRegion.getDataRegionIdString(),
            Tag.TYPE.toString(),
            Metric.LOAD_POINT_COUNT.toString());
    // Because we cannot accurately judge who is the leader here,
    // we directly divide the writePointCount by the replicationNum to ensure the
    // correctness of this metric, which will be accurate in most cases
    final int replicationNum =
        DataRegionConsensusImpl.getInstance()
            .getReplicationNum(
                ConsensusGroupId.Factory.create(
                    TConsensusGroupType.DataRegion.getValue(),
                    Integer.parseInt(dataRegion.getDataRegionIdString())));
    // It may happen that the replicationNum is 0 when load and db deletion occurs
    // concurrently, so we can just not to count the number of points in this case
    if (replicationNum != 0 && !isGeneratedByIoTConsensusV2Leader) {
      MetricService.getInstance()
          .count(
              writePointCount / replicationNum,
              Metric.LEADER_QUANTITY.toString(),
              MetricLevel.CORE,
              Tag.NAME.toString(),
              Metric.POINTS_IN.toString(),
              Tag.DATABASE.toString(),
              databaseName,
              Tag.REGION.toString(),
              dataRegion.getDataRegionIdString(),
              Tag.TYPE.toString(),
              Metric.LOAD_POINT_COUNT.toString());
    }
  }

  public static void cleanTsFile(final File tsFile) {
    try {
      Files.deleteIfExists(tsFile.toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).toPath());
      Files.deleteIfExists(ModificationFile.getExclusiveMods(tsFile).toPath());
      Files.deleteIfExists(
          new File(tsFile.getAbsolutePath() + ModificationFileV1.FILE_SUFFIX).toPath());
    } catch (final IOException e) {
      LOGGER.warn(StorageEngineMessages.DELETE_AFTER_LOADING_ERROR, tsFile, e);
    }
  }
}
