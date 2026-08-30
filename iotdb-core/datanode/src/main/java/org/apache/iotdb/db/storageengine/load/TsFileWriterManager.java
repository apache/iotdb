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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.load.LoadSnapshotManager.StagedFileSnapshot;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileDataType;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lifecycle of one LOAD task: owns the task directory, the {@link PartitionContext} set of this
 * task and the WAL/ref bookkeeping (raw pieces, restored snapshot partitions, applied piece
 * checksums). All mutations are serialized by the per-task {@link #taskLock}, so different LOAD
 * tasks never contend on a global manager lock.
 */
final class TsFileWriterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileWriterManager.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final String MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED =
      "%s TsFileWriterManager has been closed.";
  private static final String MESSAGE_DELETE_FAIL = "failed to delete {}.";

  /** Sub-directory holding the retained serialized PIECE bytes (the backfill source). */
  private static final String RETAINED_PIECES_DIR_NAME = "pieces";

  /**
   * Durable per-task bookkeeping inside the task directory: the applied-piece prefix and the staged
   * partition files (same format as the snapshot meta). Written on every applied PIECE and on
   * PREPARE so a restart can rebuild this manager instead of discarding the load.
   */
  private static final String TASK_META_NAME = "task.meta";

  /**
   * Marker written right before the terminal cleanup of a load. It is the only restart-time cleanup
   * exception: a task dir carrying this marker already reached COMMIT (files were loaded) or ABORT
   * (files are intentionally discarded), so the leftover dir is garbage and can be deleted at
   * startup. Any dir without it is an in-progress load and must be rebuilt, not cleaned.
   */
  private static final String TERMINAL_MARKER_NAME = "terminal.marker";

  private final File taskDir;
  private final ReentrantLock taskLock = new ReentrantLock();

  /** One cohesive context per staged partition file: writer, resource, mods, cursor, finalized. */
  private final Map<DataPartitionInfo, PartitionContext> partitionContexts = new HashMap<>();

  /** Devices currently routed to partition files, used to end one chunk group across partitions. */
  private final Map<IDeviceID, Set<DataPartitionInfo>> device2Partition = new HashMap<>();

  private final List<LoadTsFileConsensusNode.PieceRef> pendingPieceRefs = new ArrayList<>();
  private final List<RawTsFile> rawTsFiles = new ArrayList<>();
  private final Set<String> rawTsFilePaths = new HashSet<>();
  private final Map<RestoredPartitionKey, RestoredLoadFile> restoredPartitions = new HashMap<>();

  /**
   * Piece data delivered to this node (pulled back from the write node) but not yet applied. A
   * follower applies the cached data only when the corresponding WAL marker arrives, because the
   * marker (not the delivery order) is the ordering authority of the load.
   */
  private final Map<Long, CachedPiece> cachedPieces = new HashMap<>();

  /**
   * Serialized bytes of every chunk-data piece applied by this (write) node, retained until COMMIT
   * or ABORT so a follower that missed the delivery can pull the piece back.
   */
  private final Map<Long, byte[]> retainedPieces = new HashMap<>();

  /**
   * Tracks the checksum of every chunk-data piece already applied to this task so that a scheduler
   * retry (or a duplicated consensus log entry) is acknowledged idempotently instead of appending
   * the same bytes twice. Keyed by the coordinator-assigned piece index, which is unique per load.
   */
  private final Map<Long, Long> appliedPieceIndex2Checksum = new HashMap<>();

  /**
   * Length of the contiguous applied-piece prefix ({@code 0..appliedContiguousCount-1}). Kept in
   * sync with {@link #appliedPieceIndex2Checksum} so the failover fence is O(1).
   */
  private long appliedContiguousCount;

  private boolean isClosed;

  /**
   * Whether this manager was rebuilt from a durable task meta (or has nothing on disk yet). A task
   * dir whose meta cannot be reconciled must not be resumed: applying further pieces into it could
   * silently fork or drop data, so the coordinator is left to fail loudly and abort instead.
   */
  private boolean recoveredFromDisk = true;

  TsFileWriterManager(File taskDir) {
    this(taskDir, true);
  }

  TsFileWriterManager(File taskDir, boolean clearExistingDir) {
    this.taskDir = taskDir;
    if (clearExistingDir) {
      clearDir(taskDir);
    } else {
      // A task dir rebuilt after a restart or restored from a snapshot already carries the durable
      // retained PIECE bytes and the task meta; reload them so marker replay can be backfilled
      // locally and the already-applied prefix is respected instead of re-appending the data.
      loadRetainedPiecesFromDisk();
      recoverFromDisk();
    }
  }

  String getTaskName() {
    return taskDir.getName();
  }

  File getTaskDir() {
    return taskDir;
  }

  /**
   * Marks this task as terminated (COMMIT/ABORT reached) so a restart can discard the leftovers.
   */
  void markTerminal() {
    taskLock.lock();
    try {
      final File marker = new File(taskDir, TERMINAL_MARKER_NAME);
      try {
        Files.write(marker.toPath(), new byte[0]);
        try (final FileChannel channel =
            FileChannel.open(marker.toPath(), StandardOpenOption.WRITE)) {
          channel.force(true);
        }
      } catch (IOException e) {
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_TERMINAL_MARKER_WRITE_FAILED_4D6D7433,
            getTaskName(),
            marker,
            e.getMessage());
      }
    } finally {
      taskLock.unlock();
    }
  }

  boolean isTerminal() {
    return new File(taskDir, TERMINAL_MARKER_NAME).isFile();
  }

  boolean isRecoveredFromDisk() {
    return recoveredFromDisk;
  }

  private void clearDir(File dir) {
    if (dir.exists()) {
      FileUtils.deleteFileOrDirectoryWithRetry(dir);
    }
    if (dir.mkdirs()) {
      LOGGER.info(StorageEngineMessages.LOAD_TSFILE_DIR_CREATED, dir.getPath());
    }
  }

  boolean hasLiveWriter() {
    taskLock.lock();
    try {
      return !partitionContexts.isEmpty();
    } finally {
      taskLock.unlock();
    }
  }

  boolean isPieceAlreadyApplied(long pieceIndex, long checksum) {
    taskLock.lock();
    try {
      return appliedPieceIndex2Checksum.containsKey(pieceIndex)
          && appliedPieceIndex2Checksum.get(pieceIndex) == checksum;
    } finally {
      taskLock.unlock();
    }
  }

  boolean isPieceConflicting(long pieceIndex, long checksum) {
    taskLock.lock();
    try {
      return appliedPieceIndex2Checksum.containsKey(pieceIndex)
          && appliedPieceIndex2Checksum.get(pieceIndex) != checksum;
    } finally {
      taskLock.unlock();
    }
  }

  /** Whether every piece {@code 0..pieceIndex} (inclusive) has been applied contiguously. */
  boolean hasAppliedAllUpTo(long pieceIndex) {
    taskLock.lock();
    try {
      // appliedContiguousCount tracks how many pieces (starting from 0) are already applied, so
      // the failover fence is a single comparison instead of an O(n) scan per piece.
      return pieceIndex < 0 || pieceIndex < appliedContiguousCount;
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Caches a chunk-data PIECE pushed back by the write node in response to a PULL. Returns {@code
   * false} when a piece with the same index is already cached with a different checksum (a
   * divergent delivery).
   */
  boolean cachePiece(long pieceIndex, long checksum, List<TsFileData> dataList) throws IOException {
    taskLock.lock();
    try {
      checkNotClosed();
      final CachedPiece existing = cachedPieces.get(pieceIndex);
      if (existing != null) {
        return existing.checksum == checksum;
      }
      cachedPieces.put(pieceIndex, new CachedPiece(pieceIndex, checksum, dataList));
      return true;
    } finally {
      taskLock.unlock();
    }
  }

  boolean hasCachedPiece(long pieceIndex, long checksum) {
    taskLock.lock();
    try {
      final CachedPiece cached = cachedPieces.get(pieceIndex);
      return cached != null && cached.checksum == checksum;
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Applies the cached data of {@code pieceIndex} (whose WAL marker has arrived) to this task's own
   * writers, exactly like the write node applies the chunk-data PIECE, then records the piece as
   * applied so retries and the continuity check are idempotent.
   */
  void applyCachedPiece(DataRegion dataRegion, long pieceIndex, long checksum)
      throws IOException, PageException, LoadFileException {
    taskLock.lock();
    try {
      checkNotClosed();
      if (isPieceAlreadyApplied(pieceIndex, checksum)) {
        cachedPieces.remove(pieceIndex);
        return;
      }
      final CachedPiece cached = cachedPieces.get(pieceIndex);
      if (cached == null || cached.checksum != checksum) {
        throw new IOException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_LOAD_CONSENSUS_PIECE_DATA_MISSING_OR_CHECKSUM_MISMATCH_AFTER_PULL_35F4972E,
                getTaskName(),
                pieceIndex));
      }
      appendChunkPieceAndRecord(dataRegion, cached.dataList, pieceIndex, checksum);
      cachedPieces.remove(pieceIndex);
    } finally {
      taskLock.unlock();
    }
  }

  void retainPiece(long pieceIndex, byte[] serializedPiece) {
    taskLock.lock();
    try {
      retainedPieces.put(pieceIndex, serializedPiece);
      writeRetainedPieceToDisk(pieceIndex, serializedPiece);
    } finally {
      taskLock.unlock();
    }
  }

  Optional<byte[]> getRetainedPiece(long pieceIndex) {
    taskLock.lock();
    try {
      final byte[] inMemory = retainedPieces.get(pieceIndex);
      if (inMemory != null) {
        return Optional.of(inMemory);
      }
      final File pieceFile = retainedPieceFile(pieceIndex);
      if (!pieceFile.isFile()) {
        return Optional.empty();
      }
      try {
        final byte[] fromDisk = Files.readAllBytes(pieceFile.toPath());
        retainedPieces.put(pieceIndex, fromDisk);
        return Optional.of(fromDisk);
      } catch (IOException e) {
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_RETAINED_PIECE_READ_FAILED_0659D19B,
            pieceIndex,
            getTaskName(),
            pieceFile,
            e.getMessage());
        return Optional.empty();
      }
    } finally {
      taskLock.unlock();
    }
  }

  void clearRetainedPieces() {
    taskLock.lock();
    try {
      retainedPieces.clear();
      cachedPieces.clear();
      deleteRetainedPieceFiles();
    } finally {
      taskLock.unlock();
    }
  }

  void recordAppliedPiece(long pieceIndex, long checksum) {
    taskLock.lock();
    try {
      recordAppliedPieceUnlocked(pieceIndex, checksum);
      persistTaskMeta();
    } finally {
      taskLock.unlock();
    }
  }

  /** Number of chunk pieces already applied to this task, for the PREPARE reconciliation. */
  long getAppliedPieceCount() {
    taskLock.lock();
    try {
      return appliedPieceIndex2Checksum.size();
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Order-sensitive aggregate of every applied piece checksum, for the PREPARE reconciliation. The
   * pieces are folded in ascending piece-index order so a swapped or reordered piece changes the
   * result, matching the coordinator's {@link
   * org.apache.iotdb.db.queryengine.plan.scheduler.load.RegionConsensusContext} accumulation.
   */
  long getAppliedPiecesChecksum() {
    taskLock.lock();
    try {
      return computeAppliedPiecesChecksumUnlocked();
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Verifies that the pieces applied on this node match the PREPARE summary accumulated by the
   * coordinator: the applied piece count and the order-sensitive aggregate of every applied piece
   * checksum must equal the expected values. A mismatch means a piece was lost, replaced or
   * reordered on this node (e.g. the write node switched mid-load and this node never received some
   * markers), so the staged file must not be sealed or loaded silently.
   */
  boolean verifyAppliedPieces(int expectedCount, long expectedChecksum) {
    taskLock.lock();
    try {
      if (appliedPieceIndex2Checksum.size() != expectedCount) {
        return false;
      }
      return computeAppliedPiecesChecksumUnlocked() == expectedChecksum;
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Folds {@link #appliedPieceIndex2Checksum} in ascending piece-index order with {@link
   * LoadTsFileChecksumUtils#combine(long, long)}. Caller must hold {@link #taskLock}.
   */
  private long computeAppliedPiecesChecksumUnlocked() {
    long checksum = 0;
    for (long pieceIndex = 0; pieceIndex < appliedPieceIndex2Checksum.size(); pieceIndex++) {
      final Long pieceChecksum = appliedPieceIndex2Checksum.get(pieceIndex);
      if (pieceChecksum == null) {
        // A hole in the applied prefix: the aggregate cannot be computed, which is itself a
        // mismatch (a non-contiguous task must never pass PREPARE).
        return Long.MIN_VALUE;
      }
      checksum = LoadTsFileChecksumUtils.combine(checksum, pieceChecksum);
    }
    return checksum;
  }

  /** Whether this task was rebuilt from raw byte refs (legacy WAL format without piece records). */
  boolean isLegacyRawRefTask() {
    taskLock.lock();
    try {
      return !rawTsFiles.isEmpty();
    } finally {
      taskLock.unlock();
    }
  }

  private void recordAppliedPieceUnlocked(long pieceIndex, long checksum) {
    appliedPieceIndex2Checksum.put(pieceIndex, checksum);
    while (appliedPieceIndex2Checksum.containsKey(appliedContiguousCount)) {
      appliedContiguousCount++;
    }
  }

  private File retainedPiecesDir() {
    return new File(taskDir, RETAINED_PIECES_DIR_NAME);
  }

  private File retainedPieceFile(long pieceIndex) {
    return new File(retainedPiecesDir(), "piece-" + pieceIndex + ".bin");
  }

  private void writeRetainedPieceToDisk(long pieceIndex, byte[] bytes) {
    final File piecesDir = retainedPiecesDir();
    if (!piecesDir.isDirectory() && !piecesDir.mkdirs()) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_RETAINED_PIECE_WRITE_FAILED_99697608,
          pieceIndex,
          getTaskName(),
          piecesDir,
          "failed to create directory");
      return;
    }
    final File target = retainedPieceFile(pieceIndex);
    final File tmp = new File(piecesDir, target.getName() + ".tmp");
    try {
      Files.write(
          tmp.toPath(), bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
      Files.move(
          tmp.toPath(),
          target.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_RETAINED_PIECE_WRITE_FAILED_99697608,
          pieceIndex,
          getTaskName(),
          target,
          e.getMessage());
    }
  }

  private void loadRetainedPiecesFromDisk() {
    final File piecesDir = retainedPiecesDir();
    final File[] files = piecesDir.isDirectory() ? piecesDir.listFiles() : null;
    if (files == null) {
      return;
    }
    for (final File file : files) {
      if (!file.isFile()
          || !file.getName().startsWith("piece-")
          || !file.getName().endsWith(".bin")) {
        continue;
      }
      final String name = file.getName();
      try {
        final long pieceIndex =
            Long.parseLong(name.substring("piece-".length(), name.length() - ".bin".length()));
        retainedPieces.put(pieceIndex, Files.readAllBytes(file.toPath()));
      } catch (IOException | NumberFormatException e) {
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_RETAINED_PIECE_READ_FAILED_0659D19B,
            name,
            getTaskName(),
            file,
            e.getMessage());
      }
    }
  }

  private void deleteRetainedPieceFiles() {
    final File piecesDir = retainedPiecesDir();
    if (!piecesDir.isDirectory()) {
      return;
    }
    final File[] files = piecesDir.listFiles();
    if (files != null) {
      for (final File file : files) {
        try {
          Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
          LOGGER.warn(MESSAGE_DELETE_FAIL, file, e);
        }
      }
    }
    try {
      Files.deleteIfExists(piecesDir.toPath());
    } catch (IOException e) {
      LOGGER.warn(MESSAGE_DELETE_FAIL, piecesDir, e);
    }
  }

  boolean belongsTo(DataRegion dataRegion) {
    taskLock.lock();
    try {
      for (PartitionContext context : partitionContexts.values()) {
        if (context.belongsTo(dataRegion)) {
          return true;
        }
      }
      return false;
    } finally {
      taskLock.unlock();
    }
  }

  public void writeChunk(ChunkData chunkData, DataRegion dataRegion)
      throws IOException, PageException, LoadFileException {
    taskLock.lock();
    try {
      final DataPartitionInfo partitionInfo =
          new DataPartitionInfo(dataRegion, chunkData.getTimePartitionSlot());
      write(partitionInfo, chunkData);
    } finally {
      taskLock.unlock();
    }
  }

  public void writeDeletion(DeletionData deletionData, DataRegion dataRegion) throws IOException {
    taskLock.lock();
    try {
      applyDeletionToContexts(dataRegion, deletionData);
    } finally {
      taskLock.unlock();
    }
  }

  /** Legacy direct-load path: writes every piece datum through the typed write helpers. */
  void writePieceNode(DataRegion dataRegion, LoadTsFilePieceNode pieceNode)
      throws IOException, PageException, LoadFileException {
    taskLock.lock();
    try {
      checkNotClosed();
      for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
        if (tsFileData.getType() == TsFileDataType.CHUNK) {
          writeChunk((ChunkData) tsFileData, dataRegion);
        } else if (tsFileData.getType() == TsFileDataType.DELETION) {
          writeDeletion((DeletionData) tsFileData, dataRegion);
        } else {
          throw new IOException(
              StorageEngineMessages.UNSUPPORTED_TSFILE_DATA_TYPE + tsFileData.getType());
        }
      }
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * It should be noted that all AlignedChunkData of the same partition split from a source file
   * should be guaranteed to be written to the same new file. Otherwise, for detached
   * BatchedAlignedChunkData, it may result in no data for the time column in the new file.
   */
  private void write(DataPartitionInfo partitionInfo, ChunkData chunkData)
      throws IOException, PageException, LoadFileException {
    checkNotClosed();
    final PartitionContext context = getOrCreatePartitionContext(partitionInfo);
    if (context == null) {
      // The staged file already exists; the original behavior logs the error and skips the chunk.
      return;
    }

    final IDeviceID device = chunkData.getDevice();
    final IDeviceID lastDevice = context.getCurrentWritingDevice();
    if (!Objects.equals(device, lastDevice)) {
      if (lastDevice != null && device2Partition.containsKey(lastDevice)) {
        final Set<DataPartitionInfo> partitions = device2Partition.get(lastDevice);
        for (DataPartitionInfo partition : new ArrayList<>(partitions)) {
          final PartitionContext partitionContext = partitionContexts.get(partition);
          if (partitionContext != null && partitionContext.getCurrentWritingDevice() != null) {
            partitionContext.endChunkGroupAndCheckMetadataSize();
          }
        }
        device2Partition.remove(lastDevice);
      }
      context.startChunkGroup(device);
      device2Partition.computeIfAbsent(device, k -> new HashSet<>()).add(partitionInfo);
    }

    context.writeChunk(chunkData);
  }

  private PartitionContext getOrCreatePartitionContext(DataPartitionInfo partitionInfo)
      throws IOException {
    PartitionContext context = partitionContexts.get(partitionInfo);
    if (context != null) {
      return context;
    }

    final long chunkMetadataMaxSizeForEachWriter =
        CONFIG.getLoadChunkMetadataMemorySizeInBytes() / (partitionContexts.size() + 1);
    context = PartitionContext.create(partitionInfo, taskDir, chunkMetadataMaxSizeForEachWriter);
    if (context == null) {
      return null;
    }

    // When a new writer is added, we need to reduce the metadata size limit of all existing
    // writers for memory control
    for (final PartitionContext existingContext : partitionContexts.values()) {
      existingContext.getWriter().setMaxMetadataSize(chunkMetadataMaxSizeForEachWriter);
    }
    partitionContexts.put(partitionInfo, context);
    return context;
  }

  private void applyDeletionToContexts(DataRegion dataRegion, DeletionData deletionData)
      throws IOException {
    checkNotClosed();
    for (final PartitionContext context : partitionContexts.values()) {
      if (context.belongsTo(dataRegion)) {
        context.writeDeletion(deletionData);
      }
    }
  }

  /**
   * Applies a consensus PIECE to this task. Chunk data is written into the single writer of each
   * affected partition, then the chunk group is ended and the newly written byte range captured so
   * it can be synced to replicas (or rebuilt from the WAL) at the exact offset of the final file;
   * deletion data is routed to the modification files of the matching partitions.
   */
  void appendChunkPiece(DataRegion dataRegion, List<TsFileData> dataList)
      throws IOException, PageException, LoadFileException {
    taskLock.lock();
    try {
      checkNotClosed();
      appendChunkPieceUnlocked(dataRegion, dataList);
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Appends a consensus PIECE and records it as applied under the same task lock. Making the file
   * capture (inside {@link #appendChunkPieceUnlocked}) and the applied-piece map atomic with
   * respect to snapshotting guarantees a snapshot never observes the staged-file prefix ahead of
   * the applied-piece prefix (or vice versa), which would otherwise fork the restored file on
   * replay.
   */
  void appendChunkPieceAndRecord(
      DataRegion dataRegion, List<TsFileData> dataList, long pieceIndex, long checksum)
      throws IOException, PageException, LoadFileException {
    taskLock.lock();
    try {
      checkNotClosed();
      appendChunkPieceUnlocked(dataRegion, dataList);
      recordAppliedPieceUnlocked(pieceIndex, checksum);
      // Force every staged file before persisting the applied-piece prefix: a restart must never
      // observe an applied prefix whose bytes are not durable, otherwise it would rebuild the task
      // from a prefix that overstates the staged data.
      for (final PartitionContext context : partitionContexts.values()) {
        context.forceStagedFile();
      }
      // Persist the applied-piece prefix durably right after so a restart can rebuild this task
      // from disk instead of discarding the load.
      persistTaskMeta();
    } finally {
      taskLock.unlock();
    }
  }

  private void appendChunkPieceUnlocked(DataRegion dataRegion, List<TsFileData> dataList)
      throws IOException, PageException, LoadFileException {
    restorePartitionWriters(dataRegion);
    for (final TsFileData data : dataList) {
      if (data.getType() == TsFileDataType.CHUNK) {
        final ChunkData chunkData = (ChunkData) data;
        final DataPartitionInfo partitionInfo =
            new DataPartitionInfo(dataRegion, chunkData.getTimePartitionSlot());
        write(partitionInfo, chunkData);
      } else if (data.getType() == TsFileDataType.DELETION) {
        writeDeletion((DeletionData) data, dataRegion);
      } else {
        throw new IOException(StorageEngineMessages.UNSUPPORTED_TSFILE_DATA_TYPE + data.getType());
      }
    }
  }

  /**
   * Rebuilds an in-memory {@link TsFileIOWriter} for every non-finalized staged partition file that
   * was restored from a snapshot, so this node can keep appending subsequent chunk-data pieces to
   * the same file. The rebuild uses the unsealed-TsFile recovery mechanism ({@link
   * org.apache.tsfile.write.writer.RestorableTsFileIOWriter}), the same writer class the DataRegion
   * uses to continue an interrupted flush, and truncates the restored file to its last complete
   * chunk-group boundary. Finalized files (footer already written) are left untouched and loaded
   * directly at COMMIT.
   */
  private void restorePartitionWriters(DataRegion dataRegion) throws IOException {
    if (restoredPartitions.isEmpty()) {
      return;
    }
    final List<Map.Entry<RestoredPartitionKey, RestoredLoadFile>> toRestore = new ArrayList<>();
    for (final Map.Entry<RestoredPartitionKey, RestoredLoadFile> entry :
        restoredPartitions.entrySet()) {
      final RestoredPartitionKey key = entry.getKey();
      final RestoredLoadFile restored = entry.getValue();
      if (restored.finalized
          || !dataRegion.getDatabaseName().equals(restored.database)
          || !dataRegion.getDataRegionIdString().equals(restored.regionId)) {
        continue;
      }
      if (hasPartitionContext(dataRegion, key.timePartitionStart)) {
        // This partition already has a live writer (e.g. a snapshot fragment merged into a task
        // that kept writing); leave the live context in charge.
        continue;
      }
      toRestore.add(entry);
    }
    if (toRestore.isEmpty()) {
      return;
    }
    final long chunkMetadataMaxSizeForEachWriter =
        CONFIG.getLoadChunkMetadataMemorySizeInBytes() / Math.max(1, toRestore.size());
    for (final Map.Entry<RestoredPartitionKey, RestoredLoadFile> entry : toRestore) {
      final RestoredPartitionKey key = entry.getKey();
      final RestoredLoadFile restored = entry.getValue();
      final DataPartitionInfo partitionInfo =
          new DataPartitionInfo(dataRegion, new TTimePartitionSlot(key.timePartitionStart));
      final PartitionContext context =
          PartitionContext.restore(
              partitionInfo, taskDir, restored.file, chunkMetadataMaxSizeForEachWriter);
      if (context == null) {
        continue;
      }
      partitionContexts.put(partitionInfo, context);
      // The file is now writer-managed; the COMMIT path loads it through the writer contexts.
      restoredPartitions.remove(entry.getKey());
    }
  }

  private boolean hasPartitionContext(DataRegion dataRegion, long timePartitionStart) {
    for (final DataPartitionInfo partitionInfo : partitionContexts.keySet()) {
      if (partitionInfo.getDataRegion() == dataRegion
          && partitionInfo.getTimePartitionSlot().getStartTime() == timePartitionStart) {
        return true;
      }
    }
    return false;
  }

  /** Applies only the deletion data of a raw-ref PIECE to the matching partition files. */
  void applyDeletion(DataRegion dataRegion, List<TsFileData> dataList)
      throws IOException, PageException, LoadFileException {
    taskLock.lock();
    try {
      checkNotClosed();
      for (final TsFileData data : dataList) {
        if (data.getType() == TsFileDataType.DELETION) {
          writeDeletion((DeletionData) data, dataRegion);
        }
      }
    } finally {
      taskLock.unlock();
    }
  }

  /** Ends every partition file (chunk groups + footer) and captures the final byte ranges. */
  void finalizeAll() throws IOException {
    taskLock.lock();
    try {
      checkNotClosed();
      for (final PartitionContext context : partitionContexts.values()) {
        context.finalizeFile(pendingPieceRefs);
      }
      // PREPARE sealed the staged files; persist the finalized flag so a restart can bind the
      // sealed files at COMMIT instead of re-opening them as unsealed writers.
      persistTaskMeta();
    } finally {
      taskLock.unlock();
    }
  }

  void appendRawTsFilePieces(
      DataRegion dataRegion, List<LoadTsFileConsensusNode.PieceRef> pieceRefs) throws IOException {
    taskLock.lock();
    try {
      checkNotClosed();
      for (final LoadTsFileConsensusNode.PieceRef ref : pieceRefs) {
        final String relativePath = ref.getRelativePath();
        final File targetFile =
            SystemFileFactory.INSTANCE.getFile(taskDir, new File(relativePath).getName());
        if (isManagedByLiveWriter(targetFile)) {
          // The partition file was already written by this node's single partition writer from the
          // chunk-data PIECE; the reference only needs to be applied by nodes that rebuild the file
          // from the WAL (no in-memory writer).
          continue;
        }
        byte[] content = ref.getContent();
        if (content == null) {
          content = readFileRange(relativePath, ref.getOffset(), (int) ref.getSize());
        }
        if (targetFile.getParentFile() != null) {
          Files.createDirectories(targetFile.getParentFile().toPath());
        }
        try (final FileChannel channel =
            FileChannel.open(
                targetFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
          final long currentLength = channel.size();
          if (currentLength != ref.getOffset()) {
            // Last line of defense on the replica side: the leader guarantees the refs advance
            // continuously from offset 0, so a hole or overlap here means the staged file cannot
            // be repaired by appending and must be aborted instead of corrupted.
            throw new IOException(
                String.format(
                    StorageEngineMessages
                        .EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_NOT_CONTINUOUS_F9408C19,
                    targetFile,
                    taskDir.getName(),
                    ref.getOffset(),
                    currentLength));
          }
          channel.position(ref.getOffset());
          final ByteBuffer buffer = ByteBuffer.wrap(content);
          while (buffer.hasRemaining()) {
            // FileChannel.write does not guarantee a full write on one call (short write under
            // load); loop until every byte is written, then verify the resulting length.
            channel.write(buffer);
          }
          channel.force(true);
          if (channel.size() != ref.getOffset() + content.length) {
            throw new IOException(
                String.format(
                    StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_SHORT_WRITE_E7392FAD,
                    targetFile,
                    taskDir.getName(),
                    ref.getOffset(),
                    ref.getOffset() + content.length,
                    channel.size()));
          }
        }
        if (rawTsFilePaths.add(targetFile.getAbsolutePath())) {
          rawTsFiles.add(new RawTsFile(targetFile, dataRegion));
        }
      }
    } finally {
      taskLock.unlock();
    }
  }

  private boolean isManagedByLiveWriter(File targetFile) {
    for (final PartitionContext context : partitionContexts.values()) {
      if (context.getWriter().getFile().getAbsolutePath().equals(targetFile.getAbsolutePath())) {
        return true;
      }
    }
    return false;
  }

  private byte[] readFileRange(String relativePath, long offset, int length) throws IOException {
    final byte[] content = new byte[length];
    if (length == 0) {
      return content;
    }
    final String message =
        String.format(
            StorageEngineMessages
                .MESSAGE_NO_LOAD_TSFILE_UUID_ARG_RECORDED_EXECUTE_LOAD_COMMAND_ARG_66722D80,
            relativePath);
    final File file =
        LoadTsFileManager.findLoadTsFile(relativePath).orElseThrow(() -> new IOException(message));
    try (final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      channel.position(offset);
      final ByteBuffer buffer = ByteBuffer.wrap(content);
      while (buffer.hasRemaining()) {
        final int read = channel.read(buffer);
        if (read < 0) {
          throw new IOException(
              String.format(
                  StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_EOF_8743387D,
                  relativePath,
                  offset));
        }
      }
    }
    return content;
  }

  List<LoadTsFileConsensusNode.PieceRef> drainPendingPieceRefs() {
    taskLock.lock();
    try {
      final List<LoadTsFileConsensusNode.PieceRef> refs = new ArrayList<>(pendingPieceRefs);
      pendingPieceRefs.clear();
      return refs;
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Copies the already-synced byte prefix of every staged partition file of this task into the
   * snapshot task dir and returns the metadata needed to restore it.
   */
  LoadSnapshotManager.TaskSnapshot snapshotTask(File targetDir) throws IOException {
    taskLock.lock();
    try {
      final List<StagedFileSnapshot> snapshots = new ArrayList<>();
      for (final PartitionContext context : partitionContexts.values()) {
        final StagedFileSnapshot snapshot = context.snapshotTo(targetDir);
        if (snapshot != null) {
          snapshots.add(snapshot);
        }
      }
      final StringBuilder appliedPieces = new StringBuilder();
      for (final Map.Entry<Long, Long> entry : appliedPieceIndex2Checksum.entrySet()) {
        appliedPieces.append(entry.getKey()).append(':').append(entry.getValue()).append(',');
      }
      return new LoadSnapshotManager.TaskSnapshot(snapshots, appliedPieces.toString());
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Seeds the applied-piece prefix captured by a snapshot. The staged files restored from the
   * snapshot already contain the data of every piece in the prefix, so the continuity fence must
   * treat them as applied; otherwise the first replayed marker after a failover would be rejected
   * as non-contiguous even though the file is complete up to that point.
   */
  void restoreAppliedPieces(String serialized) {
    taskLock.lock();
    try {
      if (serialized == null || serialized.isEmpty()) {
        return;
      }
      for (final String entry : serialized.split(",")) {
        if (entry.isEmpty()) {
          continue;
        }
        final int separator = entry.indexOf(':');
        if (separator <= 0) {
          continue;
        }
        try {
          appliedPieceIndex2Checksum.put(
              Long.parseLong(entry.substring(0, separator)),
              Long.parseLong(entry.substring(separator + 1)));
        } catch (NumberFormatException e) {
          LOGGER.warn(
              StorageEngineMessages.LOG_LOAD_CONSENSUS_APPLIED_PIECE_RESTORE_FAILED_5BC74BBA,
              getTaskName(),
              entry);
        }
      }
      appliedContiguousCount = 0;
      while (appliedPieceIndex2Checksum.containsKey(appliedContiguousCount)) {
        appliedContiguousCount++;
      }
    } finally {
      taskLock.unlock();
    }
  }

  void registerRestoredPartitions(List<StagedFileSnapshot> stagedFiles) {
    taskLock.lock();
    try {
      for (StagedFileSnapshot snapshot : stagedFiles) {
        final File file = new File(taskDir, snapshot.getFileName());
        if (!file.isFile() || file.length() == 0) {
          continue;
        }
        restoredPartitions.put(
            new RestoredPartitionKey(
                snapshot.getDatabase(), snapshot.getRegionId(), snapshot.getTimePartitionStart()),
            new RestoredLoadFile(
                file, snapshot.getDatabase(), snapshot.getRegionId(), snapshot.isFinalized()));
      }
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Collects the durable bookkeeping of this task in the same format as the snapshot meta: every
   * staged partition file (live writers plus files restored from a snapshot) and the applied-piece
   * prefix. Caller must hold {@link #taskLock}.
   */
  private LoadSnapshotManager.TaskSnapshot collectTaskMeta() {
    final List<StagedFileSnapshot> stagedFiles = new ArrayList<>();
    for (final PartitionContext context : partitionContexts.values()) {
      stagedFiles.add(
          new StagedFileSnapshot(
              context.getWriter().getFile().getName(),
              context.getDatabaseName(),
              context.getRegionId(),
              context.getTimePartitionStart(),
              context.isFinalized()));
    }
    for (final Map.Entry<RestoredPartitionKey, RestoredLoadFile> entry :
        restoredPartitions.entrySet()) {
      final RestoredLoadFile restored = entry.getValue();
      stagedFiles.add(
          new StagedFileSnapshot(
              restored.file.getName(),
              restored.database,
              restored.regionId,
              entry.getKey().timePartitionStart,
              restored.finalized));
    }
    final StringBuilder appliedPieces = new StringBuilder();
    for (final Map.Entry<Long, Long> entry : appliedPieceIndex2Checksum.entrySet()) {
      appliedPieces.append(entry.getKey()).append(':').append(entry.getValue()).append(',');
    }
    return new LoadSnapshotManager.TaskSnapshot(stagedFiles, appliedPieces.toString());
  }

  /**
   * Persists the task meta ({@value #TASK_META_NAME}) next to the staged files. It is the source of
   * truth for a restart: the applied-piece prefix makes marker replay idempotent and the staged
   * file list lets the next startup rebuild the unsealed writers. A failure only costs the resume
   * capability (the load then fails loudly at PREPARE and the coordinator aborts it); it never
   * corrupts the staged data itself.
   */
  void persistTaskMeta() {
    taskLock.lock();
    try {
      final File metaFile = new File(taskDir, TASK_META_NAME);
      try {
        LoadSnapshotManager.writeSnapshotMeta(metaFile, collectTaskMeta());
        // Force the meta so the applied prefix is durable together with the forced staged bytes.
        try (final FileChannel channel =
            FileChannel.open(metaFile.toPath(), StandardOpenOption.WRITE)) {
          channel.force(true);
        }
      } catch (IOException e) {
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_TASK_META_WRITE_FAILED_5D2420BF,
            getTaskName(),
            metaFile,
            e.getMessage());
      }
    } finally {
      taskLock.unlock();
    }
  }

  /**
   * Rebuilds the in-memory applied-piece prefix and the restored-partition registry from the task
   * meta left by the previous session, so an in-progress LOAD survives a restart: replayed markers
   * are idempotent and the next PIECE re-opens the unsealed staged files at the exact durable
   * boundary. Called by the constructor when the task dir is reused (restart or snapshot restore).
   */
  private void recoverFromDisk() {
    final File metaFile = new File(taskDir, TASK_META_NAME);
    if (!metaFile.isFile()) {
      // No durable bookkeeping: the task is resumable only when nothing was staged yet. A dir with
      // staged files but no meta (e.g. a torn first piece or a legacy raw-ref task) cannot be
      // reconciled, so it must not be resumed.
      recoveredFromDisk = !hasStagedFilesOnDisk();
      return;
    }
    try {
      final LoadSnapshotManager.TaskSnapshot snapshot =
          LoadSnapshotManager.parseSnapshotMeta(metaFile);
      restoreAppliedPieces(snapshot.getAppliedPieces());
      registerRestoredPartitions(snapshot.getStagedFiles());
      persistTaskMeta();
      recoveredFromDisk = true;
    } catch (IOException e) {
      LOGGER.warn(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_TASK_META_FAILED_C39E04BB,
          getTaskName(),
          e.getMessage());
      recoveredFromDisk = false;
    }
  }

  /** Whether the task dir already carries staged partition files that are not covered by a meta. */
  private boolean hasStagedFilesOnDisk() {
    final File[] files = taskDir.listFiles();
    if (files == null) {
      return false;
    }
    for (final File file : files) {
      if (file.isFile() && file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Closes every writer channel and modification file without deleting the staged files. Used by
   * the graceful-shutdown path so an in-progress LOAD survives a restart; the next startup's {@code
   * recoverFromDisk()} re-opens the files with the unsealed-TsFile recovery mechanism.
   */
  void closeForShutdown() {
    taskLock.lock();
    try {
      if (isClosed) {
        return;
      }
      for (final PartitionContext context : partitionContexts.values()) {
        context.closeWriterOnly();
      }
      partitionContexts.clear();
      device2Partition.clear();
      pendingPieceRefs.clear();
      rawTsFiles.clear();
      rawTsFilePaths.clear();
      restoredPartitions.clear();
      cachedPieces.clear();
      retainedPieces.clear();
      isClosed = true;
    } finally {
      taskLock.unlock();
    }
  }

  void loadAll(
      DataRegion dataRegion,
      boolean isGeneratedByPipe,
      Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    taskLock.lock();
    try {
      checkNotClosed();
      for (final PartitionContext context : partitionContexts.values()) {
        context.closeModificationFile();
      }
      for (final PartitionContext context : partitionContexts.values()) {
        context.loadIntoRegion(
            isGeneratedByPipe,
            timePartitionProgressIndexMap.getOrDefault(
                context.getTimePartitionSlot(), MinimumProgressIndex.INSTANCE));
      }
      for (final RawTsFile rawTsFile : rawTsFiles) {
        if (isManagedByLiveWriter(rawTsFile.file)) {
          continue;
        }
        final TsFileResource tsFileResource = new TsFileResource(rawTsFile.file);
        try (final TsFileSequenceReader reader =
            new TsFileSequenceReader(rawTsFile.file.getAbsolutePath(), true)) {
          if (!reader.isComplete()) {
            throw new LoadFileException(
                String.format(
                    StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_INCOMPLETE_1CDE954B,
                    rawTsFile.file,
                    taskDir.getName()));
          }
          TsFileResourceUtils.updateTsFileResource(reader, tsFileResource);
        }
        tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
        tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
        // Legacy raw-ref files carry no time-partition slot, so their progress cannot be mapped
        // back to the coordinator's per-slot progress; fall back to MinimumProgressIndex.
        tsFileResource.setProgressIndex(MinimumProgressIndex.INSTANCE);
        rawTsFile.dataRegion.loadNewTsFile(
            tsFileResource, true, isGeneratedByPipe, false, Optional.empty());
      }
      // Cached pieces and leader-retained bytes are no longer needed after COMMIT.
      clearRetainedPieces();
      // Staged files restored from a snapshot have no in-memory writer. Files that were re-synced
      // by PIECE refs after the restore were already added to rawTsFiles and are skipped here;
      // the remaining files (e.g. the load was sealed before the snapshot) are loaded directly.
      for (final Map.Entry<RestoredPartitionKey, RestoredLoadFile> entry :
          restoredPartitions.entrySet()) {
        final RestoredLoadFile restored = entry.getValue();
        if (rawTsFilePaths.contains(restored.file.getAbsolutePath())) {
          continue;
        }
        if (dataRegion == null
            || !dataRegion.getDatabaseName().equals(restored.database)
            || !dataRegion.getDataRegionIdString().equals(restored.regionId)) {
          continue;
        }
        final TsFileResource tsFileResource = new TsFileResource(restored.file);
        final ProgressIndex progressIndex =
            timePartitionProgressIndexMap.getOrDefault(
                new TTimePartitionSlot(entry.getKey().timePartitionStart),
                MinimumProgressIndex.INSTANCE);
        try (final TsFileSequenceReader reader =
            new TsFileSequenceReader(restored.file.getAbsolutePath(), true)) {
          if (!reader.isComplete()) {
            throw new LoadFileException(
                String.format(
                    StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_INCOMPLETE_1CDE954B,
                    restored.file,
                    taskDir.getName()));
          }
          TsFileResourceUtils.updateTsFileResource(reader, tsFileResource);
        }
        tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
        tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
        tsFileResource.setProgressIndex(progressIndex);
        dataRegion.loadNewTsFile(tsFileResource, true, isGeneratedByPipe, false, Optional.empty());
        rawTsFilePaths.add(restored.file.getAbsolutePath());
      }
    } finally {
      taskLock.unlock();
    }
  }

  /** Closes every writer, deletes the staged/raw files and finally the task directory. */
  void close() {
    taskLock.lock();
    try {
      if (isClosed) {
        return;
      }
      clearRetainedPieces();
      for (final PartitionContext context : partitionContexts.values()) {
        context.close();
      }
      partitionContexts.clear();

      for (final RawTsFile rawTsFile : rawTsFiles) {
        try {
          final Path path = rawTsFile.file.toPath();
          if (Files.exists(path)) {
            RetryUtils.retryOnException(
                () -> {
                  Files.delete(path);
                  return null;
                });
          }
        } catch (IOException e) {
          LOGGER.warn(
              StorageEngineMessages.CLOSE_TSFILE_IO_WRITER_ERROR, rawTsFile.file.getPath(), e);
        }
      }
      rawTsFiles.clear();
      rawTsFilePaths.clear();

      for (final RestoredLoadFile restored : restoredPartitions.values()) {
        if (rawTsFilePaths.contains(restored.file.getAbsolutePath())) {
          continue;
        }
        try {
          final Path path = restored.file.toPath();
          if (Files.exists(path)) {
            RetryUtils.retryOnException(
                () -> {
                  Files.delete(path);
                  return null;
                });
          }
        } catch (IOException e) {
          LOGGER.warn(
              StorageEngineMessages.CLOSE_TSFILE_IO_WRITER_ERROR, restored.file.getPath(), e);
        }
      }
      restoredPartitions.clear();

      try {
        RetryUtils.retryOnException(
            () -> {
              Files.delete(taskDir.toPath());
              return null;
            });
      } catch (DirectoryNotEmptyException e) {
        LOGGER.info(StorageEngineMessages.TASK_DIR_NOT_EMPTY_SKIP_DELETE, taskDir.getPath());
      } catch (IOException e) {
        LOGGER.warn(MESSAGE_DELETE_FAIL, taskDir.getPath(), e);
      }
      isClosed = true;
    } finally {
      taskLock.unlock();
    }
  }

  private void checkNotClosed() throws IOException {
    if (isClosed) {
      throw new IOException(String.format(MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED, taskDir));
    }
  }

  private static class RawTsFile {
    private final File file;
    private final DataRegion dataRegion;

    private RawTsFile(File file, DataRegion dataRegion) {
      this.file = file;
      this.dataRegion = dataRegion;
    }
  }

  private static class RestoredLoadFile {
    private final File file;
    private final String database;
    private final String regionId;
    private final boolean finalized;

    private RestoredLoadFile(File file, String database, String regionId, boolean finalized) {
      this.file = file;
      this.database = database;
      this.regionId = regionId;
      this.finalized = finalized;
    }
  }

  /** One piece cached on a follower, waiting for its WAL marker to arrive. */
  private static class CachedPiece {
    private final long pieceIndex;
    private final long checksum;
    private final List<TsFileData> dataList;

    private CachedPiece(long pieceIndex, long checksum, List<TsFileData> dataList) {
      this.pieceIndex = pieceIndex;
      this.checksum = checksum;
      this.dataList = dataList;
    }
  }

  private static class RestoredPartitionKey {
    private final String database;
    private final String regionId;
    private final long timePartitionStart;

    private RestoredPartitionKey(String database, String regionId, long timePartitionStart) {
      this.database = database;
      this.regionId = regionId;
      this.timePartitionStart = timePartitionStart;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RestoredPartitionKey that = (RestoredPartitionKey) o;
      return timePartitionStart == that.timePartitionStart
          && database.equals(that.database)
          && regionId.equals(that.regionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(database, regionId, timePartitionStart);
    }
  }
}
