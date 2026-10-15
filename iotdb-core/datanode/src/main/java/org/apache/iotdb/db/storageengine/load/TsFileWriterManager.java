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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusOp;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.load.metrics.LoadPointCountMetrics;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkPayloadRef;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.TsFilePrecalculatedChunkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Manages the physical writing, crash recovery, and consensus transitions (PIECE, PREPARE, COMMIT,
 * ABORT) of staged files for a single LOAD task within one DataRegion.
 */
final class TsFileWriterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileWriterManager.class);

  private final DataRegion dataRegion;
  private final File taskDir;

  /** Physical writer instances appending chunks to staged TsFiles per data partition. */
  private final Map<TTimePartitionSlot, TsFilePrecalculatedChunkWriter> timePartition2Writer =
      new ConcurrentHashMap<>();

  /** Metadata resource tracking time indices and progress indices per data partition. */
  private final Map<TTimePartitionSlot, TsFileResource> timePartition2Resource =
      new ConcurrentHashMap<>();

  /** Resumption ledger tracking recorded chunks and physical byte coverage. */
  private final Map<TTimePartitionSlot, LoadTsFileProgress> timePartition2Progress =
      new ConcurrentHashMap<>();

  /** The length of every staged file as last published: the next piece refs only its growth. */
  private final Map<File, Long> publishedLengths = new HashMap<>();

  /** Exclusive modification file handles writing deletions for staged TsFiles. */
  private final Map<TTimePartitionSlot, ModificationFile> timePartition2ModificationFile =
      new ConcurrentHashMap<>();

  private volatile long currentSearchIndex = -1L;
  private volatile boolean isClosed;

  /** When this task last received a command; a new or recovered task counts as heard from now. */
  private volatile long lastCommandTimeMillis = System.currentTimeMillis();

  TsFileWriterManager(final DataRegion dataRegion, final File taskDir) {
    this.dataRegion =
        Objects.requireNonNull(
            dataRegion, StorageEngineMessages.EXCEPTION_DATAREGION_CANNOT_BE_NULL_0B936879);
    this.taskDir =
        Objects.requireNonNull(
            taskDir, StorageEngineMessages.EXCEPTION_TASKDIR_CANNOT_BE_NULL_11671FD9);
    this.isClosed = false;

    ensureDir(taskDir);
  }

  File getTaskDir() {
    return taskDir;
  }

  boolean hasPendingTsFiles() {
    return !timePartition2Resource.isEmpty();
  }

  boolean hasStagedData() {
    return !timePartition2Writer.isEmpty() || !timePartition2ModificationFile.isEmpty();
  }

  // -------------------------------------------------------------------------
  // Piece Writing Pipeline
  // -------------------------------------------------------------------------

  /**
   * Applies chunks and deletions of an incoming piece stamped with the given consensus search
   * index.
   */
  List<LoadTsFileConsensusNode.PieceRef> writePiece(
      final List<TsFileData> tsFileDataList, final long searchIndex)
      throws IOException, PageException {
    this.currentSearchIndex = searchIndex;
    touch();
    return writePiece(tsFileDataList);
  }

  /** Records that a command of this task arrived now. */
  void touch() {
    lastCommandTimeMillis = System.currentTimeMillis();
  }

  long getLastCommandTimeMillis() {
    return lastCommandTimeMillis;
  }

  /**
   * Writes all TsFileData records, flushes memory buffers to disk, and computes generated payload
   * refs.
   */
  List<LoadTsFileConsensusNode.PieceRef> writePiece(final List<TsFileData> tsFileDataList)
      throws IOException, PageException {
    checkNotClosed();

    for (final TsFileData tsFileData : tsFileDataList) {
      switch (tsFileData.getType()) {
        case CHUNK:
          final ChunkData chunkData = (ChunkData) tsFileData;
          write(chunkData.getTimePartitionSlot(), chunkData);
          break;
        case DELETION:
          writeDeletion((DeletionData) tsFileData);
          break;
        default:
          throw new IOException(
              StorageEngineMessages.UNSUPPORTED_TSFILE_DATA_TYPE + tsFileData.getType());
      }
    }

    // Nothing is flushed while a piece is written: its bytes go straight to the file. Once the
    // whole piece is there, its files are forced, before the records that describe them follow.
    forceStagedFiles();
    flushRecordedChunks();
    return publishPieceRefs();
  }

  /** One fsync per staged file of the piece, after every chunk of that piece is written. */
  private void forceStagedFiles() throws IOException {
    for (final TsFilePrecalculatedChunkWriter writer : timePartition2Writer.values()) {
      writer.getOutput().force();
    }
  }

  private void flushRecordedChunks() throws IOException {
    for (final LoadTsFileProgress progress : timePartition2Progress.values()) {
      progress.flushQueuedRecords();
    }
  }

  /**
   * Records the command this task ended with, and the consensus index it reached, in the progress
   * file of every staged file: that tail is what a restart reads to release the directory instead
   * of resuming the finished task.
   */
  void recordTerminal(
      final LoadTsFileConsensusOp terminalOp,
      final long searchIndex,
      final Map<TTimePartitionSlot, Long> timePartition2ExpectedDataEnd)
      throws IOException {
    for (final Map.Entry<TTimePartitionSlot, LoadTsFileProgress> entry :
        timePartition2Progress.entrySet()) {
      final TsFilePrecalculatedChunkWriter writer = timePartition2Writer.get(entry.getKey());
      final LoadTsFileProgress progress = entry.getValue();
      final long expectedDataEnd = expectedDataEndOf(timePartition2ExpectedDataEnd, entry.getKey());
      if (writer == null
          || !canMarkTerminal(terminalOp, progress, writer.getFile().length(), expectedDataEnd)) {
        LOGGER.warn(
            StorageEngineMessages
                .LOG_STAGED_FILE_ARG_OF_THE_LOAD_TASK_IS_NOT_CONTINUOUS_SO_IT_CANNOT_BE_MARKED_ARG_AND_THE_TERMINAL_RECORD_IS_WITHHELD_7B21F365,
            writer == null
                ? progress.getProgressFile().getAbsolutePath()
                : writer.getFile().getAbsolutePath(),
            terminalOp);
        return;
      }
    }
    for (final LoadTsFileProgress progress : timePartition2Progress.values()) {
      progress.recordTerminal(terminalOp, searchIndex);
    }
  }

  /** The end the coordinator expects of a staged file, or -1 when it announced none. */
  private static long expectedDataEndOf(
      final Map<TTimePartitionSlot, Long> timePartition2ExpectedDataEnd,
      final TTimePartitionSlot timePartitionSlot) {
    return timePartition2ExpectedDataEnd == null
        ? -1L
        : timePartition2ExpectedDataEnd.getOrDefault(timePartitionSlot, -1L);
  }

  /** Whether a staged file is in the state the command a task ended with may describe. */
  static boolean canMarkTerminal(
      final LoadTsFileConsensusOp terminalOp,
      final LoadTsFileProgress progress,
      final long fileLength,
      final long expectedDataEnd)
      throws IOException {
    // The coordinator's expectation is what the replica cannot work out: a missing piece leaves no
    // record behind, so the file only looks shorter.
    if (expectedDataEnd >= 0L && fileLength < expectedDataEnd) {
      return false;
    }
    // An aborted task never imports its bytes, so only a committed one has to be complete; both
    // have
    // to be continuous and to reach the end of their records.
    return terminalOp == LoadTsFileConsensusOp.COMMIT
        ? progress.isReady(fileLength)
        : progress.getFirstHoleOffset() < 0L && fileLength >= progress.getTotalLength();
  }

  /** Writes a chunk into the target staged file or skips if the byte range was already recorded. */
  private void write(final TTimePartitionSlot timePartitionSlot, final ChunkData chunkData)
      throws IOException, PageException {
    checkNotClosed();

    TsFilePrecalculatedChunkWriter writer = timePartition2Writer.get(timePartitionSlot);
    if (writer == null) {
      writer = openWriter(timePartitionSlot);
    }

    final LoadTsFileProgress progress = timePartition2Progress.get(timePartitionSlot);
    final ChunkData.ChunkLayout layout = chunkData.getChunkLayout();
    if (layout == null) {
      throw new IOException(StorageEngineMessages.EXCEPTION_CHUNK_LAYOUT_IS_MISSING_E87C71C0);
    }

    long chunkOffset = layout.offset();
    final List<Chunk> chunks = chunkData.getChunks();
    final List<ChunkPayloadRef> incomingRefs = chunkData.getChunkPayloadRefs();
    final boolean payloadInMemory = incomingRefs.isEmpty();

    if (!payloadInMemory && incomingRefs.size() != chunks.size()) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_LOAD_PIECE_OF_THE_TASK_ARG_ARRIVED_WITHOUT_ITS_CHUNK_PAYLOAD_ARG_04664404,
              taskDir,
              incomingRefs.size() + " of " + chunks.size()));
    }

    final List<ChunkPayloadRef> chunkPayloadRefs = new ArrayList<>(chunks.size());
    for (int i = 0; i < chunks.size(); i++) {
      final Chunk chunk = chunks.get(i);
      final int chunkHeaderSize = chunk.getHeader().getSerializedSize();
      final int payloadSize =
          payloadInMemory
              ? chunk.getData().remaining()
              : Math.toIntExact(incomingRefs.get(i).getSize());
      final long chunkLength = chunkHeaderSize + (long) payloadSize;

      if (progress.hasChunkAt(chunkOffset)) {
        LOGGER.info(
            StorageEngineMessages
                .LOG_SKIPPING_THE_CHUNKS_OF_ARG_BECAUSE_THEIR_PAYLOAD_IS_ALREADY_STAGED_IN_ARG_OF_THE_LOAD_TASK_ARG_BAFEEE84,
            chunkData.getDevice(),
            writer.getFile().getAbsolutePath(),
            taskDir);
        chunkPayloadRefs.add(
            payloadInMemory
                ? new ChunkPayloadRef(
                    LoadStagingDirs.recordedPath(writer.getFile()),
                    chunkOffset + chunkHeaderSize,
                    payloadSize)
                : incomingRefs.get(i));
      } else if (!payloadInMemory) {
        throw new IOException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_LOAD_PIECE_OF_THE_TASK_ARG_ARRIVED_WITHOUT_ITS_CHUNK_PAYLOAD_ARG_04664404,
                taskDir,
                incomingRefs.get(i)));
      } else {
        final boolean firstChunkOfGroup = layout.firstChunkOfGroup() && i == 0;
        final TsFilePrecalculatedChunkWriter.ChunkWriteResult writeResult =
            writer.writeChunk(
                chunkData.getDevice(),
                chunkData.isAligned(),
                layout.chunkGroupHeaderOffset(),
                firstChunkOfGroup,
                chunk,
                chunkOffset);

        final long actualPhysicalStart =
            firstChunkOfGroup
                ? writeResult.actualChunkGroupHeaderOffset()
                : writeResult.actualChunkOffset();

        chunkPayloadRefs.add(
            new ChunkPayloadRef(
                LoadStagingDirs.recordedPath(writer.getFile()),
                writeResult.actualChunkEndOffset() - payloadSize,
                payloadSize));

        progress.queueChunk(
            chunkData.getDevice(),
            chunkData.isAligned(),
            writeResult.actualChunkGroupHeaderOffset(),
            writeResult.actualChunkOffset(),
            firstChunkOfGroup,
            chunk,
            writeResult.actualChunkEndOffset(),
            actualPhysicalStart,
            currentSearchIndex);
      }
      chunkOffset += chunkLength;
    }

    chunkData.setChunkPayloadRefs(chunkPayloadRefs);
  }

  /**
   * Opens the staged writer atomically, safely propagating checked IOExceptions out of
   * computeIfAbsent.
   */
  private TsFilePrecalculatedChunkWriter openWriter(final TTimePartitionSlot timePartitionSlot)
      throws IOException {
    final AtomicReference<IOException> openFailure = new AtomicReference<>();
    final TsFilePrecalculatedChunkWriter writer =
        timePartition2Writer.computeIfAbsent(
            timePartitionSlot,
            info -> {
              try {
                return createStagedWriter(info);
              } catch (final IOException e) {
                openFailure.set(e);
                return null;
              }
            });
    if (openFailure.get() != null) {
      throw openFailure.get();
    }
    return writer;
  }

  /**
   * Initializes the staged TsFile, resource, and progress file on disk. Fails fast if the file
   * exists.
   */
  private TsFilePrecalculatedChunkWriter createStagedWriter(
      final TTimePartitionSlot timePartitionSlot) throws IOException {
    final File newTsFile =
        SystemFileFactory.INSTANCE.getFile(
            taskDir, stagedFileName(timePartitionSlot) + TsFileConstant.TSFILE_SUFFIX);
    if (!newTsFile.createNewFile()) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_THE_STAGED_FILE_ARG_OF_LOAD_TASK_ARG_ALREADY_EXISTS_BUT_NO_WRITER_COULD_RESUME_IT_SO_THE_PIECE_CANNOT_BE_STAGED_D6AB3A06,
              newTsFile.getPath(),
              uuidDirName(newTsFile)));
    }

    try {
      timePartition2Resource.put(timePartitionSlot, new TsFileResource(newTsFile));
      timePartition2Progress.put(timePartitionSlot, new LoadTsFileProgress(newTsFile));
      final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(newTsFile);
      writer.setRejectHoles(LoadConsensusCompat.usesIoTConsensusV2());
      publishedLengths.put(newTsFile, newTsFile.length());
      return writer;
    } catch (final IOException e) {
      throw new IOException(
          String.format(
              StorageEngineMessages.EXCEPTION_FAILED_TO_INITIALIZE_WRITER_FOR_ARG_ABAF37F1,
              stagedFileName(timePartitionSlot)),
          e);
    }
  }

  /** Dispatches deletion data to matching target partition modification files. */
  private void writeDeletion(final DeletionData deletionData) throws IOException {
    checkNotClosed();

    for (final Map.Entry<TTimePartitionSlot, TsFilePrecalculatedChunkWriter> entry :
        timePartition2Writer.entrySet()) {
      final TTimePartitionSlot timePartitionSlot = entry.getKey();
      final TsFilePrecalculatedChunkWriter writer = entry.getValue();
      final ModificationFile modificationFile =
          timePartition2ModificationFile.computeIfAbsent(
              timePartitionSlot,
              info -> {
                try {
                  final File modsFile = ModificationFile.getExclusiveMods(writer.getFile());
                  if (!modsFile.createNewFile()) {
                    LOGGER.error(
                        StorageEngineMessages
                            .STORAGE_LOG_CAN_NOT_CREATE_MODIFICATIONFILE_FOR_WRITING_17D14C11,
                        modsFile.getPath());
                    return null;
                  }
                  return new ModificationFile(modsFile, false);
                } catch (final IOException e) {
                  throw new IllegalStateException(
                      StorageEngineMessages.EXCEPTION_FAILED_TO_CREATE_MODIFICATION_FILE_0D020A0C,
                      e);
                }
              });

      if (modificationFile != null) {
        deletionData.writeToModificationFile(modificationFile);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Consensus Lifecycle: PREPARE & LOAD
  // -------------------------------------------------------------------------

  /**
   * Seals modification files, validates chunk continuity, and seals TsFiles in the PREPARE round.
   */
  void prepare(
      final boolean isGeneratedByPipe,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap,
      final Map<TTimePartitionSlot, Long> timePartition2ExpectedDataEnd)
      throws IOException, LoadFileException {
    checkNotClosed();
    touch();

    // 1. Close modification files first to freeze deletion views
    for (final ModificationFile modFile : timePartition2ModificationFile.values()) {
      modFile.close();
    }

    // 2. Validate continuity and seal writer metadata zone
    for (final Map.Entry<TTimePartitionSlot, TsFilePrecalculatedChunkWriter> entry :
        timePartition2Writer.entrySet()) {
      final TsFilePrecalculatedChunkWriter writer = entry.getValue();
      if (writer.isSealed()) {
        continue;
      }

      final LoadTsFileProgress progress = timePartition2Progress.get(entry.getKey());
      if (progress != null && progress.exists() && progress.getTotalLength() > 0) {
        final long fileLength = writer.getFile().length();
        final long expectedDataEnd =
            expectedDataEndOf(timePartition2ExpectedDataEnd, entry.getKey());
        if (!progress.isReady(fileLength)
            || (expectedDataEnd >= 0L && fileLength < expectedDataEnd)) {
          throw new LoadFileException(
              String.format(
                  StorageEngineMessages
                      .EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_NOT_CONTINUOUS_F9408C19,
                  writer.getFile().getAbsolutePath(),
                  uuidDirName(writer.getFile()),
                  Math.max(progress.getTotalLength(), expectedDataEnd),
                  fileLength));
        }
      }
      writer.close();

      final TsFileResource tsFileResource = timePartition2Resource.get(entry.getKey());
      tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
      endTsFileResource(
          writer,
          tsFileResource,
          timePartitionProgressIndexMap.getOrDefault(
              entry.getKey(), MinimumProgressIndex.INSTANCE));
    }
  }

  /** Formally imports sealed staged TsFiles into the target storage engine data regions. */
  void loadAll(final boolean isGeneratedByPipe, final boolean deleteStagedSource)
      throws LoadFileException {
    for (final Map.Entry<TTimePartitionSlot, TsFileResource> entry :
        timePartition2Resource.entrySet()) {
      final DataRegion targetDataRegion = dataRegion;
      final TsFileResource tsFileResource = entry.getValue();
      final TsFilePrecalculatedChunkWriter writer = timePartition2Writer.get(entry.getKey());

      if (writer != null && !writer.isSealed()) {
        throw new LoadFileException(
            String.format(
                StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_INCOMPLETE_1CDE954B,
                tsFileResource.getTsFilePath(),
                uuidDirName(tsFileResource.getTsFile())));
      }

      targetDataRegion.loadNewTsFile(
          tsFileResource, deleteStagedSource, isGeneratedByPipe, false, Optional.empty());

      if (writer != null) {
        targetDataRegion
            .getNonSystemDatabaseName()
            .ifPresent(
                databaseName ->
                    LoadPointCountMetrics.updateWritePointCountMetrics(
                        targetDataRegion, databaseName, getTsFileWritePointCount(writer), false));
      }
    }
  }

  /**
   * Finalizes TsFileResource timestamps, updates device time index, caches last values, and
   * serializes resource metadata.
   */
  private void endTsFileResource(
      final TsFilePrecalculatedChunkWriter writer,
      final TsFileResource tsFileResource,
      final ProgressIndex progressIndex)
      throws IOException {
    Map<IDeviceID, Map<String, TimeValuePair>> deviceLastValues = null;
    if (IoTDBDescriptor.getInstance().getConfig().isCacheLastValuesForLoad()) {
      deviceLastValues = new HashMap<>();
    }
    final AtomicLong lastValuesMemCost = new AtomicLong(0);

    for (final Map.Entry<IDeviceID, List<IChunkMetadata>> entry :
        writer.getChunkMetadataListMap().entrySet()) {
      final IDeviceID device = entry.getKey();
      for (final IChunkMetadata chunkMetadata : entry.getValue()) {
        tsFileResource.updateStartTime(device, chunkMetadata.getStartTime());
        tsFileResource.updateEndTime(device, chunkMetadata.getEndTime());

        if (deviceLastValues != null) {
          final Map<String, TimeValuePair> deviceMap =
              deviceLastValues.computeIfAbsent(
                  device,
                  d -> {
                    final Map<String, TimeValuePair> map = new HashMap<>();
                    lastValuesMemCost.addAndGet(RamUsageEstimator.shallowSizeOf(map));
                    lastValuesMemCost.addAndGet(device.ramBytesUsed());
                    return map;
                  });

          final int prevSize = deviceMap.size();
          deviceMap.compute(
              chunkMetadata.getMeasurementUid(),
              (m, oldPair) -> {
                if (oldPair != null && oldPair.getTimestamp() > chunkMetadata.getEndTime()) {
                  return oldPair;
                }
                final TsPrimitiveType lastValue =
                    chunkMetadata.getStatistics() != null
                            && chunkMetadata.getDataType() != TSDataType.BLOB
                        ? TsPrimitiveType.getByType(
                            chunkMetadata.getDataType() == TSDataType.VECTOR
                                ? TSDataType.INT64
                                : chunkMetadata.getDataType(),
                            chunkMetadata.getStatistics().getLastValue())
                        : null;
                final TimeValuePair timeValuePair =
                    lastValue != null
                        ? new TimeValuePair(chunkMetadata.getEndTime(), lastValue)
                        : null;
                if (oldPair != null) {
                  lastValuesMemCost.addAndGet(-oldPair.getSize());
                }
                if (timeValuePair != null) {
                  lastValuesMemCost.addAndGet(timeValuePair.getSize());
                }
                return timeValuePair;
              });

          final int afterSize = deviceMap.size();
          lastValuesMemCost.addAndGet(
              (long) (afterSize - prevSize) * RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);

          if (lastValuesMemCost.get()
              > IoTDBDescriptor.getInstance().getConfig().getCacheLastValuesMemoryBudgetInByte()) {
            deviceLastValues = null;
          }
        }
      }
    }

    if (deviceLastValues != null) {
      final Map<IDeviceID, List<Pair<String, TimeValuePair>>> finalDeviceLastValues =
          new HashMap<>(deviceLastValues.size());
      for (final Map.Entry<IDeviceID, Map<String, TimeValuePair>> entry :
          deviceLastValues.entrySet()) {
        final List<Pair<String, TimeValuePair>> pairList =
            entry.getValue().entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        finalDeviceLastValues.put(entry.getKey(), pairList);
      }
      tsFileResource.setLastValues(finalDeviceLastValues);
    }

    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    tsFileResource.setProgressIndex(progressIndex);
    tsFileResource.serialize();
  }

  private long getTsFileWritePointCount(final TsFilePrecalculatedChunkWriter writer) {
    return writer.getChunkMetadataListMap().values().stream()
        .flatMap(List::stream)
        .mapToLong(chunkMetadata -> chunkMetadata.getStatistics().getCount())
        .sum();
  }

  // -------------------------------------------------------------------------
  // Crash Recovery & State Reconstruction
  // -------------------------------------------------------------------------

  /**
   * Scans staged task directory, repairs torn tails, truncates half-written bytes, and resumes
   * writers.
   */
  TsFileWriterManager recoverFromDisk() throws IOException {
    final File[] files = taskDir.listFiles(LoadTsFileProgress.PROGRESS_FILE_FILTER);
    if (files == null) {
      return this;
    }

    for (final File progressFile : files) {
      final String progressName = progressFile.getName();
      final File tsFile =
          new File(
              progressFile.getParentFile(),
              progressName.substring(
                  0, progressName.length() - LoadTsFileProgress.PROGRESS_SUFFIX.length()));
      if (!tsFile.isFile()) {
        continue;
      }

      final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);
      try {
        progress.readAllRecordsRepairingTornTail();
      } catch (final IOException e) {
        LOGGER.warn(
            StorageEngineMessages
                .LOG_LOAD_CONSENSUS_RECOVER_TASK_UNRESUMABLE_ARG_FROM_STAGED_FILE_ARG_3AF462A6,
            uuidDirName(tsFile),
            tsFile.getAbsolutePath(),
            e);
        continue;
      }

      final List<LoadTsFileProgress.ChunkRangeRecord> records = progress.readAllRecords();
      if (records.isEmpty()) {
        LOGGER.warn(
            StorageEngineMessages
                .LOG_LOAD_CONSENSUS_RECOVER_TASK_UNRESUMABLE_ARG_FROM_STAGED_FILE_ARG_3AF462A6,
            uuidDirName(tsFile),
            tsFile.getAbsolutePath());
        continue;
      }

      final TTimePartitionSlot timePartitionSlot =
          new TTimePartitionSlot(parseTimePartitionStart(tsFile.getName()));
      final long resumeOffset = progress.getTotalLength();
      if (tsFile.length() > resumeOffset) {
        try (final FileChannel truncatingChannel =
            FileChannel.open(tsFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
          truncatingChannel.truncate(resumeOffset);
        }
        LOGGER.warn(
            StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_SHORT_WRITE_E7392FAD,
            tsFile.getAbsolutePath(),
            uuidDirName(tsFile),
            resumeOffset,
            resumeOffset);
      } else if (tsFile.length() < resumeOffset) {
        LOGGER.warn(
            StorageEngineMessages
                .LOG_LOAD_CONSENSUS_RECOVER_TASK_UNRESUMABLE_ARG_FROM_STAGED_FILE_ARG_3AF462A6,
            uuidDirName(tsFile),
            tsFile.getAbsolutePath());
        continue;
      }

      FileChannel channel = null;
      try {
        channel =
            FileChannel.open(tsFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.position(resumeOffset);
        final TsFilePrecalculatedChunkWriter writer =
            new TsFilePrecalculatedChunkWriter(tsFile, channel);
        writer.setRejectHoles(LoadConsensusCompat.usesIoTConsensusV2());
        writer.restoreChunkMetadata(restoreChunkMetadata(records));
        publishedLengths.put(tsFile, tsFile.length());

        timePartition2Writer.put(timePartitionSlot, writer);
        timePartition2Resource.put(timePartitionSlot, new TsFileResource(tsFile));
        timePartition2Progress.put(timePartitionSlot, progress);

        final File modificationFile = ModificationFile.getExclusiveMods(tsFile);
        if (modificationFile.isFile()) {
          timePartition2ModificationFile.put(
              timePartitionSlot, new ModificationFile(modificationFile, false));
          LOGGER.info(
              StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_RESTORED_MODIFICATION_5F4D7D89,
              uuidDirName(tsFile),
              modificationFile.getAbsolutePath());
        }

        LOGGER.info(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_RESUMED_WRITER_176BEE0F,
            uuidDirName(tsFile),
            resumeOffset);
      } catch (final IOException e) {
        if (channel != null) {
          try {
            channel.close();
          } catch (final IOException ignored) {
            // Suppress secondary close exceptions during recovery abandonment
          }
        }
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_TASK_META_FAILED_C39E04BB,
            uuidDirName(tsFile),
            e.getMessage());
      }
    }
    return this;
  }

  /**
   * Replays serialized chunk headers and statistics to restore writer metadata state without
   * physical pages.
   */
  private static Map<IDeviceID, Map<String, List<IChunkMetadata>>> restoreChunkMetadata(
      final List<LoadTsFileProgress.ChunkRangeRecord> records) throws IOException {
    final Map<IDeviceID, Map<String, List<IChunkMetadata>>> device2Measurement2ChunkMetadata =
        new HashMap<>();

    for (final LoadTsFileProgress.ChunkRangeRecord record : records) {
      final ChunkHeader chunkHeader =
          LoadTsFileProgress.deserializeChunkHeader(record.chunkType(), record.chunkHeaderBody());
      final Statistics<?> statistics =
          LoadTsFileProgress.deserializeStatistics(record.dataType(), record.statisticsBody());

      final ChunkMetadata chunkMetadata =
          new ChunkMetadata(
              chunkHeader.getMeasurementID(),
              chunkHeader.getDataType(),
              chunkHeader.getEncodingType(),
              chunkHeader.getCompressionType(),
              record.chunkOffset(),
              statistics);
      chunkMetadata.setMask(
          (byte)
              (chunkHeader.getChunkType()
                  & (TsFileConstant.TIME_COLUMN_MASK | TsFileConstant.VALUE_COLUMN_MASK)));

      device2Measurement2ChunkMetadata
          .computeIfAbsent(record.deviceId(), ignored -> new HashMap<>())
          .computeIfAbsent(chunkHeader.getMeasurementID(), ignored -> new ArrayList<>())
          .add(chunkMetadata);
    }
    return device2Measurement2ChunkMetadata;
  }

  // -------------------------------------------------------------------------
  // Cleanup & Teardown
  // -------------------------------------------------------------------------

  void close() {
    close(false);
  }

  /**
   * Closes all writers, releases system handles, and cleans up staged files unless retained for
   * lagging followers.
   */
  void close(final boolean retainStagedFiles) {
    if (isClosed) {
      return;
    }
    isClosed = true;

    for (final TsFilePrecalculatedChunkWriter writer : timePartition2Writer.values()) {
      try {
        writer.getOutput().close();
        if (!retainStagedFiles) {
          deleteIfExistsWithRetry(writer.getFile().toPath());
        }
      } catch (final IOException e) {
        LOGGER.warn(
            StorageEngineMessages.CLOSE_TSFILE_IO_WRITER_ERROR, writer.getFile().getPath(), e);
      }
    }

    for (final ModificationFile modificationFile : timePartition2ModificationFile.values()) {
      try {
        modificationFile.close();
        if (!retainStagedFiles) {
          deleteIfExistsWithRetry(modificationFile.getFile().toPath());
        }
      } catch (final IOException e) {
        LOGGER.warn(
            StorageEngineMessages.CLOSE_MODIFICATION_FILE_ERROR, modificationFile.getFile(), e);
      }
    }

    for (final LoadTsFileProgress progress : timePartition2Progress.values()) {
      if (retainStagedFiles) {
        // The progress file goes with the staged file it describes: it carries the terminal record
        // that tells a restart the task is finished.
        continue;
      }
      try {
        deleteIfExistsWithRetry(progress.getProgressFile().toPath());
      } catch (final IOException e) {
        LOGGER.warn(
            StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD,
            progress.getProgressFile().getAbsolutePath(),
            e);
      }
    }

    if (!retainStagedFiles) {
      try {
        deleteIfExistsWithRetry(taskDir.toPath());
      } catch (final DirectoryNotEmptyException e) {
        LOGGER.info(StorageEngineMessages.TASK_DIR_NOT_EMPTY_SKIP_DELETE, taskDir.getPath());
      } catch (final IOException e) {
        LOGGER.warn(StorageEngineMessages.LOG_FAILED_TO_DELETE_ARG_3A7BD6FD, taskDir.getPath(), e);
      }
    }

    timePartition2Writer.clear();
    timePartition2Resource.clear();
    timePartition2Progress.clear();
    timePartition2ModificationFile.clear();
  }

  // -------------------------------------------------------------------------
  // Helpers & Data Partition Record
  // -------------------------------------------------------------------------

  private void checkNotClosed() throws IOException {
    if (isClosed) {
      throw new IOException(
          String.format(
              StorageEngineMessages.EXCEPTION_TSFILEWRITERMANAGER_OF_ARG_HAS_BEEN_CLOSED_2FA43AAB,
              taskDir));
    }
  }

  private void ensureDir(final File dir) {
    if (!dir.exists() && dir.mkdirs()) {
      LOGGER.info(StorageEngineMessages.LOAD_TSFILE_DIR_CREATED, dir.getPath());
    }
  }

  private static void deleteIfExistsWithRetry(final Path path) throws IOException {
    if (Files.exists(path)) {
      RetryUtils.retryOnException(
          () -> {
            Files.delete(path);
            return null;
          });
    }
  }

  /** The name a staged file of this region and time partition has. */
  private String stagedFileName(final TTimePartitionSlot timePartitionSlot) {
    return String.join(
        IoTDBConstant.FILE_NAME_SEPARATOR,
        dataRegion.getDatabaseName(),
        dataRegion.getDataRegionIdString(),
        Long.toString(timePartitionSlot.getStartTime()));
  }

  private static String uuidDirName(final File tsFile) {
    final File parent = tsFile.getParentFile();
    return parent == null ? tsFile.getName() : parent.getName();
  }

  private static long parseTimePartitionStart(final String tsFileName) {
    final String baseName = tsFileName.substring(0, tsFileName.lastIndexOf('.'));
    final int lastDash = baseName.lastIndexOf(IoTDBConstant.FILE_NAME_SEPARATOR);
    return Long.parseLong(baseName.substring(lastDash + 1));
  }

  /** The bytes this piece added to each staged file, as the refs a replica reads them back with. */
  private List<LoadTsFileConsensusNode.PieceRef> publishPieceRefs() {
    final List<LoadTsFileConsensusNode.PieceRef> refs = new ArrayList<>();
    for (final TsFilePrecalculatedChunkWriter writer : timePartition2Writer.values()) {
      final File file = writer.getFile();
      final long published = publishedLengths.getOrDefault(file, 0L);
      final long size = file.length() - published;
      publishedLengths.put(file, file.length());
      if (size > 0) {
        refs.add(
            new LoadTsFileConsensusNode.PieceRef(
                LoadStagingDirs.recordedPath(file), published, size));
      }
    }
    return refs;
  }
}
