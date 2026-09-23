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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The staged state of a single LOAD task of one DataRegion.
 *
 * <p>It owns the directory the pieces of that task are written into, the staged TsFile of every
 * time partition, the {@link LoadTsFileProgress} bitmaps that make out-of-order pieces resumable,
 * and the transitions the consensus protocol drives: PIECE writes, PREPARE seals the files, COMMIT
 * imports them into the region and ABORT discards them.
 *
 * <p>It never decides on its own whether the staged bytes may be dropped: a finished task is handed
 * to {@link LoadTaskRetention} by {@link LoadTsFileManager}, which keeps the directory alive for as
 * long as a follower may still read the referenced payloads back.
 */
final class TsFileWriterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileWriterManager.class);

  private static final String MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED =
      "%s TsFileWriterManager has been closed.";

  /** The region whose partitions are written, and which imports the staged files on COMMIT. */
  private final DataRegion dataRegion;

  private final File taskDir;
  private Map<DataPartitionInfo, TsFilePrecalculatedChunkWriter> dataPartition2Writer;
  private Map<DataPartitionInfo, TsFileResource> dataPartition2Resource;
  private Map<DataPartitionInfo, LoadTsFileProgress> dataPartition2Progress;
  private Map<DataPartitionInfo, ModificationFile> dataPartition2ModificationFile;

  private boolean isClosed;

  TsFileWriterManager(final DataRegion dataRegion, final File taskDir) {
    this.dataRegion = dataRegion;
    this.taskDir = taskDir;
    this.dataPartition2Writer = new HashMap<>();
    this.dataPartition2Resource = new HashMap<>();
    this.dataPartition2Progress = new HashMap<>();
    this.dataPartition2ModificationFile = new HashMap<>();
    this.isClosed = false;

    ensureDir(taskDir);
  }

  /** The staged directory this task owns. */
  File getTaskDir() {
    return taskDir;
  }

  /** Writes the chunks and deletions of one piece, and reports where their payloads landed. */
  /** The consensus index the entries written for the piece being applied belong to. */
  private long currentSearchIndex = -1L;

  List<LoadTsFileConsensusNode.PieceRef> writePiece(
      final List<TsFileData> tsFileDataList, final long searchIndex)
      throws IOException, PageException {
    this.currentSearchIndex = searchIndex;
    return writePiece(tsFileDataList);
  }

  List<LoadTsFileConsensusNode.PieceRef> writePiece(final List<TsFileData> tsFileDataList)
      throws IOException, PageException {
    final Map<File, Long> previousLengths = snapshotFileLengths(taskDir);
    for (TsFileData tsFileData : tsFileDataList) {
      switch (tsFileData.getType()) {
        case CHUNK:
          ChunkData chunkData = (ChunkData) tsFileData;
          write(new DataPartitionInfo(dataRegion, chunkData.getTimePartitionSlot()), chunkData);
          break;
        case DELETION:
          writeDeletion(dataRegion, (DeletionData) tsFileData);
          break;
        default:
          throw new IOException(
              StorageEngineMessages.UNSUPPORTED_TSFILE_DATA_TYPE + tsFileData.getType());
      }
    }
    flush();
    return createPieceRefs(taskDir, previousLengths);
  }

  private void ensureDir(File dir) {
    if (!dir.exists() && dir.mkdirs()) {
      LOGGER.info(StorageEngineMessages.LOAD_TSFILE_DIR_CREATED, dir.getPath());
    }
  }

  TsFileWriterManager recoverFromDisk() throws IOException {
    final File[] files = taskDir.listFiles();
    if (files == null) {
      return this;
    }
    for (final File progressFile : files) {
      if (!progressFile.getName().endsWith(LoadTsFileProgress.PROGRESS_SUFFIX)) {
        continue;
      }
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
      // The recorded ranges live in the progress file next to the staged file, while a freshly
      // built LoadTsFileProgress only knows the records of the pieces this run has written. They
      // have to be read back before the ranges are consulted, otherwise every staged file of a
      // previous run looks like it holds no complete chunk at all and is dropped instead of being
      // resumed.
      try {
        // A progress file that ends in the middle of an entry is what a snapshot of this directory
        // can hold, because the snapshot is taken while the pieces are still being applied: the
        // fragment is dropped and the entries before it are resumed, see
        // LoadTsFileProgress#readAllRecordsRepairingTornTail.
        progress.readAllRecordsRepairingTornTail();
      } catch (final IOException e) {
        // An unreadable progress file is as good as no progress file: the staged bytes cannot be
        // attributed to chunks any more, so the task is not resumed and the next command recreates
        // it.
        LOGGER.warn(
            StorageEngineMessages
                .LOG_LOAD_CONSENSUS_RECOVER_TASK_UNRESUMABLE_ARG_FROM_STAGED_FILE_ARG_3AF462A6,
            uuidDirName(tsFile),
            tsFile.getAbsolutePath(),
            e);
        continue;
      }
      // A piece can arrive out of order, so the file may hold a hole that belongs to a piece which
      // has not reached this node yet. Those bytes are kept: the file is written by absolute
      // offsets that every replica computes from the content of the pieces, so the missing piece
      // lands where its own offsets say, exactly as it would have before the restart. Dropping
      // everything behind an open hole would throw away the pieces that did arrive.
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
      final DataPartitionInfo partitionInfo = new DataPartitionInfo(dataRegion, timePartitionSlot);

      // The file is continued after the last chunk that was recorded, so that whatever an
      // interrupted write left beyond it is dropped rather than kept as a half written chunk.
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
        // The recorded chunk ranges claim more bytes than the file holds, so the file cannot be
        // trusted: continuing it would overwrite or duplicate chunks that are already there.
        LOGGER.warn(
            StorageEngineMessages
                .LOG_LOAD_CONSENSUS_RECOVER_TASK_UNRESUMABLE_ARG_FROM_STAGED_FILE_ARG_3AF462A6,
            uuidDirName(tsFile),
            tsFile.getAbsolutePath());
        continue;
      }

      final TsFilePrecalculatedChunkWriter writer;
      FileChannel channel = null;
      try {
        channel =
            FileChannel.open(tsFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.position(resumeOffset);
        writer = new TsFilePrecalculatedChunkWriter(tsFile, channel);
      } catch (final IOException e) {
        if (channel != null) {
          try {
            channel.close();
          } catch (final IOException ignored) {
            // The recovery of this partition is abandoned anyway.
          }
        }
        LOGGER.warn(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_TASK_META_FAILED_C39E04BB,
            uuidDirName(tsFile),
            e.getMessage());
        continue;
      }
      // The metadata zone of an unfinished staged file does not exist yet, so the writer has to
      // be
      // handed back the chunk metadata that was persisted while the chunks were written.
      // Otherwise it would seal the file with an empty metadata zone and orphan everything
      // already
      // on disk.
      writer.restoreChunkMetadata(restoreChunkMetadata(records));

      dataPartition2Writer.put(partitionInfo, writer);
      dataPartition2Resource.put(partitionInfo, new TsFileResource(tsFile));
      dataPartition2Progress.put(partitionInfo, progress);

      final File modificationFile = ModificationFile.getExclusiveMods(tsFile);
      if (modificationFile.isFile()) {
        dataPartition2ModificationFile.put(
            partitionInfo, new ModificationFile(modificationFile, false));
        LOGGER.info(
            StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_RESTORED_MODIFICATION_5F4D7D89,
            uuidDirName(tsFile),
            modificationFile.getAbsolutePath());
      }
      LOGGER.info(
          StorageEngineMessages.LOG_LOAD_CONSENSUS_RECOVER_RESUMED_WRITER_176BEE0F,
          uuidDirName(tsFile),
          resumeOffset);
    }
    return this;
  }

  /** The LOAD task a staged file belongs to, which is the name of the directory holding it. */
  private static String uuidDirName(final File tsFile) {
    final File parent = tsFile.getParentFile();
    return parent == null ? tsFile.getName() : parent.getName();
  }

  /**
   * Rebuilds the chunk metadata of a resumed staged file from the records that were persisted next
   * to it while its chunks were written.
   *
   * <p>The records are replayed in physical order, because the order of a measurement's chunk list
   * determines the order its chunk metadata is serialized in. The chunk header and the statistics
   * are stored as raw bytes precisely so that this step does not need the original pages.
   */
  private static Map<IDeviceID, Map<String, List<IChunkMetadata>>> restoreChunkMetadata(
      final List<LoadTsFileProgress.ChunkRangeRecord> records) throws IOException {
    final Map<IDeviceID, Map<String, List<IChunkMetadata>>> device2Measurement2ChunkMetadata =
        new HashMap<>();
    for (final LoadTsFileProgress.ChunkRangeRecord record : records) {
      final ChunkHeader chunkHeader =
          LoadTsFileProgress.deserializeChunkHeader(record.chunkType(), record.chunkHeaderBytes());
      final Statistics<?> statistics =
          LoadTsFileProgress.deserializeStatistics(record.dataType(), record.statisticsBytes());

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

  boolean hasPendingTsFiles() {
    return !dataPartition2Resource.isEmpty();
  }

  private long parseTimePartitionStart(final String tsFileName) {
    final String baseName = tsFileName.substring(0, tsFileName.lastIndexOf('.'));
    final int lastDash = baseName.lastIndexOf(IoTDBConstant.FILE_NAME_SEPARATOR);
    return Long.parseLong(baseName.substring(lastDash + 1));
  }

  /**
   * It should be noted that all AlignedChunkData of the same partition split from a source file
   * should be guaranteed to be written to the same new file. Otherwise, for detached
   * BatchedAlignedChunkData, it may result in no data for the time column in the new file.
   */
  @SuppressWarnings("squid:S3824")
  private void write(DataPartitionInfo partitionInfo, ChunkData chunkData)
      throws IOException, PageException {
    if (isClosed) {
      throw new IOException(String.format(MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED, taskDir));
    }
    if (!dataPartition2Writer.containsKey(partitionInfo)) {
      File newTsFile =
          SystemFileFactory.INSTANCE.getFile(
              taskDir, partitionInfo.toString() + TsFileConstant.TSFILE_SUFFIX);
      if (!newTsFile.createNewFile()) {
        // The file is there although this manager holds no writer for it, which is what a staged
        // file that could not be resumed leaves behind. The chunks of this piece cannot be written
        // anywhere else, because their offsets belong to that file, so the piece fails instead of
        // being dropped: a replica that loses a piece without reporting anything would import a
        // file that is missing it, and nothing downstream could tell.
        throw new IOException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_THE_STAGED_FILE_ARG_OF_LOAD_TASK_ARG_ALREADY_EXISTS_BUT_NO_WRITER_COULD_RESUME_IT_SO_THE_PIECE_CANNOT_BE_STAGED_D6AB3A06,
                newTsFile.getPath(),
                uuidDirName(newTsFile)));
      }

      final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(newTsFile);
      final TsFileResource resource = new TsFileResource(newTsFile);
      final LoadTsFileProgress progress = new LoadTsFileProgress(newTsFile);
      dataPartition2Writer.put(partitionInfo, writer);
      dataPartition2Resource.put(partitionInfo, resource);
      dataPartition2Progress.put(partitionInfo, progress);
    }
    final TsFilePrecalculatedChunkWriter writer = dataPartition2Writer.get(partitionInfo);
    final LoadTsFileProgress progress = dataPartition2Progress.get(partitionInfo);
    final ChunkData.ChunkLayout layout = chunkData.getChunkLayout();
    if (layout == null) {
      throw new IOException(StorageEngineMessages.EXCEPTION_CHUNK_LAYOUT_IS_MISSING_E87C71C0);
    }
    long chunkOffset = layout.offset();
    final List<Chunk> chunks = chunkData.getChunks();
    // A piece that arrives without its payload was already written here: its chunks are recorded
    // at the very offsets it would write them to, which is what the loop below checks, so the
    // references it carries are all it needs.
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
      final int chunkHeaderSize = getChunkHeaderSerializedSize(chunk.getHeader());
      final int payloadSize =
          payloadInMemory
              ? chunk.getData().remaining()
              : Math.toIntExact(incomingRefs.get(i).getSize());
      final long chunkLength = chunkHeaderSize + (long) payloadSize;
      if (progress.hasChunkAt(chunkOffset)) {
        // The bytes of this chunk are already in the staged file at the offset this write would
        // use,
        // so it is written once and every repetition is a no-op. The recorded range is exactly
        // where
        // the first write put it, so its reference can be handed over unchanged.
        LOGGER.info(
            StorageEngineMessages
                .LOG_SKIPPING_THE_CHUNKS_OF_ARG_BECAUSE_THEIR_PAYLOAD_IS_ALREADY_STAGED_IN_ARG_OF_THE_LOAD_TASK_ARG_BAFEEE84,
            chunkData.getDevice(),
            writer.getFile().getAbsolutePath(),
            taskDir);
        chunkPayloadRefs.add(
            payloadInMemory
                ? new ChunkPayloadRef(
                    writer.getFile().getAbsolutePath(), chunkOffset + chunkHeaderSize, payloadSize)
                : incomingRefs.get(i));
      } else if (!payloadInMemory) {
        // Whoever sent this piece was supposed to read the payload back from its own staged file
        // first, see ChunkPayloadRef, and this node has no chunk at that offset to fall back on.
        throw new IOException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_LOAD_PIECE_OF_THE_TASK_ARG_ARRIVED_WITHOUT_ITS_CHUNK_PAYLOAD_ARG_04664404,
                taskDir,
                incomingRefs.get(i)));
      } else {
        final TsFilePrecalculatedChunkWriter.ChunkWriteResult writeResult =
            writer.writeChunk(
                chunkData.getDevice(),
                chunkData.isAligned(),
                layout.chunkGroupHeaderOffset(),
                layout.firstChunkOfGroup() && i == 0,
                chunk,
                chunkOffset);
        final boolean firstChunkOfGroup = layout.firstChunkOfGroup() && i == 0;
        final long actualPhysicalStart =
            firstChunkOfGroup
                ? writeResult.actualChunkGroupHeaderOffset()
                : writeResult.actualChunkOffset();
        // The payload of the chunk is written last, so it ends where the chunk ends
        chunkPayloadRefs.add(
            new ChunkPayloadRef(
                writer.getFile().getAbsolutePath(),
                writeResult.actualChunkEndOffset() - payloadSize,
                payloadSize));
        progress.recordChunk(
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
    // From now on the payload lives in the staged file only, so the piece that is logged to the
    // WAL records where it is instead of storing it a second time
    chunkData.setChunkPayloadRefs(chunkPayloadRefs);
  }

  private void writeDeletion(DataRegion dataRegion, DeletionData deletionData) throws IOException {
    if (isClosed) {
      throw new IOException(String.format(MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED, taskDir));
    }
    for (Map.Entry<DataPartitionInfo, TsFilePrecalculatedChunkWriter> entry :
        dataPartition2Writer.entrySet()) {
      final DataPartitionInfo partitionInfo = entry.getKey();
      if (partitionInfo.getDataRegion().equals(dataRegion)) {
        final TsFilePrecalculatedChunkWriter writer = entry.getValue();
        if (!dataPartition2ModificationFile.containsKey(partitionInfo)) {
          File newModificationFile = ModificationFile.getExclusiveMods(writer.getFile());
          if (!newModificationFile.createNewFile()) {
            LOGGER.error(
                StorageEngineMessages
                    .STORAGE_LOG_CAN_NOT_CREATE_MODIFICATIONFILE_FOR_WRITING_17D14C11,
                newModificationFile.getPath());
            return;
          }

          dataPartition2ModificationFile.put(
              partitionInfo, new ModificationFile(newModificationFile, false));
        }
        ModificationFile modificationFile = dataPartition2ModificationFile.get(partitionInfo);
        writer.getOutput().flush();
        deletionData.writeToModificationFile(modificationFile);
      }
    }
  }

  private int getChunkHeaderSerializedSize(final ChunkHeader chunkHeader) {
    try (java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream()) {
      return chunkHeader.serializeTo(output);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void flush() throws IOException {
    for (TsFilePrecalculatedChunkWriter writer : dataPartition2Writer.values()) {
      writer.getOutput().flush();
    }
  }

  /**
   * @return true if at least one chunk or deletion of this load reached this node.
   *     <p>A load whose manager holds no writer and no modification file means its pieces never
   *     arrived here (for example the WAL of this node only carried references), so sealing it
   *     would report success while importing nothing.
   */
  boolean hasStagedData() {
    return !dataPartition2Writer.isEmpty() || !dataPartition2ModificationFile.isEmpty();
  }

  void prepare(
      boolean isGeneratedByPipe,
      Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    if (isClosed) {
      throw new IOException(String.format(MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED, taskDir));
    }
    for (final Map.Entry<DataPartitionInfo, ModificationFile> entry :
        dataPartition2ModificationFile.entrySet()) {
      entry.getValue().close();
    }
    for (final Map.Entry<DataPartitionInfo, TsFilePrecalculatedChunkWriter> entry :
        dataPartition2Writer.entrySet()) {
      final TsFilePrecalculatedChunkWriter writer = entry.getValue();
      if (writer.isSealed()) {
        // The metadata zone of this staged file has already been written, which happens when a
        // PREPARE is replayed or re-sent after a leader switch.
        continue;
      }
      final LoadTsFileProgress progress = dataPartition2Progress.get(entry.getKey());
      if (progress != null && progress.exists() && progress.getTotalLength() > 0) {
        final long fileLength = writer.getFile().length();
        // Every recorded chunk must be present, and the recorded ranges have to cover the file
        // without a hole. Pieces may arrive in any order and a later piece fills in the hole an
        // earlier one left, so a missing piece is not always visible as a short file: it can also
        // leave the file as long as the last recorded chunk while the bytes before it are zeros.
        // Sealing then would put a hole of zeros into the imported TsFile, which is why the
        // completeness check is the same one the cleaner and the resume path share.
        if (!progress.isReady(fileLength)) {
          throw new LoadFileException(
              String.format(
                  StorageEngineMessages
                      .EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_NOT_CONTINUOUS_F9408C19,
                  writer.getFile().getAbsolutePath(),
                  uuidDirName(writer.getFile()),
                  progress.getTotalLength(),
                  fileLength));
        }
      }
      writer.close();

      final TsFileResource tsFileResource = dataPartition2Resource.get(entry.getKey());
      tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
      endTsFileResource(
          writer,
          tsFileResource,
          timePartitionProgressIndexMap.getOrDefault(
              entry.getKey().getTimePartitionSlot(), MinimumProgressIndex.INSTANCE));
    }
  }

  /**
   * Imports the sealed staged files into their DataRegions.
   *
   * @param deleteStagedSource whether the import may take the staged file itself away, which is
   *     what the ordinary load does. It must be false while the WAL entries of this task can still
   *     be expanded on this node for a replica that has not applied them yet: those entries only
   *     reference the staged bytes, and the staging directory is the only place they may be read
   *     back from. In that case the imported file is a copy and the staged one is deleted when the
   *     safe-deletion watermark of the WAL got past the command that ended the task.
   */
  void loadAll(boolean isGeneratedByPipe, boolean deleteStagedSource) throws LoadFileException {
    for (final Map.Entry<DataPartitionInfo, TsFileResource> entry :
        dataPartition2Resource.entrySet()) {
      final DataRegion dataRegion = entry.getKey().getDataRegion();
      final TsFileResource tsFileResource = entry.getValue();
      final TsFilePrecalculatedChunkWriter writer = dataPartition2Writer.get(entry.getKey());
      if (writer != null && !writer.isSealed()) {
        // The metadata zone is missing, so the file on disk cannot be read back: importing it
        // would register a TsFile whose chunks are unreachable.
        throw new LoadFileException(
            String.format(
                StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_INCOMPLETE_1CDE954B,
                tsFileResource.getTsFilePath(),
                uuidDirName(tsFileResource.getTsFile())));
      }
      dataRegion.loadNewTsFile(
          tsFileResource, deleteStagedSource, isGeneratedByPipe, false, Optional.empty());

      // Metrics
      if (writer == null) {
        continue;
      }
      dataRegion
          .getNonSystemDatabaseName()
          .ifPresent(
              databaseName ->
                  LoadPointCountMetrics.updateWritePointCountMetrics(
                      dataRegion, databaseName, getTsFileWritePointCount(writer), false));
    }
  }

  private void endTsFileResource(
      TsFilePrecalculatedChunkWriter writer,
      TsFileResource tsFileResource,
      ProgressIndex progressIndex)
      throws IOException {
    // Update time index by chunk groups still in memory
    Map<IDeviceID, Map<String, TimeValuePair>> deviceLastValues = null;
    if (IoTDBDescriptor.getInstance().getConfig().isCacheLastValuesForLoad()) {
      deviceLastValues = new HashMap<>();
    }
    AtomicLong lastValuesMemCost = new AtomicLong(0);

    for (final Map.Entry<IDeviceID, List<IChunkMetadata>> entry :
        writer.getChunkMetadataListMap().entrySet()) {
      final IDeviceID device = entry.getKey();
      for (final IChunkMetadata chunkMetadata : entry.getValue()) {
        tsFileResource.updateStartTime(device, chunkMetadata.getStartTime());
        tsFileResource.updateEndTime(device, chunkMetadata.getEndTime());
        if (deviceLastValues != null) {
          Map<String, TimeValuePair> deviceMap =
              deviceLastValues.computeIfAbsent(
                  device,
                  d -> {
                    Map<String, TimeValuePair> map = new HashMap<>();
                    lastValuesMemCost.addAndGet(RamUsageEstimator.shallowSizeOf(map));
                    lastValuesMemCost.addAndGet(device.ramBytesUsed());
                    return map;
                  });
          int prevSize = deviceMap.size();
          deviceMap.compute(
              chunkMetadata.getMeasurementUid(),
              (m, oldPair) -> {
                if (oldPair != null && oldPair.getTimestamp() > chunkMetadata.getEndTime()) {
                  return oldPair;
                }
                TsPrimitiveType lastValue =
                    chunkMetadata.getStatistics() != null
                            && chunkMetadata.getDataType() != TSDataType.BLOB
                        ? TsPrimitiveType.getByType(
                            chunkMetadata.getDataType() == TSDataType.VECTOR
                                ? TSDataType.INT64
                                : chunkMetadata.getDataType(),
                            chunkMetadata.getStatistics().getLastValue())
                        : null;
                TimeValuePair timeValuePair =
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
          int afterSize = deviceMap.size();
          lastValuesMemCost.addAndGet(
              (afterSize - prevSize) * RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY);
          if (lastValuesMemCost.get()
              > IoTDBDescriptor.getInstance().getConfig().getCacheLastValuesMemoryBudgetInByte()) {
            deviceLastValues = null;
          }
        }
      }
    }
    if (deviceLastValues != null) {
      Map<IDeviceID, List<Pair<String, TimeValuePair>>> finalDeviceLastValues;
      finalDeviceLastValues = new HashMap<>(deviceLastValues.size());
      for (final Map.Entry<IDeviceID, Map<String, TimeValuePair>> entry :
          deviceLastValues.entrySet()) {
        final IDeviceID device = entry.getKey();
        Map<String, TimeValuePair> lastValues = entry.getValue();
        List<Pair<String, TimeValuePair>> pairList =
            lastValues.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        finalDeviceLastValues.put(device, pairList);
      }
      tsFileResource.setLastValues(finalDeviceLastValues);
    }
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    tsFileResource.setProgressIndex(progressIndex);
    tsFileResource.serialize();
  }

  private long getTsFileWritePointCount(TsFilePrecalculatedChunkWriter writer) {
    return writer.getChunkMetadataListMap().values().stream()
        .flatMap(List::stream)
        .mapToLong(chunkMetadata -> chunkMetadata.getStatistics().getCount())
        .sum();
  }

  void close() {
    close(false);
  }

  /**
   * @param retainStagedFiles whether the staged TsFiles must survive this call. They are kept when
   *     a follower may still read the referenced pieces back while catching up from the WAL.
   */
  void close(final boolean retainStagedFiles) {
    if (isClosed) {
      return;
    }
    if (dataPartition2Writer != null) {
      for (Map.Entry<DataPartitionInfo, TsFilePrecalculatedChunkWriter> entry :
          dataPartition2Writer.entrySet()) {
        try {
          final TsFilePrecalculatedChunkWriter writer = entry.getValue();
          writer.getOutput().close();
          if (retainStagedFiles) {
            continue;
          }
          final Path writerPath = writer.getFile().toPath();
          if (Files.exists(writerPath)) {
            RetryUtils.retryOnException(
                () -> {
                  Files.delete(writerPath);
                  return null;
                });
          }
        } catch (IOException e) {
          LOGGER.warn(
              StorageEngineMessages.CLOSE_TSFILE_IO_WRITER_ERROR,
              entry.getValue().getFile().getPath(),
              e);
        }
      }
    }
    if (dataPartition2ModificationFile != null) {
      for (Map.Entry<DataPartitionInfo, ModificationFile> entry :
          dataPartition2ModificationFile.entrySet()) {
        try {
          final ModificationFile modificationFile = entry.getValue();
          modificationFile.close();
          final Path modificationFilePath = modificationFile.getFile().toPath();
          if (Files.exists(modificationFilePath)) {
            RetryUtils.retryOnException(
                () -> {
                  Files.delete(modificationFilePath);
                  return null;
                });
          }
        } catch (IOException e) {
          LOGGER.warn(
              StorageEngineMessages.CLOSE_MODIFICATION_FILE_ERROR, entry.getValue().getFile(), e);
        }
      }
    }
    if (dataPartition2Progress != null) {
      for (final LoadTsFileProgress progress : dataPartition2Progress.values()) {
        try {
          final Path progressPath = progress.getProgressFile().toPath();
          if (Files.exists(progressPath)) {
            RetryUtils.retryOnException(
                () -> {
                  Files.delete(progressPath);
                  return null;
                });
          }
        } catch (IOException e) {
          LOGGER.warn(
              LoadStagingDirs.MESSAGE_DELETE_FAIL, progress.getProgressFile().getAbsolutePath(), e);
        }
      }
    }
    if (retainStagedFiles) {
      // The task already reached COMMIT or ABORT. Its progress files are deleted above so that a
      // restart does not resume it, while the staged TsFiles stay for lagging followers.
      dataPartition2Writer = null;
      dataPartition2Resource = null;
      dataPartition2Progress = null;
      dataPartition2ModificationFile = null;
      isClosed = true;
      return;
    }
    try {
      RetryUtils.retryOnException(
          () -> {
            Files.delete(taskDir.toPath());
            return null;
          });
    } catch (DirectoryNotEmptyException e) {
      LOGGER.info(StorageEngineMessages.TASK_DIR_NOT_EMPTY_SKIP_DELETE, taskDir.getPath());
    } catch (IOException e) {
      LOGGER.warn(LoadStagingDirs.MESSAGE_DELETE_FAIL, taskDir.getPath(), e);
    }
    dataPartition2Writer = null;
    dataPartition2Resource = null;
    dataPartition2Progress = null;
    dataPartition2ModificationFile = null;
    isClosed = true;
  }

  private static Map<File, Long> snapshotFileLengths(final File taskDir) {
    final Map<File, Long> result = new HashMap<>();
    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          result.put(file, file.length());
        }
      }
    }
    return result;
  }

  private static List<LoadTsFileConsensusNode.PieceRef> createPieceRefs(
      final File taskDir, final Map<File, Long> previousLengths) {
    final List<LoadTsFileConsensusNode.PieceRef> refs = new java.util.ArrayList<>();
    final File[] files = taskDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (!file.isFile()) {
          continue;
        }
        if (file.getName().endsWith(LoadTsFileProgress.PROGRESS_SUFFIX)) {
          continue;
        }
        // A reference points into the payload zone of a staged TsFile, so the progress files and
        // the modification files are not payload ranges. The deletions that a modification file
        // holds travel inside the piece itself, in full, whoever reads them back.
        if (file.getName().endsWith(ModificationFile.FILE_SUFFIX)
            || file.getName().endsWith(ModificationFileV1.FILE_SUFFIX)) {
          continue;
        }
        final long previousLength = previousLengths.getOrDefault(file, 0L);
        final long size = file.length() - previousLength;
        if (size > 0) {
          refs.add(
              new LoadTsFileConsensusNode.PieceRef(file.getAbsolutePath(), previousLength, size));
        }
      }
    }
    return refs;
  }

  static final class DataPartitionInfo {

    private final DataRegion dataRegion;
    private final TTimePartitionSlot timePartitionSlot;

    private DataPartitionInfo(DataRegion dataRegion, TTimePartitionSlot timePartitionSlot) {
      this.dataRegion = dataRegion;
      this.timePartitionSlot = timePartitionSlot;
    }

    public DataRegion getDataRegion() {
      return dataRegion;
    }

    public TTimePartitionSlot getTimePartitionSlot() {
      return timePartitionSlot;
    }

    @Override
    public String toString() {
      return String.join(
          IoTDBConstant.FILE_NAME_SEPARATOR,
          dataRegion.getDatabaseName(),
          dataRegion.getDataRegionIdString(),
          Long.toString(timePartitionSlot.getStartTime()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DataPartitionInfo that = (DataPartitionInfo) o;
      return Objects.equals(dataRegion, that.dataRegion)
          && timePartitionSlot.getStartTime() == that.timePartitionSlot.getStartTime();
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataRegion, timePartitionSlot.getStartTime());
    }
  }
}
