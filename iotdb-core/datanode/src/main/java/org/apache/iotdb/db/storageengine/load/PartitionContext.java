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
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.table.TsFileTableSchemaUtil;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * LOAD partition writer context: cohesive per-partition state of one in-progress LOAD task. This
 * replaces the parallel per-partition maps of the old writer manager: the staged {@link
 * TsFileIOWriter}, its {@link TsFileResource}, the modification file, the device currently being
 * written, the already-synced byte cursor and the finalized flag now live together, and every
 * single-partition behavior (write chunk / deletion, chunk-group end, ref capture, snapshot prefix
 * copy, final load and close) is owned by this object.
 */
final class PartitionContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionContext.class);

  /** Immutable identity of this partition: (DataRegion, time partition slot). */
  private final DataPartitionInfo partitionInfo;

  /** Task directory that holds the staged file of this partition (and of sibling partitions). */
  private final File taskDir;

  /**
   * One-shot, write-only staged file builder. Chunk bytes are buffered in memory and only reach the
   * file on flush(); its footer/metadata state cannot be rebuilt from the file after close.
   */
  private final TsFileIOWriter writer;

  /** Resource describing the staged file; filled with time index and serialized before loading. */
  private final TsFileResource resource;

  /** Lazy-created {@code .mods} file that records deletions applied to this staged file. */
  private ModificationFile modificationFile;

  /** Device of the currently open chunk group; {@code null} when no chunk group is open. */
  private IDeviceID currentWritingDevice;

  /**
   * Byte cursor of the durable prefix {@code [0, syncedOffset)} already captured by PieceRefs. Refs
   * advance continuously from offset 0, which is what lets followers rebuild the file from the WAL
   * and snapshots copy exactly the synced prefix without holes or overlaps.
   */
  private long syncedOffset = 0L;

  /** Whether the file footer has been written (PREPARE seals the file once). */
  private boolean finalized = false;

  private PartitionContext(
      DataPartitionInfo partitionInfo,
      File taskDir,
      TsFileIOWriter writer,
      TsFileResource resource) {
    this.partitionInfo = partitionInfo;
    this.taskDir = taskDir;
    this.writer = writer;
    this.resource = resource;
  }

  /**
   * Creates the staged partition file and its writer. Returns {@code null} when the target file
   * already exists (the original behavior logs the error and skips the chunk instead of failing the
   * whole piece); IO failures are propagated.
   */
  static PartitionContext create(
      DataPartitionInfo partitionInfo, File taskDir, long chunkMetadataMaxSizeForEachWriter)
      throws IOException {
    // One staged file per (database, region, time partition): the partition's toString is a unique
    // file name, e.g. "root.sg_1_0.tsfile".
    final File newTsFile =
        SystemFileFactory.INSTANCE.getFile(
            taskDir, partitionInfo.toString() + TsFileConstant.TSFILE_SUFFIX);
    if (!newTsFile.createNewFile()) {
      // createNewFile returns false when the file already exists: re-creating it would truncate an
      // existing staged file, so the chunk is skipped (mirrors the historical behavior).
      LOGGER.error(StorageEngineMessages.CANNOT_CREATE_TSFILE_FOR_WRITING, newTsFile.getPath());
      return null;
    }

    // chunkMetadataMaxSizeForEachWriter bounds how much chunk metadata one writer may keep in
    // memory; the manager divides the configured budget across all concurrent partition writers.
    final TsFileIOWriter writer = new TsFileIOWriter(newTsFile, chunkMetadataMaxSizeForEachWriter);
    final TsFileResource resource = new TsFileResource(writer.getFile());
    addResourceFlushListener(writer, resource);
    return new PartitionContext(partitionInfo, taskDir, writer, resource);
  }

  /**
   * Rebuilds a partition context from an unsealed staged file restored from a snapshot. {@link
   * RestorableTsFileIOWriter} is the writer used by the DataRegion's unsealed-TsFile recovery: it
   * scans the file, truncates it to the last complete chunk-group boundary and positions the writer
   * so further chunks can be appended to the same file. The already-durable prefix becomes the new
   * synced cursor; a {@code .mods} restored next to the file is re-attached lazily.
   */
  static PartitionContext restore(
      DataPartitionInfo partitionInfo,
      File taskDir,
      File restoredFile,
      long chunkMetadataMaxSizeForEachWriter)
      throws IOException {
    if (!restoredFile.isFile() || restoredFile.length() == 0) {
      return null;
    }
    final RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(restoredFile);
    final TsFileResource resource = new TsFileResource(writer.getFile());
    addResourceFlushListener(writer, resource);
    final PartitionContext context = new PartitionContext(partitionInfo, taskDir, writer, resource);
    context.syncedOffset = writer.getFile().length();
    final File modsFile = ModificationFile.getExclusiveMods(restoredFile);
    if (modsFile.isFile()) {
      context.modificationFile = new ModificationFile(modsFile, false);
    }
    return context;
  }

  private static void addResourceFlushListener(TsFileIOWriter writer, TsFileResource resource) {
    // TsFileIOWriter calls back with the chunk groups it is about to flush to disk. Update the
    // resource's per-device start/end time here, so the time index is already correct when the
    // file is loaded and we do not have to re-scan it.
    writer.addFlushListener(
        sortedChunkMetadataList ->
            sortedChunkMetadataList.forEach(
                pair -> {
                  // pair is (device, chunk metadata list) of one flushed chunk group.
                  final IDeviceID deviceId = pair.left.left;
                  pair.getRight()
                      .forEach(
                          chunkMetadata -> {
                            resource.updateStartTime(deviceId, chunkMetadata.getStartTime());
                            resource.updateEndTime(deviceId, chunkMetadata.getEndTime());
                          });
                }));
  }

  boolean belongsTo(DataRegion dataRegion) {
    // DataRegion instances are singletons per region, so identity comparison is sufficient.
    return partitionInfo.getDataRegion() == dataRegion;
  }

  TsFileIOWriter getWriter() {
    return writer;
  }

  IDeviceID getCurrentWritingDevice() {
    return currentWritingDevice;
  }

  long getTimePartitionStart() {
    return partitionInfo.getTimePartitionSlot().getStartTime();
  }

  TTimePartitionSlot getTimePartitionSlot() {
    return partitionInfo.getTimePartitionSlot();
  }

  /**
   * Starts a new chunk group for the given device, warning on the inconsistent state where this
   * writer still has an open chunk group (it should have been ended by the device fan-out).
   *
   * <p>A chunk group groups all measurements of one device in a time range; the device switch is
   * handled by the task manager across every partition of the old device before this is called.
   */
  void startChunkGroup(IDeviceID device) throws IOException {
    if (writer.isWritingChunkGroup()) {
      LOGGER.warn(
          StorageEngineMessages
              .STORAGE_LOG_WRITER_FOR_PARTITION_IS_ALREADY_WRITING_CHUNK_GROUP_FOR_903B1D66,
          writer.getFile().getAbsolutePath(),
          partitionInfo,
          device,
          currentWritingDevice);
    }
    writer.startChunkGroup(device);
    currentWritingDevice = device;
  }

  /**
   * Seals the open chunk group of this partition and asks the writer to flush if its buffered
   * metadata exceeds the size bound. Used by the device fan-out: when a source file switches to a
   * new device, every partition that was writing the old device must end its chunk group at the
   * same logical point so aligned chunks stay consistent across partitions.
   */
  void endChunkGroupAndCheckMetadataSize() throws IOException {
    if (writer.isWritingChunkGroup()) {
      writer.endChunkGroup();
    }
    writer.checkMetadataSizeAndMayFlush();
  }

  /**
   * Registers the table schema for table-model databases and writes the chunk to the writer. The
   * staged TsFile must carry the table schema in its footer for downstream readers (query,
   * compaction, pipe) to interpret the table-model chunks; a table missing from the DataNode cache
   * means it was dropped after the LOAD statement was analyzed, which must fail the load explicitly
   * instead of writing schema-less chunks or silently dropping the data.
   */
  void writeChunk(ChunkData chunkData) throws IOException, PageException, LoadFileException {
    final String tableName =
        chunkData.getDevice() != null ? chunkData.getDevice().getTableName() : null;
    if (tableName != null
        && PathUtils.isTableModelDatabase(partitionInfo.getDataRegion().getDatabaseName())) {
      final TsTable table =
          DataNodeTableCache.getInstance()
              .getTable(partitionInfo.getDataRegion().getDatabaseName(), tableName, false);
      if (Objects.nonNull(table)) {
        writer
            .getSchema()
            .getTableSchemaMap()
            .computeIfAbsent(
                tableName, t -> TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table));
      } else {
        throw new LoadFileException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_TABLE_ARG_ARG_DOES_NOT_EXIST_WHEN_APPLYING_LOAD_CHUNK_DATA_IT_MAY_HAVE_BEEN_DROPPED_AFTER_THE_LOAD_WAS_ANALYZED_DDB35F93,
                partitionInfo.getDataRegion().getDatabaseName(),
                tableName));
      }
    }
    chunkData.writeToFileWriter(writer);
  }

  /**
   * Applies one deletion to this partition's modification file. The {@code .mods} file is created
   * lazily next to the staged file on the first deletion; the DataRegion reads it when the file is
   * loaded, so the deletion never has to touch the already-written chunk bytes.
   */
  void writeDeletion(DeletionData deletionData) throws IOException {
    if (modificationFile == null) {
      final File newModificationFile = ModificationFile.getExclusiveMods(writer.getFile());
      if (!newModificationFile.isFile() && !newModificationFile.createNewFile()) {
        // The file may already exist because it was restored from a snapshot together with the
        // staged file; createNewFile returns false in that case, which is not an error.
        if (!newModificationFile.isFile()) {
          LOGGER.error(
              StorageEngineMessages
                  .STORAGE_LOG_CAN_NOT_CREATE_MODIFICATIONFILE_FOR_WRITING_17D14C11,
              newModificationFile.getPath());
          return;
        }
      }
      modificationFile = new ModificationFile(newModificationFile, false);
    }
    writer.flush();
    // Flush the chunk file first so the deletion (which references point ranges of the file) is
    // recorded after the corresponding bytes are durable.
    deletionData.writeToModificationFile(modificationFile);
  }

  /**
   * Ends the partition file (chunk groups + footer) and captures the final byte range. The footer
   * capture matters: nodes rebuilding the file from WAL refs only get a complete, readable file if
   * the refs cover the footer too. The {@code finalized} flag makes this idempotent because PREPARE
   * may be applied once per task.
   */
  void finalizeFile(List<LoadTsFileConsensusNode.PieceRef> pendingPieceRefs) throws IOException {
    if (finalized) {
      return;
    }
    if (writer.isWritingChunkGroup()) {
      writer.endChunkGroup();
    }
    writer.endFile();
    captureRefs(pendingPieceRefs);
    finalized = true;
  }

  private void captureRefs(List<LoadTsFileConsensusNode.PieceRef> pendingPieceRefs)
      throws IOException {
    // TsFileIOWriter buffers chunk bytes in memory: endChunkGroup() only appends the
    // ChunkGroupFooter to the buffer, and the bytes do not reach the file until flush(). Read the
    // length only after a flush so the captured [startOffset, endOffset) always covers a sealed,
    // durably written chunk group.
    writer.flush();
    // startOffset is the previous synced cursor; every ref starts exactly there and extends to the
    // current file length, so the sequence of refs of one staged file is contiguous from offset 0.
    final long startOffset = syncedOffset;
    final long endOffset = writer.getFile().length();
    if (endOffset <= startOffset) {
      return;
    }
    final int length = (int) (endOffset - startOffset);
    pendingPieceRefs.add(
        new LoadTsFileConsensusNode.PieceRef(
            taskDir.getName() + File.separator + writer.getFile().getName(), startOffset, length));
    syncedOffset = endOffset;
  }

  /**
   * Copies the already-synced byte prefix of this staged partition file into the snapshot task dir
   * and returns its snapshot metadata, or {@code null} when nothing has been synced yet.
   *
   * <p>Only {@code [0, syncedOffset)} is copied: bytes after the last chunk-group boundary are
   * still owned by the writer buffer and will be covered by the next PIECE ref, so a node restored
   * from this snapshot can keep appending from exactly the snapshot length without a hole or an
   * overlap.
   */
  LoadSnapshotManager.StagedFileSnapshot snapshotTo(File targetDir) throws IOException {
    if (syncedOffset <= 0) {
      return null;
    }
    final File stagedFile = writer.getFile();
    copyPrefix(stagedFile, new File(targetDir, stagedFile.getName()), syncedOffset);
    copyModsIfPresent(targetDir);
    return new LoadSnapshotManager.StagedFileSnapshot(
        stagedFile.getName(),
        partitionInfo.getDataRegion().getDatabaseName(),
        partitionInfo.getDataRegion().getDataRegionIdString(),
        getTimePartitionStart(),
        finalized);
  }

  private void copyPrefix(File source, File target, long length) throws IOException {
    if (length <= 0) {
      return;
    }
    // transferTo may copy fewer bytes than requested, so loop until the whole prefix is copied and
    // fail loudly on EOF (a truncated staged file would silently corrupt the replica otherwise).
    try (final FileChannel in = FileChannel.open(source.toPath(), StandardOpenOption.READ);
        final FileChannel out =
            FileChannel.open(
                target.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
      long transferred = 0;
      while (transferred < length) {
        final long count = in.transferTo(transferred, length - transferred, out);
        if (count <= 0) {
          throw new IOException(
              String.format(
                  StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_EOF_8743387D,
                  source,
                  transferred));
        }
        transferred += count;
      }
      out.force(true);
    }
  }

  private void copyModsIfPresent(File targetDir) throws IOException {
    // Deletions must travel with the snapshot, otherwise the restored staged file would resurrect
    // data the load had already marked as deleted.
    if (modificationFile != null && modificationFile.getFile().isFile()) {
      Files.copy(
          modificationFile.getFile().toPath(),
          new File(targetDir, modificationFile.getFile().getName()).toPath(),
          StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private void forceFile(final File file) throws IOException {
    // fsync the file so the refs logged to the WAL cover bytes that are actually durable.
    try (final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
      channel.force(true);
    }
  }

  void closeModificationFile() throws IOException {
    // Closed before loadNewTsFile so the DataRegion can read the mods of the staged file; the file
    // itself is deleted later by close().
    if (modificationFile != null) {
      modificationFile.close();
    }
  }

  /**
   * Loads this staged partition file into its DataRegion: seals the file if PREPARE has not run,
   * validates it is a complete TsFile, binds the resource (time index, last values, progress) and
   * hands it to the DataRegion, then updates the load point-count metrics.
   */
  void loadIntoRegion(boolean isGeneratedByPipe, ProgressIndex progressIndex)
      throws IOException, LoadFileException {
    if (!finalized) {
      if (writer.isWritingChunkGroup()) {
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    validateStagedFileComplete(writer.getFile());

    final DataRegion partitionRegion = partitionInfo.getDataRegion();
    resource.setGeneratedByPipe(isGeneratedByPipe);
    endTsFileResource(writer, resource, progressIndex);
    partitionRegion.loadNewTsFile(
        resource, true, isGeneratedByPipe, false, Optional.ofNullable(writer.getTableSizeMap()));

    // Metrics
    partitionRegion
        .getNonSystemDatabaseName()
        .ifPresent(
            databaseName ->
                LoadTsFileManager.updateWritePointCountMetrics(
                    partitionRegion, databaseName, getTsFileWritePointCount(writer), false));
  }

  private void validateStagedFileComplete(File stagedFile) throws LoadFileException {
    // isComplete() verifies the file has a valid magic header and footer, i.e. it is not a
    // truncated staged file left behind by a failed transfer.
    try (final TsFileSequenceReader reader =
        new TsFileSequenceReader(stagedFile.getAbsolutePath(), true)) {
      if (!reader.isComplete()) {
        throw new LoadFileException(
            String.format(
                StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_INCOMPLETE_1CDE954B,
                stagedFile,
                taskDir.getName()));
      }
    } catch (IOException e) {
      throw new LoadFileException(
          String.format(
              StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_INCOMPLETE_1CDE954B,
              stagedFile,
              taskDir.getName()),
          e);
    }
  }

  private void endTsFileResource(
      TsFileIOWriter writer, TsFileResource tsFileResource, ProgressIndex progressIndex)
      throws IOException {
    // Build the time index from every chunk group (still in the writer's memory) and optionally
    // cache the last value of each measurement for fast "last query" after load.
    Map<IDeviceID, Map<String, TimeValuePair>> deviceLastValues = null;
    if (IoTDBDescriptor.getInstance().getConfig().isCacheLastValuesForLoad()) {
      deviceLastValues = new HashMap<>();
    }
    // Tracks the estimated memory of the last-value cache; the cache is disabled as soon as the
    // configured budget is exceeded so LOAD cannot blow up the heap.
    AtomicLong lastValuesMemCost = new AtomicLong(0);

    for (final ChunkGroupMetadata chunkGroupMetadata : writer.getChunkGroupMetadataList()) {
      final IDeviceID device = chunkGroupMetadata.getDevice();
      for (final ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        // Per-device min start time / max end time across all chunks.
        tsFileResource.updateStartTime(device, chunkMetadata.getStartTime());
        tsFileResource.updateEndTime(device, chunkMetadata.getEndTime());
        if (deviceLastValues != null) {
          // deviceMap: measurement uid -> (timestamp, value) of the last point in this file.
          Map<String, TimeValuePair> deviceMap =
              deviceLastValues.computeIfAbsent(
                  device,
                  d -> {
                    // Account for the per-device map and the device id memory when computing the
                    // budget, so the estimate tracks the real allocation.
                    Map<String, TimeValuePair> map = new HashMap<>();
                    lastValuesMemCost.addAndGet(RamUsageEstimator.shallowSizeOf(map));
                    lastValuesMemCost.addAndGet(device.ramBytesUsed());
                    return map;
                  });
          int prevSize = deviceMap.size();
          deviceMap.compute(
              chunkMetadata.getMeasurementUid(),
              (m, oldPair) -> {
                // Keep the existing (later) value if it is still newer than this chunk's end.
                if (oldPair != null && oldPair.getTimestamp() > chunkMetadata.getEndTime()) {
                  return oldPair;
                }
                // Reconstruct the last value from the chunk statistics; VECTOR chunks use the time
                // column (INT64) because the vector itself has no scalar statistics.
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
                // Adjust the budget by the size difference of the replaced entry.
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
          // Give up caching once the budget is exceeded; the data is still loaded correctly, only
          // the last-value cache is dropped.
          if (lastValuesMemCost.get()
              > IoTDBDescriptor.getInstance().getConfig().getCacheLastValuesMemoryBudgetInByte()) {
            deviceLastValues = null;
          }
        }
      }
    }
    if (deviceLastValues != null) {
      // Flatten device -> {measurement -> last pair} into device -> [(measurement, pair)] for the
      // resource's compact last-value representation.
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
    // Serialize the .tsfile.resource metadata file next to the staged file.
    tsFileResource.serialize();
  }

  private long getTsFileWritePointCount(TsFileIOWriter writer) {
    // Sum of the row counts of every chunk, used only for the load point-count metric.
    return writer.getChunkGroupMetadataList().stream()
        .flatMap(chunkGroupMetadata -> chunkGroupMetadata.getChunkMetadataList().stream())
        .mapToLong(chunkMetadata -> chunkMetadata.getStatistics().getCount())
        .sum();
  }

  /**
   * Closes and deletes the writer file and the modification file, tolerating per-file errors so one
   * failing file cannot block cleanup of the remaining task directory.
   *
   * <p>Closing a stream and deleting its file are independent best-effort steps: a close failure
   * (e.g. disk full while writing the footer) must never skip the file deletion, otherwise the
   * abandoned staged file would leak as garbage in the data directory.
   */
  void close() {
    // canWrite() is false once endFile() sealed the writer; only an unsealed writer is closed.
    if (writer.canWrite()) {
      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.warn(
            StorageEngineMessages.CLOSE_TSFILE_IO_WRITER_ERROR, writer.getFile().getPath(), e);
      }
    }
    try {
      final Path writerPath = writer.getFile().toPath();
      if (Files.exists(writerPath)) {
        RetryUtils.retryOnException(
            () -> {
              Files.delete(writerPath);
              return null;
            });
      }
    } catch (Exception e) {
      LOGGER.warn(
          StorageEngineMessages.FAILED_TO_DELETE_FILE_OR_DIR, writer.getFile().getPath(), e);
    }
    if (modificationFile != null) {
      try {
        modificationFile.close();
      } catch (IOException e) {
        LOGGER.warn(
            StorageEngineMessages.CLOSE_MODIFICATION_FILE_ERROR,
            modificationFile.getFile().getPath(),
            e);
      }
      try {
        final Path modificationFilePath = modificationFile.getFile().toPath();
        if (Files.exists(modificationFilePath)) {
          RetryUtils.retryOnException(
              () -> {
                Files.delete(modificationFilePath);
                return null;
              });
        }
      } catch (Exception e) {
        LOGGER.warn(
            StorageEngineMessages.FAILED_TO_DELETE_FILE_OR_DIR,
            modificationFile.getFile().getPath(),
            e);
      }
    }
  }
}
