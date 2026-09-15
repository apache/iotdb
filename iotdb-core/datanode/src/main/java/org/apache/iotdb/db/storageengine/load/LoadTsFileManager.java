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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.disk.FolderManager;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.metrics.utils.MetricLevel;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
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
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * {@link LoadTsFileManager} is used for dealing with {@link LoadTsFilePieceNode} and {@link
 * LoadCommand}. This class turn the content of a piece of loading TsFile into a new TsFile. When
 * DataNode finish transfer pieces, this class will flush all TsFile and load them into IoTDB, or
 * delete all.
 */
public class LoadTsFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileManager.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final String MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED =
      "%s TsFileWriterManager has been closed.";
  private static final String MESSAGE_DELETE_FAIL = "failed to delete {}.";

  private static final AtomicReference<String[]> LOAD_BASE_DIRS =
      new AtomicReference<>(CONFIG.getLoadTsFileDirs());
  private static final AtomicReference<FolderManager> FOLDER_MANAGER = new AtomicReference<>();

  public static final Cache<String, String> MEASUREMENT_ID_CACHE =
      Caffeine.newBuilder()
          .maximumWeight(CONFIG.getLoadMeasurementIdCacheSizeInBytes())
          .weigher((String k, String v) -> v.length())
          .build();

  private final Map<String, TsFileWriterManager> uuid2WriterManager = new ConcurrentHashMap<>();
  private final DataRegion dataRegion;

  public LoadTsFileManager(final DataRegion dataRegion) {
    this.dataRegion = Objects.requireNonNull(dataRegion);
  }

  public void start() {}

  public void stop() {
    new HashSet<>(uuid2WriterManager.keySet()).forEach(this::forceCloseWriterManager);
  }

  public void writeToDataRegion(LoadTsFilePieceNode pieceNode, String uuid)
      throws IOException, PageException {
    writePiece(uuid, pieceNode.getAllTsFileData());
  }

  public List<LoadTsFileConsensusNode.PieceRef> writePiece(
      final String uuid, final List<TsFileData> tsFileDataList) throws IOException, PageException {
    final AtomicReference<Exception> exception = new AtomicReference<>();
    final TsFileWriterManager writerManager =
        uuid2WriterManager.computeIfAbsent(
            uuid,
            o -> {
              try {
                return getFolderManager()
                    .getNextWithRetry(
                        folder ->
                            new TsFileWriterManager(
                                new File(getDataRegionLoadDir(new File(folder)), uuid)));
              } catch (DiskSpaceInsufficientException e) {
                exception.set(e);
                return null;
              }
            });

    if (exception.get() != null || writerManager == null) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEWRITERMANAGER_FOR_UUID_S_BECAUSE_A0D68950,
              uuid),
          exception.get());
    }

    final Map<File, Long> previousLengths = snapshotFileLengths(writerManager.taskDir);
    for (TsFileData tsFileData : tsFileDataList) {
      switch (tsFileData.getType()) {
        case CHUNK:
          ChunkData chunkData = (ChunkData) tsFileData;
          writerManager.write(
              new DataPartitionInfo(dataRegion, chunkData.getTimePartitionSlot()), chunkData);
          break;
        case DELETION:
          writerManager.writeDeletion(dataRegion, (DeletionData) tsFileData);
          break;
        default:
          throw new IOException(
              StorageEngineMessages.UNSUPPORTED_TSFILE_DATA_TYPE + tsFileData.getType());
      }
    }
    writerManager.flush();
    return createPieceRefs(writerManager.taskDir, previousLengths);
  }

  public List<LoadTsFileConsensusNode.PieceRef> writePiece(final LoadTsFileConsensusNode node)
      throws IOException, PageException {
    final List<LoadTsFileConsensusNode.PieceRef> refs =
        writePiece(node.getLoadId(), node.getTsFileDataList());
    if (node.getPieceRefs().isEmpty()) {
      final LoadTsFileConsensusNode walNode =
          LoadTsFileConsensusNode.pieceRefs(
              node.getPlanNodeId(),
              node.getLoadId(),
              node.getTsFileId(),
              node.getPieceIndex(),
              refs,
              node.getChecksum(),
              node.getDataSize(),
              null);
      walNode.setSearchIndex(node.getSearchIndex());
      logLoadNodeToWAL(walNode);
      dataRegion.insertSeparatorToWAL(node);
    }
    return refs;
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

  private File getDataRegionLoadDir(final File baseDir) {
    return new File(
        baseDir,
        dataRegion.getDatabaseName()
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + dataRegion.getDataRegionIdString());
  }

  public boolean prepare(
      final String uuid,
      final int pieceCount,
      final long totalBytes,
      final boolean isGeneratedByPipe,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    LOGGER.info(
        StorageEngineMessages.LOG_PREPARING_LOAD_TSFILE_ARG_SEALING_STAGED_RESOURCES_1FDF1866,
        uuid);
    uuid2WriterManager.get(uuid).prepare(isGeneratedByPipe, timePartitionProgressIndexMap);
    return true;
  }

  public boolean prepare(
      final LoadTsFileConsensusNode node,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(node.getLoadId())) {
      return false;
    }
    LOGGER.info(
        StorageEngineMessages.LOG_PREPARING_LOAD_TSFILE_ARG_SEALING_STAGED_RESOURCES_1FDF1866,
        node.getLoadId());
    uuid2WriterManager
        .get(node.getLoadId())
        .prepare(node.isGeneratedByPipe(), timePartitionProgressIndexMap);
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  public boolean loadAll(
      String uuid,
      boolean isGeneratedByPipe,
      Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }

    LOGGER.info(
        StorageEngineMessages
            .LOG_COMMITTING_LOAD_TSFILE_ARG_LOADING_PREPARED_RESOURCES_INTO_DATAREGION_EA1D6335,
        uuid);
    uuid2WriterManager.get(uuid).loadAll(isGeneratedByPipe);
    forceCloseWriterManager(uuid);
    return true;
  }

  public boolean loadAll(
      final LoadTsFileConsensusNode node,
      final Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
      throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(node.getLoadId())) {
      return false;
    }
    LOGGER.info(
        StorageEngineMessages
            .LOG_COMMITTING_LOAD_TSFILE_ARG_LOADING_PREPARED_RESOURCES_INTO_DATAREGION_EA1D6335,
        node.getLoadId());
    uuid2WriterManager.get(node.getLoadId()).loadAll(node.isGeneratedByPipe());
    forceCloseWriterManager(node.getLoadId());
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  public boolean deleteAll(String uuid) {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    forceCloseWriterManager(uuid);
    return true;
  }

  public boolean deleteAll(final LoadTsFileConsensusNode node) throws IOException {
    if (!uuid2WriterManager.containsKey(node.getLoadId())) {
      return false;
    }
    forceCloseWriterManager(node.getLoadId());
    logLoadNodeToWAL(node);
    dataRegion.insertSeparatorToWAL(node);
    return true;
  }

  private void logLoadNodeToWAL(final LoadTsFileConsensusNode node) throws IOException {
    final AtomicReference<IOException> failure = new AtomicReference<>();
    dataRegion
        .getWALNode()
        .ifPresent(
            wal -> {
              final WALFlushListener listener = wal.log(TsFileProcessor.MEMTABLE_NOT_EXIST, node);
              if (listener.waitForResult() == WALFlushListener.Status.FAILURE) {
                final Exception cause = listener.getCause();
                failure.set(
                    cause instanceof IOException ? (IOException) cause : new IOException(cause));
              }
            });
    if (failure.get() != null) {
      throw failure.get();
    }
  }

  private void forceCloseWriterManager(String uuid) {
    final TsFileWriterManager writerManager = uuid2WriterManager.remove(uuid);
    if (Objects.nonNull(writerManager)) {
      writerManager.close();
    }
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

  private static class TsFileWriterManager {

    private final File taskDir;
    private Map<DataPartitionInfo, TsFilePrecalculatedChunkWriter> dataPartition2Writer;
    private Map<DataPartitionInfo, TsFileResource> dataPartition2Resource;
    private Map<DataPartitionInfo, ModificationFile> dataPartition2ModificationFile;
    private boolean isClosed;
    private boolean isPrepared;

    private TsFileWriterManager(File taskDir) {
      this.taskDir = taskDir;
      this.dataPartition2Writer = new HashMap<>();
      this.dataPartition2Resource = new HashMap<>();
      this.dataPartition2ModificationFile = new HashMap<>();
      this.isClosed = false;
      this.isPrepared = false;

      ensureDir(taskDir);
    }

    private void ensureDir(File dir) {
      if (!dir.exists() && dir.mkdirs()) {
        LOGGER.info(StorageEngineMessages.LOAD_TSFILE_DIR_CREATED, dir.getPath());
      }
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
          LOGGER.error(StorageEngineMessages.CANNOT_CREATE_TSFILE_FOR_WRITING, newTsFile.getPath());
          return;
        }

        final TsFilePrecalculatedChunkWriter writer = new TsFilePrecalculatedChunkWriter(newTsFile);
        final TsFileResource resource = new TsFileResource(newTsFile);
        dataPartition2Writer.put(partitionInfo, writer);
        dataPartition2Resource.put(partitionInfo, resource);
      }
      final TsFilePrecalculatedChunkWriter writer = dataPartition2Writer.get(partitionInfo);
      final ChunkData.ChunkLayout layout = chunkData.getChunkLayout();
      if (layout == null) {
        throw new IOException("Chunk layout is missing");
      }
      long chunkOffset = layout.offset();
      final List<Chunk> chunks = chunkData.getChunks();
      for (int i = 0; i < chunks.size(); i++) {
        final Chunk chunk = chunks.get(i);
        writer.writeChunk(
            chunkData.getDevice(),
            chunkData.isAligned(),
            layout.chunkGroupHeaderOffset(),
            layout.firstChunkOfGroup() && i == 0,
            chunk,
            chunkOffset);
        chunkOffset += chunk.getHeader().getSerializedSize() + (long) chunk.getData().remaining();
      }
      if (chunkOffset != layout.offset() + layout.length()) {
        throw new IOException("Chunk layout length does not match encoded chunks");
      }
    }

    private void writeDeletion(DataRegion dataRegion, DeletionData deletionData)
        throws IOException {
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

    private void flush() throws IOException {
      for (TsFilePrecalculatedChunkWriter writer : dataPartition2Writer.values()) {
        writer.getOutput().flush();
      }
    }

    private void prepare(
        boolean isGeneratedByPipe,
        Map<TTimePartitionSlot, ProgressIndex> timePartitionProgressIndexMap)
        throws IOException, LoadFileException {
      if (isClosed) {
        throw new IOException(String.format(MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED, taskDir));
      }
      if (isPrepared) {
        return;
      }
      for (final Map.Entry<DataPartitionInfo, ModificationFile> entry :
          dataPartition2ModificationFile.entrySet()) {
        entry.getValue().close();
      }
      for (final Map.Entry<DataPartitionInfo, TsFilePrecalculatedChunkWriter> entry :
          dataPartition2Writer.entrySet()) {
        final TsFilePrecalculatedChunkWriter writer = entry.getValue();
        writer.close();

        final TsFileResource tsFileResource = dataPartition2Resource.get(entry.getKey());
        tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
        endTsFileResource(
            writer,
            tsFileResource,
            timePartitionProgressIndexMap.getOrDefault(
                entry.getKey().getTimePartitionSlot(), MinimumProgressIndex.INSTANCE));
      }
      isPrepared = true;
    }

    private void loadAll(boolean isGeneratedByPipe) throws LoadFileException {
      for (final Map.Entry<DataPartitionInfo, TsFileResource> entry :
          dataPartition2Resource.entrySet()) {
        final DataRegion dataRegion = entry.getKey().getDataRegion();
        final TsFileResource tsFileResource = entry.getValue();
        dataRegion.loadNewTsFile(tsFileResource, true, isGeneratedByPipe, false, Optional.empty());

        // Metrics
        dataRegion
            .getNonSystemDatabaseName()
            .ifPresent(
                databaseName ->
                    updateWritePointCountMetrics(
                        dataRegion,
                        databaseName,
                        getTsFileWritePointCount(dataPartition2Writer.get(entry.getKey())),
                        false));
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
                > IoTDBDescriptor.getInstance()
                    .getConfig()
                    .getCacheLastValuesMemoryBudgetInByte()) {
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

    private void close() {
      if (isClosed) {
        return;
      }
      if (dataPartition2Writer != null) {
        for (Map.Entry<DataPartitionInfo, TsFilePrecalculatedChunkWriter> entry :
            dataPartition2Writer.entrySet()) {
          try {
            final TsFilePrecalculatedChunkWriter writer = entry.getValue();
            writer.getOutput().close();
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
      dataPartition2Writer = null;
      dataPartition2Resource = null;
      dataPartition2ModificationFile = null;
      isClosed = true;
    }
  }

  private static class DataPartitionInfo {

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
