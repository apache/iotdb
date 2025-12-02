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
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.table.TsFileTableSchemaUtil;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadAgent;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.metrics.utils.MetricLevel;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.writer.TsFileIOWriter;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.db.utils.constant.SqlConstant.ROOT;
import static org.apache.iotdb.db.utils.constant.SqlConstant.TREE_MODEL_DATABASE_PREFIX;

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

  private final Map<String, CleanupTask> uuid2CleanupTask = new ConcurrentHashMap<>();
  private final PriorityBlockingQueue<CleanupTask> cleanupTaskQueue = new PriorityBlockingQueue<>();

  private final ActiveLoadAgent activeLoadAgent = new ActiveLoadAgent();

  public LoadTsFileManager() {
    registerCleanupTaskExecutor();
    recover();
    activeLoadAgent.start();
  }

  private void registerCleanupTaskExecutor() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "LoadTsFileManager#cleanupTasks",
            this::cleanupTasks,
            CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds() >> 2);
  }

  private void cleanupTasks() {
    while (!cleanupTaskQueue.isEmpty()) {
      synchronized (uuid2CleanupTask) {
        if (cleanupTaskQueue.isEmpty()) {
          continue;
        }

        final CleanupTask cleanupTask = cleanupTaskQueue.peek();
        if (cleanupTask.scheduledTime <= System.currentTimeMillis()) {
          if (cleanupTask.isLoadTaskRunning) {
            cleanupTaskQueue.poll();
            cleanupTask.resetScheduledTime();
            cleanupTaskQueue.add(cleanupTask);
            continue;
          }

          cleanupTask.run();

          uuid2CleanupTask.remove(cleanupTask.uuid);
          cleanupTaskQueue.poll();
        } else {
          final long waitTimeInMs = cleanupTask.scheduledTime - System.currentTimeMillis();
          LOGGER.info(
              "Next load cleanup task {} is not ready to run, wait for at least {} ms ({}s).",
              cleanupTask.uuid,
              waitTimeInMs,
              waitTimeInMs / 1000.0);
          return;
        }
      }
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
              final String uuid = taskDir.getName();
              final TsFileWriterManager writerManager = new TsFileWriterManager(taskDir);

              uuid2WriterManager.put(uuid, writerManager);
              writerManager.close();

              synchronized (uuid2CleanupTask) {
                final CleanupTask cleanupTask =
                    new CleanupTask(
                        uuid, CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds() * 1000);
                uuid2CleanupTask.put(uuid, cleanupTask);
                cleanupTaskQueue.add(cleanupTask);
              }
            });
  }

  public void writeToDataRegion(DataRegion dataRegion, LoadTsFilePieceNode pieceNode, String uuid)
      throws IOException, PageException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      synchronized (uuid2CleanupTask) {
        final CleanupTask cleanupTask =
            new CleanupTask(uuid, CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds() * 1000);
        uuid2CleanupTask.put(uuid, cleanupTask);
        cleanupTaskQueue.add(cleanupTask);
      }
    }

    final Optional<CleanupTask> cleanupTask = Optional.of(uuid2CleanupTask.get(uuid));
    cleanupTask.ifPresent(CleanupTask::markLoadTaskRunning);
    try {
      final AtomicReference<Exception> exception = new AtomicReference<>();
      final TsFileWriterManager writerManager =
          uuid2WriterManager.computeIfAbsent(
              uuid,
              o -> {
                try {
                  return getFolderManager()
                      .getNextWithRetry(folder -> new TsFileWriterManager(new File(folder, uuid)));
                } catch (DiskSpaceInsufficientException e) {
                  exception.set(e);
                  return null;
                }
              });

      if (exception.get() != null || writerManager == null) {
        throw new IOException(
            "Failed to create TsFileWriterManager for uuid "
                + uuid
                + " because of insufficient disk space.",
            exception.get());
      }

      for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
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
            throw new IOException("Unsupported TsFileData type: " + tsFileData.getType());
        }
      }
    } finally {
      cleanupTask.ifPresent(CleanupTask::markLoadTaskNotRunning);
    }
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
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }

    final Optional<CleanupTask> cleanupTask = Optional.of(uuid2CleanupTask.get(uuid));
    cleanupTask.ifPresent(CleanupTask::markLoadTaskRunning);
    try {
      uuid2WriterManager.get(uuid).loadAll(isGeneratedByPipe, timePartitionProgressIndexMap);
    } finally {
      cleanupTask.ifPresent(CleanupTask::markLoadTaskNotRunning);
    }

    clean(uuid);
    return true;
  }

  public boolean deleteAll(String uuid) {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    clean(uuid);
    return true;
  }

  private void clean(String uuid) {
    synchronized (uuid2CleanupTask) {
      final CleanupTask cleanupTask = uuid2CleanupTask.get(uuid);
      if (cleanupTask != null) {
        cleanupTask.cancel();
      }
    }

    forceCloseWriterManager(uuid);
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
      final boolean isGeneratedByPipeConsensusLeader) {
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
    if (replicationNum != 0 && !isGeneratedByPipeConsensusLeader) {
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
      LOGGER.warn("Delete After Loading {} error.", tsFile, e);
    }
  }

  private static class TsFileWriterManager {

    private final File taskDir;
    private Map<DataPartitionInfo, TsFileIOWriter> dataPartition2Writer;
    private Map<DataPartitionInfo, TsFileResource> dataPartition2Resource;
    private Map<DataPartitionInfo, IDeviceID> dataPartition2LastDevice;
    private Map<DataPartitionInfo, ModificationFile> dataPartition2ModificationFile;
    private Map<IDeviceID, Set<DataPartitionInfo>> device2Partition;
    private boolean isClosed;

    private TsFileWriterManager(File taskDir) {
      this.taskDir = taskDir;
      this.dataPartition2Writer = new HashMap<>();
      this.dataPartition2Resource = new HashMap<>();
      this.dataPartition2LastDevice = new HashMap<>();
      this.dataPartition2ModificationFile = new HashMap<>();
      device2Partition = new HashMap<>();
      this.isClosed = false;

      clearDir(taskDir);
    }

    private void clearDir(File dir) {
      if (dir.exists()) {
        FileUtils.deleteFileOrDirectoryWithRetry(dir);
      }
      if (dir.mkdirs()) {
        LOGGER.info("Load TsFile dir {} is created.", dir.getPath());
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
          LOGGER.error("Can not create TsFile {} for writing.", newTsFile.getPath());
          return;
        }

        final long chunkMetadataMaxSizeForEachWriter =
            CONFIG.getLoadChunkMetadataMemorySizeInBytes() / (dataPartition2Writer.size() + 1);
        final TsFileIOWriter writer =
            new TsFileIOWriter(newTsFile, chunkMetadataMaxSizeForEachWriter);
        final TsFileResource resource = new TsFileResource(writer.getFile());
        writer.addFlushListener(
            // Update time index by chunk groups going to be flushed to temp file
            sortedChunkMetadataList ->
                sortedChunkMetadataList.forEach(
                    pair -> {
                      final IDeviceID deviceId = pair.left.left;
                      pair.getRight()
                          .forEach(
                              chunkMetadata -> {
                                resource.updateStartTime(deviceId, chunkMetadata.getStartTime());
                                resource.updateEndTime(deviceId, chunkMetadata.getEndTime());
                              });
                    }));

        // When a new writer is added, we need to reduce the metadata size limit of all existing
        // writers for memory control
        for (final TsFileIOWriter existingWriter : dataPartition2Writer.values()) {
          existingWriter.setMaxMetadataSize(chunkMetadataMaxSizeForEachWriter);
        }
        dataPartition2Writer.put(partitionInfo, writer);
        dataPartition2Resource.put(partitionInfo, resource);
      }
      TsFileIOWriter writer = dataPartition2Writer.get(partitionInfo);

      // Table model needs to register TableSchema
      final String tableName =
          chunkData.getDevice() != null ? chunkData.getDevice().getTableName() : null;
      if (tableName != null
          && !(tableName.startsWith(TREE_MODEL_DATABASE_PREFIX) || tableName.equals(ROOT))) {
        // If the table does not exist, it means that the table is all deleted by mods
        final TsTable table =
            DataNodeTableCache.getInstance()
                .getTable(partitionInfo.getDataRegion().getDatabaseName(), tableName, false);
        if (Objects.nonNull(table)) {
          writer
              .getSchema()
              .getTableSchemaMap()
              .computeIfAbsent(
                  tableName, t -> TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table));
        }
      }

      IDeviceID device = chunkData.getDevice();
      IDeviceID lastDevice = dataPartition2LastDevice.get(partitionInfo);

      if (!Objects.equals(device, lastDevice)) {
        if (lastDevice != null && device2Partition.containsKey(lastDevice)) {
          Set<DataPartitionInfo> partitions = device2Partition.get(lastDevice);
          for (DataPartitionInfo partition : partitions) {
            TsFileIOWriter w = dataPartition2Writer.get(partition);
            if (dataPartition2LastDevice.containsKey(partition) && w != null) {
              w.endChunkGroup();
              w.checkMetadataSizeAndMayFlush();
            }
          }
          device2Partition.remove(lastDevice);
        }
        if (writer.isWritingChunkGroup()) {
          LOGGER.warn(
              "Writer {} for partition {} is already writing chunk group for device {}, but the last device is {}. ",
              writer.getFile().getAbsolutePath(),
              partitionInfo,
              device,
              lastDevice);
        }
        writer.startChunkGroup(device);
        dataPartition2LastDevice.put(partitionInfo, device);
        device2Partition.computeIfAbsent(device, k -> new HashSet<>()).add(partitionInfo);
      }

      chunkData.writeToFileWriter(writer);
    }

    private void writeDeletion(DataRegion dataRegion, DeletionData deletionData)
        throws IOException {
      if (isClosed) {
        throw new IOException(String.format(MESSAGE_WRITER_MANAGER_HAS_BEEN_CLOSED, taskDir));
      }
      for (Map.Entry<DataPartitionInfo, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
        final DataPartitionInfo partitionInfo = entry.getKey();
        if (partitionInfo.getDataRegion().equals(dataRegion)) {
          final TsFileIOWriter writer = entry.getValue();
          if (!dataPartition2ModificationFile.containsKey(partitionInfo)) {
            File newModificationFile = ModificationFile.getExclusiveMods(writer.getFile());
            if (!newModificationFile.createNewFile()) {
              LOGGER.error(
                  "Can not create ModificationFile {} for writing.", newModificationFile.getPath());
              return;
            }

            dataPartition2ModificationFile.put(
                partitionInfo, new ModificationFile(newModificationFile, false));
          }
          ModificationFile modificationFile = dataPartition2ModificationFile.get(partitionInfo);
          writer.flush();
          deletionData.writeToModificationFile(modificationFile);
        }
      }
    }

    private void loadAll(
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
      for (final Map.Entry<DataPartitionInfo, TsFileIOWriter> entry :
          dataPartition2Writer.entrySet()) {
        final TsFileIOWriter writer = entry.getValue();
        if (writer.isWritingChunkGroup()) {
          writer.endChunkGroup();
        }
        writer.endFile();

        final DataRegion dataRegion = entry.getKey().getDataRegion();
        final TsFileResource tsFileResource = dataPartition2Resource.get(entry.getKey());
        tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
        endTsFileResource(
            writer,
            tsFileResource,
            timePartitionProgressIndexMap.getOrDefault(
                entry.getKey().getTimePartitionSlot(), MinimumProgressIndex.INSTANCE));
        dataRegion.loadNewTsFile(tsFileResource, true, isGeneratedByPipe, false);

        // Metrics
        dataRegion
            .getNonSystemDatabaseName()
            .ifPresent(
                databaseName ->
                    updateWritePointCountMetrics(
                        dataRegion, databaseName, getTsFileWritePointCount(writer), false));
      }
    }

    private void endTsFileResource(
        TsFileIOWriter writer, TsFileResource tsFileResource, ProgressIndex progressIndex)
        throws IOException {
      // Update time index by chunk groups still in memory
      Map<IDeviceID, Map<String, TimeValuePair>> deviceLastValues = null;
      if (IoTDBDescriptor.getInstance().getConfig().isCacheLastValuesForLoad()) {
        deviceLastValues = new HashMap<>();
      }
      AtomicLong lastValuesMemCost = new AtomicLong(0);

      for (final ChunkGroupMetadata chunkGroupMetadata : writer.getChunkGroupMetadataList()) {
        final IDeviceID device = chunkGroupMetadata.getDevice();
        for (final ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
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

    private long getTsFileWritePointCount(TsFileIOWriter writer) {
      return writer.getChunkGroupMetadataList().stream()
          .flatMap(chunkGroupMetadata -> chunkGroupMetadata.getChunkMetadataList().stream())
          .mapToLong(chunkMetadata -> chunkMetadata.getStatistics().getCount())
          .sum();
    }

    private void close() {
      if (isClosed) {
        return;
      }
      if (dataPartition2Writer != null) {
        for (Map.Entry<DataPartitionInfo, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
          try {
            final TsFileIOWriter writer = entry.getValue();
            if (writer.canWrite()) {
              writer.close();
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
            LOGGER.warn("Close TsFileIOWriter {} error.", entry.getValue().getFile().getPath(), e);
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
            LOGGER.warn("Close ModificationFile {} error.", entry.getValue().getFile(), e);
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
        LOGGER.info("Task dir {} is not empty, skip deleting.", taskDir.getPath());
      } catch (IOException e) {
        LOGGER.warn(MESSAGE_DELETE_FAIL, taskDir.getPath(), e);
      }
      dataPartition2Writer = null;
      dataPartition2Resource = null;
      dataPartition2LastDevice = null;
      dataPartition2ModificationFile = null;
      device2Partition = null;
      isClosed = true;
    }
  }

  private class CleanupTask implements Runnable, Comparable<CleanupTask> {

    private final String uuid;

    private final long delayInMs;
    private long scheduledTime;

    private volatile boolean isLoadTaskRunning = false;
    private volatile boolean isCanceled = false;

    private CleanupTask(String uuid, long delayInMs) {
      this.uuid = uuid;
      this.delayInMs = delayInMs;
      resetScheduledTime();
    }

    public void markLoadTaskRunning() {
      isLoadTaskRunning = true;
      resetScheduledTime();
    }

    public void markLoadTaskNotRunning() {
      isLoadTaskRunning = false;
      resetScheduledTime();
    }

    public void resetScheduledTime() {
      scheduledTime = System.currentTimeMillis() + delayInMs;
    }

    public void cancel() {
      isCanceled = true;
    }

    @Override
    public void run() {
      if (isCanceled) {
        LOGGER.info("Load cleanup task {} is canceled.", uuid);
      } else {
        LOGGER.info("Load cleanup task {} starts.", uuid);
        try {
          forceCloseWriterManager(uuid);
        } catch (Exception e) {
          LOGGER.warn("Load cleanup task {} error.", uuid, e);
        }
      }
    }

    @Override
    public int compareTo(CleanupTask that) {
      return Long.compare(this.scheduledTime, that.scheduledTime);
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
