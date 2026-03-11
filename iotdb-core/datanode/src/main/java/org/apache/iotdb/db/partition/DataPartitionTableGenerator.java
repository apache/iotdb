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

package org.apache.iotdb.db.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.commons.utils.rateLimiter.LeakyBucketRateLimiter;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator for DataPartitionTable by scanning tsfile resources. This class scans the data
 * directory structure and builds a complete DataPartitionTable based on existing tsfiles.
 */
public class DataPartitionTableGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DataPartitionTableGenerator.class);

  // Task status
  private volatile TaskStatus status = TaskStatus.NOT_STARTED;
  private volatile String errorMessage;
  private volatile DataPartitionTable dataPartitionTable;

  // Progress tracking
  private final AtomicInteger processedFiles = new AtomicInteger(0);
  private final AtomicInteger failedFiles = new AtomicInteger(0);
  private final AtomicLong totalFiles = new AtomicLong(0);

  // Configuration
  private String[] dataDirectories;
  private final ExecutorService executor;
  private final Set<String> databases;
  private final int seriesSlotNum;
  private final String seriesPartitionExecutorClass;

  private static final int EXECUTOR_MAX_TIMEOUT = 60;

  private static final LeakyBucketRateLimiter limiter =
          new LeakyBucketRateLimiter((long) IoTDBDescriptor.getInstance().getConfig().getPartitionTableRecoverMaxReadBytesPerSecond() * 1024 * 1024);

  public static final String SCAN_FILE_SUFFIX_NAME = ".tsfile";
  public static final Set<String> IGNORE_DATABASE = new HashSet<String>() {{
    add("root.__audit");
  }};

  public DataPartitionTableGenerator(
          ExecutorService executor,
          Set<String> databases,
          int seriesSlotNum,
          String seriesPartitionExecutorClass) {
    this.executor = executor;
    this.databases = databases;
    this.seriesSlotNum = seriesSlotNum;
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public DataPartitionTableGenerator(
      String dataDirectory,
      ExecutorService executor,
      Set<String> databases,
      int seriesSlotNum,
      String seriesPartitionExecutorClass) {
    this.dataDirectories = new String[]{dataDirectory};
    this.executor = executor;
    this.databases = databases;
    this.seriesSlotNum = seriesSlotNum;
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public DataPartitionTableGenerator(
          String[] dataDirectories,
          ExecutorService executor,
          Set<String> databases,
          int seriesSlotNum,
          String seriesPartitionExecutorClass) {
    this.dataDirectories = dataDirectories;
    this.executor = executor;
    this.databases = databases;
    this.seriesSlotNum = seriesSlotNum;
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public enum TaskStatus {
    NOT_STARTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED
  }

  /**
   * Start generating DataPartitionTable asynchronously.
   *
   */
  public CompletableFuture<Void> startGeneration() {
    if (status != TaskStatus.NOT_STARTED) {
      throw new IllegalStateException("Task is already started or completed");
    }

    status = TaskStatus.IN_PROGRESS;
    return CompletableFuture.runAsync(this::generateDataPartitionTableByMemory);
  }

  private void generateDataPartitionTableByMemory() {
    Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap = new ConcurrentHashMap<>();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    SeriesPartitionExecutor seriesPartitionExecutor =
            SeriesPartitionExecutor.getSeriesPartitionExecutor(
                    seriesPartitionExecutorClass, seriesSlotNum);

    for (DataRegion dataRegion : StorageEngine.getInstance().getAllDataRegions()) {
      CompletableFuture<Void> regionFuture =
              CompletableFuture.runAsync(
                      () -> {
                        TsFileManager tsFileManager = dataRegion.getTsFileManager();
                        String databaseName = dataRegion.getDatabaseName();
                        if (!databases.contains(databaseName) || IGNORE_DATABASE.contains(databaseName)) {
                          return;
                        }

                        tsFileManager.readLock();
                        List<TsFileResource> seqTsFileList = tsFileManager.getTsFileList(true);
                        List<TsFileResource> unseqTsFileList = tsFileManager.getTsFileList(false);
                        tsFileManager.readUnlock();

                        constructDataPartitionMap(seqTsFileList, seriesPartitionExecutor, dataPartitionMap);
                        constructDataPartitionMap(unseqTsFileList, seriesPartitionExecutor, dataPartitionMap);
                      },
                      executor);
      futures.add(regionFuture);
    }

    // Wait for all tasks to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    if (dataPartitionMap.isEmpty()) {
      LOG.error("Failed to generate DataPartitionTable, dataPartitionMap is empty");
      status = TaskStatus.FAILED;
      return;
    }

    dataPartitionTable = new DataPartitionTable(dataPartitionMap);
    status = TaskStatus.COMPLETED;
  }

  private static void constructDataPartitionMap(List<TsFileResource> seqTsFileList, SeriesPartitionExecutor seriesPartitionExecutor, Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap) {
    for (TsFileResource tsFileResource : seqTsFileList) {
      Set<IDeviceID> devices = tsFileResource.getDevices(limiter);
      long timeSlotId = tsFileResource.getTsFileID().timePartitionId;
      int regionId = tsFileResource.getTsFileID().regionId;

      TConsensusGroupId consensusGroupId = new TConsensusGroupId();
      consensusGroupId.setId(regionId);
      consensusGroupId.setType(TConsensusGroupType.DataRegion);

      for (IDeviceID deviceId : devices) {
        TSeriesPartitionSlot seriesSlotId = seriesPartitionExecutor.getSeriesPartitionSlot(deviceId);
        TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(TimePartitionUtils.getTimeByPartitionId(timeSlotId));
        dataPartitionMap.computeIfAbsent(seriesSlotId, empty -> newSeriesPartitionTable(consensusGroupId, timeSlotId)).putDataPartition(timePartitionSlot, consensusGroupId);
      }
    }
  }

  /** Generate DataPartitionTable by scanning all resource files. */
  private void generateDataPartitionTable() throws IOException {
    LOG.info("Starting DataPartitionTable generation from {} directories", dataDirectories.length);

    List<CompletableFuture<Void>> futures = new ArrayList<>();

    Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap = new ConcurrentHashMap<>();

    try {
      // Count total files first for progress tracking
      countTotalFiles();

      // Process all data directories
      for (String dataDirectory : dataDirectories) {
        LOG.info("Processing data directory: {}", dataDirectory);
        
        // First layer: database directories
        Files.list(Paths.get(dataDirectory))
            .filter(Files::isDirectory)
            .forEach(sequenceTypePath -> {
              try {
                Files.list(sequenceTypePath)
                        .filter(Files::isDirectory)
                        .forEach(dbPath -> {
                          String databaseName = dbPath.getFileName().toString();
                          if (!databases.contains(databaseName) || IGNORE_DATABASE.contains(databaseName)) {
                            return;
                          }

                          if (LOG.isDebugEnabled()) {
                            LOG.debug("Processing database: {}", databaseName);
                          }

                          try {
                            Files.list(dbPath)
                                    .filter(Files::isDirectory)
                                    .forEach(
                                            regionPath -> {
                                              processRegionDirectory(
                                                      regionPath,
                                                      databaseName,
                                                      dataPartitionMap,
                                                      executor,
                                                      futures);
                                            });
                          } catch (IOException e) {
                            LOG.error("Failed to process database directory: {}", dbPath, e);
                            failedFiles.incrementAndGet();
                          }
                        });
              } catch (IOException e) {
                LOG.error("Failed to process database directory: {}", sequenceTypePath, e);
                failedFiles.incrementAndGet();
              }
            });
      }

      // Wait for all tasks to complete
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      dataPartitionTable = new DataPartitionTable(dataPartitionMap);

      LOG.info(
          "DataPartitionTable generation completed. Processed: {}, Failed: {}",
          processedFiles.get(),
          failedFiles.get());

    } finally {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(EXECUTOR_MAX_TIMEOUT, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Process a region directory. */
  private void processRegionDirectory(
      java.nio.file.Path regionPath,
      String databaseName,
      Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap,
      ExecutorService executor,
      List<CompletableFuture<Void>> futures) {

    int regionId;
    try {
      regionId = Integer.parseInt(regionPath.getFileName().toString());
      LOG.debug("Processing region: {}", regionId);
    } catch (NumberFormatException e) {
      LOG.error("Invalid region directory: {}", regionPath);
      return;
    }

    TConsensusGroupId consensusGroupId = new TConsensusGroupId();
    consensusGroupId.setId(regionId);
    consensusGroupId.setType(TConsensusGroupType.DataRegion);

    // Process time partitions asynchronously
    CompletableFuture<Void> regionFuture =
        CompletableFuture.runAsync(
            () -> {
              try {
                Files.list(regionPath)
                    .filter(Files::isDirectory)
                    .forEach(
                        timeSlotPath -> {
                          processTimeSlotDirectory(
                              timeSlotPath, databaseName, consensusGroupId, dataPartitionMap);
                        });
              } catch (IOException e) {
                LOG.error("Failed to list region directory: {}", regionPath, e);
              }
            },
            executor);

    futures.add(regionFuture);
  }

  /** Process a time slot directory. */
  private void processTimeSlotDirectory(
      java.nio.file.Path timeSlotPath,
      String databaseName,
      TConsensusGroupId consensusGroupId,
      Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap) {

    long timeSlotLong;
    try {
      timeSlotLong = Long.parseLong(timeSlotPath.getFileName().toString());
      LOG.debug("Processing time slot: {}", timeSlotLong);
    } catch (NumberFormatException e) {
      LOG.error("Invalid time slot directory: {}", timeSlotPath);
      return;
    }

    try {
      // Fourth layer: .resource files
      Files.walk(timeSlotPath)
          .filter(Files::isRegularFile)
          .filter(p -> p.toString().endsWith(SCAN_FILE_SUFFIX_NAME))
          .forEach(
              tsFilePath -> {
                processTsFile(
                    tsFilePath.toFile(),
                    consensusGroupId,
                    timeSlotLong,
                    dataPartitionMap);
              });
    } catch (IOException e) {
      LOG.error("Failed to walk time slot directory: {}", timeSlotPath, e);
    }
  }

  /** Process a single tsfile. */
  private void processTsFile(
      File tsFile,
      TConsensusGroupId consensusGroupId,
      long timeSlotId,
      Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap) {
    try {
      TsFileResource tsFileResource = new TsFileResource(tsFile.getAbsoluteFile());
      tsFileResource.deserialize();

      Set<org.apache.tsfile.file.metadata.IDeviceID> devices = tsFileResource.getDevices(limiter);
      processedFiles.incrementAndGet();

      SeriesPartitionExecutor seriesPartitionExecutor =
          SeriesPartitionExecutor.getSeriesPartitionExecutor(
              seriesPartitionExecutorClass, seriesSlotNum);

      for (org.apache.tsfile.file.metadata.IDeviceID deviceId : devices) {
        TSeriesPartitionSlot seriesSlotId = seriesPartitionExecutor.getSeriesPartitionSlot(deviceId);
        TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(TimePartitionUtils.getTimeByPartitionId(timeSlotId));
        dataPartitionMap.computeIfAbsent(seriesSlotId, empty -> newSeriesPartitionTable(consensusGroupId, timeSlotId)).putDataPartition(timePartitionSlot, consensusGroupId);
      }

      if (processedFiles.get() % 1000 == 0) {
        LOG.info("Processed {} files, current: {}", processedFiles.get(), tsFile.getName());
      }
    } catch (IOException e) {
      failedFiles.incrementAndGet();
      LOG.error("Failed to process tsfile: {} -> {}", tsFile.getAbsolutePath(), e.getMessage());
    }
  }

  private static SeriesPartitionTable newSeriesPartitionTable(TConsensusGroupId consensusGroupId, long timeSlotId) {
    SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(TimePartitionUtils.getTimeByPartitionId(timeSlotId));
    seriesPartitionTable.putDataPartition(timePartitionSlot, consensusGroupId);
    return seriesPartitionTable;
  }

  /** Count total files for progress tracking. */
  private void countTotalFiles() throws IOException {
    AtomicLong fileCount = new AtomicLong(0);

    for (String dataDirectory : dataDirectories) {
      Files.list(Paths.get(dataDirectory))
              .filter(Files::isDirectory)
              .forEach(sequenceTypePath -> {
                        try {
                          Files.list(sequenceTypePath)
                                  .filter(Files::isDirectory)
                                  .forEach(dbPath -> {
                                    String databaseName = dbPath.getFileName().toString();
                                    if (!databases.contains(databaseName) || IGNORE_DATABASE.contains(databaseName)) {
                                      return;
                                    }

                                    try {
                                      Files.walk(dbPath)
                                              .filter(Files::isRegularFile)
                                              .filter(p -> p.toString().endsWith(SCAN_FILE_SUFFIX_NAME))
                                              .forEach(p -> fileCount.incrementAndGet());
                                    } catch (IOException e) {
                                      LOG.error("countTotalFiles failed when scan {}", dbPath, e);
                                    }
                                  });
              } catch (IOException e) {
                          LOG.error("countTotalFiles failed when scan {}", sequenceTypePath, e);
                        }
              });
    }

    totalFiles.set(fileCount.get());
    LOG.info("Found {} resource files to process", totalFiles.get());
  }

  // Getters
  public TaskStatus getStatus() {
    return status;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public DataPartitionTable getDataPartitionTable() {
    return dataPartitionTable;
  }

  public int getProcessedFiles() {
    return processedFiles.get();
  }

  public int getFailedFiles() {
    return failedFiles.get();
  }

  public long getTotalFiles() {
    return totalFiles.get();
  }

  public double getProgress() {
    if (totalFiles.get() == 0) {
      return 0.0;
    }
    return (double) (processedFiles.get() + failedFiles.get()) / totalFiles.get();
  }
}
