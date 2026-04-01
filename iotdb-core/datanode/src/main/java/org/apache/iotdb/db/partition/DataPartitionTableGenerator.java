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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generator for DataPartitionTable by scanning tsfile resources. This class scans the data
 * directory structure and builds a complete DataPartitionTable based on existing tsfiles.
 */
public class DataPartitionTableGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DataPartitionTableGenerator.class);

  // Task status
  private volatile TaskStatus status = TaskStatus.NOT_STARTED;
  private volatile String errorMessage;
  private Map<String, DataPartitionTable> databasePartitionTableMap = new ConcurrentHashMap<>();

  // Progress tracking
  private final AtomicInteger processedTimePartitions = new AtomicInteger(0);
  private final AtomicInteger failedTimePartitions = new AtomicInteger(0);
  private long totalTimePartitions = 0;

  // Configuration
  private final ExecutorService executor;
  private final Set<String> databases;
  private final int seriesSlotNum;
  private final String seriesPartitionExecutorClass;

  private final RateLimiter limiter =
      RateLimiter.create(
          (long)
                  IoTDBDescriptor.getInstance()
                      .getConfig()
                      .getPartitionTableRecoverMaxReadMBsPerSecond()
              * 1024
              * 1024);

  public static final Set<String> IGNORE_DATABASE =
      new HashSet<String>() {
        {
          add("root.__audit");
          add("root.__system");
        }
      };

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

  public Map<String, DataPartitionTable> getDatabasePartitionTableMap() {
    return databasePartitionTableMap;
  }

  public enum TaskStatus {
    NOT_STARTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED
  }

  /** Start generating DataPartitionTable asynchronously. */
  public CompletableFuture<Void> startGeneration() {
    if (status != TaskStatus.NOT_STARTED) {
      throw new IllegalStateException("Task is already started or completed");
    }

    status = TaskStatus.IN_PROGRESS;
    return CompletableFuture.runAsync(this::generateDataPartitionTableByMemory);
  }

  private void generateDataPartitionTableByMemory() {
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    SeriesPartitionExecutor seriesPartitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            seriesPartitionExecutorClass, seriesSlotNum);

    try {
      totalTimePartitions =
          StorageEngine.getInstance().getAllDataRegions().stream()
              .mapToLong(
                  dataRegion ->
                      (dataRegion == null)
                          ? 0
                          : dataRegion.getTsFileManager().getTimePartitions().size())
              .sum();
      for (DataRegion dataRegion : StorageEngine.getInstance().getAllDataRegions()) {
        CompletableFuture<Void> regionFuture =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    TsFileManager tsFileManager = dataRegion.getTsFileManager();
                    String databaseName = dataRegion.getDatabaseName();
                    if (!databases.contains(databaseName)
                        || IGNORE_DATABASE.contains(databaseName)) {
                      return;
                    }

                    Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap =
                        new ConcurrentHashMap<>();

                    tsFileManager.readLock();
                    List<TsFileResource> seqTsFileList = tsFileManager.getTsFileList(true);
                    List<TsFileResource> unseqTsFileList = tsFileManager.getTsFileList(false);
                    tsFileManager.readUnlock();

                    constructDataPartitionMap(
                        seqTsFileList, seriesPartitionExecutor, dataPartitionMap);
                    constructDataPartitionMap(
                        unseqTsFileList, seriesPartitionExecutor, dataPartitionMap);

                    if (dataPartitionMap.isEmpty()) {
                      LOG.error("Failed to generate DataPartitionTable, dataPartitionMap is empty");
                      status = TaskStatus.FAILED;
                      errorMessage = "DataPartitionMap is empty after processing resource file";
                      return;
                    }

                    DataPartitionTable dataPartitionTable =
                        new DataPartitionTable(dataPartitionMap);

                    databasePartitionTableMap.compute(
                        databaseName,
                        (k, v) -> {
                          if (v == null) {
                            return new DataPartitionTable(dataPartitionMap);
                          }
                          v.merge(dataPartitionTable);
                          return v;
                        });
                  } catch (Exception e) {
                    LOG.error("Error processing data region: {}", dataRegion.getDatabaseName(), e);
                    failedTimePartitions.incrementAndGet();
                    errorMessage = "Failed to process data region: " + e.getMessage();
                  }
                },
                executor);
        futures.add(regionFuture);
      }

      // Wait for all tasks to complete
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      status = TaskStatus.COMPLETED;
      LOG.info(
          "DataPartitionTable generation completed successfully. Processed: {}, Failed: {}",
          processedTimePartitions.get(),
          failedTimePartitions.get());
    } catch (Exception e) {
      LOG.error("Failed to generate DataPartitionTable", e);
      status = TaskStatus.FAILED;
      errorMessage = "Generation failed: " + e.getMessage();
    }
  }

  private void constructDataPartitionMap(
      List<TsFileResource> seqTsFileList,
      SeriesPartitionExecutor seriesPartitionExecutor,
      Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap) {
    Set<Long> timeSlotIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    for (TsFileResource tsFileResource : seqTsFileList) {
      long timeSlotId = tsFileResource.getTsFileID().timePartitionId;
      try {
        Set<IDeviceID> devices = tsFileResource.getDevices(limiter);
        int regionId = tsFileResource.getTsFileID().regionId;

        TConsensusGroupId consensusGroupId = new TConsensusGroupId();
        consensusGroupId.setId(regionId);
        consensusGroupId.setType(TConsensusGroupType.DataRegion);

        for (IDeviceID deviceId : devices) {
          TSeriesPartitionSlot seriesSlotId =
              seriesPartitionExecutor.getSeriesPartitionSlot(deviceId);
          TTimePartitionSlot timePartitionSlot =
              new TTimePartitionSlot(TimePartitionUtils.getStartTimeByPartitionId(timeSlotId));
          dataPartitionMap
              .computeIfAbsent(
                  seriesSlotId, empty -> newSeriesPartitionTable(consensusGroupId, timeSlotId))
              .putDataPartition(timePartitionSlot, consensusGroupId);
        }
        if (!timeSlotIds.contains(timeSlotId)) {
          timeSlotIds.add(timeSlotId);
          processedTimePartitions.incrementAndGet();
        }
      } catch (Exception e) {
        if (!timeSlotIds.contains(timeSlotId)) {
          timeSlotIds.add(timeSlotId);
          failedTimePartitions.incrementAndGet();
        }
        LOG.error("Failed to process tsfile {}, {}", tsFileResource.getTsFileID(), e.getMessage());
      }
    }

    timeSlotIds.clear();
  }

  private static SeriesPartitionTable newSeriesPartitionTable(
      TConsensusGroupId consensusGroupId, long timeSlotId) {
    SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();
    TTimePartitionSlot timePartitionSlot =
        new TTimePartitionSlot(TimePartitionUtils.getStartTimeByPartitionId(timeSlotId));
    seriesPartitionTable.putDataPartition(timePartitionSlot, consensusGroupId);
    return seriesPartitionTable;
  }

  // Getters
  public TaskStatus getStatus() {
    return status;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public double getProgress() {
    if (totalTimePartitions == 0) {
      return 0.0;
    }
    return (double) (processedTimePartitions.get() + failedTimePartitions.get())
        / totalTimePartitions;
  }
}
