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
package org.apache.iotdb.db.storageengine.dataregion.flush;

import org.apache.iotdb.commons.concurrent.dynamic.DynamicThread;
import org.apache.iotdb.commons.concurrent.dynamic.DynamicThreadGroup;
import org.apache.iotdb.commons.concurrent.pipeline.Task;
import org.apache.iotdb.commons.concurrent.pipeline.TaskRunner;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.tasks.FlushContext;
import org.apache.iotdb.db.storageengine.dataregion.flush.tasks.FlushDeviceContext;
import org.apache.iotdb.db.storageengine.dataregion.flush.tasks.SortSeriesTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.file.metadata.IDeviceID;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * flush task to flush one memtable using a pipeline model to flush, which is sort memtable ->
 * encoding -> write to disk (io task)
 */
public class MemTableFlushTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final FlushSubTaskPoolManager SUB_TASK_POOL_MANAGER =
      FlushSubTaskPoolManager.getInstance();
  private static final WritingMetrics WRITING_METRICS = WritingMetrics.getInstance();
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  /* storage group name -> last time */
  private static final Map<String, Long> flushPointsCache = new ConcurrentHashMap<>();
  private final DynamicThreadGroup sortTasks;
  private final DynamicThreadGroup encodingTasks;
  private final DynamicThreadGroup ioTask;

  private final LinkedBlockingQueue<Task> sortTaskQueue = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<Task> encodingTaskQueue = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<Task> ioTaskQueue =
      (SystemInfo.getInstance().isEncodingFasterThanIo())
          ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
          : new LinkedBlockingQueue<>();

  private String storageGroup;
  private String dataRegionId;

  private IMemTable memTable;
  private FlushContext allContext;
  private String taskName;

  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current database
   */
  public MemTableFlushTask(
      IMemTable memTable,
      RestorableTsFileIOWriter writer,
      String storageGroup,
      String dataRegionId) {
    this.memTable = memTable;
    this.storageGroup = storageGroup;
    this.dataRegionId = dataRegionId;
    this.allContext = new FlushContext();
    this.allContext.setWriter(writer);
    this.allContext.setMemTable(memTable);
    this.taskName = storageGroup + "-" + dataRegionId + "-" + writer.getFile();

    this.sortTasks =
        new DynamicThreadGroup(
            taskName,
            SUB_TASK_POOL_MANAGER::submit,
            this::newSortThread,
            config.getFlushMemTableMinSubThread(),
            config.getFlushMemTableMaxSubThread());
    this.encodingTasks =
        new DynamicThreadGroup(
            taskName,
            SUB_TASK_POOL_MANAGER::submit,
            this::newEncodingThread,
            config.getFlushMemTableMinSubThread(),
            config.getFlushMemTableMaxSubThread());
    this.ioTask =
        new DynamicThreadGroup(taskName, SUB_TASK_POOL_MANAGER::submit, this::newIOThread, 1, 1);

    LOGGER.debug(
        "flush task of database {} memtable is created, flushing to file {}.",
        storageGroup,
        allContext.getWriter().getFile().getName());
  }

  /** the function for flushing memtable. */
  @SuppressWarnings("squid:S3776")
  public void syncFlushMemTable() throws ExecutionException, InterruptedException {
    long avgSeriesPointsNum =
        memTable.getSeriesNumber() == 0
            ? 0
            : memTable.getTotalPointsNum() / memTable.getSeriesNumber();
    WRITING_METRICS.recordFlushingMemTableStatus(
        storageGroup,
        memTable.memSize(),
        memTable.getSeriesNumber(),
        memTable.getTotalPointsNum(),
        avgSeriesPointsNum);

    long estimatedTemporaryMemSize = 0L;
    if (SystemInfo.getInstance().isEncodingFasterThanIo()) {
      estimatedTemporaryMemSize =
          memTable.getSeriesNumber() == 0
              ? 0
              : memTable.memSize()
                  / memTable.getSeriesNumber()
                  * config.getIoTaskQueueSizeForFlushing();
      SystemInfo.getInstance().applyTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
    }
    long start = System.currentTimeMillis();

    // for map do not use get(key) to iterate
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    List<IDeviceID> deviceIDList = new ArrayList<>(memTableMap.keySet());
    // sort the IDeviceID in lexicographical order
    deviceIDList.removeIf(
        d -> memTableMap.get(d).count() == 0 || memTableMap.get(d).getMemChunkMap().isEmpty());
    Collections.sort(deviceIDList);
    allContext.setDeviceContexts(new ArrayList<>());

    if (deviceIDList.isEmpty()) {
      LOGGER.info("Nothing to be flushed for {}", memTable);
      return;
    }

    for (IDeviceID deviceID : deviceIDList) {
      // create a context for each device
      FlushDeviceContext flushDeviceContext = new FlushDeviceContext();
      flushDeviceContext.setDeviceID(deviceID);
      allContext.getDeviceContexts().add(flushDeviceContext);

      final Map<String, IWritableMemChunk> memChunkMap = memTableMap.get(deviceID).getMemChunkMap();
      List<String> seriesInOrder = new ArrayList<>(memChunkMap.keySet());
      // skip the empty device/chunk group
      seriesInOrder.removeIf(s -> memChunkMap.get(s).count() == 0);
      Collections.sort(seriesInOrder);
      // record the series order in the device context
      flushDeviceContext.setMeasurementIds(seriesInOrder);
      flushDeviceContext.setChunkWriters(new IChunkWriter[seriesInOrder.size()]);
      flushDeviceContext.setSeriesIndexMap(new HashMap<>());

      for (int j = 0; j < seriesInOrder.size(); j++) {
        // starting from sorting each series
        String seriesId = seriesInOrder.get(j);
        flushDeviceContext.getSeriesIndexMap().put(seriesId, j);
        IWritableMemChunk series = memChunkMap.get(seriesId);

        SortSeriesTask sortSeriesTask = new SortSeriesTask();
        sortSeriesTask.setSeriesId(seriesId);
        sortSeriesTask.setAllContext(allContext);
        sortSeriesTask.setDeviceId(deviceID);
        sortSeriesTask.setSeries(series);
        sortSeriesTask.setDeviceContext(flushDeviceContext);

        sortTaskQueue.put(sortSeriesTask);
      }
    }

    this.sortTasks.init();
    this.encodingTasks.init();
    this.ioTask.init();

    try {
      ioTask.join();
    } catch (InterruptedException | ExecutionException e) {
      ioTask.cancelAll();
      encodingTasks.cancelAll();
      sortTasks.cancelAll();
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    }
    encodingTasks.cancelAll();
    sortTasks.cancelAll();

    LOGGER.info(
        "Database {}, flushing memtable {} into disk: Sort data cost "
            + "{} ms. (total thread time)",
        storageGroup,
        allContext.getWriter().getFile().getName(),
        allContext.getSortTime().get());
    LOGGER.info(
        "Database {}, flushing memtable {} into disk: Encoding data cost "
            + "{} ms. (total thread time)",
        storageGroup,
        allContext.getWriter().getFile().getName(),
        allContext.getEncodingTime().get());
    LOGGER.info(
        "Database {}, flushing memtable {} into disk: IO cost " + "{} ms. (total thread time)",
        storageGroup,
        allContext.getWriter().getFile().getName(),
        allContext.getIoTime().get());

    try {
      long writePlanIndicesStartTime = System.currentTimeMillis();
      allContext.getWriter().writePlanIndices();
      WRITING_METRICS.recordFlushCost(
          WritingMetrics.WRITE_PLAN_INDICES,
          System.currentTimeMillis() - writePlanIndicesStartTime);
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    if (estimatedTemporaryMemSize != 0) {
      SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
    }
    SystemInfo.getInstance()
        .setEncodingFasterThanIo(
            allContext.getIoTime().get() >= allContext.getEncodingTime().get());
    MetricService.getInstance()
        .timer(
            System.currentTimeMillis() - start,
            TimeUnit.MILLISECONDS,
            Metric.COST_TASK.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "flush");
  }

  public static void recordFlushPointsMetricInternal(
      long totalPointsNum, String storageGroupName, String dataRegionId) {
    long currentTime = CommonDateTimeUtils.currentTime();
    // compute the flush points
    long writeTime =
        flushPointsCache.compute(
            storageGroupName,
            (storageGroup, lastTime) -> {
              if (lastTime == null || lastTime != currentTime) {
                return currentTime;
              } else {
                return currentTime + 1;
              }
            });
    // record the flush points
    MetricService.getInstance()
        .gaugeWithInternalReportAsync(
            totalPointsNum,
            Metric.POINTS.toString(),
            MetricLevel.CORE,
            writeTime,
            Tag.DATABASE.toString(),
            storageGroupName,
            Tag.TYPE.toString(),
            "flush",
            Tag.REGION.toString(),
            dataRegionId);
  }

  private DynamicThread newSortThread() {
    return new TaskRunner(
        sortTasks, this::cleanSortThread, sortTaskQueue, encodingTaskQueue, taskName + "-sort");
  }

  private DynamicThread newEncodingThread() {
    return new TaskRunner(
        encodingTasks,
        this::cleanEncodingThread,
        encodingTaskQueue,
        ioTaskQueue,
        taskName + "-encode");
  }

  private DynamicThread newIOThread() {
    return new TaskRunner(null, this::cleanIOThread, ioTaskQueue, ioTaskQueue, taskName + "-io");
  }

  private void cleanSortThread() {
    WRITING_METRICS.recordFlushCost(
        WritingMetrics.FLUSH_STAGE_SORT, allContext.getSortTime().get());
  }

  private void cleanEncodingThread() {
    DataRegion.getNonSystemDatabaseName(storageGroup)
        .ifPresent(
            databaseName ->
                recordFlushPointsMetricInternal(
                    memTable.getTotalPointsNum(), databaseName, dataRegionId));
    WRITING_METRICS.recordFlushCost(
        WritingMetrics.FLUSH_STAGE_ENCODING, allContext.getEncodingTime().get());
  }

  private void cleanIOThread() {
    WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_IO, allContext.getIoTime().get());
    WRITING_METRICS.recordFlushTsFileSize(storageGroup, allContext.getWriter().getFile().length());
  }
}
