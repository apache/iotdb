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
package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.commons.concurrent.dynamic.DynamicThread;
import org.apache.iotdb.commons.concurrent.dynamic.DynamicThreadGroup;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.flush.tasks.FlushContext;
import org.apache.iotdb.db.engine.flush.tasks.FlushDeviceContext;
import org.apache.iotdb.db.engine.flush.tasks.SortSeriesTask;
import org.apache.iotdb.db.engine.flush.tasks.Task;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.service.metrics.recorder.WritingMetricsManager;
import org.apache.iotdb.metrics.utils.IoTDBMetricsUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * flush task to flush one memtable using a pipeline model to flush, which is sort memtable ->
 * encoding -> write to disk (io task)
 */
public class MemTableFlushTaskV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTaskV2.class);
  private static final FlushSubTaskPoolManager SUB_TASK_POOL_MANAGER =
      FlushSubTaskPoolManager.getInstance();
  private static final WritingMetricsManager WRITING_METRICS = WritingMetricsManager.getInstance();
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final DynamicThreadGroup sortTasks;
  private final DynamicThreadGroup encodingTasks;
  private final Future<?> ioTaskFuture;

  private final LinkedBlockingQueue<Task> sortTaskQueue = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<Task> encodingTaskQueue = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<Task> ioTaskQueue =
      (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo())
          ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
          : new LinkedBlockingQueue<>();

  private String storageGroup;
  private String dataRegionId;

  private IMemTable memTable;
  private FlushContext allContext;

  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current database
   */
  public MemTableFlushTaskV2(
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

    this.sortTasks =
        new DynamicThreadGroup(
            storageGroup + "-" + dataRegionId + "-" + memTable,
            SUB_TASK_POOL_MANAGER::submit,
            this::newSortThread,
            config.getFlushMemTableMinSubThread(),
            config.getFlushMemTableMaxSubThread());
    this.encodingTasks =
        new DynamicThreadGroup(
            storageGroup + "-" + dataRegionId + "-" + memTable,
            SUB_TASK_POOL_MANAGER::submit,
            this::newEncodingThread,
            config.getFlushMemTableMinSubThread(),
            config.getFlushMemTableMaxSubThread());
    this.ioTaskFuture = SUB_TASK_POOL_MANAGER.submit(newIOThread());
    LOGGER.debug(
        "flush task of database {} memtable is created, flushing to file {}.",
        storageGroup,
        allContext.getWriter().getFile().getName());
  }

  /** the function for flushing memtable. */
  public void syncFlushMemTable() throws ExecutionException, InterruptedException {
    long avgSeriesPointsNum =
        memTable.getSeriesNumber() == 0
            ? 0
            : memTable.getTotalPointsNum() / memTable.getSeriesNumber();
    LOGGER.info(
        "The memTable size of SG {} is {}, the avg series points num in chunk is {}, total timeseries number is {}",
        storageGroup,
        memTable.memSize(),
        avgSeriesPointsNum,
        memTable.getSeriesNumber());
    WRITING_METRICS.recordFlushingMemTableStatus(
        storageGroup,
        memTable.memSize(),
        memTable.getSeriesNumber(),
        memTable.getTotalPointsNum(),
        avgSeriesPointsNum);

    long estimatedTemporaryMemSize = 0L;
    if (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo()) {
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
    deviceIDList.sort(Comparator.comparing(IDeviceID::toStringID));
    deviceIDList.removeIf(
        d -> memTableMap.get(d).count() == 0 || memTableMap.get(d).getMemChunkMap().isEmpty());

    allContext.setDeviceContexts(new ArrayList<>());

    for (IDeviceID deviceID : deviceIDList) {
      FlushDeviceContext flushDeviceContext = new FlushDeviceContext();
      allContext.getDeviceContexts().add(flushDeviceContext);
      flushDeviceContext.setDeviceID(deviceID);
      final Map<String, IWritableMemChunk> value = memTableMap.get(deviceID).getMemChunkMap();
      List<String> seriesInOrder = new ArrayList<>(value.keySet());
      // skip the empty device/chunk group
      seriesInOrder.removeIf(s -> value.get(s).count() == 0);
      seriesInOrder.sort((String::compareTo));
      flushDeviceContext.setMeasurementIds(seriesInOrder);
      flushDeviceContext.setChunkWriters(new IChunkWriter[seriesInOrder.size()]);
      flushDeviceContext.setSeriesIndexMap(new HashMap<>());

      for (int j = 0; j < seriesInOrder.size(); j++) {
        // starting from sorting each series
        String seriesId = seriesInOrder.get(j);
        flushDeviceContext.getSeriesIndexMap().put(seriesId, j);
        IWritableMemChunk series = value.get(seriesId);

        SortSeriesTask sortSeriesTask = new SortSeriesTask();
        sortSeriesTask.setSeriesId(seriesId);
        sortSeriesTask.setAllContext(allContext);
        sortSeriesTask.setDeviceId(deviceID);
        sortSeriesTask.setSeries(series);
        sortSeriesTask.setDeviceContext(flushDeviceContext);

        sortTaskQueue.put(sortSeriesTask);
      }
    }

    try {
      ioTaskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      ioTaskFuture.cancel(true);
      encodingTasks.cancelAll();
      sortTasks.cancelAll();
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    }
    encodingTasks.cancelAll();
    sortTasks.cancelAll();

    try {
      long writePlanIndicesStartTime = System.currentTimeMillis();
      allContext.getWriter().writePlanIndices();
      WRITING_METRICS.recordFlushCost(
          WritingMetrics.WRITE_PLAN_INDICES,
          System.currentTimeMillis() - writePlanIndicesStartTime);
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    if (config.isEnableMemControl()) {
      if (estimatedTemporaryMemSize != 0) {
        SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
      }
      SystemInfo.getInstance()
          .setEncodingFasterThanIo(
              allContext.getIoTime().get() >= allContext.getEncodingTime().get());
    }

    MetricService.getInstance()
        .timer(
            System.currentTimeMillis() - start,
            TimeUnit.MILLISECONDS,
            Metric.COST_TASK.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "flush");

    LOGGER.info(
        "Database {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup,
        memTable,
        System.currentTimeMillis() - start);
  }

  protected void metricFlush() {
    if (!storageGroup.startsWith(IoTDBMetricsUtils.DATABASE)) {
      int lastIndex = storageGroup.lastIndexOf("-");
      if (lastIndex == -1) {
        lastIndex = storageGroup.length();
      }
      MetricService.getInstance()
          .gaugeWithInternalReportAsync(
              memTable.getTotalPointsNum(),
              Metric.POINTS.toString(),
              MetricLevel.CORE,
              Tag.DATABASE.toString(),
              storageGroup.substring(0, lastIndex),
              Tag.TYPE.toString(),
              "flush",
              Tag.REGION.toString(),
              dataRegionId);
    }
  }

  protected class TaskRunner extends DynamicThread {

    private static final String TASK_NAME_SORT = "sort data";
    private static final String TASK_NAME_ENCODING = "encode data";
    private static final String TASK_NAME_IO = "write file";
    private Runnable cleanUp;
    private String taskName;
    private BlockingQueue<Task> input;
    private BlockingQueue<Task> output;

    public TaskRunner(
        DynamicThreadGroup threadGroup,
        Runnable cleanUp,
        String taskName,
        BlockingQueue<Task> input,
        BlockingQueue<Task> output) {
      super(threadGroup);
      this.cleanUp = cleanUp;
      this.taskName = taskName;
      this.input = input;
      this.output = output;
    }

    @Override
    public void runInternal() {
      LOGGER.debug(
          "Database {} memtable flushing to file {} starts to {}.",
          storageGroup,
          allContext.getWriter().getFile().getName(),
          taskName);
      while (!Thread.interrupted()) {
        Task task;
        try {
          task = input.take();
          idleToRunning();
          task.run();
          Task nextTask = task.nextTask();
          if (nextTask != null) {
            output.put(nextTask);
          }
          runningToIdle();

          if (shouldExit()) {
            break;
          }
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      cleanUp.run();
    }
  }

  private DynamicThread newSortThread() {
    return new TaskRunner(
        sortTasks,
        this::cleanSortThread,
        TaskRunner.TASK_NAME_SORT,
        sortTaskQueue,
        encodingTaskQueue);
  }

  private DynamicThread newEncodingThread() {
    return new TaskRunner(
        encodingTasks,
        this::cleanEncodingThread,
        TaskRunner.TASK_NAME_ENCODING,
        encodingTaskQueue,
        ioTaskQueue);
  }

  private DynamicThread newIOThread() {
    return new TaskRunner(
        null, this::cleanIOThread, TaskRunner.TASK_NAME_IO, ioTaskQueue, ioTaskQueue);
  }

  private void cleanSortThread() {
    metricFlush();
    LOGGER.info(
        "Database {}, flushing memtable {} into disk: Sort data cost "
            + "{} ms. (total thread time)",
        storageGroup,
        allContext.getWriter().getFile().getName(),
        allContext.getSortTime().get());
    WRITING_METRICS.recordFlushCost(
        WritingMetrics.FLUSH_STAGE_SORT, allContext.getSortTime().get());
  }

  private void cleanEncodingThread() {
    metricFlush();
    LOGGER.info(
        "Database {}, flushing memtable {} into disk: Encoding data cost "
            + "{} ms. (total thread time)",
        storageGroup,
        allContext.getWriter().getFile().getName(),
        allContext.getEncodingTime().get());
    WRITING_METRICS.recordFlushCost(
        WritingMetrics.FLUSH_STAGE_ENCODING, allContext.getEncodingTime().get());
  }

  private void cleanIOThread() {
    metricFlush();
    LOGGER.info(
        "Database {}, flushing memtable {} into disk: IO cost " + "{} ms. (total thread time)",
        storageGroup,
        allContext.getWriter().getFile().getName(),
        allContext.getIoTime().get());
    WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_IO, allContext.getIoTime().get());
    WRITING_METRICS.recordFlushTsFileSize(storageGroup, allContext.getWriter().getFile().length());
  }
}
