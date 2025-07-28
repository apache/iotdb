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

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.BatchEncodeInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
  private final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  /* storage group name -> last time */
  private static final Map<String, Long> flushPointsCache = new ConcurrentHashMap<>();
  private final Future<?> encodingTaskFuture;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;

  private final BlockingQueue<Object> encodingTaskQueue = new LinkedBlockingQueue<>();
  private final BlockingQueue<Object> ioTaskQueue =
      (SystemInfo.getInstance().isEncodingFasterThanIo())
          ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
          : new LinkedBlockingQueue<>();

  private String storageGroup;
  private String dataRegionId;

  private IMemTable memTable;

  private volatile long memSerializeTime = 0L;
  private volatile long ioTime = 0L;

  private final BatchEncodeInfo encodeInfo;
  private long[] times;

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
    this.writer = writer;
    this.storageGroup = storageGroup;
    this.dataRegionId = dataRegionId;
    this.encodingTaskFuture = SUB_TASK_POOL_MANAGER.submit(encodingTask);
    this.ioTaskFuture = SUB_TASK_POOL_MANAGER.submit(ioTask);

    long MAX_NUMBER_OF_POINTS_IN_CHUNK = config.getTargetChunkPointNum();
    long TARGET_CHUNK_SIZE = config.getTargetChunkSize();
    this.encodeInfo =
        new BatchEncodeInfo(
            0,
            0,
            0,
            MAX_NUMBER_OF_POINTS_IN_PAGE,
            MAX_NUMBER_OF_POINTS_IN_CHUNK,
            TARGET_CHUNK_SIZE);
    LOGGER.debug(
        "flush task of database {} memtable is created, flushing to file {}.",
        storageGroup,
        writer.getFile().getName());
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
    long sortTime = 0;

    // for map do not use get(key) to iterate
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    List<IDeviceID> deviceIDList = new ArrayList<>(memTableMap.keySet());
    // sort the IDeviceID in lexicographical order
    Collections.sort(deviceIDList);
    for (IDeviceID deviceID : deviceIDList) {
      final Map<String, IWritableMemChunk> value = memTableMap.get(deviceID).getMemChunkMap();
      // skip the empty device/chunk group
      if (memTableMap.get(deviceID).isEmpty() || value.isEmpty()) {
        continue;
      }
      encodingTaskQueue.put(new StartFlushGroupIOTask(deviceID));
      List<String> seriesInOrder = new ArrayList<>(value.keySet());
      Collections.sort(seriesInOrder);
      for (String seriesId : seriesInOrder) {
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = value.get(seriesId);
        if (series.count() == 0) {
          continue;
        }
        /*
         * sort task (first task of flush pipeline)
         */
        series.sortTvListForFlush();
        long subTaskTime = System.currentTimeMillis() - startTime;
        sortTime += subTaskTime;
        WRITING_METRICS.recordFlushSubTaskCost(WritingMetrics.SORT_TASK, subTaskTime);
        encodingTaskQueue.put(series);
      }

      encodingTaskQueue.put(new EndChunkGroupIoTask());
    }
    encodingTaskQueue.put(new TaskEnd());
    LOGGER.debug(
        "Database {} memtable flushing into file {}: data sort time cost {} ms.",
        storageGroup,
        writer.getFile().getName(),
        sortTime);
    WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_SORT, sortTime);

    try {
      encodingTaskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      ioTaskFuture.cancel(true);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    }

    ioTaskFuture.get();

    try {
      long writePlanIndicesStartTime = System.currentTimeMillis();
      writer.writePlanIndices();
      WRITING_METRICS.recordFlushCost(
          WritingMetrics.WRITE_PLAN_INDICES,
          System.currentTimeMillis() - writePlanIndicesStartTime);
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    if (estimatedTemporaryMemSize != 0) {
      SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
    }
    SystemInfo.getInstance().setEncodingFasterThanIo(ioTime >= memSerializeTime);

    MetricService.getInstance()
        .timer(
            System.currentTimeMillis() - start,
            TimeUnit.MILLISECONDS,
            Metric.COST_TASK.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "flush");
  }

  /** encoding task (second task of pipeline) */
  private Runnable encodingTask =
      new Runnable() {

        @SuppressWarnings("squid:S135")
        @Override
        public void run() {
          LOGGER.debug(
              "Database {} memtable flushing to file {} starts to encoding data.",
              storageGroup,
              writer.getFile().getName());
          while (true) {

            Object task;
            try {
              task = encodingTaskQueue.take();
            } catch (InterruptedException e1) {
              LOGGER.error("Take task into ioTaskQueue Interrupted");
              Thread.currentThread().interrupt();
              break;
            }
            if (task instanceof StartFlushGroupIOTask || task instanceof EndChunkGroupIoTask) {
              try {
                ioTaskQueue.put(task);
              } catch (
                  @SuppressWarnings("squid:S2142")
                  InterruptedException e) {
                LOGGER.error(
                    "Database {} memtable flushing to file {}, encoding task is interrupted.",
                    storageGroup,
                    writer.getFile().getName(),
                    e);
                // generally it is because the thread pool is shutdown so the task should be aborted
                break;
              }
            } else if (task instanceof TaskEnd) {
              break;
            } else {
              long starTime = System.currentTimeMillis();
              IWritableMemChunk writableMemChunk = (IWritableMemChunk) task;
              if (writableMemChunk instanceof AlignedWritableMemChunk && times == null) {
                times = new long[MAX_NUMBER_OF_POINTS_IN_PAGE];
              }
              writableMemChunk.encode(ioTaskQueue, encodeInfo, times);
              long subTaskTime = System.currentTimeMillis() - starTime;
              WRITING_METRICS.recordFlushSubTaskCost(WritingMetrics.ENCODING_TASK, subTaskTime);
              memSerializeTime += subTaskTime;
            }
          }
          try {
            ioTaskQueue.put(new TaskEnd());
          } catch (InterruptedException e) {
            LOGGER.error("Put task into ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
          }

          DataRegion.getNonSystemDatabaseName(storageGroup)
              .ifPresent(
                  databaseName ->
                      recordFlushPointsMetricInternal(
                          memTable.getTotalPointsNum(), databaseName, dataRegionId));
          WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_ENCODING, memSerializeTime);
        }
      };

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

  /** io task (third task of pipeline) */
  @SuppressWarnings("squid:S135")
  private Runnable ioTask =
      () -> {
        LOGGER.debug(
            "Database {} memtable flushing to file {} start io.",
            storageGroup,
            writer.getFile().getName());
        while (true) {
          Object ioMessage = null;
          try {
            ioMessage = ioTaskQueue.take();
          } catch (InterruptedException e1) {
            LOGGER.error("take task from ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
            break;
          }
          long starTime = System.currentTimeMillis();
          try {
            if (ioMessage instanceof StartFlushGroupIOTask) {
              this.writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
            } else if (ioMessage instanceof TaskEnd) {
              break;
            } else if (ioMessage instanceof EndChunkGroupIoTask) {
              this.writer.setMinPlanIndex(memTable.getMinPlanIndex());
              this.writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
              this.writer.endChunkGroup();
            } else {
              ((IChunkWriter) ioMessage).writeToFileWriter(this.writer);
            }
          } catch (IOException e) {
            LOGGER.error(
                "Database {} memtable {}, io task meets error.", storageGroup, memTable, e);
            return;
          }
          long subTaskTime = System.currentTimeMillis() - starTime;
          ioTime += subTaskTime;
          WRITING_METRICS.recordFlushSubTaskCost(WritingMetrics.IO_TASK, subTaskTime);
        }
        LOGGER.debug(
            "flushing a memtable to file {} in database {}, io cost {}ms",
            writer.getFile().getName(),
            storageGroup,
            ioTime);
        WRITING_METRICS.recordFlushTsFileSize(storageGroup, writer.getFile().length());
        WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_IO, ioTime);
      };

  static class TaskEnd {

    TaskEnd() {}
  }

  static class EndChunkGroupIoTask {

    EndChunkGroupIoTask() {}
  }

  static class StartFlushGroupIOTask {

    private final IDeviceID deviceId;

    StartFlushGroupIOTask(IDeviceID deviceId) {
      this.deviceId = deviceId;
    }
  }
}
