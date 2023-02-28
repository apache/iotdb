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

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
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
import java.util.List;
import java.util.Map;
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
  private static final WritingMetricsManager WRITING_METRICS = WritingMetricsManager.getInstance();
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Future<?> encodingTaskFuture;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;

  private final LinkedBlockingQueue<Object> encodingTaskQueue = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<Object> ioTaskQueue =
      (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo())
          ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
          : new LinkedBlockingQueue<>();

  private String storageGroup;
  private String dataRegionId;

  private IMemTable memTable;

  private volatile long memSerializeTime = 0L;
  private volatile long ioTime = 0L;

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
    LOGGER.debug(
        "flush task of database {} memtable is created, flushing to file {}.",
        storageGroup,
        writer.getFile().getName());
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
    long sortTime = 0;

    // for map do not use get(key) to iterate
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
    List<IDeviceID> deviceIDList = new ArrayList<>(memTableMap.keySet());
    // sort the IDeviceID in lexicographical order
    deviceIDList.sort(Comparator.comparing(IDeviceID::toStringID));
    for (IDeviceID deviceID : deviceIDList) {
      final Map<String, IWritableMemChunk> value = memTableMap.get(deviceID).getMemChunkMap();
      // skip the empty device/chunk group
      if (memTableMap.get(deviceID).count() == 0 || value.isEmpty()) {
        continue;
      }
      encodingTaskQueue.put(new StartFlushGroupIOTask(deviceID.toStringID()));
      List<String> seriesInOrder = new ArrayList<>(value.keySet());
      seriesInOrder.sort((String::compareTo));
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

    if (config.isEnableMemControl()) {
      if (estimatedTemporaryMemSize != 0) {
        SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
      }
      SystemInfo.getInstance().setEncodingFasterThanIo(ioTime >= memSerializeTime);
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
              IChunkWriter seriesWriter = writableMemChunk.createIChunkWriter();
              writableMemChunk.encode(seriesWriter);
              seriesWriter.sealCurrentPage();
              seriesWriter.clearPageWriter();
              try {
                ioTaskQueue.put(seriesWriter);
              } catch (InterruptedException e) {
                LOGGER.error("Put task into ioTaskQueue Interrupted");
                Thread.currentThread().interrupt();
              }
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

          if (!storageGroup.startsWith(IoTDBMetricsUtils.DATABASE)) {
            int lastIndex = storageGroup.lastIndexOf("-");
            if (lastIndex == -1) {
              lastIndex = storageGroup.length();
            }
            MetricService.getInstance()
                .gaugeWithInternalReport(
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

          LOGGER.info(
              "Database {}, flushing memtable {} into disk: Encoding data cost " + "{} ms.",
              storageGroup,
              writer.getFile().getName(),
              memSerializeTime);
          WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_ENCODING, memSerializeTime);
        }
      };

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
            throw new FlushRunTimeException(e);
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

    private final String deviceId;

    StartFlushGroupIOTask(String deviceId) {
      this.deviceId = deviceId;
    }
  }
}
