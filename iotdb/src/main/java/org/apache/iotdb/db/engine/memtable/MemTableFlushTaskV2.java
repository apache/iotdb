/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.iotdb.db.engine.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.NativeRestorableIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableFlushTaskV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final int PAGE_SIZE_THRESHOLD = TSFileConfig.pageSizeInByte;
  private static final FlushSubTaskPoolManager subTaskPoolManager = FlushSubTaskPoolManager
      .getInstance();
  private Future memoryFlushTask;
  private Future ioFlushTask;
  private NativeRestorableIOWriter tsFileIoWriter;

  private ConcurrentLinkedQueue ioTaskQueue = new ConcurrentLinkedQueue();
  private ConcurrentLinkedQueue memoryTaskQueue = new ConcurrentLinkedQueue();
  private volatile boolean stop = false;
  private String storageGroup;

  private Consumer<IMemTable> flushCallBack;
  private IMemTable memTable;

  public MemTableFlushTaskV2(NativeRestorableIOWriter writer, String storageGroup,
      Consumer<IMemTable> callBack) {
    this.tsFileIoWriter = writer;
    this.storageGroup = storageGroup;
    this.flushCallBack = callBack;
//    this.memoryFlushTask = subTaskPoolManager.submit(memoryFlushThread);

    memoryFlushThread.start();
    ioFlushThread.start();
    LOGGER.info("flush task created in Storage group {} ", storageGroup);
  }


  private Thread memoryFlushThread = new Thread(() -> {
      long memSerializeTime = 0;
      LOGGER.info("Storage group {},start serialize data into mem.", storageGroup);
      while (!stop) {
        if (!memoryTaskQueue.isEmpty()) {
          LOGGER.info("memory task queue is {}", memoryTaskQueue);
        }
        Object task = memoryTaskQueue.poll();
        if (task == null) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            LOGGER.error("Storage group {}, io flush task is interrupted.", storageGroup, e);
          }
        } else {
          if (task instanceof String) {
            LOGGER.info("add String {} to io queue", task);
            ioTaskQueue.add(task);
          } else if (task instanceof Pair) {
            LOGGER.info("add chunk writer {}", task);
            long starTime = System.currentTimeMillis();
            Pair<List<TimeValuePair>, MeasurementSchema> memorySerializeTask = (Pair<List<TimeValuePair>, MeasurementSchema>) task;
            ChunkBuffer chunkBuffer = new ChunkBuffer(memorySerializeTask.right);
            IChunkWriter seriesWriter = new ChunkWriterImpl(memorySerializeTask.right, chunkBuffer,
                PAGE_SIZE_THRESHOLD);
            try {
              writeOneSeries(memorySerializeTask.left, seriesWriter,
                  memorySerializeTask.right.getType());
              ioTaskQueue.add(seriesWriter);
            } catch (IOException e) {
              LOGGER.error("Storage group {}, io error.", storageGroup, e);
              throw new RuntimeException(e);
            }
            memSerializeTime += System.currentTimeMillis() - starTime;
          } else {
            LOGGER.info("end chunk group {} io task to io task queue", task.toString());
            ioTaskQueue.add(task);
          }
        }
      }
      LOGGER.info("Storage group {}, flushing a memtable into disk: serialize data into mem cost {} ms.",
          storageGroup, memSerializeTime);
  }, Thread.currentThread().getId() + "-1");


  //TODO a better way is: for each TsFile, assign it a Executors.singleThreadPool,
  // rather than per each memtable.
  private Thread ioFlushThread = new Thread(() -> {
      long ioTime = 0;
      LOGGER.info("Storage group {}, start io cost.", storageGroup);
      while (!stop) {
        Object seriesWriterOrEndChunkGroupTask = ioTaskQueue.poll();
        if (seriesWriterOrEndChunkGroupTask == null) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            LOGGER.error("Storage group {}, io flush task is interrupted.", storageGroup, e);
          }
        } else {
          long starTime = System.currentTimeMillis();
          try {
            if (seriesWriterOrEndChunkGroupTask instanceof IChunkWriter) {
              LOGGER.info("write series to disk");
              ((IChunkWriter) seriesWriterOrEndChunkGroupTask).writeToFileWriter(tsFileIoWriter);
            } else if (seriesWriterOrEndChunkGroupTask instanceof String) {
              LOGGER.info("start chunk group");
              tsFileIoWriter.startChunkGroup((String) seriesWriterOrEndChunkGroupTask);
            } else {
              LOGGER.info("end chunk group {} io task from task queue", seriesWriterOrEndChunkGroupTask.toString());
              LOGGER.info("end chunk group");
              ChunkGroupIoTask task = (ChunkGroupIoTask) seriesWriterOrEndChunkGroupTask;
              tsFileIoWriter.endChunkGroup(task.version);
              task.finished = true;
            }
          } catch (IOException e) {
            LOGGER.error("Storage group {}, io error.", storageGroup, e);
            throw new RuntimeException(e);
          }
          ioTime += System.currentTimeMillis() - starTime;
        }
      }
      LOGGER.info("flushing a memtable in storage group {}, cost {}ms", storageGroup, ioTime);
  }, Thread.currentThread().getId() + "-2");


  private void writeOneSeries(List<TimeValuePair> tvPairs, IChunkWriter seriesWriterImpl,
      TSDataType dataType)
      throws IOException {
    for (TimeValuePair timeValuePair : tvPairs) {
      switch (dataType) {
        case BOOLEAN:
          seriesWriterImpl
              .write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
          break;
        case INT32:
          seriesWriterImpl.write(timeValuePair.getTimestamp(),
              timeValuePair.getValue().getInt());
          break;
        case INT64:
          seriesWriterImpl.write(timeValuePair.getTimestamp(),
              timeValuePair.getValue().getLong());
          break;
        case FLOAT:
          seriesWriterImpl.write(timeValuePair.getTimestamp(),
              timeValuePair.getValue().getFloat());
          break;
        case DOUBLE:
          seriesWriterImpl
              .write(timeValuePair.getTimestamp(),
                  timeValuePair.getValue().getDouble());
          break;
        case TEXT:
          seriesWriterImpl
              .write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
          break;
        default:
          LOGGER.error("Storage group {}, don't support data type: {}", storageGroup,
              dataType);
          break;
      }
    }
  }

  /**
   * the function for flushing memtable.
   */
  public void flushMemTable(FileSchema fileSchema, IMemTable imemTable) {
    long sortTime = 0;
    ChunkGroupIoTask theLastTask = EMPTY_TASK;
    this.memTable = imemTable;
    LOGGER.info("Current thread id is {}" , Thread.currentThread().getId());
    for (String deviceId : imemTable.getMemTableMap().keySet()) {
      memoryTaskQueue.add(deviceId);
      int seriesNumber = imemTable.getMemTableMap().get(deviceId).size();
      LOGGER.info("series number: {}", seriesNumber);
      LOGGER.info("add device, memory queue {}", memoryTaskQueue);
      for (String measurementId : imemTable.getMemTableMap().get(deviceId).keySet()) {
        long startTime = System.currentTimeMillis();
        // TODO if we can not use TSFileIO writer, then we have to redesign the class of TSFileIO.
        IWritableMemChunk series = imemTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
        List<TimeValuePair> sortedTimeValuePairs = series.getSortedTimeValuePairList();
        sortTime += System.currentTimeMillis() - startTime;
        LOGGER.info("add seies writer in flush thread {}", sortedTimeValuePairs);
        memoryTaskQueue.add(new Pair<>(sortedTimeValuePairs, desc));
        LOGGER.info("add series writer, memory queue {}", memoryTaskQueue);
      }
      theLastTask = new ChunkGroupIoTask(seriesNumber, deviceId, imemTable.getVersion());
      LOGGER.info("ChunkGroupIoTask task {}", theLastTask.toString());
      memoryTaskQueue.add(theLastTask);
      LOGGER.info("add chunk group to task, memory queue {}", memoryTaskQueue);
    }
    LOGGER.info(
        "{}, flushing a memtable into disk: data sort time cost {} ms.",
        storageGroup, sortTime);
    while (!theLastTask.finished) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOGGER.error("Storage group {}, flush memtable table thread is interrupted.",
            storageGroup, e);
        throw new RuntimeException(e);
      }
    }
    stop = true;

    while (ioFlushThread.isAlive()) {
    }

    LOGGER.info("flushing a memtable finished!");
    flushCallBack.accept(memTable);
  }


  static class ChunkGroupIoTask {

    int seriesNumber;
    String deviceId;
    long version;
    volatile boolean finished;

    public ChunkGroupIoTask(int seriesNumber, String deviceId, long version) {
      this(seriesNumber, deviceId, version, false);
    }

    public ChunkGroupIoTask(int seriesNumber, String deviceId, long version, boolean finished) {
      this.seriesNumber = seriesNumber;
      this.deviceId = deviceId;
      this.version = version;
      this.finished = finished;
    }
  }

  private static ChunkGroupIoTask EMPTY_TASK = new ChunkGroupIoTask(0, "", 0, true);

}
