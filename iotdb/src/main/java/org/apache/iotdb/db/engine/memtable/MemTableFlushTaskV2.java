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
import java.util.concurrent.ExecutionException;
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
  private Future ioFlushTaskFuture;
  private NativeRestorableIOWriter tsFileIoWriter;

  private ConcurrentLinkedQueue ioTaskQueue = new ConcurrentLinkedQueue();
  private ConcurrentLinkedQueue memoryTaskQueue = new ConcurrentLinkedQueue();
  private String storageGroup;

  private Consumer<IMemTable> flushCallBack;
  private IMemTable memTable;
  private FileSchema fileSchema;

  private boolean memoryFlushNoMoreTask = false;
  private boolean ioFlushTaskCanStop = false;

  public MemTableFlushTaskV2(IMemTable memTable, FileSchema fileSchema, NativeRestorableIOWriter writer, String storageGroup,
      Consumer<IMemTable> callBack) {
    this.memTable = memTable;
    this.fileSchema = fileSchema;
    this.tsFileIoWriter = writer;
    this.storageGroup = storageGroup;
    this.flushCallBack = callBack;
    subTaskPoolManager.submit(memoryFlushTask);
    this.ioFlushTaskFuture = subTaskPoolManager.submit(ioFlushTask);
    LOGGER.info("flush task of Storage group {} memtable {} is created ",
        storageGroup, memTable.getVersion());
  }


  private Runnable memoryFlushTask = new Runnable() {
    @Override
    public void run() {
      try {
        long memSerializeTime = 0;
        boolean returnWhenNoTask = false;
        LOGGER.info("Storage group {} memtable {}, starts to serialize data into mem.", storageGroup,
            memTable.getVersion());
        while (true) {
          if (memoryFlushNoMoreTask) {
            returnWhenNoTask = true;
          }
          Object task = memoryTaskQueue.poll();
          if (task == null) {
            if (returnWhenNoTask) {
              break;
            }
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              LOGGER.error("Storage group {} memtable {}, io flush task is interrupted.",
                  storageGroup, memTable.getVersion(), e);
            }
          } else {
            if (task instanceof String) {
              LOGGER.info("Storage group {} memtable {}, issues a start flush chunk group task.",
                  storageGroup, memTable.getVersion());
              ioTaskQueue.add(task);
            } else if (task instanceof ChunkGroupIoTask) {
              LOGGER.info("Storage group {} memtable {}, issues a end flush chunk group task.",
                  storageGroup, memTable.getVersion());
              ioTaskQueue.add(task);
            } else {
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
                LOGGER.error("Storage group {} memtable {}, io error.", storageGroup,
                    memTable.getVersion(), e);
                throw new RuntimeException(e);
              }
              LOGGER.info("Storage group {} memtable {}, issues a write chunk task.",
                  storageGroup, memTable.getVersion());
              memSerializeTime += System.currentTimeMillis() - starTime;
            }
          }
        }
        ioFlushTaskCanStop = true;
        LOGGER.info("Storage group {}, flushing memtable {} into disk: serialize data into mem cost "
                + "{} ms.",
            storageGroup, memTable.getVersion(), memSerializeTime);
      } catch (RuntimeException e) {
        LOGGER.error("memoryFlush thread is dead", e);
      }
    }
  };


  //TODO a better way is: for each TsFile, assign it a Executors.singleThreadPool,
  // rather than per each memtable.
  private Runnable ioFlushTask = new Runnable() {
    @Override
    public void run() {
      try {
        long ioTime = 0;
        boolean returnWhenNoTask = false;
        LOGGER.info("Storage group {} memtable {}, start io.", storageGroup, memTable.getVersion());
        while (true) {
          if (ioFlushTaskCanStop) {
            returnWhenNoTask = true;
          }
          Object seriesWriterOrEndChunkGroupTask = ioTaskQueue.poll();
          if (seriesWriterOrEndChunkGroupTask == null) {
            if (returnWhenNoTask) {
              break;
            }
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              LOGGER.error("Storage group {} memtable, io flush task is interrupted.", storageGroup
                  , memTable.getVersion(), e);
            }
          } else {
            long starTime = System.currentTimeMillis();
            try {
              if (seriesWriterOrEndChunkGroupTask instanceof IChunkWriter) {
                LOGGER.info("Storage group {} memtable {}, writing a series to file.", storageGroup,
                    memTable.getVersion());
                ((IChunkWriter) seriesWriterOrEndChunkGroupTask).writeToFileWriter(tsFileIoWriter);
              } else if (seriesWriterOrEndChunkGroupTask instanceof String) {
                LOGGER.info("Storage group {} memtable {}, start a chunk group.", storageGroup,
                    memTable.getVersion());
                tsFileIoWriter.startChunkGroup((String) seriesWriterOrEndChunkGroupTask);
              } else {
                LOGGER.info("Storage group {} memtable {}, end a chunk group.", storageGroup,
                    memTable.getVersion());
                ChunkGroupIoTask task = (ChunkGroupIoTask) seriesWriterOrEndChunkGroupTask;
                tsFileIoWriter.endChunkGroup(task.version);
                task.finished = true;
              }
            } catch (IOException e) {
              LOGGER.error("Storage group {} memtable {}, io error.", storageGroup,
                  memTable.getVersion(), e);
              throw new RuntimeException(e);
            }
            ioTime += System.currentTimeMillis() - starTime;
          }
        }
        LOGGER.info("flushing a memtable {} in storage group {}, cost {}ms", memTable.getVersion(),
            storageGroup, ioTime);
      } catch (RuntimeException e) {
        LOGGER.error("ioflush thread is dead", e);
      }
    }
  };


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
  public void flushMemTable() {
    long sortTime = 0;
    ChunkGroupIoTask theLastTask = EMPTY_TASK;
    for (String deviceId : memTable.getMemTableMap().keySet()) {
      memoryTaskQueue.add(deviceId);
      int seriesNumber = memTable.getMemTableMap().get(deviceId).size();
      for (String measurementId : memTable.getMemTableMap().get(deviceId).keySet()) {
        long startTime = System.currentTimeMillis();
        // TODO if we can not use TSFileIO writer, then we have to redesign the class of TSFileIO.
        IWritableMemChunk series = memTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
        List<TimeValuePair> sortedTimeValuePairs = series.getSortedTimeValuePairList();
        sortTime += System.currentTimeMillis() - startTime;
        memoryTaskQueue.add(new Pair<>(sortedTimeValuePairs, desc));
      }
      theLastTask = new ChunkGroupIoTask(seriesNumber, deviceId, memTable.getVersion());
      memoryTaskQueue.add(theLastTask);
    }
    memoryFlushNoMoreTask = true;
    LOGGER.info(
        "Storage group {} memtable {}, flushing into disk: data sort time cost {} ms.",
        storageGroup, memTable.getVersion(), sortTime);

    try {
      ioFlushTaskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Waiting for IO flush task end meets error", e);
    }

    LOGGER.info("Storage group {} memtable {} flushing a memtable finished!", storageGroup, memTable);
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
