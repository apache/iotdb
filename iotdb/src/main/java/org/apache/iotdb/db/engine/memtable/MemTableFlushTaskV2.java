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
import java.util.function.Consumer;
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

  private NativeRestorableIOWriter tsFileIoWriter;

  private ConcurrentLinkedQueue ioTaskQueue = new ConcurrentLinkedQueue();
  private ConcurrentLinkedQueue memoryTaskQueue = new ConcurrentLinkedQueue();
  private boolean stop = false;
  private String processorName;

  private Consumer<IMemTable> flushCallBack;
  private IMemTable memTable;

  public MemTableFlushTaskV2(NativeRestorableIOWriter writer, String processorName,
      Consumer<IMemTable> callBack) {
    this.tsFileIoWriter = writer;
    this.processorName = processorName;
    this.flushCallBack = callBack;
    ioFlushThread.start();
    memoryFlushThread.start();
    LOGGER.info("Processor {} flushMetadata task created", processorName);
  }


  private Thread memoryFlushThread = new Thread(() -> {
    long memSerializeTime = 0;
    LOGGER.info(
        "BufferWrite Processor {},start serialize data into mem.", processorName);
    while (!stop) {
      Object task = memoryTaskQueue.poll();
      if (task == null) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOGGER.error("BufferWrite Processor {}, io flushMetadata task is interrupted.", processorName, e);
        }
      } else {
        if (task instanceof String) {
          ioTaskQueue.add(task);
        } else if (task instanceof ChunkGroupIoTask) {
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
            LOGGER.error("BufferWrite Processor {}, io error.", processorName, e);
            throw new RuntimeException(e);
          }
          memSerializeTime += System.currentTimeMillis() - starTime;
        }
      }
    }
    LOGGER.info(
        "BufferWrite Processor {},flushing a memtable into disk: serialize data into mem cost {} ms.",
        processorName, memSerializeTime);
  });


  //TODO more better way is: for each TsFile, assign it a Executors.singleThreadPool,
  // rather than per each memtable.
  private Thread ioFlushThread = new Thread(() -> {
    long ioTime = 0;
    LOGGER.info("BufferWrite Processor {}, start io cost.", processorName);
    while (!stop) {
      Object seriesWriterOrEndChunkGroupTask = ioTaskQueue.poll();
      if (seriesWriterOrEndChunkGroupTask == null) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOGGER.error("BufferWrite Processor {}, io flushMetadata task is interrupted.", processorName, e);
        }
      } else {
        long starTime = System.currentTimeMillis();
        try {
          if (seriesWriterOrEndChunkGroupTask instanceof IChunkWriter) {
            ((IChunkWriter) seriesWriterOrEndChunkGroupTask).writeToFileWriter(tsFileIoWriter);
          } else if (seriesWriterOrEndChunkGroupTask instanceof String) {
            tsFileIoWriter.startFlushChunkGroup((String) seriesWriterOrEndChunkGroupTask);
          } else {
            ChunkGroupIoTask task = (ChunkGroupIoTask) seriesWriterOrEndChunkGroupTask;
            tsFileIoWriter.endChunkGroup(task.version);
            task.finished = true;
          }
        } catch (IOException e) {
          LOGGER.error("BufferWrite Processor {}, io error.", processorName, e);
          throw new RuntimeException(e);
        }
        ioTime += System.currentTimeMillis() - starTime;
      }
    }
  });


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
          LOGGER.error("BufferWrite Processor {}, don't support data type: {}", processorName,
              dataType);
          break;
      }
    }
  }

  /**
   * the function for flushing memtable.
   */
  public void flushMemTable(FileSchema fileSchema, IMemTable imemTable, long version) {
    long sortTime = 0;
    ChunkGroupIoTask theLastTask = EMPTY_TASK;
    this.memTable = imemTable;
    for (String deviceId : imemTable.getMemTableMap().keySet()) {
      memoryTaskQueue.add(deviceId);
      int seriesNumber = imemTable.getMemTableMap().get(deviceId).size();
      for (String measurementId : imemTable.getMemTableMap().get(deviceId).keySet()) {
        long startTime = System.currentTimeMillis();
        // TODO if we can not use TSFileIO writer, then we have to redesign the class of TSFileIO.
        IWritableMemChunk series = imemTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
        List<TimeValuePair> sortedTimeValuePairs = series.getSortedTimeValuePairList();
        sortTime += System.currentTimeMillis() - startTime;
        memoryTaskQueue.add(new Pair<>(sortedTimeValuePairs, desc));
      }
      theLastTask = new ChunkGroupIoTask(seriesNumber, deviceId, version);
      memoryTaskQueue.add(theLastTask);
    }
    LOGGER.info(
        "{}, flushing a memtable into disk: data sort time cost {} ms.",
        processorName, sortTime);
    while (!theLastTask.finished) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOGGER.error("BufferWrite Processor {}, flushMetadata memtable table thread is interrupted.",
            processorName, e);
        throw new RuntimeException(e);
      }
    }
    stop = true;
    while (ioFlushThread.isAlive()) {
      LOGGER.info("io flush thread is alive");
      // wait until the after works are done
    }

    LOGGER.info("flushing a memtable finished!");
    flushCallBack.accept(memTable);
  }


  static class ChunkGroupIoTask {

    int seriesNumber;
    String deviceId;
    long version;
    boolean finished;

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
