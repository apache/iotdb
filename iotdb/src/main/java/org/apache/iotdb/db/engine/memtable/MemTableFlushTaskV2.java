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
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.datastructure.TVList;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTaskV2.class);
  private static final int PAGE_SIZE_THRESHOLD = TSFileConfig.pageSizeInByte;
  private static final FlushSubTaskPoolManager subTaskPoolManager = FlushSubTaskPoolManager
      .getInstance();
  private Future ioFlushTaskFuture;
  private NativeRestorableIOWriter tsFileIoWriter;

  private ConcurrentLinkedQueue ioTaskQueue = new ConcurrentLinkedQueue();
  private ConcurrentLinkedQueue encodingTaskQueue = new ConcurrentLinkedQueue();
  private String storageGroup;

  private Consumer<IMemTable> flushCallBack;
  private IMemTable memTable;
  private FileSchema fileSchema;

  private volatile boolean noMoreEncodingTask = false;
  private volatile boolean noMoreIOTask = false;

  public MemTableFlushTaskV2(IMemTable memTable, FileSchema fileSchema, NativeRestorableIOWriter writer, String storageGroup,
      Consumer<IMemTable> callBack) {
    this.memTable = memTable;
    this.fileSchema = fileSchema;
    this.tsFileIoWriter = writer;
    this.storageGroup = storageGroup;
    this.flushCallBack = callBack;
    subTaskPoolManager.submit(EncodingTask);
    this.ioFlushTaskFuture = subTaskPoolManager.submit(IOTask);
    LOGGER.info("flush task of Storage group {} memtable {} is created ",
        storageGroup, memTable.getVersion());
  }


  /**
   * the function for flushing memtable.
   */
  public void flushMemTable() {
    long sortTime = 0;
    for (String deviceId : memTable.getMemTableMap().keySet()) {
      encodingTaskQueue.add(deviceId);
      int seriesNumber = memTable.getMemTableMap().get(deviceId).size();
      for (String measurementId : memTable.getMemTableMap().get(deviceId).keySet()) {
        long startTime = System.currentTimeMillis();
        // TODO if we can not use TSFileIO writer, then we have to redesign the class of TSFileIO.
        IWritableMemChunk series = memTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
        TVList tvList = series.getSortedTVList();
        sortTime += System.currentTimeMillis() - startTime;
        encodingTaskQueue.add(new Pair<>(tvList, desc));
      }
      encodingTaskQueue.add(new ChunkGroupIoTask(seriesNumber, deviceId, memTable.getVersion()));
    }
    noMoreEncodingTask = true;
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


  private Runnable EncodingTask = new Runnable() {
    @Override
    public void run() {
      String currDevice = null;
      try {
        long memSerializeTime = 0;
        boolean noMoreMessages = false;
        LOGGER.info("Storage group {} memtable {}, starts to encoding data.", storageGroup,
            memTable.getVersion());
        while (true) {
          if (noMoreEncodingTask) {
            noMoreMessages = true;
          }
          Object task = encodingTaskQueue.poll();
          if (task == null) {
            if (noMoreMessages) {
              break;
            }
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              LOGGER.error("Storage group {} memtable {}, encoding task is interrupted.",
                  storageGroup, memTable.getVersion(), e);
            }
          } else {
            if (task instanceof String) {
              currDevice = (String) task;
              ioTaskQueue.add(task);
            } else if (task instanceof ChunkGroupIoTask) {
              ioTaskQueue.add(task);
            } else {
              long starTime = System.currentTimeMillis();
              Pair<TVList, MeasurementSchema> encodingMessage = (Pair<TVList, MeasurementSchema>) task;
              ChunkBuffer chunkBuffer = FlushTaskPool.getInstance().getEmptyChunkBuffer(this, encodingMessage.right);

              IChunkWriter seriesWriter = new ChunkWriterImpl(encodingMessage.right, chunkBuffer,
                  PAGE_SIZE_THRESHOLD);
              try {
                writeOneSeries(encodingMessage.left, seriesWriter,
                    encodingMessage.right.getType());
                memTable.getTVListAllocator().release(currDevice + IoTDBConstant.PATH_SEPARATOR
                    + encodingMessage.right.getMeasurementId(), encodingMessage.left);
                ioTaskQueue.add(seriesWriter);
              } catch (IOException e) {
                LOGGER.error("Storage group {} memtable {}, encoding task error.", storageGroup,
                    memTable.getVersion(), e);
                throw new RuntimeException(e);
              }
              memSerializeTime += System.currentTimeMillis() - starTime;
            }
          }
        }
        noMoreIOTask = true;
        LOGGER.info("Storage group {}, flushing memtable {} into disk: Encoding data cost "
                + "{} ms.",
            storageGroup, memTable.getVersion(), memSerializeTime);
      } catch (RuntimeException e) {
        LOGGER.error("memoryFlush thread is dead", e);
      }
    }
  };


  //TODO a better way is: for each TsFile, assign it a Executors.singleThreadPool,
  // rather than per each memtable.
  private Runnable IOTask = new Runnable() {
    @Override
    public void run() {
      try {
        long ioTime = 0;
        boolean returnWhenNoTask = false;
        LOGGER.info("Storage group {} memtable {}, start io.", storageGroup, memTable.getVersion());
        while (true) {
          if (noMoreIOTask) {
            returnWhenNoTask = true;
          }
          Object ioMessage = ioTaskQueue.poll();
          if (ioMessage == null) {
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
              if (ioMessage instanceof String) {
                tsFileIoWriter.startChunkGroup((String) ioMessage);
              } else if (ioMessage instanceof IChunkWriter) {
                ChunkWriterImpl writer = (ChunkWriterImpl) ioMessage;
                writer.writeToFileWriter(tsFileIoWriter);
                FlushTaskPool.getInstance().putBack(writer.getChunkBuffer());
              } else {
                ChunkGroupIoTask endGroupTask = (ChunkGroupIoTask) ioMessage;
                tsFileIoWriter.endChunkGroup(endGroupTask.version);
                endGroupTask.finished = true;
              }
            } catch (IOException e) {
              LOGGER.error("Storage group {} memtable {}, io error.", storageGroup,
                  memTable.getVersion(), e);
              throw new RuntimeException(e);
            }
            ioTime += System.currentTimeMillis() - starTime;
          }
        }
        LOGGER.info("flushing a memtable {} in storage group {}, io cost {}ms", memTable.getVersion(),
            storageGroup, ioTime);
      } catch (RuntimeException e) {
        LOGGER.error("io thread is dead", e);
      }
    }
  };


  private void writeOneSeries(TVList tvPairs, IChunkWriter seriesWriterImpl,
      TSDataType dataType)
      throws IOException {
    for (int i = 0; i < tvPairs.size(); i++) {
      long time = tvPairs.getTime(i);
      if (time < tvPairs.getTimeOffset() ||
          (i+1 < tvPairs.size() && (time == tvPairs.getTime(i+1)))) {
        continue;
      }

      switch (dataType) {
        case BOOLEAN:
          seriesWriterImpl.write(time, tvPairs.getBoolean(i));
          break;
        case INT32:
          seriesWriterImpl.write(time, tvPairs.getInt(i));
          break;
        case INT64:
          seriesWriterImpl.write(time, tvPairs.getLong(i));
          break;
        case FLOAT:
          seriesWriterImpl.write(time, tvPairs.getFloat(i));
          break;
        case DOUBLE:
          seriesWriterImpl.write(time, tvPairs.getDouble(i));
          break;
        case TEXT:
          seriesWriterImpl.write(time, tvPairs.getBinary(i));
          break;
        default:
          LOGGER.error("Storage group {}, don't support data type: {}", storageGroup,
              dataType);
          break;
      }
    }
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

}
