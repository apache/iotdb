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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.engine.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.exception.FlushRunTimeException;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableFlushTask {

  private static final Logger logger = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final int PAGE_SIZE_THRESHOLD = TSFileConfig.pageSizeInByte;
  private static final FlushSubTaskPoolManager subTaskPoolManager = FlushSubTaskPoolManager
      .getInstance();
  private Future ioTaskFuture;
  private RestorableTsFileIOWriter writer;

  private ConcurrentLinkedQueue ioTaskQueue = new ConcurrentLinkedQueue();
  private ConcurrentLinkedQueue encodingTaskQueue = new ConcurrentLinkedQueue();
  private String storageGroup;

  private IMemTable memTable;
  private FileSchema fileSchema;

  private volatile boolean noMoreEncodingTask = false;
  private volatile boolean noMoreIOTask = false;

  public MemTableFlushTask(IMemTable memTable, FileSchema fileSchema, RestorableTsFileIOWriter writer, String storageGroup) {
    this.memTable = memTable;
    this.fileSchema = fileSchema;
    this.writer = writer;
    this.storageGroup = storageGroup;
    subTaskPoolManager.submit(encodingTask);
    this.ioTaskFuture = subTaskPoolManager.submit(ioTask);
    logger.debug("flush task of Storage group {} memtable {} is created ",
        storageGroup, memTable.getVersion());
  }


  /**
   * the function for flushing memtable.
   */
  public void syncFlushMemTable() throws ExecutionException, InterruptedException {
    long start = System.currentTimeMillis();
    long sortTime = 0;
    for (String deviceId : memTable.getMemTableMap().keySet()) {
      encodingTaskQueue.add(new StartFlushGroupIOTask(deviceId));
      for (String measurementId : memTable.getMemTableMap().get(deviceId).keySet()) {
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = memTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
        TVList tvList = series.getSortedTVList();
        sortTime += System.currentTimeMillis() - startTime;
        encodingTaskQueue.add(new Pair<>(tvList, desc));
      }
      encodingTaskQueue.add(new EndChunkGroupIoTask(memTable.getVersion()));
    }
    noMoreEncodingTask = true;
    logger.debug(
        "Storage group {} memtable {}, flushing into disk: data sort time cost {} ms.",
        storageGroup, memTable.getVersion(), sortTime);

    ioTaskFuture.get();

    logger.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup, memTable, System.currentTimeMillis() - start);
  }


  private Runnable encodingTask = new Runnable() {
    private void writeOneSeries(TVList tvPairs, IChunkWriter seriesWriterImpl,
        TSDataType dataType){
      for (int i = 0; i < tvPairs.size(); i++) {
        long time = tvPairs.getTime(i);

        // skip duplicated data
        if ((i+1 < tvPairs.size() && (time == tvPairs.getTime(i+1)))) {
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
            logger.error("Storage group {} does not support data type: {}", storageGroup,
                dataType);
            break;
        }
      }
    }

    @Override
    public void run() {
      long memSerializeTime = 0;
      boolean noMoreMessages = false;
      logger.debug("Storage group {} memtable {}, starts to encoding data.", storageGroup,
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
            logger.error("Storage group {} memtable {}, encoding task is interrupted.",
                storageGroup, memTable.getVersion(), e);
            Thread.currentThread().interrupt();
          }
        } else {
          if (task instanceof StartFlushGroupIOTask) {
            ioTaskQueue.add(task);
          } else if (task instanceof EndChunkGroupIoTask) {
            ioTaskQueue.add(task);
          } else {
            long starTime = System.currentTimeMillis();
            Pair<TVList, MeasurementSchema> encodingMessage = (Pair<TVList, MeasurementSchema>) task;
            ChunkBuffer chunkBuffer = ChunkBufferPool.getInstance()
                .getEmptyChunkBuffer(this, encodingMessage.right);
            IChunkWriter seriesWriter = new ChunkWriterImpl(chunkBuffer, PAGE_SIZE_THRESHOLD);
            writeOneSeries(encodingMessage.left, seriesWriter, encodingMessage.right.getType());
            ioTaskQueue.add(seriesWriter);
            memSerializeTime += System.currentTimeMillis() - starTime;
          }
        }
      }
      noMoreIOTask = true;
      logger.debug("Storage group {}, flushing memtable {} into disk: Encoding data cost "
              + "{} ms.",
          storageGroup, memTable.getVersion(), memSerializeTime);
    }
  };

  private Runnable ioTask = () -> {
      long ioTime = 0;
      boolean returnWhenNoTask = false;
      logger.debug("Storage group {} memtable {}, start io.", storageGroup, memTable.getVersion());
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
            logger.error("Storage group {} memtable, io task is interrupted.", storageGroup
                , memTable.getVersion(), e);
            Thread.currentThread().interrupt();
          }
        } else {
          long starTime = System.currentTimeMillis();
          try {
            if (ioMessage instanceof StartFlushGroupIOTask) {
              writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
            } else if (ioMessage instanceof IChunkWriter) {
              ChunkWriterImpl chunkWriter = (ChunkWriterImpl) ioMessage;
              chunkWriter.writeToFileWriter(MemTableFlushTask.this.writer);
              ChunkBufferPool.getInstance().putBack(chunkWriter.getChunkBuffer());
            } else {
              EndChunkGroupIoTask endGroupTask = (EndChunkGroupIoTask) ioMessage;
              writer.endChunkGroup(endGroupTask.version);
            }
          } catch (IOException e) {
            logger.error("Storage group {} memtable {}, io task meets error.", storageGroup,
                memTable.getVersion(), e);
            throw new FlushRunTimeException(e);
          }
          ioTime += System.currentTimeMillis() - starTime;
        }
      }
      logger.debug("flushing a memtable {} in storage group {}, io cost {}ms", memTable.getVersion(),
          storageGroup, ioTime);
    };

  static class EndChunkGroupIoTask {
    private long version;

    EndChunkGroupIoTask(long version) {
      this.version = version;
    }
  }

  static class StartFlushGroupIOTask {
    private String deviceId;

    StartFlushGroupIOTask(String deviceId) {
      this.deviceId = deviceId;
    }
  }

}
