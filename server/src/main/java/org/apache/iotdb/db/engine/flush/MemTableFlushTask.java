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

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.FLUSH_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableFlushTask {

  private static final Logger logger = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final FlushSubTaskPoolManager subTaskPoolManager = FlushSubTaskPoolManager
      .getInstance();
  private final Future<?> encodingTaskFuture;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;
  private RestorableTsFileIOWriter tsFileIOWriter;

  private final ConcurrentLinkedQueue<Object> ioTaskQueue = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Object> encodingTaskQueue = new ConcurrentLinkedQueue<>();
  private String storageGroup;

  private IMemTable memTable;

  private volatile boolean noMoreEncodingTask = false;
  private volatile boolean noMoreIOTask = false;

  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current storage group
   * @param tsFileIOWriter current tsfile writer (use it to create flushLog)
   */

  public MemTableFlushTask(IMemTable memTable, RestorableTsFileIOWriter writer, String storageGroup,
      RestorableTsFileIOWriter tsFileIOWriter) {
    this.memTable = memTable;
    this.writer = writer;
    this.tsFileIOWriter = tsFileIOWriter;
    this.storageGroup = storageGroup;
    this.encodingTaskFuture = subTaskPoolManager.submit(encodingTask);
    this.ioTaskFuture = subTaskPoolManager.submit(ioTask);
    logger.debug("flush task of Storage group {} memtable {} is created ",
        storageGroup, memTable.getVersion());
  }

  /**
   * the function for flushing memtable.
   */
  public void syncFlushMemTable()
      throws ExecutionException, InterruptedException, IOException {
    long start = System.currentTimeMillis();
    long sortTime = 0;

    File flushLogFile = getFlushLogFile(tsFileIOWriter);
    if (!flushLogFile.createNewFile()) {
      logger.error("Failed to create file {}", flushLogFile);
    }

    for (String deviceId : memTable.getMemTableMap().keySet()) {
      encodingTaskQueue.add(new StartFlushGroupIOTask(deviceId));
      for (String measurementId : memTable.getMemTableMap().get(deviceId).keySet()) {
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = memTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = series.getSchema();
        TVList tvList = series.getSortedTVList();
        sortTime += System.currentTimeMillis() - startTime;
        encodingTaskQueue.add(new Pair<>(tvList, desc));
        // register active time series to the ActiveTimeSeriesCounter
        if (IoTDBDescriptor.getInstance().getConfig().isEnableParameterAdapter()) {
          ActiveTimeSeriesCounter.getInstance().offer(storageGroup, deviceId, measurementId);
        }
      }
      encodingTaskQueue.add(new EndChunkGroupIoTask());
    }
    if (IoTDBDescriptor.getInstance().getConfig().isEnableParameterAdapter()) {
      ActiveTimeSeriesCounter.getInstance().updateActiveRatio(storageGroup);
    }
    noMoreEncodingTask = true;
    logger.debug(
        "Storage group {} memtable {}, flushing into disk: data sort time cost {} ms.",
        storageGroup, memTable.getVersion(), sortTime);

    try {
      encodingTaskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      // avoid ioTask waiting forever
      noMoreIOTask = true;
      ioTaskFuture.cancel(true);
      throw e;
    }

    ioTaskFuture.get();

    if (flushLogFile.exists()) {
      Files.delete(flushLogFile.toPath());
    }

    try {
      writer.writeVersion(memTable.getVersion());
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    logger.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup, memTable, System.currentTimeMillis() - start);
  }

  private Runnable encodingTask = new Runnable() {
    private void writeOneSeries(TVList tvPairs, IChunkWriter seriesWriterImpl,
        TSDataType dataType) {
      for (int i = 0; i < tvPairs.size(); i++) {
        long time = tvPairs.getTime(i);

        // skip duplicated data
        if ((i + 1 < tvPairs.size() && (time == tvPairs.getTime(i + 1)))) {
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

    @SuppressWarnings("squid:S135")
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
            TimeUnit.MILLISECONDS.sleep(10);
          } catch (@SuppressWarnings("squid:S2142") InterruptedException e) {
            logger.error("Storage group {} memtable {}, encoding task is interrupted.",
                storageGroup, memTable.getVersion(), e);
            // generally it is because the thread pool is shutdown so the task should be aborted
            break;
          }
        } else {
          if (task instanceof StartFlushGroupIOTask || task instanceof EndChunkGroupIoTask) {
            ioTaskQueue.add(task);
          } else {
            long starTime = System.currentTimeMillis();
            Pair<TVList, MeasurementSchema> encodingMessage = (Pair<TVList, MeasurementSchema>) task;
            IChunkWriter seriesWriter = new ChunkWriterImpl(encodingMessage.right);
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

  public static File getFlushLogFile(RestorableTsFileIOWriter writer) {
    File parent = writer.getFile().getParentFile();
    return FSFactoryProducer.getFSFactory()
        .getFile(parent, writer.getFile().getName() + FLUSH_SUFFIX);
  }

  @SuppressWarnings("squid:S135")
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
          TimeUnit.MILLISECONDS.sleep(10);
        } catch (@SuppressWarnings("squid:S2142") InterruptedException e) {
          logger.error("Storage group {} memtable {}, io task is interrupted.", storageGroup
              , memTable.getVersion(), e);
          // generally it is because the thread pool is shutdown so the task should be aborted
          break;
        }
      } else {
        long starTime = System.currentTimeMillis();
        try {
          if (ioMessage instanceof StartFlushGroupIOTask) {
            this.writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
          } else if (ioMessage instanceof IChunkWriter) {
            ChunkWriterImpl chunkWriter = (ChunkWriterImpl) ioMessage;
            chunkWriter.writeToFileWriter(this.writer);
          } else {
            this.writer.endChunkGroup();
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

    EndChunkGroupIoTask() {

    }
  }

  static class StartFlushGroupIOTask {

    private final String deviceId;

    StartFlushGroupIOTask(String deviceId) {
      this.deviceId = deviceId;
    }
  }
}
