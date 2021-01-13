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

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableFlushTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final FlushSubTaskPoolManager SUB_TASK_POOL_MANAGER = FlushSubTaskPoolManager
      .getInstance();
  private final Future<?> encodingTaskFuture;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;

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
   */

  public MemTableFlushTask(IMemTable memTable, RestorableTsFileIOWriter writer, String storageGroup) {
    this.memTable = memTable;
    this.writer = writer;
    this.storageGroup = storageGroup;
    this.encodingTaskFuture = SUB_TASK_POOL_MANAGER.submit(encodingTask);
    this.ioTaskFuture = SUB_TASK_POOL_MANAGER.submit(ioTask);
    LOGGER.debug("flush task of Storage group {} memtable {} is created ",
        storageGroup, memTable.getVersion());
  }

  public void syncSerialFlushMemTable() throws ExecutionException, InterruptedException {
    LOGGER.info("The memTable size of SG {} is {}, the avg series points num in chunk is {} ",
        storageGroup,
        memTable.memSize(),
        memTable.getTotalPointsNum() / memTable.getSeriesNumber());
    long start = System.currentTimeMillis();
    AtomicLong sortTime = new AtomicLong();
    AtomicLong memSerializeTime = new AtomicLong();

    CompletableFuture.runAsync(() -> {
      Set<Entry<String, Map<String, IWritableMemChunk>>> ite = memTable.getMemTableMap().entrySet();
      try {
        for (Entry<String, Map<String, IWritableMemChunk>> memTableEntry : ite) {
          //start flush io
          this.writer.startChunkGroup(memTableEntry.getKey());
          final Map<String, IWritableMemChunk> value = memTableEntry.getValue();
          for (Entry<String, IWritableMemChunk> iWritableMemChunkEntry : value.entrySet()) {
            long startTime = System.currentTimeMillis();
            IWritableMemChunk series = iWritableMemChunkEntry.getValue();
            MeasurementSchema desc = series.getSchema();
            TVList tvList = series.getSortedTVListForFlush();
            sortTime.addAndGet(System.currentTimeMillis() - startTime);

            //start flush
            long starTime = System.currentTimeMillis();
            IChunkWriter seriesWriter = new ChunkWriterImpl(desc);
            writeOneSeries(tvList, seriesWriter, desc.getType());
            seriesWriter.writeToFileWriter(this.writer);
            memSerializeTime.addAndGet(System.currentTimeMillis() - starTime);
          }
          this.writer.setMinPlanIndex(memTable.getMinPlanIndex());
          this.writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
          this.writer.endChunkGroup();
        }

        writer.writeVersion(memTable.getVersion());
        writer.writePlanIndices();
      } catch (IOException e) {
        LOGGER.error("Storage group {} memtable {}, io task meets error.", storageGroup,
            memTable.getVersion(), e);
        throw new FlushRunTimeException(e);
      }
    }).get();
    noMoreEncodingTask = true;
    LOGGER.debug(
        "Storage group {} memtable {}, flushing into disk: data sort time cost {} ms.",
        storageGroup, memTable.getVersion(), sortTime);
    LOGGER.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup, memTable, System.currentTimeMillis() - start);
  }


  /**
   * the function for flushing memtable.
   */
  public void syncFlushMemTable()
      throws ExecutionException, InterruptedException {
    LOGGER.info("The memTable size of SG {} is {}, the avg series points num in chunk is {} ",
        storageGroup,
        memTable.memSize(),
        memTable.getTotalPointsNum() / memTable.getSeriesNumber());
    long start = System.currentTimeMillis();
    long sortTime = 0;

    //for map do not use get(key) to iteratate
    for (Map.Entry<String, Map<String, IWritableMemChunk>> memTableEntry : memTable.getMemTableMap().entrySet()) {
      encodingTaskQueue.add(new StartFlushGroupIOTask(memTableEntry.getKey()));

      final Map<String, IWritableMemChunk> value = memTableEntry.getValue();
      for (Map.Entry<String, IWritableMemChunk> iWritableMemChunkEntry : value.entrySet()) {
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = iWritableMemChunkEntry.getValue();
        MeasurementSchema desc = series.getSchema();
        TVList tvList = series.getSortedTVListForFlush();
        sortTime += System.currentTimeMillis() - startTime;
        encodingTaskQueue.add(new Pair<>(tvList, desc));
      }

      encodingTaskQueue.add(new EndChunkGroupIoTask());
    }

    noMoreEncodingTask = true;
    LOGGER.debug(
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

    try {
      writer.writeVersion(memTable.getVersion());
      writer.writePlanIndices();
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    LOGGER.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup, memTable, System.currentTimeMillis() - start);
  }


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
          LOGGER.error("Storage group {} does not support data type: {}", storageGroup,
              dataType);
          break;
      }
    }
  }

  private Runnable encodingTask = new Runnable() {
    @SuppressWarnings("squid:S135")
    @Override
    public void run() {
      long memSerializeTime = 0;
      boolean noMoreMessages = false;
      LOGGER.debug("Storage group {} memtable {}, starts to encoding data.", storageGroup,
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
            LOGGER.error("Storage group {} memtable {}, encoding task is interrupted.",
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
      LOGGER.debug("Storage group {}, flushing memtable {} into disk: Encoding data cost "
              + "{} ms.",
          storageGroup, memTable.getVersion(), memSerializeTime);
    }
  };

  @SuppressWarnings("squid:S135")
  private Runnable ioTask = () -> {
    long ioTime = 0;
    boolean returnWhenNoTask = false;
    LOGGER.debug("Storage group {} memtable {}, start io.", storageGroup, memTable.getVersion());
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
          LOGGER.error("Storage group {} memtable {}, io task is interrupted.", storageGroup
              , memTable.getVersion());
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
            this.writer.setMinPlanIndex(memTable.getMinPlanIndex());
            this.writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
            this.writer.endChunkGroup();
          }
        } catch (IOException e) {
          LOGGER.error("Storage group {} memtable {}, io task meets error.", storageGroup,
              memTable.getVersion(), e);
          throw new FlushRunTimeException(e);
        }
        ioTime += System.currentTimeMillis() - starTime;
      }
    }
    LOGGER.debug("flushing a memtable {} in storage group {}, io cost {}ms", memTable.getVersion(),
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
