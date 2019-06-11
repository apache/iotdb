/**
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
package org.apache.iotdb.db.engine.bufferwrite;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.MemTablePool;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferWriteProcessor extends Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessor.class);
  private RestorableTsFileIOWriter writer;
  private FileSchema fileSchema;
  private volatile Future<Boolean> flushFuture = new ImmediateFuture<>(true);
  private volatile Future<Boolean> closeFuture = new BWCloseFuture(new ImmediateFuture<>(true));
  private ReentrantLock flushQueryLock = new ReentrantLock();
  private AtomicLong memSize = new AtomicLong();
  private long memThreshold = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
  private IMemTable workMemTable;

  // each flush task has a flushId, IO task should scheduled by this id
  private long flushId = -1;
  private List<IMemTable> flushingMemTables = new ArrayList<>();

  private Action bufferwriteFlushAction;
  //private Action bufferwriteCloseAction;
  private Consumer<BufferWriteProcessor> bufferwriteCloseConsumer;
  private Action filenodeFlushAction;

  //lastFlushTime time unit: millisecond
  private long lastFlushTime = -1;
  private long valueCount = 0;

  private String baseDir;
  private String insertFilePath;
  private String bufferWriteRelativePath;

  private WriteLogNode logNode;
  private VersionController versionController;

  private boolean isClosing = true;

  private boolean isClosed = false;
  private boolean isFlush = false;



  private TsFileResource currentTsFileResource;

  /**
   * constructor of BufferWriteProcessor.
   *
   * @param baseDir base dir
   * @param processorName processor name
   * @param fileName file name
   * @param parameters parameters in Map(String, Action) structure
   * @param fileSchema file schema
   * @throws BufferWriteProcessorException BufferWriteProcessorException
   */
  public BufferWriteProcessor(String baseDir, String processorName, String fileName,
      Map<String, Action> parameters, Consumer<BufferWriteProcessor> bufferwriteCloseConsumer,
      VersionController versionController,
      FileSchema fileSchema) throws BufferWriteProcessorException {
    super(processorName);
    this.fileSchema = fileSchema;
    this.baseDir = baseDir;

    bufferwriteFlushAction = parameters.get(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
    //bufferwriteCloseAction = parameters.get(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
    filenodeFlushAction = parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
    this.bufferwriteCloseConsumer = bufferwriteCloseConsumer;
    reopen(fileName);
    try {
      getLogNode();
    } catch (IOException e) {
      throw new BufferWriteProcessorException(e);
    }
    this.versionController = versionController;

  }

  public void reopen(String fileName) throws BufferWriteProcessorException {
    if (!isClosing) {
      return;
    }

    try {
      LOGGER.info("wait for flush to reopen a BufferWrite Processor {}", processorName);
      this.flushFuture.get();
      LOGGER.info("wait for close to reopen a BufferWrite Processor {}", processorName);
      this.closeFuture.get();
      LOGGER.info("now begin to reopen the bufferwrite processor {}", processorName);
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("reopen error in Bufferwrite Processor {}", processorName, e);
    }
    new File(baseDir, processorName).mkdirs();
    this.insertFilePath = Paths.get(baseDir, processorName, fileName).toString();
    bufferWriteRelativePath = processorName + File.separatorChar + fileName;
    try {
      writer = new RestorableTsFileIOWriter(processorName, insertFilePath);
    } catch (IOException e) {
      throw new BufferWriteProcessorException(e);
    }
    if (workMemTable == null) {
      workMemTable = MemTablePool.getInstance().getEmptyMemTable();
    } else {
      workMemTable.clear();
    }
    isClosing = false;
    isFlush = false;
  }

  public void checkOpen() throws BufferWriteProcessorException {
    if (isClosing) {
      throw new BufferWriteProcessorException("BufferWriteProcessor already closed");
    }
  }


  /**
   * Only for Test
   *
   * write one data point to the buffer write.
   *
   * @param deviceId device name
   * @param measurementId sensor name
   * @param timestamp timestamp of the data point
   * @param dataType the data type of the value
   * @param value data point value
   * @return true -the size of tsfile or metadata reaches to the threshold. false -otherwise
   * @throws BufferWriteProcessorException if a flushing operation occurs and failed.
   */
  public boolean write(String deviceId, String measurementId, long timestamp, TSDataType dataType,
      String value)
      throws BufferWriteProcessorException {
    checkOpen();
    TSRecord record = new TSRecord(timestamp, deviceId);
    DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, value);
    record.addTuple(dataPoint);
    return write(record);
  }

  /**
   * wrete a ts record into the memtable. If the memory usage is beyond the memThreshold, an async
   * flushing operation will be called.
   *
   * @param tsRecord data to be written
   * @return FIXME what is the mean about the return value??
   * @throws BufferWriteProcessorException if a flushing operation occurs and failed.
   */
  public boolean write(TSRecord tsRecord) throws BufferWriteProcessorException {
    long start1 = System.currentTimeMillis();
    checkOpen();
    long memUsage = MemUtils.getRecordSize(tsRecord);
    BasicMemController.UsageLevel level = BasicMemController.getInstance()
        .acquireUsage(this, memUsage);

    start1 = System.currentTimeMillis() - start1;
    if (start1 > 1000) {
      LOGGER.info("BufferWriteProcessor.write step1 cost: {}", start1);
    }

    String memory;
    switch (level) {
      case SAFE:
        for (DataPoint dataPoint : tsRecord.dataPointList) {
          workMemTable.write(tsRecord.deviceId, dataPoint.getMeasurementId(), dataPoint.getType(),
              tsRecord.time,
              dataPoint.getValue().toString());
        }
        valueCount++;
        checkMemThreshold4Flush(memUsage);
        return true;
      case WARNING:
        memory = MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage());
        LOGGER.warn("Memory usage will exceed warning threshold, current : {}.", memory);
        for (DataPoint dataPoint : tsRecord.dataPointList) {
          workMemTable.write(tsRecord.deviceId, dataPoint.getMeasurementId(), dataPoint.getType(),
              tsRecord.time,
              dataPoint.getValue().toString());
        }
        valueCount++;
        try {
          flush();
        } catch (IOException e) {
          throw new BufferWriteProcessorException(e);
        }
        return true;
      case DANGEROUS:
      default:
        memory = MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage());
        LOGGER.warn("Memory usage will exceed dangerous threshold, current : {}.", memory);
        return false;
    }
  }

  private void checkMemThreshold4Flush(long addedMemory) throws BufferWriteProcessorException {
    long start1 = System.currentTimeMillis();
    long newMem = memSize.addAndGet(addedMemory);
    if (newMem > memThreshold) {
      String usageMem = MemUtils.bytesCntToStr(newMem);
      String threshold = MemUtils.bytesCntToStr(memThreshold);
      String processorName = getProcessorName();
      LOGGER.info("The usage of memory {} in bufferwrite processor {} reaches the threshold {}",
          usageMem, processorName, threshold);
      try {
        flush();
      } catch (IOException e) {
        LOGGER.error("Flush bufferwrite error.", e);
        throw new BufferWriteProcessorException(e);
      }
    }
    start1 = System.currentTimeMillis() - start1;
    if (start1 > 1000) {
      LOGGER.info("BufferWriteProcessor.checkMemThreshold4Flush step-1, cost: {}", start1);
    }
  }

  /**
   * get the one (or two) chunk(s) in the memtable ( and the other one in flushing status and then
   * compact them into one TimeValuePairSorter). Then get its (or their) ChunkMetadata(s).
   *
   * @param deviceId device id
   * @param measurementId sensor id
   * @param dataType data type
   * @return corresponding chunk data and chunk metadata in memory
   */
  public Pair<ReadOnlyMemChunk, List<ChunkMetaData>> queryBufferWriteData(String deviceId,
      String measurementId, TSDataType dataType, Map<String, String> props)
      throws BufferWriteProcessorException {
    checkOpen();
    flushQueryLock.lock();
    try {
      MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
      if (flushingMemTables != null) {
        for (int i = flushingMemTables.size() - 1; i >= 0; i--) {
          memSeriesLazyMerger.addMemSeries(flushingMemTables.get(i).query(deviceId, measurementId, dataType, props));
        }
      }
      memSeriesLazyMerger.addMemSeries(workMemTable.query(deviceId, measurementId, dataType, props));
      // memSeriesLazyMerger has handled the props,
      // so we do not need to handle it again in the following readOnlyMemChunk
      ReadOnlyMemChunk timeValuePairSorter = new ReadOnlyMemChunk(dataType, memSeriesLazyMerger,
          Collections.emptyMap());
      return new Pair<>(timeValuePairSorter,
          writer.getMetadatas(deviceId, measurementId, dataType));
    } finally {
      flushQueryLock.unlock();
    }
  }


  private void switchFlushToWork() {
    LOGGER.info("BufferWrite Processor {} try to get flushQueryLock for switchFlushToWork", getProcessorName());
    flushQueryLock.lock();
    LOGGER.info("BufferWrite Processor {} get flushQueryLock for switchFlushToWork", getProcessorName());
    try {
      writer.appendMetadata();
      isFlush = false;
    } finally {
      flushQueryLock.unlock();
      LOGGER.info("BufferWrite Processor {} release the flushQueryLock for switchFlushToWork successfully", getProcessorName());
    }
  }

  private void removeFlushedMemTable(IMemTable memTable, TsFileIOWriter tsFileIOWriter) {
    long start = System.currentTimeMillis();
    this.writeLock();
    tsFileIOWriter.mergeChunkGroupMetaData();
    try {
      flushingMemTables.remove(memTable);
    } finally {
      this.writeUnlock();
    }
    start = System.currentTimeMillis() - start;
    if (start > 1000) {
      LOGGER.info("removeFlushedMemTable is too slow!!!  cost: {}ms", start);
    }
  }


  /**
   * the caller mast guarantee no other concurrent caller entering this function.
   *
   * @param displayMessage message that will appear in system log.
   * @param tmpMemTableToFlush
   * @param version the operation version that will tagged on the to be flushed memtable
   * (i.e., ChunkGroup)
   * @param walTaskId used for declaring what the wal file name suffix is.
   * @return true if successfully.
   */
  private boolean flushTask(String displayMessage,
      IMemTable tmpMemTableToFlush, long version,
      long walTaskId, long flushId) {
    boolean result;
    long flushStartTime = System.currentTimeMillis();
    LOGGER.info("The bufferwrite processor {} starts flushing {}.", getProcessorName(),
        displayMessage);
    try {
      if (tmpMemTableToFlush != null && !tmpMemTableToFlush.isEmpty()) {
        // flush data
        MemTableFlushTask tableFlushTask = new MemTableFlushTask(writer, getProcessorName(), flushId,
            this::removeFlushedMemTable);
        tableFlushTask.flushMemTable(fileSchema, tmpMemTableToFlush, version);
        // write restore information
        writer.flush();
      }

      filenodeFlushAction.act();
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        logNode.notifyEndFlush(null, walTaskId, new File(writer.getInsertFilePath()).getName());
      }
      result = true;
    } catch (Exception e) {
      LOGGER.error(
          "The bufferwrite processor {} failed to flush {}, when calling the filenodeFlushAction.",
          getProcessorName(), displayMessage, e);
      result = false;
    } finally {
      switchFlushToWork();
      LOGGER.info("The bufferwrite processor {} ends flushing {}.", getProcessorName(),
            displayMessage);
    }
    if (LOGGER.isInfoEnabled()) {
      long flushEndTime = System.currentTimeMillis();
      LOGGER.info(
          "The bufferwrite processor {} flush {}, start time is {}, flush end time is {}, "
              + "flush time consumption is {}ms",
          getProcessorName(), displayMessage,
          DatetimeUtils.convertMillsecondToZonedDateTime(flushStartTime),
          DatetimeUtils.convertMillsecondToZonedDateTime(flushEndTime),
          flushEndTime - flushStartTime);
    }
    return result;
  }

  // keyword synchronized is added in this method, so that only one flush task can be submitted now.
  @Override
  public synchronized Future<Boolean> flush() throws IOException {
    return flush(false);
  }

  // keyword synchronized is added in this method, so that only one flush task can be submitted now.
  private Future<Boolean> flush(boolean isCloseTaskCalled) throws IOException {
    if (!isCloseTaskCalled && isClosing) {
      throw new IOException("BufferWriteProcessor closed");
    }
    // statistic information for flush
    if (lastFlushTime > 0) {
      if (LOGGER.isInfoEnabled()) {
        long thisFlushTime = System.currentTimeMillis();
        LOGGER.info(
            "The bufferwrite processor {} will submit a flush task."
                + "The last flush time is {}, this flush time is {}, "
                + "flush time interval is {}s", getProcessorName(),
            DatetimeUtils.convertMillsecondToZonedDateTime(lastFlushTime),
            DatetimeUtils.convertMillsecondToZonedDateTime(thisFlushTime),
            (thisFlushTime - lastFlushTime) / 1000);
      }
    }
    lastFlushTime = System.currentTimeMillis();
    // check value count
    if (valueCount > 0) {
      // update the lastUpdatetime, prepare for flush
      try {
        bufferwriteFlushAction.act();
      } catch (Exception e) {
        LOGGER.error("Failed to flush bufferwrite row group when calling the action function.");
        throw new IOException(e);
      }
      final long walTaskId;
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        walTaskId = logNode.notifyStartFlush(new File(writer.getInsertFilePath()).getName());
        LOGGER.info("BufferWrite Processor {} has notified WAL for flushing.", getProcessorName());
      } else {
        walTaskId = 0;
      }
      valueCount = 0;

      flushingMemTables.add(workMemTable);
      IMemTable tmpMemTableToFlush = workMemTable;

      long start = System.currentTimeMillis();
      workMemTable = MemTablePool.getInstance().getEmptyMemTable();

      start = System.currentTimeMillis() - start;
      if (start > 1000) {
        LOGGER.info("get empty memtable cost: {}", start);
      }

      flushId++;
      long version = versionController.nextVersion();
      BasicMemController.getInstance().releaseUsage(this, memSize.get());
      memSize.set(0);
      // switch
      if (isCloseTaskCalled) {
        LOGGER.info(
            "flush memtable for bufferwrite processor {} synchronously for close task.",
            getProcessorName(), FlushManager.getInstance().getWaitingTasksNumber(),
            FlushManager.getInstance().getCorePoolSize());
        flushTask("synchronously", tmpMemTableToFlush, version, walTaskId, flushId);
        flushFuture = new ImmediateFuture<>(true);
      } else {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Begin to submit flush task for bufferwrite processor {}, current Flush Queue is {}, core pool size is {}.",
              getProcessorName(), FlushManager.getInstance().getWaitingTasksNumber(),
              FlushManager.getInstance().getCorePoolSize());
        }
        flushFuture = FlushManager.getInstance().submit(() -> flushTask("asynchronously",
            tmpMemTableToFlush, version, walTaskId, flushId));
      }
    } else {
      flushFuture = new ImmediateFuture<>(true);
    }
    return flushFuture;
  }

  @Override
  public boolean canBeClosed() {
    return true;
  }

  @Override
  public synchronized void close() throws BufferWriteProcessorException {
    if (isClosing) {
      return;
    }
    try {
      isClosing = true;
      // flush data (if there are flushing task, flush() will be blocked)
      //Future<Boolean> flush = flush();
      //and wait for finishing flush async
      LOGGER.info("Submit a BufferWrite ({}) close task.", getProcessorName());
      closeFuture = new BWCloseFuture(FlushManager.getInstance().submit(() -> closeTask()));
      //now, we omit the future of the closeTask.
    } catch (Exception e) {
      LOGGER
          .error("Failed to close the bufferwrite processor when calling the action function.", e);
      throw new BufferWriteProcessorException(e);
    }
  }

  private boolean closeTask() {
    long closeStartTime = System.currentTimeMillis();
    try {
      LOGGER.info("Bufferwrite {} Close Task: begin to wait for the flush.", getProcessorName());
      flush(true);
      LOGGER.info("Bufferwrite {} Close Task: finishing the flush.", getProcessorName());
      // end file
      writer.endFile(fileSchema);
      //FIXME suppose the flush-thread-pool is 2.
      // then if a flush task and a close task are running in the same time
      // and the close task is faster, then writer == null, and the flush task will throw nullpointer
      // exception. Add "synchronized" keyword on both flush and close may solve the issue.
      writer = null;
      // update the IntervalFile for interval list
      bufferwriteCloseConsumer.accept(this);
      // flush the changed information for filenode
      filenodeFlushAction.act();
      // delete the restore for this bufferwrite processor
      if (LOGGER.isInfoEnabled()) {
        long closeEndTime = System.currentTimeMillis();
        LOGGER.info(
            "Close bufferwrite processor {}, the file name is {}, start time is {}, end time is {}, "
                + "time consumption is {}ms",
            getProcessorName(), insertFilePath,
            DatetimeUtils.convertMillsecondToZonedDateTime(closeStartTime),
            DatetimeUtils.convertMillsecondToZonedDateTime(closeEndTime),
            closeEndTime - closeStartTime);
      }

    }catch (IOException | ActionException e) {
      LOGGER.error("Close bufferwrite processor {} failed.", getProcessorName(), e);
      return false;
    } finally {
      isClosed = true;
    }
    return true;
  }

  @Override
  public long memoryUsage() {
    return memSize.get();
  }

  /**
   * check if is flushing.
   *
   * @return True if flushing
   */
  public boolean isFlush() {
    return isFlush;
  }

  /**
   * get metadata size.
   *
   * @return The sum of all timeseries's metadata size within this file.
   */
  public long getMetaSize() {
    // TODO : [MemControl] implement this
    return 0;
  }

  /**
   * get file size.
   *
   * @return The file size of the TsFile corresponding to this processor.
   */
  public long getFileSize() {
    // TODO : save this variable to avoid object creation?
    File file = new File(insertFilePath);
    return file.length() + memoryUsage();
  }

  public String getBaseDir() {
    return baseDir;
  }


  public String getFileRelativePath() {
    return bufferWriteRelativePath;
  }

  private String getBufferwriteRestoreFilePath() {
    return writer.getRestoreFilePath();
  }

  public boolean isNewProcessor() {
    return writer.isNewResource();
  }

  public void setNewProcessor(boolean isNewProcessor) {
    writer.setNewResource(isNewProcessor);
  }

  public WriteLogNode getLogNode() throws IOException {
    if (logNode == null) {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        logNode = MultiFileLogNodeManager.getInstance().getNode(
            processorName + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX,
            getBufferwriteRestoreFilePath(),
            FileNodeManager.getInstance().getRestoreFilePath(processorName));
      }
    }
    return logNode;
  }

  /**
   * used for test. We can know when the flush() is called.
   * @return the last flush() time. Time unit: millisecond.
   */
  public long getLastFlushTime() {
    return lastFlushTime;
  }

  /**
   * used for test. We can block to wait for finishing flushing.
   * @return the future of the flush() task.
   */
  public Future<Boolean> getFlushFuture() {
    return flushFuture;
  }

  /**
   * Delete data whose timestamp <= 'timestamp' and belonging to timeseries deviceId.measurementId.
   * Delete data in both working MemTable and flushing MemTable.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the upper-bound of deletion time.
   */
  public void delete(String deviceId, String measurementId, long timestamp)
      throws BufferWriteProcessorException {
    checkOpen();
    workMemTable.delete(deviceId, measurementId, timestamp);
    if (isFlush()) {
      // flushing MemTable cannot be directly modified since another thread is reading it
      for (IMemTable memTable : flushingMemTables) {
        if (memTable.containSeries(deviceId, measurementId)) {
          memTable.delete(new Deletion(deviceId + PATH_SEPARATOR + measurementId, 0, timestamp));
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }

  @Override
  public String toString() {
    return "BufferWriteProcessor in " + insertFilePath;
  }

  public String getInsertFilePath() {
    return insertFilePath;
  }

  public boolean isClosing() {
    return isClosing;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public TsFileResource getCurrentTsFileResource() {
    return currentTsFileResource;
  }
  public void setCurrentTsFileResource(TsFileResource resource) {
    this.currentTsFileResource = resource;
  }

  public Future<Boolean> getCloseFuture() {
    return closeFuture;
  }


  class BWCloseFuture implements Future<Boolean> {
    Future<Boolean> future;
    public BWCloseFuture(Future<Boolean> closeFuture) {
      this.future = closeFuture;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
      return flushFuture.get() && future.get();
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return flushFuture.get(timeout, unit) && future.get(timeout, unit);
    }
  }
}
