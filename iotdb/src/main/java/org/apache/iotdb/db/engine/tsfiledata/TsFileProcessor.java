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

package org.apache.iotdb.db.engine.tsfiledata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.engine.bufferwrite.RestorableTsFileIOWriter;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushUtil;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessor extends Processor {


  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileProcessor.class);

  private FileSchema fileSchema;
  private volatile Future<Boolean> flushFuture = new ImmediateFuture<>(true);
  private ReentrantLock flushQueryLock = new ReentrantLock();
  private AtomicLong memSize = new AtomicLong();
  private long memThreshold = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;


  //lastFlushTime (system time, rather than data time) time unit: nanosecond
  private long lastFlushTime = -1;
  //the times of calling insertion function.
  private long valueCount = 0;


  private IMemTable workMemTable;
  private IMemTable flushMemTable;
  private RestorableTsFileIOWriter writer;
  private Action beforeFlushAction;
  private Action afterCloseAction;
  private Action afterFlushAction;
  private File insertFile;
  private TsFileResource currentResource;

  private List<TsFileResource> tsFileResources;
  private Map<String, List<TsFileResource>> inverseIndexOfResource;

  private Map<String, Long> lastFlushedTime;

  private WriteLogNode logNode;
  private VersionController versionController;


  /**
   * constructor of BufferWriteProcessor.
   * data will be stored in baseDir/processorName/ folder.
   *
   * @param processorName processor name
   * @param fileSchema file schema
   * @throws BufferWriteProcessorException BufferWriteProcessorException
   */
  public TsFileProcessor(String processorName,
      Action beforeFlushAction, Action afterFlushAction, Action afterCloseAction, VersionController versionController,
      FileSchema fileSchema) throws BufferWriteProcessorException {
    super(processorName);
    this.fileSchema = fileSchema;
    this.processorName = processorName;

    this.beforeFlushAction = beforeFlushAction;
    this.afterCloseAction = afterCloseAction;
    this.afterFlushAction = afterFlushAction;
    workMemTable = new PrimitiveMemTable();
    tsFileResources = new ArrayList<>();
    inverseIndexOfResource = new HashMap<>();
    lastFlushedTime = new HashMap<>();
    for (String folderPath : Directories.getInstance().getAllTsFileFolders()) {
      File dataFolder = new File(folderPath, processorName);
      if(dataFolder.exists()) {
        for(File tsfile : dataFolder.listFiles(x -> !x.getName().contains(RestorableTsFileIOWriter.RESTORE_SUFFIX))) {
          //TODO we'd better define a file suffix for TsFile, e.g., .ts
          TsFileResource resource = new TsFileResource(tsfile, true);
          tsFileResources.add(resource);
          //maintain the inverse index and lastFlushTime
          for (String device : resource.getDevices()) {
            inverseIndexOfResource.computeIfAbsent(device, k -> new ArrayList<>()).add(resource);
            lastFlushedTime.merge(device, resource.getEndTime(device), (x, y) -> x > y ? x : y);
          }
        }
      }
    }

    initNewTsFile();

    this.versionController = versionController;
    // we need to know  the last flushed timestamps of all devices
  }


  private void initNewTsFile() throws BufferWriteProcessorException {
    String dataDir = Directories.getInstance().getNextFolderForTsfile();
    File dataFolder = new File(dataDir, processorName);
    if (!dataFolder.exists()) {
      if (!dataFolder.mkdirs()) {
        throw new BufferWriteProcessorException(
            String.format("Can not create TsFileProcess related folder: %s", dataFolder));
      }
      LOGGER.debug("The bufferwrite processor data dir doesn't exists, create new directory {}.",
          dataFolder.getAbsolutePath());
    }
    String fileName = (lastFlushTime + 1) + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR
        + System.currentTimeMillis();
    this.insertFile = new File(dataDir, fileName);
    try {
      writer = new RestorableTsFileIOWriter(processorName, insertFile.getAbsolutePath());
      this.currentResource = new TsFileResource(insertFile, true);
      //TODO how to recover the index data.
    } catch (IOException e) {
      throw new BufferWriteProcessorException(e);
    }
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        logNode = MultiFileLogNodeManager.getInstance().getNode(
            processorName + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX,
            writer.getRestoreFilePath(),
            FileNodeManager.getInstance().getRestoreFilePath(processorName));
      } catch (IOException e) {
        throw new BufferWriteProcessorException(e);
      }
    }

  }

  /**
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
  public boolean insert(String deviceId, String measurementId, long timestamp, TSDataType dataType,
      String value)
      throws BufferWriteProcessorException {
    if (!lastFlushedTime.containsKey(deviceId)) {
      lastFlushedTime.put(deviceId, 0L);
    } else if (timestamp < lastFlushedTime.get(deviceId)) {
      return false;
    }
    workMemTable.write(deviceId, measurementId, dataType, timestamp, value);
    valueCount++;
    long memUsage = MemUtils.getPointSize(dataType, value);
    checkMemThreshold4Flush(memUsage);
    return true;
  }

  /**
   * wrete a ts record into the memtable. If the memory usage is beyond the memThreshold, an async
   * flushing operation will be called.
   *
   * @param tsRecord data to be written
   * @return true if the tsRecord can be inserted into tsFile. otherwise false (, then you need to insert it into overflow)
   * @throws BufferWriteProcessorException if a flushing operation occurs and failed.
   */
  public boolean insert(TSRecord tsRecord) throws BufferWriteProcessorException {
    if (!lastFlushedTime.containsKey(tsRecord.deviceId)) {
      lastFlushedTime.put(tsRecord.deviceId, 0L);
    } else if (tsRecord.time < lastFlushedTime.get(tsRecord.deviceId)) {
        return false;
    }
    for (DataPoint dataPoint : tsRecord.dataPointList) {
      workMemTable.write(tsRecord.deviceId, dataPoint.getMeasurementId(), dataPoint.getType(),
          tsRecord.time, dataPoint.getValue());
    }
    valueCount++;
    long memUsage = MemUtils.getRecordSize(tsRecord);
    checkMemThreshold4Flush(memUsage);
    return true;
  }


  /**
   * Delete data whose timestamp <= 'timestamp' and belonging to timeseries deviceId.measurementId.
   * Delete data in both working MemTable and flushing MemTable.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the upper-bound of deletion time.
   */
  public void delete(String deviceId, String measurementId, long timestamp) throws IOException {
    workMemTable.delete(deviceId, measurementId, timestamp);
    boolean deleteFlushTable = false;
    if (isFlush()) {
      // flushing MemTable cannot be directly modified since another thread is reading it
      flushMemTable = flushMemTable.copy();
      deleteFlushTable = flushMemTable.delete(deviceId, measurementId, timestamp);
    }
    String fullPath = deviceId +
        IoTDBConstant.PATH_SEPARATOR + measurementId;
    Deletion deletion = new Deletion(fullPath, versionController.nextVersion(), timestamp);
    if (deleteFlushTable || currentResource.getStartTime(deviceId) < timestamp) {
      currentResource.getModFile().write(deletion);
    }
    for (TsFileResource resource : tsFileResources) {
      if (resource.containsDevice(deviceId) && resource.getStartTime(deviceId) < timestamp) {
          resource.getModFile().write(deletion);
      }
    }
  }


  private void checkMemThreshold4Flush(long addedMemory) throws BufferWriteProcessorException {
    BasicMemController.UsageLevel level = BasicMemController.getInstance().reportUse(this, addedMemory);
    String memory;
    switch (level) {
      case SAFE:
        break;
      case WARNING:
        memory = MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage());
        LOGGER.warn("Memory usage will exceed warning threshold, current : {}.", memory);
        break;
      case DANGEROUS:
      default:
        memory = MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage());
        LOGGER.warn("Memory usage will exceed dangerous threshold, current : {}.", memory);
        throw new BufferWriteProcessorException("Memory usage will exceed dangerous threshold");
    }
    long newMem = memSize.addAndGet(addedMemory);
    if (newMem > memThreshold) {
      String usageMem = MemUtils.bytesCntToStr(newMem);
      String threshold = MemUtils.bytesCntToStr(memThreshold);
      LOGGER.info("The usage of memory {} in bufferwrite processor {} reaches the threshold {}",
          usageMem, processorName, threshold);
      try {
        flush();
      } catch (IOException e) {
        LOGGER.error("Flush bufferwrite error.", e);
        throw new BufferWriteProcessorException(e);
      }
    }
  }



  // keyword synchronized is added in this method, so that only one flush task can be submitted now.
  @Override
  public synchronized Future<Boolean> flush() throws IOException {
    // statistic information for flush
    if (lastFlushTime > 0) {
      if (LOGGER.isInfoEnabled()) {
        long thisFlushTime = System.currentTimeMillis();
        LOGGER.info(
            "The bufferwrite processor {}: last flush time is {}, this flush time is {}, "
                + "flush time interval is {}s", getProcessorName(),
            DatetimeUtils.convertMillsecondToZonedDateTime(lastFlushTime / 1000),
            DatetimeUtils.convertMillsecondToZonedDateTime(thisFlushTime),
            (thisFlushTime - lastFlushTime / 1000) / 1000);
      }
    }
    lastFlushTime = System.nanoTime();
    // check value count
    if (valueCount > 0) {
      // waiting for the end of last flush operation.
      try {
        flushFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Encounter an interrupt error when waitting for the flushing, "
                + "the bufferwrite processor is {}.",
            getProcessorName(), e);
        Thread.currentThread().interrupt();
      }
      // update the lastUpdatetime, prepare for flush
      try {
        beforeFlushAction.act();
      } catch (Exception e) {
        LOGGER.error("Failed to flush memtable into tsfile when calling the action function.");
        throw new IOException(e);
      }
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        logNode.notifyStartFlush();
      }
      valueCount = 0;
      switchWorkToFlush();
      long version = versionController.nextVersion();
      BasicMemController.getInstance().reportFree(this, memSize.get());
      memSize.set(0);
      // switch
      flushFuture = FlushManager.getInstance().submit(() -> flushTask("asynchronously",
          version));
    } else {
      flushFuture = new ImmediateFuture<>(true);
    }
    return flushFuture;
  }

  /**
   * the caller mast guarantee no other concurrent caller entering this function.
   *
   * @param displayMessage message that will appear in system log.
   * @param version the operation version that will tagged on the to be flushed memtable
   * (i.e., ChunkGroup)
   * @return true if successfully.
   */
  private boolean flushTask(String displayMessage, long version) {
    boolean result;
    long flushStartTime = System.currentTimeMillis();
    LOGGER.info("The bufferwrite processor {} starts flushing {}.", getProcessorName(),
        displayMessage);
    Map <String, Pair<Long, Long>> minMaxTimeInMemTable = null;
    try {
      if (flushMemTable != null && !flushMemTable.isEmpty()) {
        // flush data
        minMaxTimeInMemTable = MemTableFlushUtil.flushMemTable(fileSchema, writer, flushMemTable,
            version);
        // write restore information
        writer.flush();
      }

      afterFlushAction.act();
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        logNode.notifyEndFlush(null);
      }
      //we update the index of currentTsResource.
      for (Map.Entry<String, Pair<Long, Long>> timePair : minMaxTimeInMemTable.entrySet()) {
        lastFlushedTime.put(timePair.getKey(), timePair.getValue().right);
        if (!currentResource.containsDevice(timePair.getKey())) {
          //the start time has not been set.
          currentResource.setStartTime(timePair.getKey(), timePair.getValue().left);
        }
        // new end time must be larger than the old one.
        currentResource.setEndTime(timePair.getKey(), timePair.getValue().right);
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

  private void switchWorkToFlush() {
    flushQueryLock.lock();
    try {
      if (flushMemTable == null) {
        flushMemTable = workMemTable;
        workMemTable = new PrimitiveMemTable();
      }
    } finally {
      flushQueryLock.unlock();
    }
  }

  private void switchFlushToWork() {
    flushQueryLock.lock();
    try {
      flushMemTable.clear();
      flushMemTable = null;
      writer.appendMetadata();
    } finally {
      flushQueryLock.unlock();
    }
  }

  @Override
  public boolean canBeClosed() {
    return true;
  }

  //very dangerous, how to make sure this function is thread safe (no other functions are running)
  @Override
  public void close() throws BufferWriteProcessorException {
    try {
      long closeStartTime = System.currentTimeMillis();
      // flush data and wait for finishing flush
      flush().get();
      // end file
      writer.endFile(fileSchema);
      // update the IntervalFile for interval list
      afterCloseAction.act();
      // flush the changed information for filenode
      afterFlushAction.act();

      tsFileResources.add(currentResource);
      //maintain the inverse index
      for (String device : currentResource.getDevices()) {
        inverseIndexOfResource.computeIfAbsent(device, k -> new ArrayList<>()).add(currentResource);
      }

      // delete the restore for this bufferwrite processor
      if (LOGGER.isInfoEnabled()) {
        long closeEndTime = System.currentTimeMillis();
        LOGGER.info(
            "Close bufferwrite processor {}, the file name is {}, start time is {}, end time is {}, "
                + "time consumption is {}ms",
            getProcessorName(), insertFile.getAbsolutePath(),
            DatetimeUtils.convertMillsecondToZonedDateTime(closeStartTime),
            DatetimeUtils.convertMillsecondToZonedDateTime(closeEndTime),
            closeEndTime - closeStartTime);
      }

      workMemTable.clear();
      flushMemTable.clear();
      logNode.close();
      initNewTsFile();

    } catch (IOException e) {
      LOGGER.error("Close the bufferwrite processor error, the bufferwrite is {}.",
          getProcessorName(), e);
      throw new BufferWriteProcessorException(e);
    } catch (Exception e) {
      LOGGER
          .error("Failed to close the bufferwrite processor when calling the action function.", e);
      throw new BufferWriteProcessorException(e);
    }
  }

  @Override
  public long memoryUsage() {
    return 0;
  }

  /**
   * check if is flushing.
   *
   * @return True if flushing
   */
  public boolean isFlush() {
    // starting a flush task has two steps: set the flushMemtable, and then set the flushFuture
    // So, the following case exists: flushMemtable != null but flushFuture is done (because the
    // flushFuture refers to the last finished flush.
    // And, the following case exists,too: flushMemtable == null, but flushFuture is not done.
    // (flushTask() is not finished, but switchToWork() has done)
    // So, checking flushMemTable is more meaningful than flushFuture.isDone().
    return  flushMemTable != null;
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
      String measurementId, TSDataType dataType, Map<String, String> props) {
    flushQueryLock.lock();
    try {
      MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
      if (flushMemTable != null) {
        memSeriesLazyMerger.addMemSeries(flushMemTable.query(deviceId, measurementId, dataType, props));
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

  public String getInsertFilePath() {
    return insertFile.getAbsolutePath();
  }

  public WriteLogNode getLogNode() {
    return logNode;
  }

  /**
   * used for test. We can know when the flush() is called.
   * @return the last flush() time. Time unit: nanosecond.
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
}
