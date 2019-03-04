/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.overflow.io;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.MergeSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.OverflowProcessorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverflowProcessor extends Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
  private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.getInstance().getConfig();
  private OverflowResource workResource;
  private OverflowResource mergeResource;

  private OverflowSupport workSupport;
  private OverflowSupport flushSupport;

  private volatile Future<Boolean> flushFuture = new ImmediateFuture<>(true);
  private volatile boolean isMerge;
  private int valueCount;
  private String parentPath;
  private long lastFlushTime = -1;
  private AtomicLong dataPathCount = new AtomicLong();
  private ReentrantLock queryFlushLock = new ReentrantLock();

  private Action overflowFlushAction;
  private Action filenodeFlushAction;
  private FileSchema fileSchema;

  private long memThreshold = TSFileConfig.groupSizeInByte;
  private AtomicLong memSize = new AtomicLong();

  private WriteLogNode logNode;
  private VersionController versionController;

  public OverflowProcessor(String processorName, Map<String, Action> parameters,
      FileSchema fileSchema, VersionController versionController)
      throws IOException {
    super(processorName);
    this.fileSchema = fileSchema;
    this.versionController = versionController;
    String overflowDirPath = TsFileDBConf.overflowDataDir;
    if (overflowDirPath.length() > 0
        && overflowDirPath.charAt(overflowDirPath.length() - 1) != File.separatorChar) {
      overflowDirPath = overflowDirPath + File.separatorChar;
    }
    this.parentPath = overflowDirPath + processorName;
    File processorDataDir = new File(parentPath);
    if (!processorDataDir.exists()) {
      processorDataDir.mkdirs();
    }
    // recover file
    recovery(processorDataDir);
    // memory
    workSupport = new OverflowSupport();
    overflowFlushAction = parameters.get(FileNodeConstants.OVERFLOW_FLUSH_ACTION);
    filenodeFlushAction = parameters
        .get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);

    if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
      logNode = MultiFileLogNodeManager.getInstance().getNode(
          processorName + IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX,
          getOverflowRestoreFile(),
          FileNodeManager.getInstance().getRestoreFilePath(processorName));
    }
  }

  private void recovery(File parentFile) throws IOException {
    String[] subFilePaths = clearFile(parentFile.list());
    if (subFilePaths.length == 0) {
      workResource = new OverflowResource(parentPath,
          String.valueOf(dataPathCount.getAndIncrement()), versionController);
    } else if (subFilePaths.length == 1) {
      long count = Long.parseLong(subFilePaths[0]);
      dataPathCount.addAndGet(count + 1);
      workResource = new OverflowResource(parentPath, String.valueOf(count), versionController);
      LOGGER.info("The overflow processor {} recover from work status.", getProcessorName());
    } else {
      long count1 = Long.parseLong(subFilePaths[0]);
      long count2 = Long.parseLong(subFilePaths[1]);
      if (count1 > count2) {
        long temp = count1;
        count1 = count2;
        count2 = temp;
      }
      dataPathCount.addAndGet(count2 + 1);
      // work dir > merge dir
      workResource = new OverflowResource(parentPath, String.valueOf(count2), versionController);
      mergeResource = new OverflowResource(parentPath, String.valueOf(count1), versionController);
      LOGGER.info("The overflow processor {} recover from merge status.", getProcessorName());
    }
  }

  private String[] clearFile(String[] subFilePaths) {
    // just clear the files whose name are number.
    List<String> files = new ArrayList<>();
    for (String file : subFilePaths) {
      try {
        Long.valueOf(file);
        files.add(file);
      } catch (NumberFormatException e) {
        // ignore the exception, if the name of file is not a number.

      }
    }
    return files.toArray(new String[files.size()]);
  }

  /**
   * insert one time-series record
   */
  public void insert(TSRecord tsRecord) throws IOException {
    // memory control
    long memUage = MemUtils.getRecordSize(tsRecord);
    BasicMemController.getInstance().reportUse(this, memUage);
    // write data
    workSupport.insert(tsRecord);
    valueCount++;
    // check flush
    memUage = memSize.addAndGet(memUage);
    if (memUage > memThreshold) {
      LOGGER.warn("The usage of memory {} in overflow processor {} reaches the threshold {}",
          MemUtils.bytesCntToStr(memUage), getProcessorName(),
          MemUtils.bytesCntToStr(memThreshold));
      flush();
    }
  }

  /**
   * @deprecated update one time-series data which time range is from startTime from endTime.
   */
  @Deprecated
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, byte[] value) {
    workSupport.update(deviceId, measurementId, startTime, endTime, type, value);
    valueCount++;
  }

  /**
   * @deprecated this function need to be re-implemented.
   */
  @Deprecated
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, String value) {
    workSupport.update(deviceId, measurementId, startTime, endTime, type,
        convertStringToBytes(type, value));
    valueCount++;
  }

  private byte[] convertStringToBytes(TSDataType type, String o) {
    switch (type) {
      case INT32:
        return BytesUtils.intToBytes(Integer.valueOf(o));
      case INT64:
        return BytesUtils.longToBytes(Long.valueOf(o));
      case BOOLEAN:
        return BytesUtils.boolToBytes(Boolean.valueOf(o));
      case FLOAT:
        return BytesUtils.floatToBytes(Float.valueOf(o));
      case DOUBLE:
        return BytesUtils.doubleToBytes(Double.valueOf(o));
      case TEXT:
        return BytesUtils.stringToBytes(o);
      default:
        LOGGER.error("Unsupport data type: {}", type);
        throw new UnsupportedOperationException("Unsupport data type:" + type);
    }
  }

  /**
   * Delete data of a timeseries whose time ranges from 0 to timestamp.
   *
   * @param deviceId the deviceId of the timeseries.
   * @param measurementId the measurementId of the timeseries.
   * @param timestamp the upper-bound of deletion time.
   * @param version the version number of this deletion.
   * @param updatedModFiles add successfully updated Modification files to the list, and abort them
   * when exception is raised
   */
  public void delete(String deviceId, String measurementId, long timestamp, long version,
      List<ModificationFile> updatedModFiles) throws IOException {
    workResource.delete(deviceId, measurementId, timestamp, version, updatedModFiles);
    workSupport.delete(deviceId, measurementId, timestamp, false);
    if (isFlush()) {
      mergeResource.delete(deviceId, measurementId, timestamp, version, updatedModFiles);
      flushSupport.delete(deviceId, measurementId, timestamp, true);
    }
  }

  /**
   * query all overflow data which contain insert data in memory, insert data in file, update/delete
   * data in memory, update/delete data in file.
   *
   * @return OverflowSeriesDataSource
   */
  public OverflowSeriesDataSource query(String deviceId, String measurementId,
      TSDataType dataType, Map<String, String> props, QueryContext context)
      throws IOException {
    queryFlushLock.lock();
    try {
      // query insert data in memory and unseqTsFiles
      // memory
      ReadOnlyMemChunk insertInMem = queryOverflowInsertInMemory(deviceId, measurementId,
          dataType, props);
      List<OverflowInsertFile> overflowInsertFileList = new ArrayList<>();
      // work file
      Pair<String, List<ChunkMetaData>> insertInDiskWork = queryWorkDataInOverflowInsert(deviceId,
          measurementId,
          dataType, context);
      if (insertInDiskWork.left != null) {
        overflowInsertFileList
            .add(0, new OverflowInsertFile(insertInDiskWork.left,
                insertInDiskWork.right));
      }
      // merge file
      Pair<String, List<ChunkMetaData>> insertInDiskMerge = queryMergeDataInOverflowInsert(deviceId,
          measurementId, dataType, context);
      if (insertInDiskMerge.left != null) {
        overflowInsertFileList
            .add(0, new OverflowInsertFile(insertInDiskMerge.left
                , insertInDiskMerge.right));
      }
      // work file
      return new OverflowSeriesDataSource(new Path(deviceId + "." + measurementId), dataType,
          overflowInsertFileList, insertInMem);
    } finally {
      queryFlushLock.unlock();
    }
  }

  /**
   * query insert data in memory table. while flushing, merge the work memory table with flush
   * memory table.
   *
   * @return insert data in SeriesChunkInMemTable
   */
  private ReadOnlyMemChunk queryOverflowInsertInMemory(String deviceId, String measurementId,
      TSDataType dataType, Map<String, String> props) {

    MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
    queryFlushLock.lock();
    try {
      if (flushSupport != null && isFlush()) {
        memSeriesLazyMerger
            .addMemSeries(
                flushSupport.queryOverflowInsertInMemory(deviceId, measurementId, dataType, props));
      }
      memSeriesLazyMerger
          .addMemSeries(workSupport.queryOverflowInsertInMemory(deviceId, measurementId,
              dataType, props));
      // memSeriesLazyMerger has handled the props,
      // so we do not need to handle it again in the following readOnlyMemChunk
      return new ReadOnlyMemChunk(dataType, memSeriesLazyMerger, Collections.emptyMap());
    } finally {
      queryFlushLock.unlock();
    }
  }

  /**
   * Get the insert data which is WORK in unseqTsFile.
   *
   * @param deviceId deviceId of the target time-series
   * @param measurementId measurementId of the target time-series
   * @param dataType data type of the target time-series
   * @return the seriesPath of unseqTsFile, List of TimeSeriesChunkMetaData for the special
   * time-series.
   */
  private Pair<String, List<ChunkMetaData>> queryWorkDataInOverflowInsert(String deviceId,
      String measurementId, TSDataType dataType, QueryContext context) {
    return new Pair<>(
        workResource.getInsertFilePath(),
        workResource.getInsertMetadatas(deviceId, measurementId, dataType, context));
  }

  /**
   * Get the all merge data in unseqTsFile and overflowFile.
   *
   * @return MergeSeriesDataSource
   */
  public MergeSeriesDataSource queryMerge(String deviceId, String measurementId,
      TSDataType dataType, QueryContext context) {
    Pair<String, List<ChunkMetaData>> mergeInsert = queryMergeDataInOverflowInsert(deviceId,
        measurementId,
        dataType, context);
    return new MergeSeriesDataSource(new OverflowInsertFile(mergeInsert.left, mergeInsert.right));
  }

  public OverflowSeriesDataSource queryMerge(String deviceId, String measurementId,
      TSDataType dataType, boolean isMerge, QueryContext context) {
    Pair<String, List<ChunkMetaData>> mergeInsert = queryMergeDataInOverflowInsert(deviceId,
        measurementId,
        dataType, context);
    OverflowSeriesDataSource overflowSeriesDataSource = new OverflowSeriesDataSource(
        new Path(deviceId + "." + measurementId));
    overflowSeriesDataSource.setReadableMemChunk(null);
    overflowSeriesDataSource
        .setOverflowInsertFileList(
            Arrays.asList(new OverflowInsertFile(mergeInsert.left, mergeInsert.right)));
    return overflowSeriesDataSource;
  }

  /**
   * Get the insert data which is MERGE in unseqTsFile
   *
   * @return the seriesPath of unseqTsFile, List of TimeSeriesChunkMetaData for the special
   * time-series.
   **/
  private Pair<String, List<ChunkMetaData>> queryMergeDataInOverflowInsert(String deviceId,
      String measurementId, TSDataType dataType, QueryContext context) {
    if (!isMerge) {
      return new Pair<>(null, null);
    }
    return new Pair<>(
        mergeResource.getInsertFilePath(),
        mergeResource.getInsertMetadatas(deviceId, measurementId, dataType, context));
  }

  private void switchWorkToFlush() {
    queryFlushLock.lock();
    try {
      flushSupport = workSupport;
      workSupport = new OverflowSupport();
    } finally {
      queryFlushLock.unlock();
    }
  }

  private void switchFlushToWork() {
    queryFlushLock.lock();
    try {
      flushSupport.clear();
      workResource.appendMetadatas();
      flushSupport = null;
    } finally {
      queryFlushLock.unlock();
    }
  }

  public void switchWorkToMerge() throws IOException {
    if (mergeResource == null) {
      mergeResource = workResource;
      workResource = new OverflowResource(parentPath,
          String.valueOf(dataPathCount.getAndIncrement()), versionController);
    }
    isMerge = true;
    LOGGER.info("The overflow processor {} switch from WORK to MERGE", getProcessorName());
  }

  public void switchMergeToWork() throws IOException {
    if (mergeResource != null) {
      mergeResource.close();
      mergeResource.deleteResource();
      mergeResource = null;
    }
    isMerge = false;
    LOGGER.info("The overflow processor {} switch from MERGE to WORK", getProcessorName());
  }

  public boolean isMerge() {
    return isMerge;
  }

  public boolean isFlush() {
    //see BufferWriteProcess.isFlush()
    return  flushSupport != null;
  }

  private boolean flushTask(String displayMessage) {
    boolean result;
    long flushStartTime = System.currentTimeMillis();
    try {
      LOGGER.info("The overflow processor {} starts flushing {}.", getProcessorName(),
                  displayMessage);
      // flush data
      workResource
          .flush(fileSchema, flushSupport.getMemTabale(),
              getProcessorName());
      filenodeFlushAction.act();
      // write-ahead log
      if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
        logNode.notifyEndFlush(null);
      }
      result = true;
    } catch (IOException e) {
      LOGGER.error("Flush overflow processor {} rowgroup to file error in {}. Thread {} exits.",
          getProcessorName(), displayMessage, Thread.currentThread().getName(), e);
      result = false;
    } catch (Exception e) {
      LOGGER.error("FilenodeFlushAction action failed. Thread {} exits.",
          Thread.currentThread().getName(), e);
      result = false;
    } finally {
        // switch from flush to work.
        switchFlushToWork();
    }
    // log flush time
    if (LOGGER.isInfoEnabled()) {
      LOGGER
          .info("The overflow processor {} ends flushing {}.", getProcessorName(), displayMessage);
      long flushEndTime = System.currentTimeMillis();
      LOGGER.info(
          "The overflow processor {} flush {}, start time is {}, flush end time is {}," +
              " time consumption is {}ms",
          getProcessorName(), displayMessage,
          DatetimeUtils.convertMillsecondToZonedDateTime(flushStartTime),
          DatetimeUtils.convertMillsecondToZonedDateTime(flushEndTime),
          flushEndTime - flushStartTime);
    }
    return result;
  }

  @Override
  public synchronized Future<Boolean> flush() throws IOException {
    // statistic information for flush
    if (lastFlushTime > 0) {
      long thisFLushTime = System.currentTimeMillis();
      ZonedDateTime lastDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(lastFlushTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      ZonedDateTime thisDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(thisFLushTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      LOGGER.info(
          "The overflow processor {} last flush time is {}, this flush time is {},"
              + " flush time interval is {}s",
          getProcessorName(), lastDateTime, thisDateTime,
          (thisFLushTime - lastFlushTime) / 1000);
    }
    lastFlushTime = System.currentTimeMillis();
    // value count
    if (valueCount > 0) {
      try {
        flushFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Encounter an interrupt error when waitting for the flushing, "
                + "the bufferwrite processor is {}.",
            getProcessorName(), e);
        Thread.currentThread().interrupt();
      }
      try {
        // backup newIntervalFile list and emptyIntervalFileNode
        overflowFlushAction.act();
      } catch (Exception e) {
        LOGGER.error("Flush the overflow rowGroup to file faied, when overflowFlushAction act");
        throw new IOException(e);
      }

      if (IoTDBDescriptor.getInstance().getConfig().enableWal) {
        try {
          logNode.notifyStartFlush();
        } catch (IOException e) {
          LOGGER.error("Overflow processor {} encountered an error when notifying log node, {}",
              getProcessorName(), e);
        }
      }
      BasicMemController.getInstance().reportFree(this, memSize.get());
      memSize.set(0);
      valueCount = 0;
      // switch from work to flush
      switchWorkToFlush();
      flushFuture = FlushManager.getInstance().submit( () ->
          flushTask("asynchronously"));
    } else {
      flushFuture = new ImmediateFuture(true);
    }
    return flushFuture;

  }

  @Override
  public void close() throws OverflowProcessorException {
    LOGGER.info("The overflow processor {} starts close operation.", getProcessorName());
    long closeStartTime = System.currentTimeMillis();
    // flush data
    try {
      flush().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Encounter an interrupt error when waitting for the flushing, "
              + "the bufferwrite processor is {}.",
          getProcessorName(), e);
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      throw new OverflowProcessorException(e);
    }
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("The overflow processor {} ends close operation.", getProcessorName());
      // log close time
      long closeEndTime = System.currentTimeMillis();
      LOGGER.info(
          "The close operation of overflow processor {} starts at {} and ends at {}."
              + " It comsumes {}ms.",
          getProcessorName(), DatetimeUtils.convertMillsecondToZonedDateTime(closeStartTime),
          DatetimeUtils.convertMillsecondToZonedDateTime(closeEndTime),
          closeEndTime - closeStartTime);
    }
  }

  public void clear() throws IOException {
    if (workResource != null) {
      workResource.close();
    }
    if (mergeResource != null) {
      mergeResource.close();
    }
  }

  @Override
  public boolean canBeClosed() {
    // TODO: consider merge
    return !isMerge;
  }

  @Override
  public long memoryUsage() {
    return memSize.get();
  }

  public String getOverflowRestoreFile() {
    return workResource.getPositionFilePath();
  }

  /**
   * @return The sum of all timeseries's metadata size within this file.
   */
  public long getMetaSize() {
    // TODO : [MemControl] implement this
    return 0;
  }

  /**
   * @return The size of overflow file corresponding to this processor.
   */
  public long getFileSize() {
    return workResource.getInsertFile().length() + memoryUsage();
  }

  /**
   * Check whether current overflow file contains too many metadata or size of current overflow file
   * is too large If true, close current file and open a new one.
   */
  private boolean checkSize() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long metaSize = getMetaSize();
    long fileSize = getFileSize();
    LOGGER.info(
        "The overflow processor {}, the size of metadata reaches {},"
            + " the size of file reaches {}.",
        getProcessorName(), MemUtils.bytesCntToStr(metaSize), MemUtils.bytesCntToStr(fileSize));
    if (metaSize >= config.overflowMetaSizeThreshold
        || fileSize >= config.overflowFileSizeThreshold) {
      LOGGER.info(
          "The overflow processor {}, size({}) of the file {} reaches threshold {},"
              + " size({}) of metadata reaches threshold {}.",
          getProcessorName(), MemUtils.bytesCntToStr(fileSize), workResource.getInsertFilePath(),
          MemUtils.bytesCntToStr(config.overflowMetaSizeThreshold),
          MemUtils.bytesCntToStr(metaSize),
          MemUtils.bytesCntToStr(config.overflowMetaSizeThreshold));
      return true;
    } else {
      return false;
    }
  }

  public WriteLogNode getLogNode() {
    return logNode;
  }

  public OverflowResource getWorkResource() {
    return workResource;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    OverflowProcessor that = (OverflowProcessor) o;
    return isMerge == that.isMerge &&
            valueCount == that.valueCount &&
            lastFlushTime == that.lastFlushTime &&
            memThreshold == that.memThreshold &&
            Objects.equals(workResource, that.workResource) &&
            Objects.equals(mergeResource, that.mergeResource) &&
            Objects.equals(workSupport, that.workSupport) &&
            Objects.equals(flushSupport, that.flushSupport) &&
            Objects.equals(flushFuture, that.flushFuture) &&
            Objects.equals(parentPath, that.parentPath) &&
            Objects.equals(dataPathCount, that.dataPathCount) &&
            Objects.equals(queryFlushLock, that.queryFlushLock) &&
            Objects.equals(overflowFlushAction, that.overflowFlushAction) &&
            Objects.equals(filenodeFlushAction, that.filenodeFlushAction) &&
            Objects.equals(fileSchema, that.fileSchema) &&
            Objects.equals(memSize, that.memSize) &&
            Objects.equals(logNode, that.logNode) &&
            Objects.equals(flushFuture, that.flushFuture);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), workResource, mergeResource, workSupport,
            flushSupport, flushFuture, isMerge, valueCount, parentPath, lastFlushTime,
            dataPathCount, queryFlushLock, overflowFlushAction, filenodeFlushAction, fileSchema,
            memThreshold, memSize, logNode, flushFuture);
  }

  /**
   * used for test. We can block to wait for finishing flushing.
   * @return the future of the flush() task.
   */
  public Future<Boolean> getFlushFuture() {
    return flushFuture;
  }

  /**
   * used for test. We can know when the flush() is called.
   * @return the last flush() time.
   */
  public long getLastFlushTime() {
    return lastFlushTime;
  }
}