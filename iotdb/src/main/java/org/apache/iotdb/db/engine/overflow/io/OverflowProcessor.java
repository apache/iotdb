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

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

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
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController.UsageLevel;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushCallBack;
import org.apache.iotdb.db.engine.memtable.MemTablePool;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.pool.FlushPoolManager;
import org.apache.iotdb.db.engine.querycontext.MergeSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.OverflowProcessorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.monitor.collector.MemTableWriteTimeCost;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.db.writelog.recover.LogReplayer;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverflowProcessor extends Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
  private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.getInstance().getConfig();
  private OverflowResource workResource;
  private OverflowResource mergeResource;

  private List<IMemTable> overflowFlushMemTables = new ArrayList<>();
  private IMemTable workSupport;
//  private OverflowMemtable flushSupport;
  private long flushId = -1;
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

  private VersionController versionController;

  private boolean isClosed = true;
  private boolean isFlush = false;

  public OverflowProcessor(String processorName, Map<String, Action> parameters,
      FileSchema fileSchema, VersionController versionController)
      throws ProcessorException {
    super(processorName);
    this.fileSchema = fileSchema;
    this.versionController = versionController;
    String overflowDirPath = TsFileDBConf.getOverflowDataDir();
    if (overflowDirPath.length() > 0
        && overflowDirPath.charAt(overflowDirPath.length() - 1) != File.separatorChar) {
      overflowDirPath = overflowDirPath + File.separatorChar;
    }
    this.parentPath = overflowDirPath + processorName;

    overflowFlushAction = parameters.get(FileNodeConstants.OVERFLOW_FLUSH_ACTION);
    filenodeFlushAction = parameters
        .get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
    reopen();
    try {
      getLogNode();
    } catch (IOException e) {
      throw new ProcessorException(e);
    }
  }

  public void reopen() throws ProcessorException {
    if (!isClosed) {
      return;
    }
    // recover file
    File processorDataDir = new File(parentPath);
    if (!processorDataDir.exists()) {
      processorDataDir.mkdirs();
    }
    recover(processorDataDir);

    // memory
    if (workSupport == null) {
      workSupport = new PrimitiveMemTable();
    } else {
      workSupport.clear();
    }
    isClosed = false;
    isFlush = false;
  }

  public void checkOpen() throws OverflowProcessorException {
    if (isClosed) {
      throw new OverflowProcessorException("OverflowProcessor already closed");
    }
  }


  private void recover(File parentFile) throws ProcessorException {
    String[] subFilePaths = clearFile(parentFile.list());

    try {
      if (subFilePaths.length == 0) {
        workResource = new OverflowResource(parentPath,
            String.valueOf(dataPathCount.getAndIncrement()), versionController, processorName);
      } else if (subFilePaths.length == 1) {
        long count = Long.parseLong(subFilePaths[0]);
        dataPathCount.addAndGet(count + 1);
        workResource = new OverflowResource(parentPath, String.valueOf(count), versionController,
            processorName);
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
        workResource = new OverflowResource(parentPath, String.valueOf(count2), versionController,
            processorName);
        mergeResource = new OverflowResource(parentPath, String.valueOf(count1), versionController,
            processorName);
        LOGGER.info("The overflow processor {} recover from merge status.", getProcessorName());
      }
    } catch (IOException e) {
      throw new ProcessorException(e);
    }

    IMemTable memTable = new PrimitiveMemTable();
    LogReplayer replayer = new LogReplayer(processorName, workResource.getInsertFilePath(),
        workResource.getModificationFile(), versionController, null, fileSchema,
        memTable);
    replayer.replayLogs();
    flushTask("recover asyncFlush", memTable, 0, (a,b) -> {});
    try {
      WriteLogNode node = MultiFileLogNodeManager.getInstance().getNode(
          workResource.logNodePrefix() + IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX);
      node.delete();
    } catch (IOException e) {
      throw new ProcessorException(e);
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
    MemTableWriteTimeCost.getInstance().init();
    try {
      checkOpen();
    } catch (OverflowProcessorException e) {
      throw new IOException(e);
    }
    // memory control
    long memUage = MemUtils.getRecordSize(tsRecord);
    UsageLevel usageLevel = BasicMemController.getInstance().acquireUsage(this, memUage);
    switch (usageLevel) {
      case SAFE:
        // write data
        workSupport.insert(tsRecord);
        valueCount++;
        // check asyncFlush
        memUage = memSize.addAndGet(memUage);
        if (memUage > memThreshold) {
          if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("The usage of memory {} in overflow processor {} reaches the threshold {}",
                MemUtils.bytesCntToStr(memUage), getProcessorName(),
                MemUtils.bytesCntToStr(memThreshold));
          }
          flush();
        }
        break;
      case WARNING:
        // write data
        workSupport.insert(tsRecord);
        valueCount++;
        // asyncFlush
        memSize.addAndGet(memUage);
        flush();
        break;
      case DANGEROUS:
        throw new IOException("The insertion is rejected because dangerous memory level hit");
    }


  }

  /**
   * @deprecated update one time-series data which time range is from startTime from endTime.
   */
  @Deprecated
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, byte[] value) {
//    workSupport.update(deviceId, measurementId, startTime, endTime, type, value);
//    valueCount++;
    throw new UnsupportedOperationException("update has been deprecated");
  }

  /**
   * @deprecated this function need to be re-implemented.
   */
  @Deprecated
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, String value) {
//    workSupport.update(deviceId, measurementId, startTime, endTime, type,
//        convertStringToBytes(type, value));
//    valueCount++;
    throw new UnsupportedOperationException("update has been deprecated");
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
    try {
      checkOpen();
    } catch (OverflowProcessorException e) {
      throw new IOException(e);
    }
    workResource.delete(deviceId, measurementId, timestamp, version, updatedModFiles);
    workSupport.delete(deviceId, measurementId, timestamp);
    if (isFlush()) {
      mergeResource.delete(deviceId, measurementId, timestamp, version, updatedModFiles);
      for (IMemTable memTable : overflowFlushMemTables) {
        if (memTable.containSeries(deviceId, measurementId)) {
          memTable.delete(new Deletion(deviceId + PATH_SEPARATOR + measurementId, 0, timestamp));
        }
      }
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
    try {
      checkOpen();
    } catch (OverflowProcessorException e) {
      throw new IOException(e);
    }
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
   * query insert data in memory table. while flushing, merge the work memory table with asyncFlush
   * memory table.
   *
   * @return insert data in SeriesChunkInMemTable
   */
  private ReadOnlyMemChunk queryOverflowInsertInMemory(String deviceId, String measurementId,
      TSDataType dataType, Map<String, String> props) {

    MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
    queryFlushLock.lock();
    try {
      if (!overflowFlushMemTables.isEmpty() && isFlush()) {
        for (int i = overflowFlushMemTables.size() - 1; i >= 0; i--) {
          memSeriesLazyMerger.addMemSeries(
              overflowFlushMemTables.get(i).query(deviceId, measurementId, dataType, props));
        }
      }
      memSeriesLazyMerger
          .addMemSeries(workSupport.query(deviceId, measurementId, dataType, props));
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


  private void switchFlushToWork() {
    LOGGER.info("Overflow Processor {} try to get flushQueryLock for switchFlushToWork", getProcessorName());
    queryFlushLock.lock();
    LOGGER.info("Overflow Processor {} get flushQueryLock for switchFlushToWork", getProcessorName());
    try {
//      flushSupport.clear();
      workResource.appendMetadatas();
      isFlush = false;
    } finally {
      queryFlushLock.unlock();
    }
  }

  public void switchWorkToMerge() throws IOException {
    if (mergeResource == null) {
      mergeResource = workResource;
      workResource = new OverflowResource(parentPath,
          String.valueOf(dataPathCount.getAndIncrement()), versionController, processorName);
    }
    isMerge = true;
    LOGGER.info("The overflow processor {} switch from WORK to MERGE", getProcessorName());
  }

  public void switchMergeToWork() throws IOException {
    if (mergeResource != null) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(mergeResource.getInsertFilePath());
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
    return isFlush;
  }

  private void removeFlushedMemTable(IMemTable memTable, TsFileIOWriter overflowIOWriter) {
    this.writeLock();
    //TODO check this implementation in BufferWriteProcessor
    try {
      overflowFlushMemTables.remove(memTable);
    } finally {
      this.writeUnlock();
    }
  }

  private boolean flushTask(String displayMessage, IMemTable currentMemTableToFlush,
      long flushId, MemTableFlushCallBack removeFlushedMemTable) {
    boolean result;
    long flushStartTime = System.currentTimeMillis();
    try {
      LOGGER.info("The overflow processor {} starts flushing {}.", getProcessorName(),
          displayMessage);
      // asyncFlush data
      workResource
          .flush(fileSchema, currentMemTableToFlush, getProcessorName(), flushId, removeFlushedMemTable);
      filenodeFlushAction.act();
      // write-ahead log
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        getLogNode().notifyEndFlush();
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
      // switch from asyncFlush to work.
      switchFlushToWork();
    }
    // log asyncFlush time
    if (LOGGER.isInfoEnabled()) {
      LOGGER
          .info("The overflow processor {} ends flushing {}.", getProcessorName(), displayMessage);
      long flushEndTime = System.currentTimeMillis();
      LOGGER.info(
          "The overflow processor {} asyncFlush {}, start time is {}, asyncFlush end time is {}," +
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
    // statistic information for asyncFlush
    if (lastFlushTime > 0 && LOGGER.isInfoEnabled()) {
      long thisFLushTime = System.currentTimeMillis();
      ZonedDateTime lastDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(lastFlushTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      ZonedDateTime thisDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(thisFLushTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      LOGGER.info(
          "The overflow processor {} last asyncFlush time is {}, this asyncFlush time is {},"
              + " asyncFlush time interval is {}s",
          getProcessorName(), lastDateTime, thisDateTime,
          (thisFLushTime - lastFlushTime) / 1000);
    }
    lastFlushTime = System.currentTimeMillis();
//    try {
//      flushFuture.get();
//    } catch (InterruptedException | ExecutionException e) {
//      throw new IOException(e);
//    }
    if (valueCount > 0) {
      try {
        // backup newIntervalFile list and emptyIntervalFileNode
        overflowFlushAction.act();
      } catch (Exception e) {
        LOGGER.error("Flush the overflow rowGroup to file faied, when overflowFlushAction act");
        throw new IOException(e);
      }
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        try {
          getLogNode().notifyStartFlush();
        } catch (IOException e) {
          LOGGER.error("Overflow processor {} encountered an error when notifying log node, {}",
              getProcessorName(), e);
        }
      }
      BasicMemController.getInstance().releaseUsage(this, memSize.get());
      memSize.set(0);
      valueCount = 0;

//      long version = versionController.nextVersion();
      //add mmd
      overflowFlushMemTables.add(workSupport);
      IMemTable tmpMemTableToFlush = workSupport;
      workSupport = MemTablePool.getInstance().getEmptyMemTable(this);
      flushId++;
      flushFuture = FlushPoolManager.getInstance().submit(() -> flushTask("asynchronously",
          tmpMemTableToFlush, flushId, this::removeFlushedMemTable));

      // switch from work to asyncFlush
//      switchWorkToFlush();
//      flushFuture = FlushPoolManager.getInstance().submit(() ->
//          flushTask("asynchronously", walTaskId));
    } else {
//      flushFuture = new ImmediateFuture(true);
      LOGGER.info("Nothing data points to be flushed");
    }
    return flushFuture;

  }

  @Override
  public void close() throws OverflowProcessorException {
    if (isClosed) {
      return;
    }
    LOGGER.info("The overflow processor {} starts close operation.", getProcessorName());
    long closeStartTime = System.currentTimeMillis();
    // asyncFlush data
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
    try {
      clear();
    } catch (IOException e) {
      throw new OverflowProcessorException(e);
    }
    isClosed = true;
  }

  public void clear() throws IOException {
    if (workResource != null) {
      workResource.close();
      workResource = null;
    }
    if (mergeResource != null) {
      mergeResource.close();
      mergeResource = null;
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
    if (metaSize >= config.getOverflowMetaSizeThreshold()
        || fileSize >= config.getOverflowFileSizeThreshold()) {
      LOGGER.info(
          "The overflow processor {}, size({}) of the file {} reaches threshold {},"
              + " size({}) of metadata reaches threshold {}.",
          getProcessorName(), MemUtils.bytesCntToStr(fileSize), workResource.getInsertFilePath(),
          MemUtils.bytesCntToStr(config.getOverflowMetaSizeThreshold()),
          MemUtils.bytesCntToStr(metaSize),
          MemUtils.bytesCntToStr(config.getOverflowMetaSizeThreshold()));
      return true;
    } else {
      return false;
    }
  }

  public WriteLogNode getLogNode() throws IOException {
    return workResource.getLogNode();
  }

  public OverflowResource getWorkResource() {
    return workResource;
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }

  /**
   * used for test. We can block to wait for finishing flushing.
   *
   * @return the future of the asyncFlush() task.
   */
  public Future<Boolean> getFlushFuture() {
    return flushFuture;
  }

  /**
   * used for test. We can know when the asyncFlush() is called.
   *
   * @return the last asyncFlush() time.
   */
  public long getLastFlushTime() {
    return lastFlushTime;
  }

  @Override
  public String toString() {
    return "OverflowProcessor in " + parentPath;
  }

  public boolean isClosed() {
    return isClosed;
  }


//  private void switchWorkToFlush() {
//    queryFlushLock.lock();
//    try {
//      Pair<> workSupport;
//      workSupport = new OverflowMemtable();
//      if(isFlushing){
//        // isFlushing = true, indicating an AsyncFlushThread has been running, only add Current overflowInfo
//        // into List.
//
//
//      }else {
//        isFlushing = true;
////        flushFuture = FlushPoolManager.getInstance().submit(() ->
//            flushTask("asynchronously", walTaskId));
//      }
//    } finally {
//      queryFlushLock.unlock();
//    }
//  }

//  private List<Pair<OverflowMemtable, OverflowResource>> flushTaskList;
//
//  private class AsyncFlushThread implements Runnable {
//
//    @Override
//    public void run() {
//      Pair<OverflowMemtable, OverflowResource> flushInfo;
//      while (true) {
//        queryFlushLock.lock();
//        try {
//          if (flushTaskList.isEmpty()) {
//            // flushTaskList is empty, thus all asyncFlush tasks have done and switch
//            OverflowMemtable temp = flushSupport == null ? new OverflowMemtable() : flushSupport;
//            flushSupport = workSupport;
//            workSupport = temp;
//            isFlushing = true;
//            break;
//          }
//          flushInfo = flushTaskList.remove(0);
//        } finally {
//          queryFlushLock.unlock();
//        }
//        asyncFlush(flushInfo);
//      }
//    }
//  }
}