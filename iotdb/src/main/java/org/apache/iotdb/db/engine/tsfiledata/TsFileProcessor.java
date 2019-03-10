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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.engine.bufferwrite.RestorableTsFileIOWriter;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushUtil;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each storage group has a TsFileProcessor instance. Though there are many clients read/write data
 * by accessing TsFileProcessor, they need to get the readWriteReentrantLock of this processor
 * first. Therefore, as for different clients, the class looks like thread safe.
 * <br/> The class has two backend tasks:
 *  (1) submit a flush job to flush data from memtable to disk async;
 *  (2) close a tsfile and open a new one.
 * users also need to get the write lock to call these two tasks.
 */
public class TsFileProcessor extends Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileProcessor.class);

  //this is just a part of fileSchemaRef: only the measurements that belong to this TsFileProcessor
  // are in this fileSchemaRef. And, this filed is shared with other classes (i.e., storage group
  // processor), so be careful if you modify it.
  private FileSchema fileSchemaRef;


  private volatile Future<Boolean> flushFuture = new ImmediateFuture<>(true);
  //when a flush task is in the backend, then it has not conflict with write operatons (because
  // write operation modifies wokMemtable, while flush task uses flushMemtable). However, the flush
  // task has concurrent problem with query operations, because the query needs to read data from
  // flushMemtable and the table may be clear if the flush operation ends. So, a Lock is needed.
  private ReentrantLock flushQueryLock = new ReentrantLock();
  private AtomicLong memSize = new AtomicLong();

  //fileNamePrefix (system time, rather than data time) time unit: nanosecond
  // this is used for generate the new TsFile name
  private long fileNamePrefix = System.nanoTime();
  //the times of calling insertion function (between two flush operations).
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

  private Map<String, Long> minWrittenTimeForEachDeviceInCurrentFile;
  private Map<String, Long> maxWrittenTimeForEachDeviceInCurrentFile;
  private Map<String, Long> lastFlushedTimeForEachDevice;

  private WriteLogNode logNode;
  private VersionController versionController;

  /**
   * constructor of BufferWriteProcessor. data will be stored in baseDir/processorName/ folder.
   *
   * @param processorName processor name
   * @param fileSchemaRef file schema
   * @throws BufferWriteProcessorException BufferWriteProcessorException
   */
  @SuppressWarnings({"squid:S2259", "squid:S3776"})
  public TsFileProcessor(String processorName,
      Action beforeFlushAction, Action afterFlushAction, Action afterCloseAction,
      VersionController versionController,
      FileSchema fileSchemaRef) throws BufferWriteProcessorException, IOException {
    super(processorName);
    this.fileSchemaRef = fileSchemaRef;
    this.processorName = processorName;

    this.beforeFlushAction = beforeFlushAction;
    this.afterCloseAction = afterCloseAction;
    this.afterFlushAction = afterFlushAction;
    workMemTable = new PrimitiveMemTable();
    tsFileResources = new ArrayList<>();
    inverseIndexOfResource = new HashMap<>();
    lastFlushedTimeForEachDevice = new HashMap<>();
    minWrittenTimeForEachDeviceInCurrentFile = new HashMap<>();
    maxWrittenTimeForEachDeviceInCurrentFile = new HashMap<>();
    File unclosedFile = null;
    String unclosedFileName = null;
    int unclosedFileCount = 0;
    for (String folderPath : Directories.getInstance().getAllTsFileFolders()) {
      File dataFolder = new File(folderPath, processorName);
      if (dataFolder.exists()) {
        // we do not add the unclosed tsfile into tsFileResources.
        File[] unclosedFiles = dataFolder
            .listFiles(x -> x.getName().contains(RestorableTsFileIOWriter.RESTORE_SUFFIX));
        unclosedFileCount += unclosedFiles.length;
        if (unclosedFileCount > 1) {
          break;
        } else if (unclosedFileCount == 1) {
          unclosedFileName = unclosedFiles[0].getName()
              .split(RestorableTsFileIOWriter.RESTORE_SUFFIX)[0];
          unclosedFile = new File(unclosedFiles[0].getParentFile(), unclosedFileName);
        }
        File[] datas = dataFolder
            .listFiles(x -> !x.getName().contains(RestorableTsFileIOWriter.RESTORE_SUFFIX) && x.getName().split(FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR).length == 2);
        Arrays.sort(datas, Comparator.comparingLong(x -> Long.parseLong(x.getName().split(FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR)[0])));
        for (File tsfile : datas) {
          //TODO we'd better define a file suffix for TsFile, e.g., .ts
          String[] names = tsfile.getName().split(FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR);
          long time = Long.parseLong(names[0]);
          if (fileNamePrefix < time) {
            fileNamePrefix = time;
          }
          if (unclosedFileCount == 0 || !tsfile.getName().equals(unclosedFileName)) {
            TsFileResource resource = new TsFileResource(tsfile, true);
            tsFileResources.add(resource);
            //maintain the inverse index and fileNamePrefix
            for (String device : resource.getDevices()) {
              inverseIndexOfResource.computeIfAbsent(device, k -> new ArrayList<>()).add(resource);
              lastFlushedTimeForEachDevice
                  .merge(device, resource.getEndTime(device), (x, y) -> x > y ? x : y);
            }
          }
        }
      } else {
        //processor folder does not exist
        dataFolder.mkdirs();
      }
    }
    if (unclosedFileCount > 1) {
      throw new BufferWriteProcessorException(String
          .format("TsProcessor %s has more than one unclosed TsFile. please repair it",
              processorName));
    } else if (unclosedFileCount == 0){
      unclosedFile = generateNewTsFilePath();
    }

    initCurrentTsFile(unclosedFile);

    this.versionController = versionController;
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


  private File generateNewTsFilePath() throws BufferWriteProcessorException {
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
    String fileName = (fileNamePrefix + 1) + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR
        + System.currentTimeMillis();
    return new File(dataFolder, fileName);
  }


  private void initCurrentTsFile(File file) throws BufferWriteProcessorException {
    this.insertFile = file;
    try {
      writer = new RestorableTsFileIOWriter(processorName, insertFile.getAbsolutePath());
      this.currentResource = new TsFileResource(insertFile, writer);
    } catch (IOException e) {
      throw new BufferWriteProcessorException(e);
    }

    minWrittenTimeForEachDeviceInCurrentFile.clear();
    maxWrittenTimeForEachDeviceInCurrentFile.clear();

  }

  /**
   * wrete a ts record into the memtable. If the memory usage is beyond the memThreshold, an async
   * flushing operation will be called.
   *
   * @param plan data to be written
   * @return true if the tsRecord can be inserted into tsFile. otherwise false (, then you need to
   * insert it into overflow)
   * @throws BufferWriteProcessorException if a flushing operation occurs and failed.
   */
  public boolean insert(InsertPlan plan) throws BufferWriteProcessorException, IOException {
    if (lastFlushedTimeForEachDevice.containsKey(plan.getDeviceId()) && plan.getTime() <= lastFlushedTimeForEachDevice.get(plan.getDeviceId())) {
      return false;
    }
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      logNode.write(plan);
    }
    String deviceId = plan.getDeviceId();
    long time = plan.getTime();
    long memUsage = 0;
    TSDataType type;
    String measurement;
    for (int i=0; i < plan.getMeasurements().length; i++){
      measurement = plan.getMeasurements()[i];
      type = fileSchemaRef.getMeasurementDataType(measurement);
      workMemTable.write(deviceId, measurement, type, time, plan.getValues()[i]);
      memUsage += MemUtils.getPointSize(type, measurement);
    }
    if (!minWrittenTimeForEachDeviceInCurrentFile.containsKey(deviceId)) {
      minWrittenTimeForEachDeviceInCurrentFile.put(deviceId, time);
    }
    if (!maxWrittenTimeForEachDeviceInCurrentFile.containsKey(deviceId) || maxWrittenTimeForEachDeviceInCurrentFile
        .get(deviceId) < time) {
      maxWrittenTimeForEachDeviceInCurrentFile.put(deviceId, time);
    }
    valueCount++;
    BasicMemController.getInstance().reportUse(this, memUsage);
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
    if (maxWrittenTimeForEachDeviceInCurrentFile.containsKey(deviceId) && maxWrittenTimeForEachDeviceInCurrentFile
        .get(deviceId) < timestamp) {
      maxWrittenTimeForEachDeviceInCurrentFile
          .put(deviceId, lastFlushedTimeForEachDevice.getOrDefault(deviceId, 0L));
    }
    boolean deleteFlushTable = false;
    if (isFlush()) {
      // flushing MemTable cannot be directly modified since another thread is reading it
      flushMemTable = flushMemTable.copy();
      deleteFlushTable = flushMemTable.delete(deviceId, measurementId, timestamp);
    }
    String fullPath = deviceId +
        IoTDBConstant.PATH_SEPARATOR + measurementId;
    Deletion deletion = new Deletion(fullPath, versionController.nextVersion(), timestamp);
    if (deleteFlushTable || (currentResource.containsDevice(deviceId) && currentResource.getStartTime(deviceId) <= timestamp)) {
      currentResource.getModFile().write(deletion);
    }
    for (TsFileResource resource : tsFileResources) {
      if (resource.containsDevice(deviceId) && resource.getStartTime(deviceId) <= timestamp) {
        resource.getModFile().write(deletion);
      }
    }
    if (lastFlushedTimeForEachDevice.containsKey(deviceId) && lastFlushedTimeForEachDevice.get(deviceId) <= timestamp) {
      lastFlushedTimeForEachDevice.put(deviceId, 0L);
    }
  }


  private void checkMemThreshold4Flush(long addedMemory) throws BufferWriteProcessorException {
    long newMem = memSize.addAndGet(addedMemory);
    if (newMem > TSFileConfig.groupSizeInByte) {
      if (LOGGER.isInfoEnabled()) {
        String usageMem = MemUtils.bytesCntToStr(newMem);
        String threshold = MemUtils.bytesCntToStr(TSFileConfig.groupSizeInByte);
        LOGGER.info("The usage of memory {} in bufferwrite processor {} reaches the threshold {}",
            usageMem, processorName, threshold);
      }
      try {
        flush();
      } catch (IOException e) {
        LOGGER.error("Flush bufferwrite error.", e);
        throw new BufferWriteProcessorException(e);
      }
    }
  }


  /**
   * this method is for preparing a task task and then submitting it.
   * @return
   * @throws IOException
   */
  @Override
  public Future<Boolean> flush() throws IOException {
    // waiting for the end of last flush operation.
    try {
      flushFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(
          "Encounter an interrupt error when waitting for the flushing, the TsFile Processor is {}.",
          getProcessorName(), e);
      Thread.currentThread().interrupt();
    }
    // statistic information for flush
    if (valueCount <= 0) {
      LOGGER.debug(
          "TsFile Processor {} has zero data to be flushed, will return directly.", processorName);
      flushFuture = new ImmediateFuture<>(true);
      return flushFuture;
    }

    if (LOGGER.isInfoEnabled()) {
      long thisFlushTime = System.currentTimeMillis();
      LOGGER.info(
          "The TsFile Processor {}: last flush time is {}, this flush time is {}, "
              + "flush time interval is {} s", getProcessorName(),
          DatetimeUtils.convertMillsecondToZonedDateTime(fileNamePrefix / 1000),
          DatetimeUtils.convertMillsecondToZonedDateTime(thisFlushTime),
          (thisFlushTime - fileNamePrefix / 1000) / 1000);
    }
    fileNamePrefix = System.nanoTime();

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
    flushFuture = FlushManager.getInstance().submit(() -> flushTask(version));
    return flushFuture;
  }

  /**
   * this method will be concurrent with other methods..
   *
   * @param version the operation version that will tagged on the to be flushed memtable (i.e.,
   * ChunkGroup)
   * @return true if successfully.
   */
  private boolean flushTask(long version) {
    boolean result;
    long flushStartTime = System.currentTimeMillis();
    LOGGER.info("The TsFile Processor {} starts flushing.", processorName);
    try {
      if (flushMemTable != null && !flushMemTable.isEmpty()) {
        // flush data
        MemTableFlushUtil.flushMemTable(fileSchemaRef, writer, flushMemTable,
            version);
        // write restore information
        writer.flush();
      } else {
        //no need to flush.
        return true;
      }

      afterFlushAction.act();
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        logNode.notifyEndFlush(null);
      }
      result = true;
    } catch (Exception e) {
      LOGGER.error(
          "The TsFile Processor {} failed to flush, when calling the afterFlushAction（filenodeFlushAction）.",
          processorName, e);
      result = false;
    } finally {
      try {
        switchFlushToWork();
      } catch (BufferWriteProcessorException e) {
        LOGGER.error(e.getMessage());
        result = false;
      }
      LOGGER.info("The TsFile Processor {} ends flushing.", processorName);
    }
    if (LOGGER.isInfoEnabled()) {
      long flushEndTime = System.currentTimeMillis();
      LOGGER.info(
          "The TsFile Processor {} flush, start time is {}, flush end time is {}, "
              + "flush time consumption is {}ms",
          processorName,
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
        for (String device : flushMemTable.getMemTableMap().keySet()) {
          lastFlushedTimeForEachDevice.put(device, maxWrittenTimeForEachDeviceInCurrentFile.get(device));
        }
        workMemTable = new PrimitiveMemTable();
      }
    } finally {
      flushQueryLock.unlock();
    }
  }

  private void switchFlushToWork() throws BufferWriteProcessorException {
    flushQueryLock.lock();
    try {
      //we update the index of currentTsResource.
      for (String device : flushMemTable.getMemTableMap().keySet()) {
        currentResource.setStartTime(device, minWrittenTimeForEachDeviceInCurrentFile.get(device));
        // new end time must be larger than the old one.
        currentResource.setEndTime(device, lastFlushedTimeForEachDevice.get(device));
      }
      flushMemTable.clear();
      flushMemTable = null;
      //make chunk groups in this flush task visble
      writer.appendMetadata();
      if (needCloseCurrentFile()) {
        closeCurrentTsFileAndOpenNewOne();
      }
    } finally {
      flushQueryLock.unlock();
    }
  }

  @Override
  public boolean canBeClosed() {
    return true;
  }


  /**
   * this method do not call flush() to flush data in memory to disk.
   * @throws BufferWriteProcessorException
   */
  private void closeCurrentTsFileAndOpenNewOne() throws BufferWriteProcessorException {
    closeCurrentFile();
    initCurrentTsFile(generateNewTsFilePath());
  }

  //very dangerous, how to make sure this function is thread safe (no other functions are running)
  private void closeCurrentFile() throws BufferWriteProcessorException {
    try {
      long closeStartTime = System.currentTimeMillis();
      // end file
      if (writer.getChunkGroupMetaDatas().isEmpty()){
        //this is an empty TsFile, we do not need to save it...
        writer.endFile(fileSchemaRef);
        Files.delete(Paths.get(insertFile.getAbsolutePath()));
      } else {
        writer.endFile(fileSchemaRef);
        // update the IntervalFile for interval list
        afterCloseAction.act();
        // flush the changed information for filenode
        afterFlushAction.act();

        tsFileResources.add(currentResource);
        //maintain the inverse index
        for (String device : currentResource.getDevices()) {
          inverseIndexOfResource.computeIfAbsent(device, k -> new ArrayList<>())
              .add(currentResource);
        }
      }

      // delete the restore for this bufferwrite processor
      if (LOGGER.isInfoEnabled()) {
        long closeEndTime = System.currentTimeMillis();
        LOGGER.info(
            "Close current TsFile {}, start time is {}, end time is {}, time consumption is {}ms",
            insertFile.getAbsolutePath(),
            DatetimeUtils.convertMillsecondToZonedDateTime(closeStartTime),
            DatetimeUtils.convertMillsecondToZonedDateTime(closeEndTime),
            closeEndTime - closeStartTime);
      }

      workMemTable.clear();
      if (flushMemTable != null) {
        flushMemTable.clear();
      }
      if (logNode != null) {
        logNode.close();
      }

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
    return flushMemTable != null;
  }


  /**
   * query data.
   */
  public GlobalSortedSeriesDataSource query(String deviceId, String measurementId,
      QueryContext context) throws IOException {
    MeasurementSchema mSchema;
    TSDataType dataType;

    mSchema = fileSchemaRef.getMeasurementSchema(measurementId);
    dataType = mSchema.getType();

    // tsfile dataØØ
    List<TsFileResource> dataFiles = new ArrayList<>();
    for (TsFileResource tsfile : tsFileResources) {
      //TODO in the old version, tsfile is deep copied. I do not know why
      dataFiles.add(tsfile);
    }
    // bufferwrite data
    //TODO unsealedTsFile class is a little redundant.
    UnsealedTsFile unsealedTsFile = null;

    if (currentResource.getStartTime(deviceId) >= 0) {
      unsealedTsFile = new UnsealedTsFile();
      unsealedTsFile.setFilePath(currentResource.getFile().getAbsolutePath());
      List<ChunkMetaData> chunks = writer.getMetadatas(deviceId, measurementId, dataType);
        List<Modification> pathModifications = context.getPathModifications(
            currentResource.getModFile(), deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId);
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(chunks, pathModifications);
        }

      unsealedTsFile.setTimeSeriesChunkMetaDatas(chunks);
    }
    return new GlobalSortedSeriesDataSource(
        new Path(deviceId + "." + measurementId), dataFiles, unsealedTsFile,
        queryDataInMemtable(deviceId, measurementId, dataType, mSchema.getProps()));
  }

  /**
   * get the one (or two) chunk(s) in the memtable ( and the other one in flushing status and then
   * compact them into one TimeValuePairSorter). Then get its (or their) ChunkMetadata(s).
   *
   * @param deviceId device id
   * @param measurementId sensor id
   * @param dataType data type
   * @return corresponding chunk data in memory
   */
  private ReadOnlyMemChunk queryDataInMemtable(String deviceId,
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
      return new ReadOnlyMemChunk(dataType, memSeriesLazyMerger, Collections.emptyMap());
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
   *
   * @return the last flush() time. Time unit: nanosecond.
   */
  public long getFileNamePrefix() {
    return fileNamePrefix;
  }

  /**
   * used for test. We can block to wait for finishing flushing.
   *
   * @return the future of the flush() task.
   */
  public Future<Boolean> getFlushFuture() {
    return flushFuture;
  }

  @Override
  public void close() throws BufferWriteProcessorException {
    closeCurrentFile();
    try {
      if (currentResource != null) {
        currentResource.close();
      }
      for (TsFileResource resource : tsFileResources) {
        resource.close();
      }

    } catch (IOException e) {
      throw new BufferWriteProcessorException(e);
    }
  }

  /**
   * remove all data of this processor. Used For UT
   */
  public void removeMe() throws BufferWriteProcessorException, IOException {
    close();
    for (String folder : Directories.getInstance().getAllTsFileFolders()) {
      File dataFolder = new File(folder, processorName);
      if (dataFolder.exists()) {
        for (File file: dataFolder.listFiles()) {
          Files.deleteIfExists(Paths.get(file.getAbsolutePath()));
        }
      }
    }
  }

  /**
   * Check if the currentTsFileResource is toooo large.
   * @return  true if the file is too large.
   */
  private boolean needCloseCurrentFile() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long fileSize = currentResource.getFile().length();
    if (fileSize >= config.getBufferwriteFileSizeThreshold()) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "The bufferwrite processor {}, size({}) of the file {} reaches threshold {}.",
            processorName, MemUtils.bytesCntToStr(fileSize), currentResource.getFilePath(),
            MemUtils.bytesCntToStr(config.getBufferwriteFileSizeThreshold()));
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TsFileProcessor)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TsFileProcessor that = (TsFileProcessor) o;
    return fileNamePrefix == that.fileNamePrefix &&
        valueCount == that.valueCount &&
        Objects.equals(fileSchemaRef, that.fileSchemaRef) &&
        Objects.equals(flushFuture, that.flushFuture) &&
        Objects.equals(flushQueryLock, that.flushQueryLock) &&
        Objects.equals(memSize, that.memSize) &&
        Objects.equals(workMemTable, that.workMemTable) &&
        Objects.equals(flushMemTable, that.flushMemTable) &&
        Objects.equals(writer, that.writer) &&
        Objects.equals(beforeFlushAction, that.beforeFlushAction) &&
        Objects.equals(afterCloseAction, that.afterCloseAction) &&
        Objects.equals(afterFlushAction, that.afterFlushAction) &&
        Objects.equals(insertFile, that.insertFile) &&
        Objects.equals(currentResource, that.currentResource) &&
        Objects.equals(tsFileResources, that.tsFileResources) &&
        Objects.equals(inverseIndexOfResource, that.inverseIndexOfResource) &&
        Objects.equals(lastFlushedTimeForEachDevice, that.lastFlushedTimeForEachDevice) &&
        Objects.equals(logNode, that.logNode) &&
        Objects.equals(versionController, that.versionController);
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(super.hashCode(), fileSchemaRef, flushFuture, flushQueryLock, memSize, fileNamePrefix,
            valueCount, workMemTable, flushMemTable, writer, beforeFlushAction, afterCloseAction,
            afterFlushAction, insertFile, currentResource, tsFileResources, inverseIndexOfResource,
            lastFlushedTimeForEachDevice, logNode, versionController);
  }
}
