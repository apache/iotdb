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
package org.apache.iotdb.db.engine.filenode;

import static java.time.ZonedDateTime.ofInstant;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.ActionException;
import org.apache.iotdb.db.engine.bufferwrite.BufferWriteProcessor;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.overflow.io.OverflowProcessor;
import org.apache.iotdb.db.engine.pool.MergePoolManager;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.exception.ErrorDebugException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.OverflowProcessorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.sync.conf.Constans;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeProcessor extends Processor implements IStatistic {

  private static final String WARN_NO_SUCH_OVERFLOWED_FILE = "Can not find any tsfile which"
      + " will be overflowed in the filenode processor {}, ";
  public static final String RESTORE_FILE_SUFFIX = ".restore";
  private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessor.class);
  private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.getInstance().getConfig();
  private static final MManager mManager = MManager.getInstance();
  private static final Directories directories = Directories.getInstance();
  private final String statStorageDeltaName;
  private final HashMap<String, AtomicLong> statParamsHashMap = new HashMap<>();
  /**
   * Used to keep the oldest timestamp for each deviceId. The key is deviceId.
   */
  private volatile boolean isOverflowed;
  private Map<String, Long> lastUpdateTimeMap;
  private Map<String, Long> flushLastUpdateTimeMap;
  private Map<String, List<TsFileResource>> invertedIndexOfFiles;
  private TsFileResource emptyTsFileResource;
//  private TsFileResourceV2 currentTsFileResource;
  private List<TsFileResource> newFileNodes;
  private FileNodeProcessorStatus isMerging;

  /**
   * this is used when work->merge operation
   */
  private int numOfMergeFile;
  private FileNodeProcessorStore fileNodeProcessorStore;
  private String fileNodeRestoreFilePath;
  private final Object fileNodeRestoreLock = new Object();

  /**
   * last merge time
   */
  private long lastMergeTime = -1;
  private BufferWriteProcessor bufferWriteProcessor = null;

  //the bufferwrite Processors that are closing. (Because they are not closed well,
  // their memtable are not released and we have to query data from them.
  //private ConcurrentSkipListSet<BufferWriteProcessor> closingBufferWriteProcessor = new ConcurrentSkipListSet<>();
  private CopyOnWriteLinkedList<BufferWriteProcessor> closingBufferWriteProcessor = new CopyOnWriteLinkedList<>();

  private OverflowProcessor overflowProcessor = null;
  private Set<Integer> oldMultiPassTokenSet = null;
  private Set<Integer> newMultiPassTokenSet = new HashSet<>();

  /**
   * Represent the number of old queries that have not ended.
   * This parameter only decreases but not increase.
   */
  private CountDownLatch oldMultiPassCount = null;

  /**
   * Represent the number of new queries that have not ended.
   */
  private AtomicInteger newMultiPassCount = new AtomicInteger(0);

  /**
   * statistic monitor parameters
   */
  private Map<String, Action> parameters;
  private FileSchema fileSchema;

  private Action fileNodeFlushAction = () -> {
    synchronized (fileNodeProcessorStore) {
      try {
        writeStoreToDisk(fileNodeProcessorStore);
      } catch (FileNodeProcessorException e) {
        throw new ActionException(e);
      }
    }
  };

  private Action bufferwriteFlushAction = () -> {
    // update the lastUpdateTime Notice: Thread safe
    synchronized (fileNodeProcessorStore) {
      // deep copy
      Map<String, Long> tempLastUpdateMap = new HashMap<>(lastUpdateTimeMap);
      // update flushLastUpdateTimeMap
      for (Entry<String, Long> entry : lastUpdateTimeMap.entrySet()) {
        flushLastUpdateTimeMap.put(entry.getKey(), entry.getValue() + 1);
      }
      fileNodeProcessorStore.setLastUpdateTimeMap(tempLastUpdateMap);
    }
  };

//  private Action bufferwriteCloseAction = new Action() {
//
//    @Override
//    public void act() {
//      synchronized (fileNodeProcessorStore) {
//        fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
//        addLastTimeToIntervalFile();
//        fileNodeProcessorStore.setNewFileNodes(newFileNodes);
//      }
//    }
//
//    private void addLastTimeToIntervalFile() {
//
//      if (!newFileNodes.isEmpty()) {
//        // end time with one start time
//        Map<String, Long> endTimeMap = new HashMap<>();
//        for (Entry<String, Long> startTime : currentTsFileResource.getStartTimeMap().entrySet()) {
//          String deviceId = startTime.getKey();
//          endTimeMap.put(deviceId, lastUpdateTimeMap.get(deviceId));
//        }
//        currentTsFileResource.setEndTimeMap(endTimeMap);
//      }
//    }
//  };

  private Consumer<BufferWriteProcessor> bufferwriteCloseConsumer = (bwProcessor) -> {
    synchronized (fileNodeProcessorStore) {
      fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);

      if (!newFileNodes.isEmpty()) {
        // end time with one start time
        Map<String, Long> endTimeMap = new HashMap<>();
        TsFileResource resource = bwProcessor.getCurrentTsFileResource();
        for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
          String deviceId = startTime.getKey();
          endTimeMap.put(deviceId, lastUpdateTimeMap.get(deviceId));
        }
        resource.setEndTimeMap(endTimeMap);
      }
      fileNodeProcessorStore.setNewFileNodes(newFileNodes);
    }
  };


  private Action overflowFlushAction = () -> {

    // update the new TsFileResourceV2 List and emptyIntervalFile.
    // Notice: thread safe
    synchronized (fileNodeProcessorStore) {
      fileNodeProcessorStore.setOverflowed(isOverflowed);
      fileNodeProcessorStore.setEmptyTsFileResource(emptyTsFileResource);
      fileNodeProcessorStore.setNewFileNodes(newFileNodes);
    }
  };
  // Token for query which used to
  private int multiPassLockToken = 0;
  private VersionController versionController;
  private ReentrantLock mergeDeleteLock = new ReentrantLock();

  /**
   * This is the modification file of the result of the current merge.
   */
  private ModificationFile mergingModification;

  private TsFileIOWriter mergeFileWriter = null;
  private String mergeOutputPath = null;
  private String mergeBaseDir = null;
  private String mergeFileName = null;
  private boolean mergeIsChunkGroupHasData = false;
  private long mergeStartPos;

  /**
   * constructor of FileNodeProcessor.
   */
  FileNodeProcessor(String fileNodeDirPath, String processorName)
      throws FileNodeProcessorException {
    super(processorName);
    for (MonitorConstants.FileNodeProcessorStatConstants statConstant :
        MonitorConstants.FileNodeProcessorStatConstants.values()) {
      statParamsHashMap.put(statConstant.name(), new AtomicLong(0));
    }
    statStorageDeltaName =
        MonitorConstants.STAT_STORAGE_GROUP_PREFIX + MonitorConstants.MONITOR_PATH_SEPARATOR
            + MonitorConstants.FILE_NODE_PATH + MonitorConstants.MONITOR_PATH_SEPARATOR
            + processorName.replaceAll("\\.", "_");

    this.parameters = new HashMap<>();
    String dirPath = fileNodeDirPath;
    if (dirPath.length() > 0
        && dirPath.charAt(dirPath.length() - 1) != File.separatorChar) {
      dirPath = dirPath + File.separatorChar;
    }

    File restoreFolder = new File(dirPath + processorName);
    if (!restoreFolder.exists()) {
      restoreFolder.mkdirs();
      LOGGER.info(
          "The restore directory of the filenode processor {} doesn't exist. Create new " +
              "directory {}",
          getProcessorName(), restoreFolder.getAbsolutePath());
    }
    fileNodeRestoreFilePath = new File(restoreFolder, processorName + RESTORE_FILE_SUFFIX)
        .getPath();
    try {
      fileNodeProcessorStore = readStoreFromDisk();
    } catch (FileNodeProcessorException e) {
      LOGGER.error(
          "The fileNode processor {} encountered an error when recoverying restore " +
              "information.", processorName);
      throw new FileNodeProcessorException(e);
    }
    // TODO deep clone the lastupdate time
    emptyTsFileResource = fileNodeProcessorStore.getEmptyTsFileResource();
    newFileNodes = fileNodeProcessorStore.getNewFileNodes();
    isMerging = fileNodeProcessorStore.getFileNodeProcessorStatus();
    numOfMergeFile = fileNodeProcessorStore.getNumOfMergeFile();
    invertedIndexOfFiles = new HashMap<>();

    // construct the fileschema
    try {
      this.fileSchema = constructFileSchema(processorName);
    } catch (WriteProcessException e) {
      throw new FileNodeProcessorException(e);
    }

    recover();

    // RegistStatService
    if (TsFileDBConf.isEnableStatMonitor()) {
      StatMonitor statMonitor = StatMonitor.getInstance();
      registerStatMetadata();
      statMonitor.registerStatistics(statStorageDeltaName, this);
    }
    try {
      versionController = new SimpleFileVersionController(restoreFolder.getPath());
    } catch (IOException e) {
      throw new FileNodeProcessorException(e);
    }
  }

  @Override
  public Map<String, AtomicLong> getStatParamsHashMap() {
    return statParamsHashMap;
  }

  @Override
  public void registerStatMetadata() {
    Map<String, String> hashMap = new HashMap<>();
    for (MonitorConstants.FileNodeProcessorStatConstants statConstant :
        MonitorConstants.FileNodeProcessorStatConstants.values()) {
      hashMap
          .put(statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPARATOR + statConstant.name(),
              MonitorConstants.DATA_TYPE_INT64);
    }
    StatMonitor.getInstance().registerStatStorageGroup(hashMap);
  }

  @Override
  public List<String> getAllPathForStatistic() {
    List<String> list = new ArrayList<>();
    for (MonitorConstants.FileNodeProcessorStatConstants statConstant :
        MonitorConstants.FileNodeProcessorStatConstants.values()) {
      list.add(
          statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPARATOR + statConstant.name());
    }
    return list;
  }

  @Override
  public Map<String, TSRecord> getAllStatisticsValue() {
    Long curTime = System.currentTimeMillis();
    HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
    TSRecord tsRecord = new TSRecord(curTime, statStorageDeltaName);

    Map<String, AtomicLong> hashMap = getStatParamsHashMap();
    tsRecord.dataPointList = new ArrayList<>();
    for (Map.Entry<String, AtomicLong> entry : hashMap.entrySet()) {
      tsRecord.dataPointList.add(new LongDataPoint(entry.getKey(), entry.getValue().get()));
    }

    tsRecordHashMap.put(statStorageDeltaName, tsRecord);
    return tsRecordHashMap;
  }

  /**
   * add interval FileNode.
   */
  void addIntervalFileNode(TsFileResource tsFileResource) throws ActionException {
    newFileNodes.add(tsFileResource);
    fileNodeProcessorStore.setNewFileNodes(newFileNodes);
    fileNodeFlushAction.act();
  }

  /**
   * set interval filenode start time.
   *
   * @param deviceId device ID
   */
  void setIntervalFileNodeStartTime(String deviceId) {
    if (getBufferWriteProcessor().getCurrentTsFileResource().getStartTime(deviceId) == -1) {
      getBufferWriteProcessor().getCurrentTsFileResource().setStartTime(deviceId,
          flushLastUpdateTimeMap.get(deviceId));
      if (!invertedIndexOfFiles.containsKey(deviceId)) {
        invertedIndexOfFiles.put(deviceId, new ArrayList<>());
      }
      invertedIndexOfFiles.get(deviceId).add(getBufferWriteProcessor().getCurrentTsFileResource());
    }
  }

  void setIntervalFileNodeStartTime(String deviceId, long time) {
    if (time != -1) {
      getBufferWriteProcessor().getCurrentTsFileResource().setStartTime(deviceId, time);
    } else {
      getBufferWriteProcessor().getCurrentTsFileResource().removeTime(deviceId);
      invertedIndexOfFiles.get(deviceId).remove(getBufferWriteProcessor().getCurrentTsFileResource());
    }
  }

  long getIntervalFileNodeStartTime(String deviceId) {
    return getBufferWriteProcessor().getCurrentTsFileResource().getStartTime(deviceId);
  }

  private void addAllFileIntoIndex(List<TsFileResource> fileList) {
    // clear map
    invertedIndexOfFiles.clear();
    // add all file to index
    for (TsFileResource fileNode : fileList) {
      if (fileNode.getStartTimeMap().isEmpty()) {
        continue;
      }
      for (String deviceId : fileNode.getStartTimeMap().keySet()) {
        if (!invertedIndexOfFiles.containsKey(deviceId)) {
          invertedIndexOfFiles.put(deviceId, new ArrayList<>());
        }
        invertedIndexOfFiles.get(deviceId).add(fileNode);
      }
    }
  }

  public boolean isOverflowed() {
    return isOverflowed;
  }

  /**
   * if overflow insert, update and delete write into this filenode processor, set
   * <code>isOverflowed</code> to true.
   */
  public void setOverflowed(boolean isOverflowed) {
    if (this.isOverflowed != isOverflowed) {
      this.isOverflowed = isOverflowed;
    }
  }

  public FileNodeProcessorStatus getFileNodeProcessorStatus() {
    return isMerging;
  }

  /**
   * execute filenode recovery.
   */
  public void recover() throws FileNodeProcessorException {
    // restore sequential files
    parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
    //parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
    parameters
        .put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fileNodeFlushAction);
    parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
    parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fileNodeFlushAction);

    recoverUpdateTimeMap();

    for (int i = 0; i < newFileNodes.size(); i++) {
      TsFileResource tsFile = newFileNodes.get(i);
      String baseDir = directories
          .getTsFileFolder(tsFile.getBaseDirIndex());
      try {
        // recover in initialization
        new BufferWriteProcessor(baseDir,
            getProcessorName(),
            tsFile.getFile().getName(), parameters, bufferwriteCloseConsumer,
            versionController, fileSchema, tsFile);
      } catch (BufferWriteProcessorException e) {
        LOGGER.error(
            "The filenode processor {} failed to recover the bufferwrite processor, "
                + "the last bufferwrite file is {}.",
            getProcessorName(), tsFile.getFile().getName());
        throw new FileNodeProcessorException(e);
      }
    }
    recoverUpdateTimeMap();

    // restore the overflow processor
    LOGGER.info("The filenode processor {} will recover the overflow processor.",
        getProcessorName());

    try {
      overflowProcessor = new OverflowProcessor(getProcessorName(), parameters, fileSchema,
          versionController);
    } catch (ProcessorException e) {
      LOGGER.error("The filenode processor {} failed to recovery the overflow processor.",
          getProcessorName());
      throw new FileNodeProcessorException(e);
    }

    if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
      // re-merge all file
      // if bufferwrite processor is not null, and close
      LOGGER.info("The filenode processor {} is recovering, the filenode status is {}.",
          getProcessorName(), isMerging);
      merge();
    } else if (isMerging == FileNodeProcessorStatus.WAITING) {
      LOGGER.info("The filenode processor {} is recovering, the filenode status is {}.",
          getProcessorName(), isMerging);
      switchWaitingToWorking();
    }
    // add file into index of file
    addAllFileIntoIndex(newFileNodes);
  }

  private void recoverUpdateTimeMap() {
    lastUpdateTimeMap = new HashMap<>();
    flushLastUpdateTimeMap = new HashMap<>();
    for (TsFileResource tsFileResource : newFileNodes) {
      Map<String, Long> endTimeMap =  tsFileResource.getEndTimeMap();
      endTimeMap.forEach((key, value) -> {
        Long lastTime = lastUpdateTimeMap.get(key);
        if (lastTime == null || lastTime < value) {
          lastUpdateTimeMap.put(key, value);
          flushLastUpdateTimeMap.put(key, value);
        }
      });
    }
  }

  //when calling this method, the bufferWriteProcessor must not be null
  private BufferWriteProcessor getBufferWriteProcessor() {
    return bufferWriteProcessor;
  }

  /**
   * get buffer write processor by processor name and insert time.
   */
  public BufferWriteProcessor getBufferWriteProcessor(String processorName, long insertTime)
      throws FileNodeProcessorException {
    if (bufferWriteProcessor == null) {
      Map<String, Action> params = new HashMap<>();
      params.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
      //params.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
      params
          .put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fileNodeFlushAction);
      String baseDir = directories.getNextFolderForTsfile();
      LOGGER.info("Allocate folder {} for the new bufferwrite processor.", baseDir);
      // construct processor or restore
      try {
        bufferWriteProcessor = new BufferWriteProcessor(baseDir, processorName,
            insertTime + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR
                + System.currentTimeMillis(),
            params, bufferwriteCloseConsumer, versionController, fileSchema);
      } catch (BufferWriteProcessorException e) {
        throw new FileNodeProcessorException(String
            .format("The filenode processor %s failed to get the bufferwrite processor.",
                processorName), e);
      }
    }
    return bufferWriteProcessor;
  }

  /**
   * get overflow processor by processor name.
   */
  public OverflowProcessor getOverflowProcessor(String processorName) throws ProcessorException {
    if (overflowProcessor == null) {
      Map<String, Action> params = new HashMap<>();
      // construct processor or restore
      params.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
      params
          .put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fileNodeFlushAction);
      overflowProcessor = new OverflowProcessor(processorName, params, fileSchema,
          versionController);
    } else if (overflowProcessor.isClosed()) {
      overflowProcessor.reopen();
    }
    return overflowProcessor;
  }

  /**
   * get overflow processor.
   */
  public OverflowProcessor getOverflowProcessor() {
    if (overflowProcessor == null || overflowProcessor.isClosed()) {
      LOGGER.error("The overflow processor is null when getting the overflowProcessor");
    }
    return overflowProcessor;
  }

  public boolean hasOverflowProcessor() {
    return overflowProcessor != null && !overflowProcessor.isClosed();
  }

  public void setBufferwriteProcessroToClosed() {

    bufferWriteProcessor = null;
  }

  public boolean hasBufferwriteProcessor() {

    return bufferWriteProcessor != null;
  }

  /**
   * set last update time.
   */
  public void setLastUpdateTime(String deviceId, long timestamp) {
    if (!lastUpdateTimeMap.containsKey(deviceId) || lastUpdateTimeMap.get(deviceId) < timestamp) {
      lastUpdateTimeMap.put(deviceId, timestamp);
    }
    if (timestamp == -1) {
      lastUpdateTimeMap.remove(deviceId);
    }
  }

  /**
   * get last update time.
   */
  public long getLastUpdateTime(String deviceId) {

    if (lastUpdateTimeMap.containsKey(deviceId)) {
      return lastUpdateTimeMap.get(deviceId);
    } else {
      return -1;
    }
  }

  /**
   * get flushMetadata last update time.
   */
  public long getFlushLastUpdateTime(String deviceId) {
    if (!flushLastUpdateTimeMap.containsKey(deviceId)) {
      flushLastUpdateTimeMap.put(deviceId, 0L);
    }
    return flushLastUpdateTimeMap.get(deviceId);
  }

  public Map<String, Long> getLastUpdateTimeMap() {
    return lastUpdateTimeMap;
  }

  /**
   * For insert overflow.
   */
  public void changeTypeToChanged(String deviceId, long timestamp) {
    if (!invertedIndexOfFiles.containsKey(deviceId)) {
      LOGGER.warn(
          WARN_NO_SUCH_OVERFLOWED_FILE
              + "the data is [device:{},time:{}]",
          getProcessorName(), deviceId, timestamp);
      emptyTsFileResource.setStartTime(deviceId, 0L);
      emptyTsFileResource.setEndTime(deviceId, getLastUpdateTime(deviceId));
      emptyTsFileResource.changeTypeToChanged(isMerging);
    } else {
      List<TsFileResource> temp = invertedIndexOfFiles.get(deviceId);
      int index = searchIndexNodeByTimestamp(deviceId, timestamp, temp);
      changeTypeToChanged(temp.get(index), deviceId);
    }
  }

  private void changeTypeToChanged(TsFileResource fileNode, String deviceId) {
    fileNode.changeTypeToChanged(isMerging);
    if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
      fileNode.addMergeChanged(deviceId);
    }
  }

  /**
   * For update overflow.
   */
  public void changeTypeToChanged(String deviceId, long startTime, long endTime) {
    if (!invertedIndexOfFiles.containsKey(deviceId)) {
      LOGGER.warn(
          WARN_NO_SUCH_OVERFLOWED_FILE
              + "the data is [device:{}, start time:{}, end time:{}]",
          getProcessorName(), deviceId, startTime, endTime);
      emptyTsFileResource.setStartTime(deviceId, 0L);
      emptyTsFileResource.setEndTime(deviceId, getLastUpdateTime(deviceId));
      emptyTsFileResource.changeTypeToChanged(isMerging);
    } else {
      List<TsFileResource> temp = invertedIndexOfFiles.get(deviceId);
      int left = searchIndexNodeByTimestamp(deviceId, startTime, temp);
      int right = searchIndexNodeByTimestamp(deviceId, endTime, temp);
      for (int i = left; i <= right; i++) {
        changeTypeToChanged(temp.get(i), deviceId);
      }
    }
  }

  /**
   * For delete overflow.
   */
  public void changeTypeToChangedForDelete(String deviceId, long timestamp) {
    if (!invertedIndexOfFiles.containsKey(deviceId)) {
      LOGGER.warn(
          WARN_NO_SUCH_OVERFLOWED_FILE
              + "the data is [device:{}, delete time:{}]",
          getProcessorName(), deviceId, timestamp);
      emptyTsFileResource.setStartTime(deviceId, 0L);
      emptyTsFileResource.setEndTime(deviceId, getLastUpdateTime(deviceId));
      emptyTsFileResource.changeTypeToChanged(isMerging);
    } else {
      List<TsFileResource> temp = invertedIndexOfFiles.get(deviceId);
      int index = searchIndexNodeByTimestamp(deviceId, timestamp, temp);
      for (int i = 0; i <= index; i++) {
        temp.get(i).changeTypeToChanged(isMerging);
        if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
          temp.get(i).addMergeChanged(deviceId);
        }
      }
    }
  }

  /**
   * Search the index of the interval by the timestamp.
   *
   * @return index of interval
   */
  private int searchIndexNodeByTimestamp(String deviceId, long timestamp,
      List<TsFileResource> fileList) {
    int index = 1;
    while (index < fileList.size()) {
      if (timestamp < fileList.get(index).getStartTime(deviceId)) {
        break;
      } else {
        index++;
      }
    }
    return index - 1;
  }

  /**
   * add multiple pass lock.
   */
  public int addMultiPassCount() {
    LOGGER.debug("Add MultiPassCount: read lock newMultiPassCount.");
    newMultiPassCount.incrementAndGet();
    while (newMultiPassTokenSet.contains(multiPassLockToken)) {
      multiPassLockToken++;
    }
    newMultiPassTokenSet.add(multiPassLockToken);
    LOGGER.debug("Add multi token:{}, nsPath:{}.", multiPassLockToken, getProcessorName());
    return multiPassLockToken;
  }

  /**
   * decrease multiple pass count. TODO: use the return value or remove it.
   */
  public boolean decreaseMultiPassCount(int token) throws FileNodeProcessorException {
    if (newMultiPassTokenSet.contains(token)) {
      int newMultiPassCountValue = newMultiPassCount.decrementAndGet();
      if (newMultiPassCountValue < 0) {
        throw new FileNodeProcessorException(String
            .format("Remove MultiPassCount error, newMultiPassCount:%d", newMultiPassCountValue));
      }
      newMultiPassTokenSet.remove(token);
      LOGGER.debug("Remove multi token:{}, nspath:{}, new set:{}, count:{}", token,
          getProcessorName(),
          newMultiPassTokenSet, newMultiPassCount);
      return true;
    } else if (oldMultiPassTokenSet != null && oldMultiPassTokenSet.contains(token)) {
      // remove token first, then unlock
      oldMultiPassTokenSet.remove(token);
      oldMultiPassCount.countDown();
      long oldMultiPassCountValue = oldMultiPassCount.getCount();
      if (oldMultiPassCountValue < 0) {
        throw new FileNodeProcessorException(String
            .format("Remove MultiPassCount error, oldMultiPassCount:%d", oldMultiPassCountValue));
      }
      LOGGER.debug("Remove multi token:{}, old set:{}, count:{}", token, oldMultiPassTokenSet,
          oldMultiPassCount.getCount());
      return true;
    } else {
      LOGGER.error("remove token error:{},new set:{}, old set:{}", token, newMultiPassTokenSet,
          oldMultiPassTokenSet);
      // should add throw exception
      return false;
    }
  }

  /**
   * query data.
   */
  public <T extends Comparable<T>> QueryDataSource query(String deviceId, String measurementId,
      QueryContext context) throws FileNodeProcessorException {
    // query overflow data
    MeasurementSchema mSchema;
    TSDataType dataType;

    //mSchema = mManager.getSchemaForOnePath(deviceId + "." + measurementId);
    mSchema = fileSchema.getMeasurementSchema(measurementId);
    dataType = mSchema.getType();

    OverflowSeriesDataSource overflowSeriesDataSource;
    try {
      overflowSeriesDataSource = overflowProcessor.query(deviceId, measurementId, dataType,
          mSchema.getProps(), context);
    } catch (IOException e) {
      throw new FileNodeProcessorException(e);
    }
    // tsfile dataØØ
    List<TsFileResource> bufferwriteDataInFiles = new ArrayList<>();
    for (TsFileResource tsFileResource : newFileNodes) {
      // add the same tsFileResource, but not the same reference
      if (tsFileResource.isClosed()) {
        bufferwriteDataInFiles.add(tsFileResource.backUp());
      }
    }
    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> bufferwritedata = new Pair<>(null, null);
    // bufferwrite data
    UnsealedTsFile unsealedTsFile = null;

    if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()
        && !newFileNodes.get(newFileNodes.size() - 1).getStartTimeMap().isEmpty()) {
      unsealedTsFile = new UnsealedTsFile();
      unsealedTsFile.setFilePath(newFileNodes.get(newFileNodes.size() - 1).getFile().getAbsolutePath());
      if (bufferWriteProcessor == null) {
        throw new FileNodeProcessorException(String.format(
            "The last of tsfile %s in filenode processor %s is not closed, "
                + "but the bufferwrite processor is null.",
            newFileNodes.get(newFileNodes.size() - 1).getFile().getAbsolutePath(), getProcessorName()));
      }
      bufferwritedata = bufferWriteProcessor
          .queryBufferWriteData(deviceId, measurementId, dataType, mSchema.getProps());

      try {
        List<Modification> pathModifications = context.getPathModifications(
            bufferWriteProcessor.getCurrentTsFileResource().getModFile(), deviceId
                + IoTDBConstant.PATH_SEPARATOR + measurementId
        );
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(bufferwritedata.right, pathModifications);
        }
      } catch (IOException e) {
        throw new FileNodeProcessorException(e);
      }

      unsealedTsFile.setTimeSeriesChunkMetaDatas(bufferwritedata.right);
    }
    GlobalSortedSeriesDataSource globalSortedSeriesDataSource = new GlobalSortedSeriesDataSource(
        new Path(deviceId + "." + measurementId), bufferwriteDataInFiles, unsealedTsFile,
        bufferwritedata.left);
    return new QueryDataSource(globalSortedSeriesDataSource, overflowSeriesDataSource);

  }

  /**
   * append one specified tsfile to this filenode processor.
   *
   * @param appendFile the appended tsfile information
   * @param appendFilePath the seriesPath of appended file
   */
  public void appendFile(TsFileResource appendFile, String appendFilePath)
      throws FileNodeProcessorException {
    try {
      if (!appendFile.getFile().getParentFile().exists()) {
        appendFile.getFile().getParentFile().mkdirs();
      }
      // move file
      File originFile = new File(appendFilePath);
      File targetFile = appendFile.getFile();
      if (!originFile.exists()) {
        throw new FileNodeProcessorException(
            String.format("The appended file %s does not exist.", appendFilePath));
      }
      if (targetFile.exists()) {
        throw new FileNodeProcessorException(
            String.format("The appended target file %s already exists.",
                appendFile.getFile().getAbsolutePath()));
      }
      if (!originFile.renameTo(targetFile)) {
        LOGGER.warn("File renaming failed when appending new file. Origin: {}, Target: {}",
            originFile.getPath(), targetFile.getPath());
      }
      // append the new tsfile
      this.newFileNodes.add(appendFile);
      // update the lastUpdateTime
      for (Entry<String, Long> entry : appendFile.getEndTimeMap().entrySet()) {
        lastUpdateTimeMap.put(entry.getKey(), entry.getValue());
      }
      bufferwriteFlushAction.act();
      fileNodeProcessorStore.setNewFileNodes(newFileNodes);
      // reconstruct the inverted index of the newFileNodes
      fileNodeFlushAction.act();
      addAllFileIntoIndex(newFileNodes);
    } catch (Exception e) {
      LOGGER.error("Failed to append the tsfile {} to filenode processor {}.", appendFile,
          getProcessorName());
      throw new FileNodeProcessorException(e);
    }
  }

  /**
   * get overlap tsfiles which are conflict with the appendFile.
   *
   * @param appendFile the appended tsfile information
   */
  public List<String> getOverlapFiles(TsFileResource appendFile, String uuid)
      throws FileNodeProcessorException {
    List<String> overlapFiles = new ArrayList<>();
    try {
      for (TsFileResource tsFileResource : newFileNodes) {
        getOverlapFiles(appendFile, tsFileResource, uuid, overlapFiles);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to get overlap tsfiles which conflict with the appendFile.");
      throw new FileNodeProcessorException(e);
    }
    return overlapFiles;
  }

  private void getOverlapFiles(TsFileResource appendFile, TsFileResource tsFileResource,
      String uuid, List<String> overlapFiles) throws IOException {
    for (Entry<String, Long> entry : appendFile.getStartTimeMap().entrySet()) {
      if (tsFileResource.getStartTimeMap().containsKey(entry.getKey()) &&
          tsFileResource.getEndTime(entry.getKey()) >= entry.getValue()
          && tsFileResource.getStartTime(entry.getKey()) <= appendFile
          .getEndTime(entry.getKey())) {
        String relativeFilePath =
            Constans.SYNC_SERVER + File.separatorChar + uuid + File.separatorChar
                + Constans.BACK_UP_DIRECTORY_NAME
                + File.separatorChar + tsFileResource.getRelativePath();
        File newFile = new File(
            Directories.getInstance().getTsFileFolder(tsFileResource.getBaseDirIndex()),
            relativeFilePath);
        if (!newFile.getParentFile().exists()) {
          newFile.getParentFile().mkdirs();
        }
        java.nio.file.Path link = FileSystems.getDefault().getPath(newFile.getPath());
        java.nio.file.Path target = FileSystems.getDefault()
            .getPath(tsFileResource.getFile().getAbsolutePath());
        Files.createLink(link, target);
        overlapFiles.add(newFile.getPath());
        break;
      }
    }
  }

  /**
   * add time series.
   */
  public void addTimeSeries(String measurementId, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) {
    fileSchema.registerMeasurement(new MeasurementSchema(measurementId, dataType, encoding,
        compressor, props));
  }

  /**
   * submit the merge task to the <code>MergePool</code>.
   *
   * @return null -can't submit the merge task, because this filenode is not overflowed or it is
   * merging now. Future - submit the merge task successfully.
   */
  Future submitToMerge() {
    ZoneId zoneId = IoTDBDescriptor.getInstance().getConfig().getZoneID();
    if (lastMergeTime > 0) {
      long thisMergeTime = System.currentTimeMillis();
      long mergeTimeInterval = thisMergeTime - lastMergeTime;
      ZonedDateTime lastDateTime = ofInstant(Instant.ofEpochMilli(lastMergeTime),
          zoneId);
      ZonedDateTime thisDateTime = ofInstant(Instant.ofEpochMilli(thisMergeTime),
          zoneId);
      LOGGER.info(
          "The filenode {} last merge time is {}, this merge time is {}, "
              + "merge time interval is {}s",
          getProcessorName(), lastDateTime, thisDateTime, mergeTimeInterval / 1000);
    }
    lastMergeTime = System.currentTimeMillis();

    if (overflowProcessor != null && !overflowProcessor.isClosed()) {
      if (overflowProcessor.getFileSize() < IoTDBDescriptor.getInstance()
          .getConfig().getOverflowFileSizeThreshold()) {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Skip this merge taks submission, because the size{} of overflow processor {} "
                  + "does not reaches the threshold {}.",
              MemUtils.bytesCntToStr(overflowProcessor.getFileSize()), getProcessorName(),
              MemUtils.bytesCntToStr(
                  IoTDBDescriptor.getInstance().getConfig().getOverflowFileSizeThreshold()));
        }
        return null;
      }
    } else {
      LOGGER.info(
          "Skip this merge taks submission, because the filenode processor {} "
              + "has no overflow processor.",
          getProcessorName());
      return null;
    }
    if (isOverflowed && isMerging == FileNodeProcessorStatus.NONE) {
      Runnable mergeThread;
      mergeThread = new MergeRunnale();
      LOGGER.info("Submit the merge task, the merge filenode is {}", getProcessorName());
      return MergePoolManager.getInstance().submit(mergeThread);
    } else {
      if (!isOverflowed) {
        LOGGER.info(
            "Skip this merge taks submission, because the filenode processor {} is not " +
                "overflowed.",
            getProcessorName());
      } else {
        LOGGER.warn(
            "Skip this merge task submission, because last merge task is not over yet, "
                + "the merge filenode processor is {}",
            getProcessorName());
      }
    }
    return null;
  }

  /**
   * Prepare for merge, close the bufferwrite and overflow.
   */
  private void prepareForMerge() {
    try {
      LOGGER.info("The filenode processor {} prepares for merge, closes the bufferwrite processor",
          getProcessorName());
      Future<Boolean> future = closeBufferWrite();
      future.get();
      LOGGER.info("The bufferwrite processor {} is closed successfully",
          getProcessorName());
      // try to get overflow processor
      getOverflowProcessor(getProcessorName());
      // must close the overflow processor
      while (!getOverflowProcessor().canBeClosed()) {
        waitForClosing();
      }
      LOGGER.info("The filenode processor {} prepares for merge, closes the overflow processor",
          getProcessorName());
      getOverflowProcessor().close();
    } catch (ProcessorException | InterruptedException | ExecutionException e) {
      LOGGER.error("The filenode processor {} prepares for merge error.", getProcessorName());
      writeUnlock();
      throw new ErrorDebugException(e);
    }
  }

  private void waitForClosing() {
    try {
      LOGGER.info(
          "The filenode processor {} prepares for merge, the overflow {} can't be closed, "
              + "wait 100ms,",
          getProcessorName(), getProcessorName());
      TimeUnit.MICROSECONDS.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Merge this storage group, merge the tsfile data with overflow data.
   */
  public void merge() throws FileNodeProcessorException {
    // close bufferwrite and overflow, prepare for merge
    LOGGER.info("The filenode processor {} begins to merge.", getProcessorName());
    writeLock();
    prepareForMerge();
    // change status from overflowed to no overflowed
    isOverflowed = false;
    // change status from work to merge
    isMerging = FileNodeProcessorStatus.MERGING_WRITE;
    // check the empty file
    Map<String, Long> startTimeMap = emptyTsFileResource.getStartTimeMap();
    mergeCheckEmptyFile(startTimeMap);

    for (TsFileResource tsFileResource : newFileNodes) {
      if (tsFileResource.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
        tsFileResource.setOverflowChangeType(OverflowChangeType.CHANGED);
      }
    }

    addAllFileIntoIndex(newFileNodes);
    synchronized (fileNodeProcessorStore) {
      fileNodeProcessorStore.setOverflowed(isOverflowed);
      fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
      fileNodeProcessorStore.setNewFileNodes(newFileNodes);
      fileNodeProcessorStore.setEmptyTsFileResource(emptyTsFileResource);
      // flushMetadata this filenode information
      try {
        writeStoreToDisk(fileNodeProcessorStore);
      } catch (FileNodeProcessorException e) {
        LOGGER.error("The filenode processor {} writes restore information error when merging.",
            getProcessorName(), e);
        writeUnlock();
        throw new FileNodeProcessorException(e);
      }
    }
    // add numOfMergeFile to control the number of the merge file
    List<TsFileResource> backupIntervalFiles;

    backupIntervalFiles = switchFileNodeToMerge();
    //
    // clear empty file
    //
    boolean needEmtpy = false;
    if (emptyTsFileResource.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
      needEmtpy = true;
    }
    emptyTsFileResource.clear();
    // attention
    try {
      if (overflowProcessor.isClosed()) {
        overflowProcessor.reopen();
      }
      overflowProcessor.switchWorkToMerge();
    } catch (ProcessorException | IOException e) {
      LOGGER.error("The filenode processor {} can't switch overflow processor from work to merge.",
          getProcessorName(), e);
      writeUnlock();
      throw new FileNodeProcessorException(e);
    }
    LOGGER.info("The filenode processor {} switches from {} to {}.", getProcessorName(),
        FileNodeProcessorStatus.NONE, FileNodeProcessorStatus.MERGING_WRITE);
    writeUnlock();

    // query tsfile data and overflow data, and merge them
    int numOfMergeFiles = 0;
    int allNeedMergeFiles = backupIntervalFiles.size();
    for (TsFileResource backupIntervalFile : backupIntervalFiles) {
      numOfMergeFiles++;
      if (backupIntervalFile.getOverflowChangeType() == OverflowChangeType.CHANGED) {
        // query data and merge
        String filePathBeforeMerge = backupIntervalFile.getRelativePath();
        try {
          LOGGER.info(
              "The filenode processor {} begins merging the {}/{} tsfile[{}] with "
                  + "overflow file, the process is {}%",
              getProcessorName(), numOfMergeFiles, allNeedMergeFiles, filePathBeforeMerge,
              (int) (((numOfMergeFiles - 1) / (float) allNeedMergeFiles) * 100));
          long startTime = System.currentTimeMillis();
          String newFile = queryAndWriteDataForMerge(backupIntervalFile);
          long endTime = System.currentTimeMillis();
          long timeConsume = endTime - startTime;
          ZoneId zoneId = IoTDBDescriptor.getInstance().getConfig().getZoneID();
          LOGGER.info(
              "The fileNode processor {} has merged the {}/{} tsfile[{}->{}] over, "
                  + "start time of merge is {}, end time of merge is {}, "
                  + "time consumption is {}ms,"
                  + " the process is {}%",
              getProcessorName(), numOfMergeFiles, allNeedMergeFiles, filePathBeforeMerge,
              newFile, ofInstant(Instant.ofEpochMilli(startTime),
                  zoneId), ofInstant(Instant.ofEpochMilli(endTime), zoneId), timeConsume,
              numOfMergeFiles / (float) allNeedMergeFiles * 100);
        } catch (IOException | PathErrorException e) {
          LOGGER.error("Merge: query and write data error.", e);
          throw new FileNodeProcessorException(e);
        }
      } else if (backupIntervalFile.getOverflowChangeType() == OverflowChangeType.MERGING_CHANGE) {
        LOGGER.error("The overflowChangeType of backupIntervalFile must not be {}",
            OverflowChangeType.MERGING_CHANGE);
        // handle this error, throw one runtime exception
        throw new FileNodeProcessorException(
            "The overflowChangeType of backupIntervalFile must not be "
                + OverflowChangeType.MERGING_CHANGE);
      } else {
        LOGGER.debug(
            "The filenode processor {} is merging, the interval file {} doesn't "
                + "need to be merged.",
            getProcessorName(), backupIntervalFile.getRelativePath());
      }
    }

    // change status from merge to wait
    switchMergeToWaiting(backupIntervalFiles, needEmtpy);

    // change status from wait to work
    switchWaitingToWorking();
  }

  private void mergeCheckEmptyFile(Map<String, Long> startTimeMap) {
    if (emptyTsFileResource.getOverflowChangeType() == OverflowChangeType.NO_CHANGE) {
      return;
    }
    Iterator<Entry<String, Long>> iterator = emptyTsFileResource.getEndTimeMap().entrySet()
        .iterator();
    while (iterator.hasNext()) {
      Entry<String, Long> entry = iterator.next();
      String deviceId = entry.getKey();
      if (invertedIndexOfFiles.containsKey(deviceId)) {
        invertedIndexOfFiles.get(deviceId).get(0).setOverflowChangeType(OverflowChangeType.CHANGED);
        startTimeMap.remove(deviceId);
        iterator.remove();
      }
    }
    if (emptyTsFileResource.checkEmpty()) {
      emptyTsFileResource.clear();
    } else {
      if (!newFileNodes.isEmpty()) {
        TsFileResource first = newFileNodes.get(0);
        for (String deviceId : emptyTsFileResource.getStartTimeMap().keySet()) {
          first.setStartTime(deviceId, emptyTsFileResource.getStartTime(deviceId));
          first.setEndTime(deviceId, emptyTsFileResource.getEndTime(deviceId));
          first.setOverflowChangeType(OverflowChangeType.CHANGED);
        }
        emptyTsFileResource.clear();
      } else {
        emptyTsFileResource.setOverflowChangeType(OverflowChangeType.CHANGED);
      }
    }
  }

  private List<TsFileResource> switchFileNodeToMerge() throws FileNodeProcessorException {
    List<TsFileResource> result = new ArrayList<>();
    if (emptyTsFileResource.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
      // add empty
      result.add(emptyTsFileResource.backUp());
      if (!newFileNodes.isEmpty()) {
        throw new FileNodeProcessorException(
            String.format("The status of empty file is %s, but the new file list is not empty",
                emptyTsFileResource.getOverflowChangeType()));
      }
      return result;
    }
    if (newFileNodes.isEmpty()) {
      LOGGER.error("No file was changed when merging, the filenode is {}", getProcessorName());
      throw new FileNodeProcessorException(
          "No file was changed when merging, the filenode is " + getProcessorName());
    }
    for (TsFileResource tsFileResource : newFileNodes) {
      updateFileNode(tsFileResource, result);
    }
    return result;
  }

  private void updateFileNode(TsFileResource tsFileResource, List<TsFileResource> result) {
    if (tsFileResource.getOverflowChangeType() == OverflowChangeType.NO_CHANGE) {
      result.add(tsFileResource.backUp());
    } else {
      Map<String, Long> startTimeMap = new HashMap<>();
      Map<String, Long> endTimeMap = new HashMap<>();
      for (String deviceId : tsFileResource.getEndTimeMap().keySet()) {
        List<TsFileResource> temp = invertedIndexOfFiles.get(deviceId);
        int index = temp.indexOf(tsFileResource);
        int size = temp.size();
        // start time
        if (index == 0) {
          startTimeMap.put(deviceId, 0L);
        } else {
          startTimeMap.put(deviceId, tsFileResource.getStartTime(deviceId));
        }
        // end time
        if (index < size - 1) {
          endTimeMap.put(deviceId, temp.get(index + 1).getStartTime(deviceId) - 1);
        } else {
          endTimeMap.put(deviceId, tsFileResource.getEndTime(deviceId));
        }
      }
      TsFileResource node = new TsFileResource(startTimeMap, endTimeMap,
          tsFileResource.getOverflowChangeType(), tsFileResource.getFile());
      result.add(node);
    }
  }

  private void switchMergeToWaiting(List<TsFileResource> backupIntervalFiles, boolean needEmpty)
      throws FileNodeProcessorException {
    LOGGER.info("The status of filenode processor {} switches from {} to {}.", getProcessorName(),
        FileNodeProcessorStatus.MERGING_WRITE, FileNodeProcessorStatus.WAITING);
    writeLock();
    try {
      oldMultiPassTokenSet = newMultiPassTokenSet;
      oldMultiPassCount = new CountDownLatch(newMultiPassCount.get());
      newMultiPassTokenSet = new HashSet<>();
      newMultiPassCount = new AtomicInteger(0);
      List<TsFileResource> result = new ArrayList<>();
      int beginIndex = 0;
      if (needEmpty) {
        TsFileResource empty = backupIntervalFiles.get(0);
        if (!empty.checkEmpty()) {
          updateEmpty(empty, result);
          beginIndex++;
        }
      }
      // reconstruct the file index
      addAllFileIntoIndex(backupIntervalFiles);
      // check the merge changed file
      for (int i = beginIndex; i < backupIntervalFiles.size(); i++) {
        TsFileResource newFile = newFileNodes.get(i - beginIndex);
        TsFileResource temp = backupIntervalFiles.get(i);
        if (newFile.getOverflowChangeType() == OverflowChangeType.MERGING_CHANGE) {
          updateMergeChanged(newFile, temp);
        }
        if (!temp.checkEmpty()) {
          result.add(temp);
        }
      }
      // add new file when merge
      for (int i = backupIntervalFiles.size() - beginIndex; i < newFileNodes.size(); i++) {
        TsFileResource fileNode = newFileNodes.get(i);
        if (fileNode.isClosed()) {
          result.add(fileNode.backUp());
        } else {
          result.add(fileNode);
        }
      }

      isMerging = FileNodeProcessorStatus.WAITING;
      newFileNodes = result;
      // reconstruct the index
      addAllFileIntoIndex(newFileNodes);
      // clear merge changed
      for (TsFileResource fileNode : newFileNodes) {
        fileNode.clearMergeChanged();
      }

      synchronized (fileNodeProcessorStore) {
        fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
        fileNodeProcessorStore.setEmptyTsFileResource(emptyTsFileResource);
        fileNodeProcessorStore.setNewFileNodes(newFileNodes);
        try {
          writeStoreToDisk(fileNodeProcessorStore);
        } catch (FileNodeProcessorException e) {
          LOGGER.error(
              "Merge: failed to write filenode information to revocery file, the filenode is " +
                  "{}.",
              getProcessorName(), e);
          throw new FileNodeProcessorException(
              "Merge: write filenode information to revocery file failed, the filenode is "
                  + getProcessorName());
        }
      }
    } finally {
      writeUnlock();
    }
  }

  private void updateEmpty(TsFileResource empty, List<TsFileResource> result) {
    for (String deviceId : empty.getStartTimeMap().keySet()) {
      if (invertedIndexOfFiles.containsKey(deviceId)) {
        TsFileResource temp = invertedIndexOfFiles.get(deviceId).get(0);
        if (temp.getMergeChanged().contains(deviceId)) {
          empty.setOverflowChangeType(OverflowChangeType.CHANGED);
          break;
        }
      }
    }
    empty.clearMergeChanged();
    result.add(empty.backUp());
  }

  private void updateMergeChanged(TsFileResource newFile, TsFileResource temp) {
    for (String deviceId : newFile.getMergeChanged()) {
      if (temp.getStartTimeMap().containsKey(deviceId)) {
        temp.setOverflowChangeType(OverflowChangeType.CHANGED);
      } else {
        changeTypeToChanged(deviceId, newFile.getStartTime(deviceId),
            newFile.getEndTime(deviceId));
      }
    }
  }


  private void switchWaitingToWorking()
      throws FileNodeProcessorException {

    LOGGER.info("The status of filenode processor {} switches from {} to {}.", getProcessorName(),
        FileNodeProcessorStatus.WAITING, FileNodeProcessorStatus.NONE);

    if (oldMultiPassCount != null) {
      LOGGER.info("The old Multiple Pass Token set is {}, the old Multiple Pass Count is {}",
          oldMultiPassTokenSet,
          oldMultiPassCount);
      try {
        oldMultiPassCount.await();
      } catch (InterruptedException e) {
        LOGGER.info(
            "The filenode processor {} encountered an error when it waits for all old queries over.",
            getProcessorName());
        throw new FileNodeProcessorException(e);
      }
    }

    try {
      writeLock();
      try {
        // delete the all files which are in the newFileNodes
        // notice: the last restore file of the interval file

        List<String> bufferwriteDirPathList = directories.getAllTsFileFolders();
        List<File> bufferwriteDirList = new ArrayList<>();
        collectBufferWriteDirs(bufferwriteDirPathList, bufferwriteDirList);

        Set<String> bufferFiles = new HashSet<>();
        collectBufferWriteFiles(bufferFiles);

        // add the restore file, if the last file is not closed
        if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()) {
          String bufferFileRestorePath =
              newFileNodes.get(newFileNodes.size() - 1).getFile().getAbsolutePath() + RESTORE_FILE_SUFFIX;
          bufferFiles.add(bufferFileRestorePath);
        }

        deleteBufferWriteFiles(bufferwriteDirList, bufferFiles);

        // merge switch
        changeFileNodes();

        // overflow switch from merge to work
        overflowProcessor.switchMergeToWork();

        // write status to file
        isMerging = FileNodeProcessorStatus.NONE;
        synchronized (fileNodeProcessorStore) {
          fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
          fileNodeProcessorStore.setNewFileNodes(newFileNodes);
          fileNodeProcessorStore.setEmptyTsFileResource(emptyTsFileResource);
          writeStoreToDisk(fileNodeProcessorStore);
        }
      } catch (IOException e) {
        LOGGER.info(
            "The filenode processor {} encountered an error when its "
                + "status switched from {} to {}.",
            getProcessorName(), FileNodeProcessorStatus.NONE,
            FileNodeProcessorStatus.MERGING_WRITE);
        throw new FileNodeProcessorException(e);
      } finally {
        writeUnlock();
      }
    } finally {
      oldMultiPassTokenSet = null;
      oldMultiPassCount = null;
    }

  }

  private void collectBufferWriteDirs(List<String> bufferwriteDirPathList,
      List<File> bufferwriteDirList) {
    for (String bufferwriteDirPath : bufferwriteDirPathList) {
      if (bufferwriteDirPath.length() > 0
          && bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1)
          != File.separatorChar) {
        bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
      }
      bufferwriteDirPath = bufferwriteDirPath + getProcessorName();
      File bufferwriteDir = new File(bufferwriteDirPath);
      bufferwriteDirList.add(bufferwriteDir);
      if (!bufferwriteDir.exists()) {
        bufferwriteDir.mkdirs();
      }
    }
  }

  private void collectBufferWriteFiles(Set<String> bufferFiles) {
    for (TsFileResource bufferFileNode : newFileNodes) {
      String bufferFilePath = bufferFileNode.getFile().getAbsolutePath();
      if (bufferFilePath != null) {
        bufferFiles.add(bufferFilePath);
      }
    }
  }

  private void deleteBufferWriteFiles(List<File> bufferwriteDirList, Set<String> bufferFiles)
      throws IOException {
    for (File bufferwriteDir : bufferwriteDirList) {
      File[] files = bufferwriteDir.listFiles();
      if (files == null) {
        continue;
      }
      for (File file : files) {
        if (!bufferFiles.contains(file.getPath())) {
          FileReaderManager.getInstance().closeFileAndRemoveReader(file.getPath());
          if (!file.delete()) {
            LOGGER.warn("Cannot delete BufferWrite file {}", file.getPath());
          }
        }
      }
    }
  }

  private void changeFileNodes() {
    for (TsFileResource fileNode : newFileNodes) {
      if (fileNode.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
        fileNode.setOverflowChangeType(OverflowChangeType.CHANGED);
      }
    }
  }

  private String queryAndWriteDataForMerge(TsFileResource backupIntervalFile)
      throws IOException, FileNodeProcessorException, PathErrorException {
    Map<String, Long> startTimeMap = new HashMap<>();
    Map<String, Long> endTimeMap = new HashMap<>();

    mergeFileWriter = null;
    mergeOutputPath = null;
    mergeBaseDir = null;
    mergeFileName = null;
    // modifications are blocked before mergeModification is created to avoid
    // losing some modification.
    mergeDeleteLock.lock();
    QueryContext context = new QueryContext();
    try {
      FileReaderManager.getInstance().increaseFileReaderReference(backupIntervalFile.getFilePath(),
          true);
      for (String deviceId : backupIntervalFile.getStartTimeMap().keySet()) {
        // query one deviceId
        List<Path> pathList = new ArrayList<>();
        mergeIsChunkGroupHasData = false;
        mergeStartPos = -1;
        ChunkGroupFooter footer;
        int numOfChunk = 0;
        try {
          List<String> pathStrings = mManager.getLeafNodePathInNextLevel(deviceId);
          for (String string : pathStrings) {
            pathList.add(new Path(string));
          }
        } catch (PathErrorException e) {
          LOGGER.error("Can't get all the paths from MManager, the deviceId is {}", deviceId);
          throw new FileNodeProcessorException(e);
        }
        if (pathList.isEmpty()) {
          continue;
        }
        for (Path path : pathList) {
          // query one measurement in the special deviceId
          String measurementId = path.getMeasurement();
          TSDataType dataType = mManager.getSeriesType(path.getFullPath());
          OverflowSeriesDataSource overflowSeriesDataSource = overflowProcessor.queryMerge(deviceId,
              measurementId, dataType, true, context);
          Filter timeFilter = FilterFactory
              .and(TimeFilter.gtEq(backupIntervalFile.getStartTime(deviceId)),
                  TimeFilter.ltEq(backupIntervalFile.getEndTime(deviceId)));
          SingleSeriesExpression seriesFilter = new SingleSeriesExpression(path, timeFilter);

          for (OverflowInsertFile overflowInsertFile : overflowSeriesDataSource
              .getOverflowInsertFileList()) {
            FileReaderManager.getInstance()
                .increaseFileReaderReference(overflowInsertFile.getFilePath(),
                    false);
          }

          IReader seriesReader = SeriesReaderFactory.getInstance()
              .createSeriesReaderForMerge(backupIntervalFile,
                  overflowSeriesDataSource, seriesFilter, context);
          numOfChunk += queryAndWriteSeries(seriesReader, path, seriesFilter, dataType,
              startTimeMap, endTimeMap, overflowSeriesDataSource);
        }
        if (mergeIsChunkGroupHasData) {
          // end the new rowGroupMetadata
          mergeFileWriter.endChunkGroup(0);
        }
      }
    } finally {
      FileReaderManager.getInstance().decreaseFileReaderReference(backupIntervalFile.getFilePath(),
          true);

      if (mergeDeleteLock.isLocked()) {
        mergeDeleteLock.unlock();
      }
    }

    if (mergeFileWriter != null) {
      mergeFileWriter.endFile(fileSchema);
    }
    backupIntervalFile.setFile(new File(mergeBaseDir + File.separator + mergeFileName));
    backupIntervalFile.setOverflowChangeType(OverflowChangeType.NO_CHANGE);
    backupIntervalFile.setStartTimeMap(startTimeMap);
    backupIntervalFile.setEndTimeMap(endTimeMap);
    backupIntervalFile.setModFile(mergingModification);
    mergingModification = null;
    return mergeFileName;
  }

  private int queryAndWriteSeries(IReader seriesReader, Path path,
      SingleSeriesExpression seriesFilter, TSDataType dataType,
      Map<String, Long> startTimeMap, Map<String, Long> endTimeMap,
      OverflowSeriesDataSource overflowSeriesDataSource)
      throws IOException {
    int numOfChunk = 0;
    try {
      if (!seriesReader.hasNext()) {
        LOGGER.debug(
            "The time-series {} has no data with the filter {} in the filenode processor {}",
            path, seriesFilter, getProcessorName());
      } else {
        numOfChunk++;
        TimeValuePair timeValuePair = seriesReader.next();
        if (mergeFileWriter == null) {
          mergeBaseDir = directories.getNextFolderForTsfile();
          mergeFileName = timeValuePair.getTimestamp()
              + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis();
          mergeOutputPath = constructOutputFilePath(mergeBaseDir, getProcessorName(),
              mergeFileName);
          mergeFileName = getProcessorName() + File.separatorChar + mergeFileName;
          mergeFileWriter = new TsFileIOWriter(new File(mergeOutputPath));
          mergingModification = new ModificationFile(mergeOutputPath
              + ModificationFile.FILE_SUFFIX);
          mergeDeleteLock.unlock();
        }
        if (!mergeIsChunkGroupHasData) {
          // start a new rowGroupMetadata
          mergeIsChunkGroupHasData = true;
          // the datasize and numOfChunk is fake
          // the accurate datasize and numOfChunk will get after write all this device data.
          mergeFileWriter.startFlushChunkGroup(path.getDevice());// TODO please check me.
          mergeStartPos = mergeFileWriter.getPos();
        }
        // init the serieswWriteImpl
        MeasurementSchema measurementSchema = fileSchema
            .getMeasurementSchema(path.getMeasurement());
        ChunkBuffer pageWriter = new ChunkBuffer(measurementSchema);
        int pageSizeThreshold = TSFileConfig.pageSizeInByte;
        ChunkWriterImpl seriesWriterImpl = new ChunkWriterImpl(measurementSchema, pageWriter,
            pageSizeThreshold);
        // write the series data
        writeOneSeries(path.getDevice(), seriesWriterImpl, dataType,
            seriesReader,
            startTimeMap, endTimeMap, timeValuePair);
        // flushMetadata the series data
        seriesWriterImpl.writeToFileWriter(mergeFileWriter);
      }
    } finally {
      for (OverflowInsertFile overflowInsertFile : overflowSeriesDataSource
          .getOverflowInsertFileList()) {
        FileReaderManager.getInstance()
            .decreaseFileReaderReference(overflowInsertFile.getFilePath(),
                false);
      }
    }
    return numOfChunk;
  }


  private void writeOneSeries(String deviceId, ChunkWriterImpl seriesWriterImpl,
      TSDataType dataType, IReader seriesReader, Map<String, Long> startTimeMap,
      Map<String, Long> endTimeMap, TimeValuePair firstTVPair) throws IOException {
    long startTime;
    long endTime;
    TimeValuePair localTV = firstTVPair;
    writeTVPair(seriesWriterImpl, dataType, localTV);
    startTime = endTime = localTV.getTimestamp();
    if (!startTimeMap.containsKey(deviceId) || startTimeMap.get(deviceId) > startTime) {
      startTimeMap.put(deviceId, startTime);
    }
    if (!endTimeMap.containsKey(deviceId) || endTimeMap.get(deviceId) < endTime) {
      endTimeMap.put(deviceId, endTime);
    }
    while (seriesReader.hasNext()) {
      localTV = seriesReader.next();
      endTime = localTV.getTimestamp();
      writeTVPair(seriesWriterImpl, dataType, localTV);
    }
    if (!endTimeMap.containsKey(deviceId) || endTimeMap.get(deviceId) < endTime) {
      endTimeMap.put(deviceId, endTime);
    }
  }

  private void writeTVPair(ChunkWriterImpl seriesWriterImpl, TSDataType dataType,
      TimeValuePair timeValuePair) throws IOException {
    switch (dataType) {
      case BOOLEAN:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT32:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case INT64:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case FLOAT:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      case DOUBLE:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case TEXT:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      default:
        LOGGER.error("Not support data type: {}", dataType);
        break;
    }
  }


  private String constructOutputFilePath(String baseDir, String processorName, String fileName) {

    String localBaseDir = baseDir;
    if (localBaseDir.charAt(localBaseDir.length() - 1) != File.separatorChar) {
      localBaseDir = localBaseDir + File.separatorChar + processorName;
    }
    File dataDir = new File(localBaseDir);
    if (!dataDir.exists()) {
      LOGGER.warn("The bufferwrite processor data dir doesn't exists, create new directory {}",
          localBaseDir);
      dataDir.mkdirs();
    }
    File outputFile = new File(dataDir, fileName);
    return outputFile.getPath();
  }

  private FileSchema constructFileSchema(String processorName) throws WriteProcessException {

    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = mManager.getSchemaForFileName(processorName);

    FileSchema schema = new FileSchema();
    for (MeasurementSchema measurementSchema : columnSchemaList) {
      schema.registerMeasurement(measurementSchema);
    }
    return schema;

  }

  @Override
  public boolean canBeClosed() {
    if (isMerging != FileNodeProcessorStatus.NONE) {
      LOGGER.info("The filenode {} can't be closed, because the filenode status is {}",
          getProcessorName(),
          isMerging);
      return false;
    }
    if (newMultiPassCount.get() != 0) {
      LOGGER.warn("The filenode {} can't be closed, because newMultiPassCount is {}. The newMultiPassTokenSet is {}",
          getProcessorName(), newMultiPassCount, newMultiPassTokenSet);
      return false;
    }

    if (oldMultiPassCount == null) {
      return true;
    }
    if (oldMultiPassCount.getCount() == 0) {
      return true;
    } else {
      LOGGER.info("The filenode {} can't be closed, because oldMultiPassCount is {}",
          getProcessorName(), oldMultiPassCount.getCount());
      return false;
    }
  }

  @Override
  public FileNodeFlushFuture flush() throws IOException {
    Future<Boolean> bufferWriteFlushFuture = null;
    Future<Boolean> overflowFlushFuture = null;
    if (bufferWriteProcessor != null) {
      bufferWriteFlushFuture = bufferWriteProcessor.flush();
    }
    if (overflowProcessor != null && !overflowProcessor.isClosed()) {
      overflowFlushFuture = overflowProcessor.flush();
    }
    return new FileNodeFlushFuture(bufferWriteFlushFuture, overflowFlushFuture);
  }

  /**
   * Close the bufferwrite processor.
   */
  public Future<Boolean> closeBufferWrite() throws FileNodeProcessorException {
    if (bufferWriteProcessor == null) {
      return new ImmediateFuture<>(true);
    }
    try {
      while (!bufferWriteProcessor.canBeClosed()) {
        waitForBufferWriteClose();
      }
      bufferWriteProcessor.close();
      Future<Boolean> result = bufferWriteProcessor.getCloseFuture();
      closingBufferWriteProcessor.add(bufferWriteProcessor);
      bufferWriteProcessor = null;
      return result;
    } catch (BufferWriteProcessorException e) {
      throw new FileNodeProcessorException(e);
    }
  }



  private void waitForBufferWriteClose() {
    try {
      LOGGER.info("The bufferwrite {} can't be closed, wait 100ms",
          bufferWriteProcessor.getProcessorName());
      TimeUnit.MICROSECONDS.sleep(100);
    } catch (InterruptedException e) {
      LOGGER.error("Unexpected interruption", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Close the overflow processor.
   */
  public void closeOverflow() throws FileNodeProcessorException {
    if (overflowProcessor == null || overflowProcessor.isClosed()) {
      return;
    }
    try {
      while (!overflowProcessor.canBeClosed()) {
        waitForOverflowClose();
      }
      overflowProcessor.close();
    } catch (OverflowProcessorException e) {
      throw new FileNodeProcessorException(e);
    }
  }

  private void waitForOverflowClose() {
    try {
      LOGGER.info("The overflow {} can't be closed, wait 100ms",
          overflowProcessor.getProcessorName());
      TimeUnit.MICROSECONDS.sleep(100);
    } catch (InterruptedException e) {
      LOGGER.error("Unexpected interruption", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() throws FileNodeProcessorException {
    LOGGER.info("Will close FileNode Processor {}.", getProcessorName());
    Future<Boolean> result = closeBufferWrite();
    try {
      result.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new FileNodeProcessorException(e);
    }
    closeOverflow();
    for (TsFileResource fileNode : newFileNodes) {
      if (fileNode.getModFile() != null) {
        try {
          fileNode.getModFile().close();
        } catch (IOException e) {
          throw new FileNodeProcessorException(e);
        }
      }
    }
  }

  /**
   * deregister the filenode processor.
   */
  public void delete() throws ProcessorException {
    if (TsFileDBConf.isEnableStatMonitor()) {
      // remove the monitor
      LOGGER.info("Deregister the filenode processor: {} from monitor.", getProcessorName());
      StatMonitor.getInstance().deregisterStatistics(statStorageDeltaName);
    }
    closeBufferWrite();
    closeOverflow();
    for (TsFileResource fileNode : newFileNodes) {
      if (fileNode.getModFile() != null) {
        try {
          fileNode.getModFile().close();
        } catch (IOException e) {
          throw new FileNodeProcessorException(e);
        }
      }
    }
  }

  @Override
  public long memoryUsage() {
    long memSize = 0;
    if (bufferWriteProcessor != null) {
      memSize += bufferWriteProcessor.memoryUsage();
    }
    if (overflowProcessor != null) {
      memSize += overflowProcessor.memoryUsage();
    }
    return memSize;
  }

  private void writeStoreToDisk(FileNodeProcessorStore fileNodeProcessorStore)
      throws FileNodeProcessorException {

    synchronized (fileNodeRestoreLock) {
      try (FileOutputStream fileOutputStream = new FileOutputStream(fileNodeRestoreFilePath)) {
        fileNodeProcessorStore.serialize(fileOutputStream);
        LOGGER.debug("The filenode processor {} writes restore information to the restore file",
            getProcessorName());
      } catch (IOException e) {
        throw new FileNodeProcessorException(e);
      }
    }
  }

  private FileNodeProcessorStore readStoreFromDisk() throws FileNodeProcessorException {

    synchronized (fileNodeRestoreLock) {
      File restoreFile = new File(fileNodeRestoreFilePath);
      if (!restoreFile.exists() || restoreFile.length() == 0) {
        try {
          return new FileNodeProcessorStore(false, new HashMap<>(),
              new TsFileResource(null, false),
              new ArrayList<>(), FileNodeProcessorStatus.NONE, 0);
        } catch (IOException e) {
          throw new FileNodeProcessorException(e);
        }
      }
      try (FileInputStream inputStream = new FileInputStream(fileNodeRestoreFilePath)) {
        return FileNodeProcessorStore.deSerialize(inputStream);
      } catch (IOException e) {
        LOGGER
            .error("Failed to deserialize the FileNodeRestoreFile {}, {}", fileNodeRestoreFilePath,
                e);
        throw new FileNodeProcessorException(e);
      }
    }
  }

  String getFileNodeRestoreFilePath() {
    return fileNodeRestoreFilePath;
  }

  /**
   * Delete data whose timestamp <= 'timestamp' and belong to timeseries deviceId.measurementId.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the delete range is (0, timestamp].
   */
  public void delete(String deviceId, String measurementId, long timestamp) throws IOException {
    // TODO: how to avoid partial deletion?
    mergeDeleteLock.lock();
    long version = versionController.nextVersion();

    // record what files are updated so we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();

    try {
      String fullPath = deviceId +
          IoTDBConstant.PATH_SEPARATOR + measurementId;
      Deletion deletion = new Deletion(fullPath, version, timestamp);
      if (mergingModification != null) {
        mergingModification.write(deletion);
        updatedModFiles.add(mergingModification);
      }
      deleteBufferWriteFiles(deviceId, deletion, updatedModFiles);
      // delete data in memory
      OverflowProcessor ofProcessor = getOverflowProcessor(getProcessorName());
      ofProcessor.delete(deviceId, measurementId, timestamp, version, updatedModFiles);
      if (bufferWriteProcessor != null) {
        bufferWriteProcessor.delete(deviceId, measurementId, timestamp);
      }
    } catch (Exception e) {
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
      }
      throw new IOException(e);
    } finally {
      mergeDeleteLock.unlock();
    }
  }

  private void deleteBufferWriteFiles(String deviceId, Deletion deletion,
      List<ModificationFile> updatedModFiles) throws IOException {
    BufferWriteProcessor bufferWriteProcessor = getBufferWriteProcessor();
    TsFileResource resource = null;
    if (bufferWriteProcessor != null) {
      //bufferWriteProcessor == null means the bufferWriteProcessor is closed now.
      resource = bufferWriteProcessor.getCurrentTsFileResource();
      if (resource != null && resource.containsDevice(deviceId)) {
        resource.getModFile().write(deletion);
        updatedModFiles.add(resource.getModFile());
      }
    }

    for (TsFileResource fileNode : newFileNodes) {
      if (fileNode != resource && fileNode.containsDevice(deviceId)
          && fileNode.getStartTime(deviceId) <= deletion.getTimestamp()) {
        fileNode.getModFile().write(deletion);
        updatedModFiles.add(fileNode.getModFile());
      }
    }
  }

  /**
   * Similar to delete(), but only deletes data in BufferWrite. Only used by WAL recovery.
   */
  public void deleteBufferWrite(String deviceId, String measurementId, long timestamp)
      throws IOException, BufferWriteProcessorException {
    String fullPath = deviceId +
        IoTDBConstant.PATH_SEPARATOR + measurementId;
    long version = versionController.nextVersion();
    Deletion deletion = new Deletion(fullPath, version, timestamp);

    List<ModificationFile> updatedModFiles = new ArrayList<>();
    try {
      deleteBufferWriteFiles(deviceId, deletion, updatedModFiles);
    } catch (IOException e) {
      for (ModificationFile modificationFile : updatedModFiles) {
        modificationFile.abort();
      }
      throw e;
    }
    if (bufferWriteProcessor != null) {
      try {
        bufferWriteProcessor.delete(deviceId, measurementId, timestamp);
      } catch (BufferWriteProcessorException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Similar to delete(), but only deletes data in Overflow. Only used by WAL recovery.
   */
  public void deleteOverflow(String deviceId, String measurementId, long timestamp)
      throws ProcessorException {
    long version = versionController.nextVersion();

    OverflowProcessor overflowProcessor = getOverflowProcessor(getProcessorName());
    List<ModificationFile> updatedModFiles = new ArrayList<>();
    try {
      overflowProcessor.delete(deviceId, measurementId, timestamp, version, updatedModFiles);
    } catch (IOException e) {
      for (ModificationFile modificationFile : updatedModFiles) {
        try {
          modificationFile.abort();
        } catch (IOException e1) {
          throw new ProcessorException(e);
        }
      }
      throw new ProcessorException(e);
    }
  }

  public CopyOnWriteLinkedList<BufferWriteProcessor> getClosingBufferWriteProcessor() {
    for (BufferWriteProcessor processor: closingBufferWriteProcessor.read()) {
      if (processor.isClosed()) {
        closingBufferWriteProcessor.remove(processor);
      }
    }
    closingBufferWriteProcessor.reset();
    return closingBufferWriteProcessor;
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
    FileNodeProcessor that = (FileNodeProcessor) o;
    return isOverflowed == that.isOverflowed &&
        numOfMergeFile == that.numOfMergeFile &&
        lastMergeTime == that.lastMergeTime &&
        multiPassLockToken == that.multiPassLockToken &&
        Objects.equals(statStorageDeltaName, that.statStorageDeltaName) &&
        Objects.equals(statParamsHashMap, that.statParamsHashMap) &&
        Objects.equals(lastUpdateTimeMap, that.lastUpdateTimeMap) &&
        Objects.equals(flushLastUpdateTimeMap, that.flushLastUpdateTimeMap) &&
        Objects.equals(invertedIndexOfFiles, that.invertedIndexOfFiles) &&
        Objects.equals(emptyTsFileResource, that.emptyTsFileResource) &&
        Objects.equals(newFileNodes, that.newFileNodes) &&
        isMerging == that.isMerging &&
        Objects.equals(fileNodeProcessorStore, that.fileNodeProcessorStore) &&
        Objects.equals(fileNodeRestoreFilePath, that.fileNodeRestoreFilePath) &&
        Objects.equals(bufferWriteProcessor, that.bufferWriteProcessor) &&
        Objects.equals(overflowProcessor, that.overflowProcessor) &&
        Objects.equals(oldMultiPassTokenSet, that.oldMultiPassTokenSet) &&
        Objects.equals(newMultiPassTokenSet, that.newMultiPassTokenSet) &&
        Objects.equals(oldMultiPassCount, that.oldMultiPassCount) &&
        Objects.equals(newMultiPassCount, that.newMultiPassCount) &&
        Objects.equals(parameters, that.parameters) &&
        Objects.equals(fileSchema, that.fileSchema) &&
        Objects.equals(fileNodeFlushAction, that.fileNodeFlushAction) &&
        Objects.equals(bufferwriteFlushAction, that.bufferwriteFlushAction) &&
        Objects.equals(overflowFlushAction, that.overflowFlushAction);
  }

  @Override
  public int hashCode() {
    return processorName.hashCode();
  }

  public class MergeRunnale implements Runnable {

    @Override
    public void run() {
      try {
        ZoneId zoneId = IoTDBDescriptor.getInstance().getConfig().getZoneID();
        long mergeStartTime = System.currentTimeMillis();
        merge();
        long mergeEndTime = System.currentTimeMillis();
        long intervalTime = mergeEndTime - mergeStartTime;
        LOGGER.info(
            "The filenode processor {} merge start time is {}, "
                + "merge end time is {}, merge consumes {}ms.",
            getProcessorName(), ofInstant(Instant.ofEpochMilli(mergeStartTime),
                zoneId), ofInstant(Instant.ofEpochMilli(mergeEndTime),
                zoneId), intervalTime);
      } catch (FileNodeProcessorException e) {
        LOGGER.error("The filenode processor {} encountered an error when merging.",
            getProcessorName(), e);
        throw new ErrorDebugException(e);
      }
    }
  }

  /**
   * wait for all closing processors finishing their tasks
   */
  public void waitforAllClosed() throws FileNodeProcessorException {
    close();
    while (getClosingBufferWriteProcessor().size() != 0) {
      checkAllClosingProcessors();
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOGGER.error("Filenode Processor {} is interrupted when waiting for all closed.", processorName, e);
      }
    }
  }


  void checkAllClosingProcessors() {
    Iterator<BufferWriteProcessor> iterator =
        this.getClosingBufferWriteProcessor().iterator();
    while (iterator.hasNext()) {
      BufferWriteProcessor processor = iterator.next();
      try {
        if (processor.getCloseFuture().get(10, TimeUnit.MILLISECONDS)) {
          //if finished, we can remove it.
          iterator.remove();
        }
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Close bufferwrite processor {} failed.", processor.getProcessorName(), e);
      } catch (TimeoutException e) {
        //do nothing.
      }
    }
    this.getClosingBufferWriteProcessor().reset();
  }
}