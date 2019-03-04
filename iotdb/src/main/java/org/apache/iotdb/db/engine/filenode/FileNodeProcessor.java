/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.filenode;

import java.io.File;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
import org.apache.iotdb.db.engine.pool.MergeManager;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
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
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.FileSchemaUtils;
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
import org.apache.iotdb.tsfile.write.schema.JsonConverter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.ZonedDateTime.ofInstant;

public class FileNodeProcessor extends Processor implements IStatistic {

  private static final String WARN_NO_SUCH_OVERFLOWED_FILE = "Can not find any tsfile which"
      + " will be overflowed in the filenode processor {}, ";
  private static final String RESTORE_FILE_SUFFIX = ".restore";
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
  private Map<String, List<IntervalFileNode>> invertedIndexOfFiles;
  private IntervalFileNode emptyIntervalFileNode;
  private IntervalFileNode currentIntervalFileNode;
  private List<IntervalFileNode> newFileNodes;
  private FileNodeProcessorStatus isMerging;
  // this is used when work->merge operation
  private int numOfMergeFile;
  private FileNodeProcessorStore fileNodeProcessorStore;
  private String fileNodeRestoreFilePath;
  private final Object fileNodeRestoreLock = new Object();
  private String baseDirPath;
  // last merge time
  private long lastMergeTime = -1;
  private BufferWriteProcessor bufferWriteProcessor = null;
  private OverflowProcessor overflowProcessor = null;
  private Set<Integer> oldMultiPassTokenSet = null;
  private Set<Integer> newMultiPassTokenSet = new HashSet<>();
  private ReadWriteLock oldMultiPassLock = null;
  private ReadWriteLock newMultiPassLock = new ReentrantReadWriteLock(false);
  // system recovery
  private boolean shouldRecovery = false;
  // statistic monitor parameters
  private Map<String, Action> parameters;
  private FileSchema fileSchema;
  private Action flushFileNodeProcessorAction = () -> {
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

  private Action bufferwriteCloseAction = new Action() {

    @Override
    public void act() {
      synchronized (fileNodeProcessorStore) {
        fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
        addLastTimeToIntervalFile();
        fileNodeProcessorStore.setNewFileNodes(newFileNodes);
      }
    }

    private void addLastTimeToIntervalFile() {

      if (!newFileNodes.isEmpty()) {
        // end time with one start time
        Map<String, Long> endTimeMap = new HashMap<>();
        for (Entry<String, Long> startTime : currentIntervalFileNode.getStartTimeMap().entrySet()) {
          String deviceId = startTime.getKey();
          endTimeMap.put(deviceId, lastUpdateTimeMap.get(deviceId));
        }
        currentIntervalFileNode.setEndTimeMap(endTimeMap);
      }
    }
  };
  private Action overflowFlushAction = () -> {

    // update the new IntervalFileNode List and emptyIntervalFile.
    // Notice: thread safe
    synchronized (fileNodeProcessorStore) {
      fileNodeProcessorStore.setOverflowed(isOverflowed);
      fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
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
        MonitorConstants.STAT_STORAGE_GROUP_PREFIX + MonitorConstants.MONITOR_PATH_SEPERATOR
            + MonitorConstants.FILE_NODE_PATH + MonitorConstants.MONITOR_PATH_SEPERATOR
            + processorName.replaceAll("\\.", "_");

    this.parameters = new HashMap<>();
    String dirPath = fileNodeDirPath;
    if (dirPath.length() > 0
        && dirPath.charAt(dirPath.length() - 1) != File.separatorChar) {
      dirPath = dirPath + File.separatorChar;
    }
    this.baseDirPath = dirPath + processorName;
    File dataDir = new File(this.baseDirPath);
    if (!dataDir.exists()) {
      dataDir.mkdirs();
      LOGGER.info(
          "The data directory of the filenode processor {} doesn't exist. Create new " +
              "directory {}",
          getProcessorName(), baseDirPath);
    }
    fileNodeRestoreFilePath = new File(dataDir, processorName + RESTORE_FILE_SUFFIX).getPath();
    try {
      fileNodeProcessorStore = readStoreFromDisk();
    } catch (FileNodeProcessorException e) {
      LOGGER.error(
          "The fileNode processor {} encountered an error when recoverying restore " +
              "information.",
          processorName, e);
      throw new FileNodeProcessorException(e);
    }
    // TODO deep clone the lastupdate time
    lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
    emptyIntervalFileNode = fileNodeProcessorStore.getEmptyIntervalFileNode();
    newFileNodes = fileNodeProcessorStore.getNewFileNodes();
    isMerging = fileNodeProcessorStore.getFileNodeProcessorStatus();
    numOfMergeFile = fileNodeProcessorStore.getNumOfMergeFile();
    invertedIndexOfFiles = new HashMap<>();
    // deep clone
    flushLastUpdateTimeMap = new HashMap<>();
    for (Entry<String, Long> entry : lastUpdateTimeMap.entrySet()) {
      flushLastUpdateTimeMap.put(entry.getKey(), entry.getValue() + 1);
    }
    // construct the fileschema
    try {
      this.fileSchema = constructFileSchema(processorName);
    } catch (WriteProcessException e) {
      throw new FileNodeProcessorException(e);
    }
    // status is not NONE, or the last intervalFile is not closed
    if (isMerging != FileNodeProcessorStatus.NONE
        || (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed())) {
      shouldRecovery = true;
    } else {
      // add file into the index of file
      addAllFileIntoIndex(newFileNodes);
    }
    // RegistStatService
    if (TsFileDBConf.enableStatMonitor) {
      StatMonitor statMonitor = StatMonitor.getInstance();
      registStatMetadata();
      statMonitor.registStatistics(statStorageDeltaName, this);
    }
    try {
      versionController = new SimpleFileVersionController(fileNodeDirPath);
    } catch (IOException e) {
      throw new FileNodeProcessorException(e);
    }
  }

  @Override
  public Map<String, AtomicLong> getStatParamsHashMap() {
    return statParamsHashMap;
  }

  @Override
  public void registStatMetadata() {
    Map<String, String> hashMap = new HashMap<>();
    for (MonitorConstants.FileNodeProcessorStatConstants statConstant :
        MonitorConstants.FileNodeProcessorStatConstants.values()) {
      hashMap
          .put(statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name(),
              MonitorConstants.DATA_TYPE);
    }
    StatMonitor.getInstance().registStatStorageGroup(hashMap);
  }

  @Override
  public List<String> getAllPathForStatistic() {
    List<String> list = new ArrayList<>();
    for (MonitorConstants.FileNodeProcessorStatConstants statConstant :
        MonitorConstants.FileNodeProcessorStatConstants.values()) {
      list.add(
          statStorageDeltaName + MonitorConstants.MONITOR_PATH_SEPERATOR + statConstant.name());
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
  void addIntervalFileNode(String baseDir, String fileName) throws ActionException {

    IntervalFileNode intervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, baseDir,
        fileName);
    this.currentIntervalFileNode = intervalFileNode;
    newFileNodes.add(intervalFileNode);
    fileNodeProcessorStore.setNewFileNodes(newFileNodes);
    flushFileNodeProcessorAction.act();
  }

  /**
   * set interval filenode start time.
   *
   * @param deviceId device ID
   */
  void setIntervalFileNodeStartTime(String deviceId) {
    if (currentIntervalFileNode.getStartTime(deviceId) == -1) {
      currentIntervalFileNode.setStartTime(deviceId, flushLastUpdateTimeMap.get(deviceId));
      if (!invertedIndexOfFiles.containsKey(deviceId)) {
        invertedIndexOfFiles.put(deviceId, new ArrayList<>());
      }
      invertedIndexOfFiles.get(deviceId).add(currentIntervalFileNode);
    }
  }

  /**
   * clear filenode.
   */
  public void clearFileNode() {
    isOverflowed = false;
    emptyIntervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, null);
    newFileNodes = new ArrayList<>();
    isMerging = FileNodeProcessorStatus.NONE;
    numOfMergeFile = 0;
    fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
    fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
    fileNodeProcessorStore.setNewFileNodes(newFileNodes);
    fileNodeProcessorStore.setNumOfMergeFile(numOfMergeFile);
    fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
  }

  private void addAllFileIntoIndex(List<IntervalFileNode> fileList) {
    // clear map
    invertedIndexOfFiles.clear();
    // add all file to index
    for (IntervalFileNode fileNode : fileList) {
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

  public boolean shouldRecovery() {
    return shouldRecovery;
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
  public void fileNodeRecovery() throws FileNodeProcessorException {
    // restore bufferwrite
    if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()) {
      //
      // add the current file
      //
      currentIntervalFileNode = newFileNodes.get(newFileNodes.size() - 1);

      // this bufferwrite file is not close by normal operation
      String damagedFilePath = newFileNodes.get(newFileNodes.size() - 1).getFilePath();
      String[] fileNames = damagedFilePath.split("\\" + File.separator);
      // all information to recovery the damaged file.
      // contains file seriesPath, action parameters and processorName
      parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
      parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
      parameters
          .put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
      String baseDir = directories
          .getTsFileFolder(newFileNodes.get(newFileNodes.size() - 1).getBaseDirIndex());
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "The filenode processor {} will recovery the bufferwrite processor, "
                + "the bufferwrite file is {}",
            getProcessorName(), fileNames[fileNames.length - 1]);
      }

      try {
        bufferWriteProcessor = new BufferWriteProcessor(baseDir, getProcessorName(),
            fileNames[fileNames.length - 1], parameters, versionController, fileSchema);
      } catch (BufferWriteProcessorException e) {
        LOGGER.error(
            "The filenode processor {} failed to recovery the bufferwrite processor, "
                + "the last bufferwrite file is {}.",
            getProcessorName(), fileNames[fileNames.length - 1]);
        throw new FileNodeProcessorException(e);
      }
    }
    // restore the overflow processor
    LOGGER.info("The filenode processor {} will recovery the overflow processor.",
        getProcessorName());
    parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
    parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
    try {
      overflowProcessor = new OverflowProcessor(getProcessorName(), parameters, fileSchema,
          versionController);
    } catch (IOException e) {
      LOGGER.error("The filenode processor {} failed to recovery the overflow processor.",
          getProcessorName());
      throw new FileNodeProcessorException(e);
    }

    shouldRecovery = false;

    if (isMerging == FileNodeProcessorStatus.MERGING_WRITE) {
      // re-merge all file
      // if bufferwrite processor is not null, and close
      LOGGER.info("The filenode processor {} is recovering, the filenode status is {}.",
          getProcessorName(), isMerging);
      merge();
    } else if (isMerging == FileNodeProcessorStatus.WAITING) {
      // unlock
      LOGGER.info("The filenode processor {} is recovering, the filenode status is {}.",
          getProcessorName(), isMerging);
      //writeUnlock();
      switchWaitingToWorking();
    } else {
      //writeUnlock();
    }
    // add file into index of file
    addAllFileIntoIndex(newFileNodes);
  }

  /**
   * get buffer write processor by processor name and insert time.
   */
  public BufferWriteProcessor getBufferWriteProcessor(String processorName, long insertTime)
      throws FileNodeProcessorException {
    if (bufferWriteProcessor == null) {
      Map<String, Action> params = new HashMap<>();
      params.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bufferwriteFlushAction);
      params.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bufferwriteCloseAction);
      params
          .put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
      String baseDir = directories.getNextFolderForTsfile();
      LOGGER.info("Allocate folder {} for the new bufferwrite processor.", baseDir);
      // construct processor or restore
      try {
        bufferWriteProcessor = new BufferWriteProcessor(baseDir, processorName,
            insertTime + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR
                + System.currentTimeMillis(),
            params, versionController, fileSchema);
      } catch (BufferWriteProcessorException e) {
        LOGGER.error("The filenode processor {} failed to get the bufferwrite processor.",
            processorName, e);
        throw new FileNodeProcessorException(e);
      }
    }
    return bufferWriteProcessor;
  }

  /**
   * get buffer write processor.
   */
  public BufferWriteProcessor getBufferWriteProcessor() throws FileNodeProcessorException {
    if (bufferWriteProcessor == null) {
      LOGGER.error("The bufferwrite processor is null when get the bufferwriteProcessor");
      throw new FileNodeProcessorException("The bufferwrite processor is null");
    }
    return bufferWriteProcessor;
  }

  /**
   * get overflow processor by processor name.
   */
  public OverflowProcessor getOverflowProcessor(String processorName) throws IOException {
    if (overflowProcessor == null) {
      Map<String, Action> params = new HashMap<>();
      // construct processor or restore
      params.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowFlushAction);
      params
          .put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, flushFileNodeProcessorAction);
      overflowProcessor = new OverflowProcessor(processorName, params, fileSchema,
          versionController);
    }
    return overflowProcessor;
  }

  /**
   * get overflow processor.
   */
  public OverflowProcessor getOverflowProcessor() {
    if (overflowProcessor == null) {
      LOGGER.error("The overflow processor is null when getting the overflowProcessor");
    }
    return overflowProcessor;
  }

  public boolean hasOverflowProcessor() {
    return overflowProcessor != null;
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
   * get flush last update time.
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
      emptyIntervalFileNode.setStartTime(deviceId, 0L);
      emptyIntervalFileNode.setEndTime(deviceId, getLastUpdateTime(deviceId));
      emptyIntervalFileNode.changeTypeToChanged(isMerging);
    } else {
      List<IntervalFileNode> temp = invertedIndexOfFiles.get(deviceId);
      int index = searchIndexNodeByTimestamp(deviceId, timestamp, temp);
      changeTypeToChanged(temp.get(index), deviceId);
    }
  }

  private void changeTypeToChanged(IntervalFileNode fileNode, String deviceId) {
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
      emptyIntervalFileNode.setStartTime(deviceId, 0L);
      emptyIntervalFileNode.setEndTime(deviceId, getLastUpdateTime(deviceId));
      emptyIntervalFileNode.changeTypeToChanged(isMerging);
    } else {
      List<IntervalFileNode> temp = invertedIndexOfFiles.get(deviceId);
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
      emptyIntervalFileNode.setStartTime(deviceId, 0L);
      emptyIntervalFileNode.setEndTime(deviceId, getLastUpdateTime(deviceId));
      emptyIntervalFileNode.changeTypeToChanged(isMerging);
    } else {
      List<IntervalFileNode> temp = invertedIndexOfFiles.get(deviceId);
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
      List<IntervalFileNode> fileList) {
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
  public int addMultiPassLock() {
    LOGGER.debug("Add MultiPassLock: read lock newMultiPassLock.");
    newMultiPassLock.readLock().lock();
    while (newMultiPassTokenSet.contains(multiPassLockToken)) {
      multiPassLockToken++;
    }
    newMultiPassTokenSet.add(multiPassLockToken);
    LOGGER.debug("Add multi token:{}, nsPath:{}.", multiPassLockToken, getProcessorName());
    return multiPassLockToken;
  }

  /**
   * remove multiple pass lock. TODO: use the return value or remove it.
   */
  public boolean removeMultiPassLock(int token) {
    if (newMultiPassTokenSet.contains(token)) {
      newMultiPassLock.readLock().unlock();
      newMultiPassTokenSet.remove(token);
      LOGGER.debug("Remove multi token:{}, nspath:{}, new set:{}, lock:{}", token,
              getProcessorName(),
              newMultiPassTokenSet, newMultiPassLock);
      return true;
    } else if (oldMultiPassTokenSet != null && oldMultiPassTokenSet.contains(token)) {
      // remove token first, then unlock
      oldMultiPassLock.readLock().unlock();
      oldMultiPassTokenSet.remove(token);
      LOGGER.debug("Remove multi token:{}, old set:{}, lock:{}", token, oldMultiPassTokenSet,
          oldMultiPassLock);
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
    List<IntervalFileNode> bufferwriteDataInFiles = new ArrayList<>();
    for (IntervalFileNode intervalFileNode : newFileNodes) {
      // add the same intervalFileNode, but not the same reference
      if (intervalFileNode.isClosed()) {
        bufferwriteDataInFiles.add(intervalFileNode.backUp());
      }
    }
    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> bufferwritedata = new Pair<>(null, null);
    // bufferwrite data
    UnsealedTsFile unsealedTsFile = null;

    if (!newFileNodes.isEmpty() && !newFileNodes.get(newFileNodes.size() - 1).isClosed()
        && !newFileNodes.get(newFileNodes.size() - 1).getStartTimeMap().isEmpty()) {
      unsealedTsFile = new UnsealedTsFile();
      unsealedTsFile.setFilePath(newFileNodes.get(newFileNodes.size() - 1).getFilePath());
      if (bufferWriteProcessor == null) {
        LOGGER.error(
            "The last of tsfile {} in filenode processor {} is not closed, "
                + "but the bufferwrite processor is null.",
            newFileNodes.get(newFileNodes.size() - 1).getRelativePath(), getProcessorName());
        throw new FileNodeProcessorException(String.format(
            "The last of tsfile %s in filenode processor %s is not closed, "
                + "but the bufferwrite processor is null.",
            newFileNodes.get(newFileNodes.size() - 1).getRelativePath(), getProcessorName()));
      }
      bufferwritedata = bufferWriteProcessor
          .queryBufferWriteData(deviceId, measurementId, dataType, mSchema.getProps());

      try {
        List<Modification> pathModifications = context.getPathModifications(
            currentIntervalFileNode.getModFile(), deviceId
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
  public void appendFile(IntervalFileNode appendFile, String appendFilePath)
      throws FileNodeProcessorException {
    try {
      if (!new File(appendFile.getFilePath()).getParentFile().exists()) {
        new File(appendFile.getFilePath()).getParentFile().mkdirs();
      }
      // move file
      File originFile = new File(appendFilePath);
      File targetFile = new File(appendFile.getFilePath());
      if (!originFile.exists()) {
        throw new FileNodeProcessorException(
            String.format("The appended file %s does not exist.", appendFilePath));
      }
      if (targetFile.exists()) {
        throw new FileNodeProcessorException(
            String.format("The appended target file %s already exists.",
                appendFile.getFilePath()));
      }
      if (!originFile.renameTo(targetFile)) {
        LOGGER.warn("File renaming failed when appending new file. Origin: {}, target: {}",
            originFile.getPath(),
            targetFile.getPath());
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
      flushFileNodeProcessorAction.act();
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
  public List<String> getOverlapFiles(IntervalFileNode appendFile, String uuid)
      throws FileNodeProcessorException {
    List<String> overlapFiles = new ArrayList<>();
    try {
      for (IntervalFileNode intervalFileNode : newFileNodes) {
        getOverlapFiles(appendFile, intervalFileNode, uuid, overlapFiles);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to get overlap tsfiles which conflict with the appendFile.");
      throw new FileNodeProcessorException(e);
    }
    return overlapFiles;
  }

  private void getOverlapFiles(IntervalFileNode appendFile, IntervalFileNode intervalFileNode,
      String uuid, List<String> overlapFiles) throws IOException {
    for (Entry<String, Long> entry : appendFile.getStartTimeMap().entrySet()) {
      if (intervalFileNode.getStartTimeMap().containsKey(entry.getKey()) &&
          intervalFileNode.getEndTime(entry.getKey()) >= entry.getValue()
          && intervalFileNode.getStartTime(entry.getKey()) <= appendFile
          .getEndTime(entry.getKey())) {
        String relativeFilePath = "postback" + File.separator + uuid + File.separator + "backup"
            + File.separator + intervalFileNode.getRelativePath();
        File newFile = new File(
            Directories.getInstance().getTsFileFolder(intervalFileNode.getBaseDirIndex()),
            relativeFilePath);
        if (!newFile.getParentFile().exists()) {
          newFile.getParentFile().mkdirs();
        }
        java.nio.file.Path link = FileSystems.getDefault().getPath(newFile.getPath());
        java.nio.file.Path target = FileSystems.getDefault()
            .getPath(intervalFileNode.getFilePath());
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

    if (overflowProcessor != null) {
      if (overflowProcessor.getFileSize() < IoTDBDescriptor.getInstance()
          .getConfig().overflowFileSizeThreshold) {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Skip this merge taks submission, because the size{} of overflow processor {} "
                  + "does not reaches the threshold {}.",
              MemUtils.bytesCntToStr(overflowProcessor.getFileSize()), getProcessorName(),
              MemUtils.bytesCntToStr(
                  IoTDBDescriptor.getInstance().getConfig().overflowFileSizeThreshold));
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
      return MergeManager.getInstance().submit(mergeThread);
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
      closeBufferWrite();
      // try to get overflow processor
      getOverflowProcessor(getProcessorName());
      // must close the overflow processor
      while (!getOverflowProcessor().canBeClosed()) {
        waitForClosing();
      }
      LOGGER.info("The filenode processor {} prepares for merge, closes the overflow processor",
          getProcessorName());
      getOverflowProcessor().close();
    } catch (FileNodeProcessorException | OverflowProcessorException | IOException e) {
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
    prepareForMerge();
    // change status from overflowed to no overflowed
    isOverflowed = false;
    // change status from work to merge
    isMerging = FileNodeProcessorStatus.MERGING_WRITE;
    // check the empty file
    Map<String, Long> startTimeMap = emptyIntervalFileNode.getStartTimeMap();
    mergeCheckEmptyFile(startTimeMap);

    for (IntervalFileNode intervalFileNode : newFileNodes) {
      if (intervalFileNode.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
        intervalFileNode.setOverflowChangeType(OverflowChangeType.CHANGED);
      }
    }

    addAllFileIntoIndex(newFileNodes);
    synchronized (fileNodeProcessorStore) {
      fileNodeProcessorStore.setOverflowed(isOverflowed);
      fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
      fileNodeProcessorStore.setNewFileNodes(newFileNodes);
      fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
      // flush this filenode information
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
    List<IntervalFileNode> backupIntervalFiles;

    backupIntervalFiles = switchFileNodeToMerge();
    //
    // clear empty file
    //
    boolean needEmtpy = false;
    if (emptyIntervalFileNode.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
      needEmtpy = true;
    }
    emptyIntervalFileNode.clear();
    // attention
    try {
      overflowProcessor.switchWorkToMerge();
    } catch (IOException e) {
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
    for (IntervalFileNode backupIntervalFile : backupIntervalFiles) {
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
    if (emptyIntervalFileNode.getOverflowChangeType() == OverflowChangeType.NO_CHANGE) {
      return;
    }
    Iterator<Entry<String, Long>> iterator = emptyIntervalFileNode.getEndTimeMap().entrySet()
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
    if (emptyIntervalFileNode.checkEmpty()) {
      emptyIntervalFileNode.clear();
    } else {
      if (!newFileNodes.isEmpty()) {
        IntervalFileNode first = newFileNodes.get(0);
        for (String deviceId : emptyIntervalFileNode.getStartTimeMap().keySet()) {
          first.setStartTime(deviceId, emptyIntervalFileNode.getStartTime(deviceId));
          first.setEndTime(deviceId, emptyIntervalFileNode.getEndTime(deviceId));
          first.setOverflowChangeType(OverflowChangeType.CHANGED);
        }
        emptyIntervalFileNode.clear();
      } else {
        emptyIntervalFileNode.setOverflowChangeType(OverflowChangeType.CHANGED);
      }
    }
  }

  private List<IntervalFileNode> switchFileNodeToMerge() throws FileNodeProcessorException {
    List<IntervalFileNode> result = new ArrayList<>();
    if (emptyIntervalFileNode.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
      // add empty
      result.add(emptyIntervalFileNode.backUp());
      if (!newFileNodes.isEmpty()) {
        throw new FileNodeProcessorException(
            String.format("The status of empty file is %s, but the new file list is not empty",
                emptyIntervalFileNode.getOverflowChangeType()));
      }
      return result;
    }
    if (newFileNodes.isEmpty()) {
      LOGGER.error("No file was changed when merging, the filenode is {}", getProcessorName());
      throw new FileNodeProcessorException(
          "No file was changed when merging, the filenode is " + getProcessorName());
    }
    for (IntervalFileNode intervalFileNode : newFileNodes) {
      updateFileNode(intervalFileNode, result);
    }
    return result;
  }

  private void updateFileNode(IntervalFileNode intervalFileNode, List<IntervalFileNode> result) {
    if (intervalFileNode.getOverflowChangeType() == OverflowChangeType.NO_CHANGE) {
      result.add(intervalFileNode.backUp());
    } else {
      Map<String, Long> startTimeMap = new HashMap<>();
      Map<String, Long> endTimeMap = new HashMap<>();
      for (String deviceId : intervalFileNode.getEndTimeMap().keySet()) {
        List<IntervalFileNode> temp = invertedIndexOfFiles.get(deviceId);
        int index = temp.indexOf(intervalFileNode);
        int size = temp.size();
        // start time
        if (index == 0) {
          startTimeMap.put(deviceId, 0L);
        } else {
          startTimeMap.put(deviceId, intervalFileNode.getStartTime(deviceId));
        }
        // end time
        if (index < size - 1) {
          endTimeMap.put(deviceId, temp.get(index + 1).getStartTime(deviceId) - 1);
        } else {
          endTimeMap.put(deviceId, intervalFileNode.getEndTime(deviceId));
        }
      }
      IntervalFileNode node = new IntervalFileNode(startTimeMap, endTimeMap,
          intervalFileNode.getOverflowChangeType(), intervalFileNode.getBaseDirIndex(),
          intervalFileNode.getRelativePath());
      result.add(node);
    }
  }

  private void switchMergeToWaiting(List<IntervalFileNode> backupIntervalFiles, boolean needEmpty)
      throws FileNodeProcessorException {
    LOGGER.info("The status of filenode processor {} switches from {} to {}.", getProcessorName(),
        FileNodeProcessorStatus.MERGING_WRITE, FileNodeProcessorStatus.WAITING);
    writeLock();
    try {
      oldMultiPassTokenSet = newMultiPassTokenSet;
      oldMultiPassLock = newMultiPassLock;
      newMultiPassTokenSet = new HashSet<>();
      newMultiPassLock = new ReentrantReadWriteLock(false);
      List<IntervalFileNode> result = new ArrayList<>();
      int beginIndex = 0;
      if (needEmpty) {
        IntervalFileNode empty = backupIntervalFiles.get(0);
        if (!empty.checkEmpty()) {
          updateEmpty(empty, result);
          beginIndex++;
        }
      }
      // reconstruct the file index
      addAllFileIntoIndex(backupIntervalFiles);
      // check the merge changed file
      for (int i = beginIndex; i < backupIntervalFiles.size(); i++) {
        IntervalFileNode newFile = newFileNodes.get(i - beginIndex);
        IntervalFileNode temp = backupIntervalFiles.get(i);
        if (newFile.getOverflowChangeType() == OverflowChangeType.MERGING_CHANGE) {
          updateMergeChanged(newFile, temp);
        }
        if (!temp.checkEmpty()) {
          result.add(temp);
        }
      }
      // add new file when merge
      for (int i = backupIntervalFiles.size() - beginIndex; i < newFileNodes.size(); i++) {
        IntervalFileNode fileNode = newFileNodes.get(i);
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
      for (IntervalFileNode fileNode : newFileNodes) {
        fileNode.clearMergeChanged();
      }

      synchronized (fileNodeProcessorStore) {
        fileNodeProcessorStore.setFileNodeProcessorStatus(isMerging);
        fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
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

  private void updateEmpty(IntervalFileNode empty, List<IntervalFileNode> result) {
    for (String deviceId : empty.getStartTimeMap().keySet()) {
      if (invertedIndexOfFiles.containsKey(deviceId)) {
        IntervalFileNode temp = invertedIndexOfFiles.get(deviceId).get(0);
        if (temp.getMergeChanged().contains(deviceId)) {
          empty.setOverflowChangeType(OverflowChangeType.CHANGED);
          break;
        }
      }
    }
    empty.clearMergeChanged();
    result.add(empty.backUp());
  }

  private void updateMergeChanged(IntervalFileNode newFile, IntervalFileNode temp) {
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

    if (oldMultiPassLock != null) {
      LOGGER.info("The old Multiple Pass Token set is {}, the old Multiple Pass Lock is {}",
          oldMultiPassTokenSet,
          oldMultiPassLock);
      oldMultiPassLock.writeLock().lock();
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
              newFileNodes.get(newFileNodes.size() - 1).getFilePath() + RESTORE_FILE_SUFFIX;
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
          fileNodeProcessorStore.setEmptyIntervalFileNode(emptyIntervalFileNode);
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
      if (oldMultiPassLock != null) {
        oldMultiPassLock.writeLock().unlock();
      }
      oldMultiPassLock = null;
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
    for (IntervalFileNode bufferFileNode : newFileNodes) {
      String bufferFilePath = bufferFileNode.getFilePath();
      if (bufferFilePath != null) {
        bufferFiles.add(bufferFilePath);
      }
    }
  }

  private void deleteBufferWriteFiles(List<File> bufferwriteDirList, Set<String> bufferFiles) {
    for (File bufferwriteDir : bufferwriteDirList) {
      File[] files = bufferwriteDir.listFiles();
      if (files == null) {
        continue;
      }
      for (File file : files) {
        if (!bufferFiles.contains(file.getPath()) && !file.delete()) {
          LOGGER.warn("Cannot delete BufferWrite file {}", file.getPath());
        }
      }
    }
  }

  private void changeFileNodes() {
    for (IntervalFileNode fileNode : newFileNodes) {
      if (fileNode.getOverflowChangeType() != OverflowChangeType.NO_CHANGE) {
        fileNode.setOverflowChangeType(OverflowChangeType.CHANGED);
      }
    }
  }

  private String queryAndWriteDataForMerge(IntervalFileNode backupIntervalFile)
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
          IReader seriesReader = SeriesReaderFactory.getInstance()
              .createSeriesReaderForMerge(backupIntervalFile,
                  overflowSeriesDataSource, seriesFilter, context);
          numOfChunk += queryAndWriteSeries(seriesReader, path, seriesFilter, dataType,
              startTimeMap, endTimeMap);
        }
        if (mergeIsChunkGroupHasData) {
          // end the new rowGroupMetadata
          long size = mergeFileWriter.getPos() - mergeStartPos;
          footer = new ChunkGroupFooter(deviceId, size, numOfChunk);
          mergeFileWriter.endChunkGroup(footer, 0);
        }
      }
    } finally {
      if (mergeDeleteLock.isLocked()) {
        mergeDeleteLock.unlock();
      }
    }

    if (mergeFileWriter != null) {
      mergeFileWriter.endFile(fileSchema);
    }
    backupIntervalFile.setBaseDirIndex(directories.getTsFileFolderIndex(mergeBaseDir));
    backupIntervalFile.setRelativePath(mergeFileName);
    backupIntervalFile.setOverflowChangeType(OverflowChangeType.NO_CHANGE);
    backupIntervalFile.setStartTimeMap(startTimeMap);
    backupIntervalFile.setEndTimeMap(endTimeMap);
    backupIntervalFile.setModFile(mergingModification);
    mergingModification = null;
    return mergeFileName;
  }

  private int queryAndWriteSeries(IReader seriesReader, Path path,
      SingleSeriesExpression seriesFilter, TSDataType dataType,
      Map<String, Long> startTimeMap, Map<String, Long> endTimeMap)
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
        // flush the series data
        seriesWriterImpl.writeToFileWriter(mergeFileWriter);
      }
    } finally {
      seriesReader.close();
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
    if (!newMultiPassLock.writeLock().tryLock()) {
      LOGGER.info("The filenode {} can't be closed, because it can't get newMultiPassLock {}",
          getProcessorName(), newMultiPassLock);
      return false;
    }

    try {
      if (oldMultiPassLock == null) {
        return true;
      }
      if (oldMultiPassLock.writeLock().tryLock()) {
        try {
          return true;
        } finally {
          oldMultiPassLock.writeLock().unlock();
        }
      } else {
        LOGGER.info("The filenode {} can't be closed, because it can't get"
                + " oldMultiPassLock {}",
            getProcessorName(), oldMultiPassLock);
        return false;
      }
    } finally {
      newMultiPassLock.writeLock().unlock();
    }
  }

  @Override
  public FileNodeFlushFuture flush() throws IOException {
    Future<Boolean> bufferWriteFlushFuture = null;
    Future<Boolean> overflowFlushFuture = null;
    if (bufferWriteProcessor != null) {
      bufferWriteFlushFuture = bufferWriteProcessor.flush();
    }
    if (overflowProcessor != null) {
      overflowFlushFuture = overflowProcessor.flush();
    }
    return new FileNodeFlushFuture(bufferWriteFlushFuture, overflowFlushFuture);
  }

  /**
   * Close the bufferwrite processor.
   */
  public void closeBufferWrite() throws FileNodeProcessorException {
    if (bufferWriteProcessor == null) {
      return;
    }
    try {
      while (!bufferWriteProcessor.canBeClosed()) {
        waitForBufferWriteClose();
      }
      bufferWriteProcessor.close();
      bufferWriteProcessor = null;
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
    if (overflowProcessor == null) {
      return;
    }
    try {
      while (!overflowProcessor.canBeClosed()) {
        waitForOverflowClose();
      }
      overflowProcessor.close();
      overflowProcessor.clear();
      overflowProcessor = null;
    } catch (OverflowProcessorException | IOException e) {
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
    closeBufferWrite();
    closeOverflow();
    for (IntervalFileNode fileNode : newFileNodes) {
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
    if (TsFileDBConf.enableStatMonitor) {
      // remove the monitor
      LOGGER.info("Deregister the filenode processor: {} from monitor.", getProcessorName());
      StatMonitor.getInstance().deregistStatistics(statStorageDeltaName);
    }
    closeBufferWrite();
    closeOverflow();
    for (IntervalFileNode fileNode : newFileNodes) {
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
      SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
      try {
        serializeUtil.serialize(fileNodeProcessorStore, fileNodeRestoreFilePath);
        LOGGER.debug("The filenode processor {} writes restore information to the restore file",
            getProcessorName());
      } catch (IOException e) {
        throw new FileNodeProcessorException(e);
      }
    }
  }

  private FileNodeProcessorStore readStoreFromDisk() throws FileNodeProcessorException {

    synchronized (fileNodeRestoreLock) {
      FileNodeProcessorStore processorStore;
      SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
      try {
        processorStore = serializeUtil.deserialize(fileNodeRestoreFilePath)
            .orElse(new FileNodeProcessorStore(false, new HashMap<>(),
                new IntervalFileNode(OverflowChangeType.NO_CHANGE, null),
                new ArrayList<>(), FileNodeProcessorStatus.NONE, 0));
      } catch (IOException e) {
        throw new FileNodeProcessorException(e);
      }
      return processorStore;
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
    if (currentIntervalFileNode != null && currentIntervalFileNode.containsDevice(deviceId)) {
      currentIntervalFileNode.getModFile().write(deletion);
      updatedModFiles.add(currentIntervalFileNode.getModFile());
    }
    for (IntervalFileNode fileNode : newFileNodes) {
      if (fileNode != currentIntervalFileNode && fileNode.containsDevice(deviceId)
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
      throws IOException {
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
      bufferWriteProcessor.delete(deviceId, measurementId, timestamp);
    }
  }

  /**
   * Similar to delete(), but only deletes data in Overflow. Only used by WAL recovery.
   */
  public void deleteOverflow(String deviceId, String measurementId, long timestamp)
      throws IOException {
    long version = versionController.nextVersion();

    OverflowProcessor overflowProcessor = getOverflowProcessor(getProcessorName());
    List<ModificationFile> updatedModFiles = new ArrayList<>();
    try {
      overflowProcessor.delete(deviceId, measurementId, timestamp, version, updatedModFiles);
    } catch (IOException e) {
      for (ModificationFile modificationFile : updatedModFiles) {
        modificationFile.abort();
      }
      throw e;
    }
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
        shouldRecovery == that.shouldRecovery &&
        multiPassLockToken == that.multiPassLockToken &&
        Objects.equals(statStorageDeltaName, that.statStorageDeltaName) &&
        Objects.equals(statParamsHashMap, that.statParamsHashMap) &&
        Objects.equals(lastUpdateTimeMap, that.lastUpdateTimeMap) &&
        Objects.equals(flushLastUpdateTimeMap, that.flushLastUpdateTimeMap) &&
        Objects.equals(invertedIndexOfFiles, that.invertedIndexOfFiles) &&
        Objects.equals(emptyIntervalFileNode, that.emptyIntervalFileNode) &&
        Objects.equals(currentIntervalFileNode, that.currentIntervalFileNode) &&
        Objects.equals(newFileNodes, that.newFileNodes) &&
        isMerging == that.isMerging &&
        Objects.equals(fileNodeProcessorStore, that.fileNodeProcessorStore) &&
        Objects.equals(fileNodeRestoreFilePath, that.fileNodeRestoreFilePath) &&
        Objects.equals(baseDirPath, that.baseDirPath) &&
        Objects.equals(bufferWriteProcessor, that.bufferWriteProcessor) &&
        Objects.equals(overflowProcessor, that.overflowProcessor) &&
        Objects.equals(oldMultiPassTokenSet, that.oldMultiPassTokenSet) &&
        Objects.equals(newMultiPassTokenSet, that.newMultiPassTokenSet) &&
        Objects.equals(oldMultiPassLock, that.oldMultiPassLock) &&
        Objects.equals(newMultiPassLock, that.newMultiPassLock) &&
        Objects.equals(parameters, that.parameters) &&
        Objects.equals(fileSchema, that.fileSchema) &&
        Objects.equals(flushFileNodeProcessorAction, that.flushFileNodeProcessorAction) &&
        Objects.equals(bufferwriteFlushAction, that.bufferwriteFlushAction) &&
        Objects.equals(bufferwriteCloseAction, that.bufferwriteCloseAction) &&
        Objects.equals(overflowFlushAction, that.overflowFlushAction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), statStorageDeltaName, statParamsHashMap, isOverflowed,
        lastUpdateTimeMap, flushLastUpdateTimeMap, invertedIndexOfFiles,
        emptyIntervalFileNode, currentIntervalFileNode, newFileNodes, isMerging,
        numOfMergeFile, fileNodeProcessorStore, fileNodeRestoreFilePath, baseDirPath,
        lastMergeTime, bufferWriteProcessor, overflowProcessor, oldMultiPassTokenSet,
        newMultiPassTokenSet, oldMultiPassLock, newMultiPassLock, shouldRecovery, parameters,
        fileSchema, flushFileNodeProcessorAction, bufferwriteFlushAction,
        bufferwriteCloseAction, overflowFlushAction, multiPassLockToken);
  }

  public class MergeRunnale implements Runnable {

    @Override
    public void run() {
      try {
        ZoneId zoneId = IoTDBDescriptor.getInstance().getConfig().getZoneID();
        long mergeStartTime = System.currentTimeMillis();
        writeLock();
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
}