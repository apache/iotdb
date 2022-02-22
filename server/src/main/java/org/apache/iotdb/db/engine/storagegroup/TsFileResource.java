/*
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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.UpgradeTsFileResourceCallBack;
import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.engine.storagegroup.timeindex.ITimeIndex;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.engine.upgrade.UpgradeTask;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SUFFIX_INDEX;
import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SUFFIX_MERGECNT_INDEX;
import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SUFFIX_SEPARATOR;
import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SUFFIX_TIME_INDEX;
import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SUFFIX_UNSEQMERGECNT_INDEX;
import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SUFFIX_VERSION_INDEX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileResource.class);

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // tsfile
  private File file;

  public static final String RESOURCE_SUFFIX = ".resource";
  static final String TEMP_SUFFIX = ".temp";

  /** version number */
  public static final byte VERSION_NUMBER = 1;

  /** time index type, fileTimeIndex = 0, deviceTimeIndex = 1 */
  private byte timeIndexType;

  protected ITimeIndex timeIndex;

  private ModificationFile modFile;

  private volatile boolean closed = false;
  private volatile boolean deleted = false;
  private volatile boolean isMerging = false;

  protected TsFileLock tsFileLock = new TsFileLock();

  private final Random random = new Random();

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  /** generated upgraded TsFile ResourceList used for upgrading v0.11.x/v2 -> 0.12/v3 */
  private List<TsFileResource> upgradedResources;

  /**
   * load upgraded TsFile Resources to storage group processor used for upgrading v0.11.x/v2 ->
   * 0.12/v3
   */
  private UpgradeTsFileResourceCallBack upgradeTsFileResourceCallBack;

  /**
   * indicate if this tsfile resource belongs to a sequence tsfile or not used for upgrading
   * v0.9.x/v1 -> 0.10/v2
   */
  private boolean isSeq;

  /** Maximum index of plans executed within this TsFile. */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  protected long minPlanIndex = Long.MAX_VALUE;

  private long version = 0;

  /**
   * Chunk metadata list of unsealed tsfile. Only be set in a temporal TsFileResource in a query
   * process.
   */
  private Map<PartialPath, List<ChunkMetadata>> pathToChunkMetadataListMap;

  /** Mem chunk data. Only be set in a temporal TsFileResource in a query process. */
  private Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap;

  /** used for unsealed file to get TimeseriesMetadata */
  private Map<PartialPath, TimeseriesMetadata> pathToTimeSeriesMetadataMap;

  /**
   * If it is not null, it indicates that the current tsfile resource is a snapshot of the
   * originTsFileResource, and if so, when we want to used the lock, we should try to acquire the
   * lock of originTsFileResource
   */
  private TsFileResource originTsFileResource;

  private TsFileProcessor processor;

  public TsFileResource() {}

  public TsFileResource(TsFileResource other) throws IOException {
    this.file = other.file;
    this.processor = other.processor;
    this.timeIndex = other.timeIndex;
    this.timeIndexType = other.timeIndexType;
    this.modFile = other.modFile;
    this.closed = other.closed;
    this.deleted = other.deleted;
    this.isMerging = other.isMerging;
    this.pathToChunkMetadataListMap = other.pathToChunkMetadataListMap;
    this.pathToReadOnlyMemChunkMap = other.pathToReadOnlyMemChunkMap;
    generatePathToTimeSeriesMetadataMap();
    this.tsFileLock = other.tsFileLock;
    this.fsFactory = other.fsFactory;
    this.maxPlanIndex = other.maxPlanIndex;
    this.minPlanIndex = other.minPlanIndex;
    this.version = FilePathUtils.splitAndGetTsFileVersion(this.file.getName());
  }

  /** for sealed TsFile, call setClosed to close TsFileResource */
  public TsFileResource(File file) {
    this.file = file;
    this.version = FilePathUtils.splitAndGetTsFileVersion(this.file.getName());
    this.timeIndex = CONFIG.getTimeIndexLevel().getTimeIndex();
    this.timeIndexType = (byte) CONFIG.getTimeIndexLevel().ordinal();
  }

  /** unsealed TsFile, for writter */
  public TsFileResource(File file, TsFileProcessor processor, int deviceNumInLastClosedTsFile) {
    this.file = file;
    this.version = FilePathUtils.splitAndGetTsFileVersion(this.file.getName());
    this.timeIndex = CONFIG.getTimeIndexLevel().getTimeIndex(deviceNumInLastClosedTsFile);
    this.timeIndexType = (byte) CONFIG.getTimeIndexLevel().ordinal();
    this.processor = processor;
  }

  /** unsealed TsFile, for query */
  public TsFileResource(
      PartialPath path,
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<ChunkMetadata> chunkMetadataList,
      TsFileResource originTsFileResource)
      throws IOException {
    this.file = originTsFileResource.file;
    this.timeIndex = originTsFileResource.timeIndex;
    this.timeIndexType = originTsFileResource.timeIndexType;
    this.pathToReadOnlyMemChunkMap = new HashMap<>();
    pathToReadOnlyMemChunkMap.put(path, readOnlyMemChunk);
    this.pathToChunkMetadataListMap = new HashMap<>();
    pathToChunkMetadataListMap.put(path, chunkMetadataList);
    this.originTsFileResource = originTsFileResource;
    this.version = originTsFileResource.version;
    generatePathToTimeSeriesMetadataMap();
  }

  /** unsealed TsFile, for query */
  public TsFileResource(
      Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap,
      Map<PartialPath, List<ChunkMetadata>> pathToChunkMetadataListMap,
      TsFileResource originTsFileResource)
      throws IOException {
    this.file = originTsFileResource.file;
    this.timeIndex = originTsFileResource.timeIndex;
    this.timeIndexType = originTsFileResource.timeIndexType;
    this.pathToReadOnlyMemChunkMap = pathToReadOnlyMemChunkMap;
    this.pathToChunkMetadataListMap = pathToChunkMetadataListMap;
    this.originTsFileResource = originTsFileResource;
    this.version = originTsFileResource.version;
    generatePathToTimeSeriesMetadataMap();
  }

  @TestOnly
  public TsFileResource(
      File file, Map<String, Integer> deviceToIndex, long[] startTimes, long[] endTimes) {
    this.file = file;
    this.timeIndex = new DeviceTimeIndex(deviceToIndex, startTimes, endTimes);
    this.timeIndexType = 1;
  }

  public synchronized void serialize() throws IOException {
    try (OutputStream outputStream =
        fsFactory.getBufferedOutputStream(file + RESOURCE_SUFFIX + TEMP_SUFFIX)) {
      ReadWriteIOUtils.write(VERSION_NUMBER, outputStream);
      ReadWriteIOUtils.write(timeIndexType, outputStream);
      timeIndex.serialize(outputStream);

      ReadWriteIOUtils.write(maxPlanIndex, outputStream);
      ReadWriteIOUtils.write(minPlanIndex, outputStream);

      if (modFile != null && modFile.exists()) {
        String modFileName = new File(modFile.getFilePath()).getName();
        ReadWriteIOUtils.write(modFileName, outputStream);
      }
    }
    File src = fsFactory.getFile(file + RESOURCE_SUFFIX + TEMP_SUFFIX);
    File dest = fsFactory.getFile(file + RESOURCE_SUFFIX);
    fsFactory.deleteIfExists(dest);
    fsFactory.moveFile(src, dest);
  }

  public void deserialize() throws IOException {
    try (InputStream inputStream = fsFactory.getBufferedInputStream(file + RESOURCE_SUFFIX)) {
      readVersionNumber(inputStream);
      timeIndexType = ReadWriteIOUtils.readByte(inputStream);
      timeIndex = TimeIndexLevel.valueOf(timeIndexType).getTimeIndex().deserialize(inputStream);
      maxPlanIndex = ReadWriteIOUtils.readLong(inputStream);
      minPlanIndex = ReadWriteIOUtils.readLong(inputStream);
      if (inputStream.available() > 0) {
        String modFileName = ReadWriteIOUtils.readString(inputStream);
        if (modFileName != null) {
          File modF = new File(file.getParentFile(), modFileName);
          modFile = new ModificationFile(modF.getPath());
        }
      }
    }
  }

  public void deserializeFromOldFile() throws IOException {
    try (InputStream inputStream = fsFactory.getBufferedInputStream(file + RESOURCE_SUFFIX)) {
      // deserialize old TsfileResource
      int size = ReadWriteIOUtils.readInt(inputStream);
      Map<String, Integer> deviceMap = new HashMap<>();
      long[] startTimesArray = new long[size];
      long[] endTimesArray = new long[size];
      for (int i = 0; i < size; i++) {
        String path = ReadWriteIOUtils.readString(inputStream);
        long time = ReadWriteIOUtils.readLong(inputStream);
        deviceMap.put(path, i);
        startTimesArray[i] = time;
      }
      size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.readString(inputStream); // String path
        long time = ReadWriteIOUtils.readLong(inputStream);
        endTimesArray[i] = time;
      }
      timeIndexType = (byte) 1;
      timeIndex = new DeviceTimeIndex(deviceMap, startTimesArray, endTimesArray);
      if (inputStream.available() > 0) {
        int versionSize = ReadWriteIOUtils.readInt(inputStream);
        for (int i = 0; i < versionSize; i++) {
          // historicalVersions
          ReadWriteIOUtils.readLong(inputStream);
        }
      }
      if (inputStream.available() > 0) {
        String modFileName = ReadWriteIOUtils.readString(inputStream);
        if (modFileName != null) {
          File modF = new File(file.getParentFile(), modFileName);
          modFile = new ModificationFile(modF.getPath());
        }
      }
    }
  }

  /** read version number, used for checking compatibility of TsFileResource in the future */
  private byte readVersionNumber(InputStream inputStream) throws IOException {
    return ReadWriteIOUtils.readByte(inputStream);
  }

  public void updateStartTime(String device, long time) {
    timeIndex.updateStartTime(device, time);
  }

  // used in merge, refresh all start time
  public void putStartTime(String device, long time) {
    timeIndex.putStartTime(device, time);
  }

  public void updateEndTime(String device, long time) {
    timeIndex.updateEndTime(device, time);
  }

  // used in merge, refresh all end time
  public void putEndTime(String device, long time) {
    timeIndex.putEndTime(device, time);
  }

  public boolean resourceFileExists() {
    return fsFactory.getFile(file + RESOURCE_SUFFIX).exists();
  }

  public synchronized ModificationFile getModFile() {
    if (modFile == null) {
      modFile = new ModificationFile(file.getPath() + ModificationFile.FILE_SUFFIX);
    }
    return modFile;
  }

  public void setFile(File file) {
    this.file = file;
  }

  public File getTsFile() {
    return file;
  }

  public String getTsFilePath() {
    return file.getPath();
  }

  public long getTsFileSize() {
    return file.length();
  }

  public long getStartTime(String deviceId) {
    return timeIndex.getStartTime(deviceId);
  }

  /** open file's end time is Long.MIN_VALUE */
  public long getEndTime(String deviceId) {
    return timeIndex.getEndTime(deviceId);
  }

  public long getOrderTime(String deviceId, boolean ascending) {
    return ascending ? getStartTime(deviceId) : getEndTime(deviceId);
  }

  public long getFileStartTime() {
    long res = Long.MAX_VALUE;
    for (String deviceId : timeIndex.getDevices()) {
      res = Math.min(res, timeIndex.getStartTime(deviceId));
    }
    return res;
  }

  /** open file's end time is Long.MIN_VALUE */
  public long getFileEndTime() {
    long res = Long.MIN_VALUE;
    for (String deviceId : timeIndex.getDevices()) {
      res = Math.max(res, timeIndex.getEndTime(deviceId));
    }
    return res;
  }

  public Set<String> getDevices() {
    return timeIndex.getDevices();
  }

  public boolean endTimeEmpty() {
    return timeIndex.endTimeEmpty();
  }

  public boolean isClosed() {
    return closed;
  }

  private void generatePathToTimeSeriesMetadataMap() throws IOException {
    pathToTimeSeriesMetadataMap = new HashMap<>();
    if (pathToChunkMetadataListMap == null || pathToReadOnlyMemChunkMap == null) return;
    for (PartialPath path : pathToChunkMetadataListMap.keySet()) {
      TimeseriesMetadata timeSeriesMetadata = new TimeseriesMetadata();
      timeSeriesMetadata.setOffsetOfChunkMetaDataList(-1);
      timeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);

      if (pathToChunkMetadataListMap.containsKey(path)
          && !pathToChunkMetadataListMap.get(path).isEmpty()) {
        timeSeriesMetadata.setMeasurementId(
            pathToChunkMetadataListMap.get(path).get(0).getMeasurementUid());
        TSDataType dataType = pathToChunkMetadataListMap.get(path).get(0).getDataType();
        timeSeriesMetadata.setTSDataType(dataType);
      } else if (pathToReadOnlyMemChunkMap.containsKey(path)
          && !pathToReadOnlyMemChunkMap.get(path).isEmpty()) {
        timeSeriesMetadata.setMeasurementId(
            pathToReadOnlyMemChunkMap.get(path).get(0).getMeasurementUid());
        TSDataType dataType = pathToReadOnlyMemChunkMap.get(path).get(0).getDataType();
        timeSeriesMetadata.setTSDataType(dataType);
      }
      if (timeSeriesMetadata.getTSDataType() != null) {
        Statistics<?> seriesStatistics =
            Statistics.getStatsByType(timeSeriesMetadata.getTSDataType());
        // flush chunkMetadataList one by one
        for (ChunkMetadata chunkMetadata : pathToChunkMetadataListMap.get(path)) {
          seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
        }

        for (ReadOnlyMemChunk memChunk : pathToReadOnlyMemChunkMap.get(path)) {
          if (!memChunk.isEmpty()) {
            seriesStatistics.mergeStatistics(memChunk.getChunkMetaData().getStatistics());
          }
        }
        timeSeriesMetadata.setStatistics(seriesStatistics);
      } else {
        timeSeriesMetadata = null;
      }
      pathToTimeSeriesMetadataMap.put(path, timeSeriesMetadata);
    }
  }

  public List<ChunkMetadata> getChunkMetadataList(PartialPath seriesPath) {
    return new ArrayList<>(pathToChunkMetadataListMap.get(seriesPath));
  }

  public List<ReadOnlyMemChunk> getReadOnlyMemChunk(PartialPath seriesPath) {
    return pathToReadOnlyMemChunkMap.get(seriesPath);
  }

  public TimeseriesMetadata getTimeSeriesMetadata(PartialPath seriesPath) {
    if (pathToTimeSeriesMetadataMap.containsKey(seriesPath)) {
      return pathToTimeSeriesMetadataMap.get(seriesPath);
    }
    return null;
  }

  public TsFileProcessor getProcessor() {
    return processor;
  }

  public void setProcessor(TsFileProcessor processor) {
    this.processor = processor;
  }

  public void close() throws IOException {
    closed = true;
    if (modFile != null) {
      modFile.close();
      modFile = null;
    }
    processor = null;
    pathToChunkMetadataListMap = null;
    pathToReadOnlyMemChunkMap = null;
    pathToTimeSeriesMetadataMap = null;
    timeIndex.close();
  }

  /**
   * If originTsFileResource is not null, we should acquire the read lock of originTsFileResource
   * before construct the current TsFileResource
   */
  public void writeLock() {
    if (originTsFileResource == null) {
      tsFileLock.writeLock();
    } else {
      originTsFileResource.writeLock();
    }
  }

  public void writeUnlock() {
    if (originTsFileResource == null) {
      tsFileLock.writeUnlock();
    } else {
      originTsFileResource.writeUnlock();
    }
  }

  public void readLock() {
    if (originTsFileResource == null) {
      tsFileLock.readLock();
    } else {
      originTsFileResource.readLock();
    }
  }

  public void readUnlock() {
    if (originTsFileResource == null) {
      tsFileLock.readUnlock();
    } else {
      originTsFileResource.readUnlock();
    }
  }

  public boolean tryWriteLock() {
    return tsFileLock.tryWriteLock();
  }

  void doUpgrade() {
    UpgradeSevice.getINSTANCE().submitUpgradeTask(new UpgradeTask(this));
  }

  public void removeModFile() throws IOException {
    getModFile().remove();
    modFile = null;
  }

  /** Remove the data file, its resource file, and its modification file physically. */
  public void remove() {
    try {
      fsFactory.deleteIfExists(file);
    } catch (IOException e) {
      LOGGER.error("TsFile {} cannot be deleted: {}", file, e.getMessage());
    }
    removeResourceFile();
    try {
      fsFactory.deleteIfExists(fsFactory.getFile(file.getPath() + ModificationFile.FILE_SUFFIX));
    } catch (IOException e) {
      LOGGER.error("ModificationFile {} cannot be deleted: {}", file, e.getMessage());
    }
  }

  public void removeResourceFile() {
    try {
      fsFactory.deleteIfExists(fsFactory.getFile(file.getPath() + RESOURCE_SUFFIX + TEMP_SUFFIX));
      fsFactory.deleteIfExists(fsFactory.getFile(file.getPath() + RESOURCE_SUFFIX));
    } catch (IOException e) {
      LOGGER.error("TsFileResource {} cannot be deleted: {}", file, e.getMessage());
    }
  }

  void moveTo(File targetDir) {
    fsFactory.moveFile(file, fsFactory.getFile(targetDir, file.getName()));
    fsFactory.moveFile(
        fsFactory.getFile(file.getPath() + RESOURCE_SUFFIX),
        fsFactory.getFile(targetDir, file.getName() + RESOURCE_SUFFIX));
    File originModFile = fsFactory.getFile(file.getPath() + ModificationFile.FILE_SUFFIX);
    if (originModFile.exists()) {
      fsFactory.moveFile(
          originModFile,
          fsFactory.getFile(targetDir, file.getName() + ModificationFile.FILE_SUFFIX));
    }
  }

  @Override
  public String toString() {
    return file.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TsFileResource that = (TsFileResource) o;
    return Objects.equals(file, that.file);
  }

  @Override
  public int hashCode() {
    return Objects.hash(file);
  }

  public void setClosed(boolean closed) {
    this.closed = closed;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public boolean isMerging() {
    return isMerging;
  }

  public void setMerging(boolean merging) {
    isMerging = merging;
  }

  /**
   * check if any of the device lives over the given time bound. If the file is not closed, then
   * return true.
   */
  public boolean stillLives(long timeLowerBound) {
    return !isClosed() || timeIndex.stillLives(timeLowerBound);
  }

  public boolean isSatisfied(
      String deviceId, Filter timeFilter, boolean isSeq, long ttl, boolean debug) {
    if (deviceId == null) {
      return isSatisfied(timeFilter, isSeq, ttl, debug);
    }

    if (!getDevices().contains(deviceId)) {
      if (debug) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of no device!", deviceId, file);
      }
      return false;
    }

    long startTime = getStartTime(deviceId);
    long endTime = closed || !isSeq ? getEndTime(deviceId) : Long.MAX_VALUE;

    if (!isAlive(endTime, ttl)) {
      if (debug) {
        DEBUG_LOGGER.info("file {} is not satisfied because of ttl!", file);
      }
      return false;
    }

    if (timeFilter != null) {
      boolean res = timeFilter.satisfyStartEndTime(startTime, endTime);
      if (debug && !res) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of time filter!", deviceId, fsFactory);
      }
      return res;
    }
    return true;
  }

  /** @return true if the TsFile lives beyond TTL */
  private boolean isSatisfied(Filter timeFilter, boolean isSeq, long ttl, boolean debug) {
    long startTime = getFileStartTime();
    long endTime = closed || !isSeq ? getFileEndTime() : Long.MAX_VALUE;

    if (!isAlive(endTime, ttl)) {
      if (debug) {
        DEBUG_LOGGER.info("file {} is not satisfied because of ttl!", file);
      }
      return false;
    }

    if (timeFilter != null) {
      boolean res = timeFilter.satisfyStartEndTime(startTime, endTime);
      if (debug && !res) {
        DEBUG_LOGGER.info("Path: file {} is not satisfied because of time filter!", fsFactory);
      }
      return res;
    }
    return true;
  }

  /** @return true if the device is contained in the TsFile */
  public boolean isSatisfied(
      String deviceId, Filter timeFilter, TsFileFilter fileFilter, boolean isSeq, boolean debug) {
    if (fileFilter != null && fileFilter.fileNotSatisfy(this)) {
      if (debug) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of fileFilter!", deviceId, file);
      }
      return false;
    }

    if (!getDevices().contains(deviceId)) {
      if (debug) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of no device!", deviceId, file);
      }
      return false;
    }

    long startTime = getStartTime(deviceId);
    long endTime = closed || !isSeq ? getEndTime(deviceId) : Long.MAX_VALUE;

    if (timeFilter != null) {
      boolean res = timeFilter.satisfyStartEndTime(startTime, endTime);
      if (debug && !res) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of time filter!", deviceId, fsFactory);
      }
      return res;
    }
    return true;
  }

  /** @return whether the given time falls in ttl */
  private boolean isAlive(long time, long dataTTL) {
    return dataTTL == Long.MAX_VALUE || (System.currentTimeMillis() - time) <= dataTTL;
  }

  public void setUpgradedResources(List<TsFileResource> upgradedResources) {
    this.upgradedResources = upgradedResources;
  }

  public List<TsFileResource> getUpgradedResources() {
    return upgradedResources;
  }

  public void setSeq(boolean isSeq) {
    this.isSeq = isSeq;
  }

  public boolean isSeq() {
    return isSeq;
  }

  public void setUpgradeTsFileResourceCallBack(
      UpgradeTsFileResourceCallBack upgradeTsFileResourceCallBack) {
    this.upgradeTsFileResourceCallBack = upgradeTsFileResourceCallBack;
  }

  public UpgradeTsFileResourceCallBack getUpgradeTsFileResourceCallBack() {
    return upgradeTsFileResourceCallBack;
  }

  /** make sure Either the deviceToIndex is not empty Or the path contains a partition folder */
  public long getTimePartition() {
    return timeIndex.getTimePartition(file.getAbsolutePath());
  }

  /**
   * Used when load new TsFiles not generated by the server Check and get the time partition
   *
   * @throws PartitionViolationException if the data of the file spans partitions or it is empty
   */
  public long getTimePartitionWithCheck() throws PartitionViolationException {
    return timeIndex.getTimePartitionWithCheck(file.toString());
  }

  /** Check whether the tsFile spans multiple time partitions. */
  public boolean isSpanMultiTimePartitions() {
    return timeIndex.isSpanMultiTimePartitions();
  }

  /**
   * Create a hardlink for the TsFile and modification file (if exists) The hardlink will have a
   * suffix like ".{sysTime}_{randomLong}"
   *
   * @return a new TsFileResource with its file changed to the hardlink or null the hardlink cannot
   *     be created.
   */
  public TsFileResource createHardlink() {
    if (!file.exists()) {
      return null;
    }

    TsFileResource newResource;
    try {
      newResource = new TsFileResource(this);
    } catch (IOException e) {
      LOGGER.error("Cannot create hardlink for {}", file, e);
      return null;
    }

    while (true) {
      String hardlinkSuffix =
          TsFileConstant.PATH_SEPARATOR + System.currentTimeMillis() + "_" + random.nextLong();
      File hardlink = new File(file.getAbsolutePath() + hardlinkSuffix);

      try {
        Files.createLink(Paths.get(hardlink.getAbsolutePath()), Paths.get(file.getAbsolutePath()));
        newResource.setFile(hardlink);
        if (modFile != null && modFile.exists()) {
          newResource.setModFile(modFile.createHardlink());
        }
        break;
      } catch (FileAlreadyExistsException e) {
        // retry a different name if the file is already created
      } catch (IOException e) {
        LOGGER.error("Cannot create hardlink for {}", file, e);
        return null;
      }
    }
    return newResource;
  }

  public synchronized void setModFile(ModificationFile modFile) {
    this.modFile = modFile;
  }

  /** @return resource map size */
  public long calculateRamSize() {
    return timeIndex.calculateRamSize();
  }

  public void delete() throws IOException {
    if (file.exists()) {
      Files.delete(file.toPath());
      Files.delete(
          FSFactoryProducer.getFSFactory()
              .getFile(file.toPath() + TsFileResource.RESOURCE_SUFFIX)
              .toPath());
    }
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public void updatePlanIndexes(long planIndex) {
    if (planIndex == Long.MIN_VALUE || planIndex == Long.MAX_VALUE) {
      return;
    }
    maxPlanIndex = Math.max(maxPlanIndex, planIndex);
    minPlanIndex = Math.min(minPlanIndex, planIndex);
    if (closed) {
      try {
        serialize();
      } catch (IOException e) {
        LOGGER.error(
            "Cannot serialize TsFileResource {} when updating plan index {}-{}",
            this,
            maxPlanIndex,
            planIndex);
      }
    }
  }

  /** For merge, the index range of the new file should be the union of all files' in this merge. */
  public void updatePlanIndexes(TsFileResource another) {
    maxPlanIndex = Math.max(maxPlanIndex, another.maxPlanIndex);
    minPlanIndex = Math.min(minPlanIndex, another.minPlanIndex);
  }

  public boolean isPlanIndexOverlap(TsFileResource another) {
    return another.maxPlanIndex >= this.minPlanIndex && another.minPlanIndex <= this.maxPlanIndex;
  }

  public boolean isPlanRangeCovers(TsFileResource another) {
    return this.minPlanIndex <= another.minPlanIndex && another.maxPlanIndex <= this.maxPlanIndex;
  }

  public void setMaxPlanIndex(long maxPlanIndex) {
    this.maxPlanIndex = maxPlanIndex;
  }

  public void setMinPlanIndex(long minPlanIndex) {
    this.minPlanIndex = minPlanIndex;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getVersion() {
    return version;
  }

  public void setTimeIndex(ITimeIndex timeIndex) {
    this.timeIndex = timeIndex;
  }

  // change tsFile name

  public static String getNewTsFileName(long time, long version, int mergeCnt, int unSeqMergeCnt) {
    return time
        + FILE_NAME_SEPARATOR
        + version
        + FILE_NAME_SEPARATOR
        + mergeCnt
        + FILE_NAME_SEPARATOR
        + unSeqMergeCnt
        + TSFILE_SUFFIX;
  }

  public static TsFileName getTsFileName(String fileName) throws IOException {
    String[] fileNameParts =
        fileName.split(FILE_NAME_SUFFIX_SEPARATOR)[FILE_NAME_SUFFIX_INDEX].split(
            FILE_NAME_SEPARATOR);
    if (fileNameParts.length != 4) {
      throw new IOException("tsfile file name format is incorrect:" + fileName);
    }
    try {
      return new TsFileName(
          Long.parseLong(fileNameParts[FILE_NAME_SUFFIX_TIME_INDEX]),
          Long.parseLong(fileNameParts[FILE_NAME_SUFFIX_VERSION_INDEX]),
          Integer.parseInt(fileNameParts[FILE_NAME_SUFFIX_MERGECNT_INDEX]),
          Integer.parseInt(fileNameParts[FILE_NAME_SUFFIX_UNSEQMERGECNT_INDEX]));
    } catch (NumberFormatException e) {
      throw new IOException("tsfile file name format is incorrect:" + fileName);
    }
  }

  public static int getMergeLevel(String fileName) throws IOException {
    TsFileName tsFileName = getTsFileName(fileName);
    return tsFileName.mergeCnt;
  }

  public static TsFileResource modifyTsFileNameUnseqMergCnt(TsFileResource tsFileResource)
      throws IOException {
    File tsFile = tsFileResource.getTsFile();
    String path = tsFile.getParent();
    TsFileName tsFileName = getTsFileName(tsFileResource.getTsFile().getName());
    tsFileName.setUnSeqMergeCnt(tsFileName.getUnSeqMergeCnt() + 1);
    tsFileResource.setFile(
        new File(
            path,
            tsFileName.time
                + FILE_NAME_SEPARATOR
                + tsFileName.version
                + FILE_NAME_SEPARATOR
                + tsFileName.mergeCnt
                + FILE_NAME_SEPARATOR
                + tsFileName.unSeqMergeCnt
                + TSFILE_SUFFIX));
    return tsFileResource;
  }

  public static File modifyTsFileNameUnseqMergCnt(File tsFile) throws IOException {
    String path = tsFile.getParent();
    TsFileName tsFileName = getTsFileName(tsFile.getName());
    tsFileName.setUnSeqMergeCnt(tsFileName.getUnSeqMergeCnt() + 1);
    return new File(
        path,
        tsFileName.time
            + FILE_NAME_SEPARATOR
            + tsFileName.version
            + FILE_NAME_SEPARATOR
            + tsFileName.mergeCnt
            + FILE_NAME_SEPARATOR
            + tsFileName.unSeqMergeCnt
            + TSFILE_SUFFIX);
  }

  public static File modifyTsFileNameMergeCnt(File tsFile) throws IOException {
    String path = tsFile.getParent();
    TsFileName tsFileName = getTsFileName(tsFile.getName());
    tsFileName.setMergeCnt(tsFileName.getMergeCnt() + 1);
    return new File(
        path,
        tsFileName.time
            + FILE_NAME_SEPARATOR
            + tsFileName.version
            + FILE_NAME_SEPARATOR
            + tsFileName.mergeCnt
            + FILE_NAME_SEPARATOR
            + tsFileName.unSeqMergeCnt
            + TSFILE_SUFFIX);
  }

  public static class TsFileName {

    private long time;
    private long version;
    private int mergeCnt;
    private int unSeqMergeCnt;

    public TsFileName(long time, long version, int mergeCnt, int unSeqMergeCnt) {
      this.time = time;
      this.version = version;
      this.mergeCnt = mergeCnt;
      this.unSeqMergeCnt = unSeqMergeCnt;
    }

    public long getTime() {
      return time;
    }

    public long getVersion() {
      return version;
    }

    public int getMergeCnt() {
      return mergeCnt;
    }

    public int getUnSeqMergeCnt() {
      return unSeqMergeCnt;
    }

    public void setTime(long time) {
      this.time = time;
    }

    public void setVersion(long version) {
      this.version = version;
    }

    public void setMergeCnt(int mergeCnt) {
      this.mergeCnt = mergeCnt;
    }

    public void setUnSeqMergeCnt(int unSeqMergeCnt) {
      this.unSeqMergeCnt = unSeqMergeCnt;
    }
  }
}
