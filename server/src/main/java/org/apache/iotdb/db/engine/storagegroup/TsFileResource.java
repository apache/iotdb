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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.DataRegion.SettleTsFileCallBack;
import org.apache.iotdb.db.engine.storagegroup.DataRegion.UpgradeTsFileResourceCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator.TsFileName;
import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.engine.storagegroup.timeindex.FileTimeIndex;
import org.apache.iotdb.db.engine.storagegroup.timeindex.ITimeIndex;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.engine.storagegroup.timeindex.V012FileTimeIndex;
import org.apache.iotdb.db.engine.upgrade.UpgradeTask;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.metadata.utils.ResourceByPathUtils;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

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

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator.getTsFileName;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

@SuppressWarnings("java:S1135") // ignore todos
public class TsFileResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileResource.class);

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  /** this tsfile */
  private File file;

  public static final String RESOURCE_SUFFIX = ".resource";
  static final String TEMP_SUFFIX = ".temp";

  /** version number */
  public static final byte VERSION_NUMBER = 1;

  /** Used in {@link TsFileResourceList TsFileResourceList} */
  protected TsFileResource prev;

  protected TsFileResource next;

  /** time index */
  protected ITimeIndex timeIndex;

  private volatile ModificationFile modFile;

  private volatile ModificationFile compactionModFile;

  protected volatile TsFileResourceStatus status = TsFileResourceStatus.UNCLOSED;

  private TsFileLock tsFileLock = new TsFileLock();

  private final Random random = new Random();

  private boolean isSeq;

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  /** generated upgraded TsFile ResourceList used for upgrading v0.11.x/v2 -> 0.12/v3 */
  private List<TsFileResource> upgradedResources;

  /**
   * load upgraded TsFile Resources to database processor used for upgrading v0.11.x/v2 -> 0.12/v3
   */
  private UpgradeTsFileResourceCallBack upgradeTsFileResourceCallBack;

  private SettleTsFileCallBack settleTsFileCallBack;

  /** Maximum index of plans executed within this TsFile. */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  protected long minPlanIndex = Long.MAX_VALUE;

  private long version = 0;

  private long ramSize;

  private volatile long tsFileSize = -1L;

  private TsFileProcessor processor;

  /**
   * Chunk metadata list of unsealed tsfile. Only be set in a temporal TsFileResource in a query
   * process.
   */
  private Map<PartialPath, List<IChunkMetadata>> pathToChunkMetadataListMap = new HashMap<>();

  /** Mem chunk data. Only be set in a temporal TsFileResource in a query process. */
  private Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap = new HashMap<>();

  /** used for unsealed file to get TimeseriesMetadata */
  private Map<PartialPath, ITimeSeriesMetadata> pathToTimeSeriesMetadataMap = new HashMap<>();

  /**
   * If it is not null, it indicates that the current tsfile resource is a snapshot of the
   * originTsFileResource, and if so, when we want to used the lock, we should try to acquire the
   * lock of originTsFileResource
   */
  private TsFileResource originTsFileResource;

  public TsFileResource() {}

  public TsFileResource(TsFileResource other) throws IOException {
    this.file = other.file;
    this.processor = other.processor;
    this.timeIndex = other.timeIndex;
    this.modFile = other.modFile;
    this.status = other.status;
    this.pathToChunkMetadataListMap = other.pathToChunkMetadataListMap;
    this.pathToReadOnlyMemChunkMap = other.pathToReadOnlyMemChunkMap;
    this.pathToTimeSeriesMetadataMap = other.pathToTimeSeriesMetadataMap;
    this.tsFileLock = other.tsFileLock;
    this.fsFactory = other.fsFactory;
    this.maxPlanIndex = other.maxPlanIndex;
    this.minPlanIndex = other.minPlanIndex;
    this.version = FilePathUtils.splitAndGetTsFileVersion(this.file.getName());
    this.tsFileSize = other.tsFileSize;
  }

  /** for sealed TsFile, call setClosed to close TsFileResource */
  public TsFileResource(File file) {
    this.file = file;
    this.version = FilePathUtils.splitAndGetTsFileVersion(this.file.getName());
    this.timeIndex = CONFIG.getTimeIndexLevel().getTimeIndex();
  }

  /** Used for compaction to create target files. */
  public TsFileResource(File file, TsFileResourceStatus status) {
    this(file);
    this.status = status;
  }

  /** unsealed TsFile, for writter */
  public TsFileResource(File file, TsFileProcessor processor) {
    this.file = file;
    this.version = FilePathUtils.splitAndGetTsFileVersion(this.file.getName());
    this.timeIndex = CONFIG.getTimeIndexLevel().getTimeIndex();
    this.processor = processor;
  }

  /** unsealed TsFile, for query */
  public TsFileResource(
      PartialPath path,
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<IChunkMetadata> chunkMetadataList,
      TsFileResource originTsFileResource)
      throws IOException {
    this.file = originTsFileResource.file;
    this.timeIndex = originTsFileResource.timeIndex;
    this.pathToReadOnlyMemChunkMap.put(path, readOnlyMemChunk);
    this.pathToChunkMetadataListMap.put(path, chunkMetadataList);
    this.originTsFileResource = originTsFileResource;
    this.version = originTsFileResource.version;
  }

  /** unsealed TsFile, for query */
  public TsFileResource(
      Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap,
      Map<PartialPath, List<IChunkMetadata>> pathToChunkMetadataListMap,
      TsFileResource originTsFileResource)
      throws IOException {
    this.file = originTsFileResource.file;
    this.timeIndex = originTsFileResource.timeIndex;
    this.pathToReadOnlyMemChunkMap = pathToReadOnlyMemChunkMap;
    this.pathToChunkMetadataListMap = pathToChunkMetadataListMap;
    generatePathToTimeSeriesMetadataMap();
    this.originTsFileResource = originTsFileResource;
    this.version = originTsFileResource.version;
  }

  @TestOnly
  public TsFileResource(
      File file, Map<String, Integer> deviceToIndex, long[] startTimes, long[] endTimes) {
    this.file = file;
    this.timeIndex = new DeviceTimeIndex(deviceToIndex, startTimes, endTimes);
  }

  public synchronized void serialize() throws IOException {
    try (OutputStream outputStream =
        fsFactory.getBufferedOutputStream(file + RESOURCE_SUFFIX + TEMP_SUFFIX)) {
      ReadWriteIOUtils.write(VERSION_NUMBER, outputStream);
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

  /** deserialize from disk */
  public void deserialize() throws IOException {
    try (InputStream inputStream = fsFactory.getBufferedInputStream(file + RESOURCE_SUFFIX)) {
      // The first byte is VERSION_NUMBER, second byte is timeIndexType.
      ReadWriteIOUtils.readByte(inputStream);
      timeIndex = ITimeIndex.createTimeIndex(inputStream);
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

    // upgrade from v0.12 to v0.13, we need to rewrite the TsFileResource if the previous time index
    // is file time index
    if (timeIndex.getTimeIndexType() == ITimeIndex.V012_FILE_TIME_INDEX_TYPE) {
      timeIndex = ((V012FileTimeIndex) timeIndex).getFileTimeIndex();
      serialize();
    }
  }

  /** deserialize tsfile resource from old file */
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
        deviceMap.put(path.intern(), i);
        startTimesArray[i] = time;
      }
      size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.readString(inputStream); // String path
        long time = ReadWriteIOUtils.readLong(inputStream);
        endTimesArray[i] = time;
      }
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

  public void updateStartTime(String device, long time) {
    timeIndex.updateStartTime(device, time);
  }

  public void updateEndTime(String device, long time) {
    timeIndex.updateEndTime(device, time);
  }

  public boolean resourceFileExists() {
    return fsFactory.getFile(file + RESOURCE_SUFFIX).exists();
  }

  public List<IChunkMetadata> getChunkMetadataList(PartialPath seriesPath) {
    return new ArrayList<>(pathToChunkMetadataListMap.get(seriesPath));
  }

  public List<ReadOnlyMemChunk> getReadOnlyMemChunk(PartialPath seriesPath) {
    return pathToReadOnlyMemChunkMap.get(seriesPath);
  }

  public ModificationFile getModFile() {
    if (modFile == null) {
      synchronized (this) {
        if (modFile == null) {
          modFile = ModificationFile.getNormalMods(this);
        }
      }
    }
    return modFile;
  }

  public ModificationFile getCompactionModFile() {
    if (compactionModFile == null) {
      synchronized (this) {
        if (compactionModFile == null) {
          compactionModFile = ModificationFile.getCompactionMods(this);
        }
      }
    }
    return compactionModFile;
  }

  public void resetModFile() {
    if (modFile != null) {
      synchronized (this) {
        modFile = null;
      }
    }
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
    if (isClosed()) {
      if (tsFileSize == -1) {
        synchronized (this) {
          if (tsFileSize == -1) {
            tsFileSize = file.length();
          }
        }
      }
      return tsFileSize;
    } else {
      return file.length();
    }
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
    return timeIndex.getMinStartTime();
  }

  /** open file's end time is Long.MIN_VALUE */
  public long getFileEndTime() {
    return timeIndex.getMaxEndTime();
  }

  public Set<String> getDevices() {
    return timeIndex.getDevices(file.getPath(), this);
  }

  public DeviceTimeIndex buildDeviceTimeIndex() throws IOException {
    readLock();
    try (InputStream inputStream =
        FSFactoryProducer.getFSFactory()
            .getBufferedInputStream(file.getPath() + TsFileResource.RESOURCE_SUFFIX)) {
      ReadWriteIOUtils.readByte(inputStream);
      ITimeIndex timeIndexFromResourceFile = ITimeIndex.createTimeIndex(inputStream);
      if (!(timeIndexFromResourceFile instanceof DeviceTimeIndex)) {
        throw new IOException("cannot build DeviceTimeIndex from resource " + file.getPath());
      }
      return (DeviceTimeIndex) timeIndexFromResourceFile;
    } catch (Exception e) {
      throw new IOException(
          "Can't read file " + file.getPath() + TsFileResource.RESOURCE_SUFFIX + " from disk", e);
    } finally {
      readUnlock();
    }
  }

  /**
   * Whether this TsFileResource contains this device, if false, it must not contain this device, if
   * true, it may or may not contain this device
   */
  public boolean mayContainsDevice(String device) {
    return timeIndex.mayContainsDevice(device);
  }

  /**
   * Get the min start time and max end time of devices matched by given devicePattern. If there's
   * no device matched by given pattern, return null.
   */
  public Pair<Long, Long> getPossibleStartTimeAndEndTime(PartialPath devicePattern) {
    return timeIndex.getPossibleStartTimeAndEndTime(devicePattern);
  }

  public boolean isClosed() {
    return this.status != TsFileResourceStatus.UNCLOSED;
  }

  public void close() throws IOException {
    this.setStatus(TsFileResourceStatus.CLOSED);
    closeWithoutSettingStatus();
  }

  /** Used for compaction. */
  public void closeWithoutSettingStatus() throws IOException {
    if (modFile != null) {
      modFile.close();
      modFile = null;
    }
    if (compactionModFile != null) {
      compactionModFile.close();
      compactionModFile = null;
    }
    processor = null;
    pathToChunkMetadataListMap = null;
    pathToReadOnlyMemChunkMap = null;
    pathToTimeSeriesMetadataMap = null;
    timeIndex.close();
  }

  TsFileProcessor getProcessor() {
    return processor;
  }

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

  /**
   * If originTsFileResource is not null, we should acquire the read lock of originTsFileResource
   * before construct the current TsFileResource
   */
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

  public boolean tryReadLock() {
    return tsFileLock.tryReadLock();
  }

  void doUpgrade() {
    UpgradeSevice.getINSTANCE().submitUpgradeTask(new UpgradeTask(this));
  }

  public void removeModFile() throws IOException {
    getModFile().remove();
    modFile = null;
  }

  /**
   * Remove the data file, its resource file, its chunk metadata temp file, and its modification
   * file physically.
   */
  public boolean remove() {
    setStatus(TsFileResourceStatus.DELETED);
    try {
      fsFactory.deleteIfExists(file);
      fsFactory.deleteIfExists(
          new File(file.getAbsolutePath() + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX));
    } catch (IOException e) {
      LOGGER.error("TsFile {} cannot be deleted: {}", file, e.getMessage());
      return false;
    }
    if (!removeResourceFile()) {
      return false;
    }
    try {
      fsFactory.deleteIfExists(fsFactory.getFile(file.getPath() + ModificationFile.FILE_SUFFIX));
    } catch (IOException e) {
      LOGGER.error("ModificationFile {} cannot be deleted: {}", file, e.getMessage());
      return false;
    }
    return true;
  }

  public boolean removeResourceFile() {
    try {
      fsFactory.deleteIfExists(fsFactory.getFile(file.getPath() + RESOURCE_SUFFIX));
      fsFactory.deleteIfExists(fsFactory.getFile(file.getPath() + RESOURCE_SUFFIX + TEMP_SUFFIX));
    } catch (IOException e) {
      LOGGER.error("TsFileResource {} cannot be deleted: {}", file, e.getMessage());
      return false;
    }
    return true;
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
    return String.format("file is %s, status: %s", file.toString(), status);
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

  public boolean isDeleted() {
    return this.status == TsFileResourceStatus.DELETED;
  }

  public boolean isCompacting() {
    return this.status == TsFileResourceStatus.COMPACTING;
  }

  public boolean isCompactionCandidate() {
    return this.status == TsFileResourceStatus.COMPACTION_CANDIDATE;
  }

  public void setStatus(TsFileResourceStatus status) {
    switch (status) {
      case CLOSED:
        this.status = TsFileResourceStatus.CLOSED;
        break;
      case UNCLOSED:
        this.status = TsFileResourceStatus.UNCLOSED;
        break;
      case DELETED:
        this.status = TsFileResourceStatus.DELETED;
        break;
      case COMPACTING:
        if (this.status == TsFileResourceStatus.COMPACTION_CANDIDATE) {
          this.status = TsFileResourceStatus.COMPACTING;
        } else {
          throw new RuntimeException(
              this.file.getAbsolutePath()
                  + " Cannot set the status of TsFileResource to COMPACTING while its status is "
                  + this.status);
        }
        break;
      case COMPACTION_CANDIDATE:
        if (this.status == TsFileResourceStatus.CLOSED) {
          this.status = TsFileResourceStatus.COMPACTION_CANDIDATE;
        } else {
          throw new RuntimeException(
              this.file.getAbsolutePath()
                  + " Cannot set the status of TsFileResource to COMPACTION_CANDIDATE while its status is "
                  + this.status);
        }
        break;
      default:
        break;
    }
  }

  public TsFileResourceStatus getStatus() {
    return this.status;
  }

  /**
   * check if any of the device lives over the given time bound. If the file is not closed, then
   * return true.
   */
  public boolean stillLives(long timeLowerBound) {
    return !isClosed() || timeIndex.stillLives(timeLowerBound);
  }

  public boolean isDeviceIdExist(String deviceId) {
    return timeIndex.checkDeviceIdExist(deviceId);
  }

  /** @return true if the device is contained in the TsFile and it lives beyond TTL */
  public boolean isSatisfied(
      String deviceId, Filter timeFilter, boolean isSeq, long ttl, boolean debug) {
    if (deviceId == null) {
      return isSatisfied(timeFilter, isSeq, ttl, debug);
    }

    long[] startAndEndTime = timeIndex.getStartAndEndTime(deviceId);

    // doesn't contain this device
    if (startAndEndTime == null) {
      if (debug) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of no device!", deviceId, file);
      }
      return false;
    }

    long startTime = startAndEndTime[0];
    long endTime = isClosed() || !isSeq ? startAndEndTime[1] : Long.MAX_VALUE;

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
    long endTime = isClosed() || !isSeq ? getFileEndTime() : Long.MAX_VALUE;

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
  public boolean isSatisfied(String deviceId, Filter timeFilter, boolean isSeq, boolean debug) {
    if (!mayContainsDevice(deviceId)) {
      if (debug) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of no device!", deviceId, file);
      }
      return false;
    }

    long startTime = getStartTime(deviceId);
    long endTime = isClosed() || !isSeq ? getEndTime(deviceId) : Long.MAX_VALUE;

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
    return dataTTL == Long.MAX_VALUE || (DateTimeUtils.currentTime() - time) <= dataTTL;
  }

  public void setProcessor(TsFileProcessor processor) {
    this.processor = processor;
  }

  /**
   * Get a timeseriesMetadata by path.
   *
   * @return TimeseriesMetadata or the first ValueTimeseriesMetadata in VectorTimeseriesMetadata
   */
  public ITimeSeriesMetadata getTimeSeriesMetadata(PartialPath seriesPath) {
    if (pathToTimeSeriesMetadataMap.containsKey(seriesPath)) {
      return pathToTimeSeriesMetadataMap.get(seriesPath);
    }
    return null;
  }

  public void setTimeSeriesMetadata(PartialPath path, ITimeSeriesMetadata timeSeriesMetadata) {
    this.pathToTimeSeriesMetadataMap.put(path, timeSeriesMetadata);
  }

  public void setUpgradedResources(List<TsFileResource> upgradedResources) {
    this.upgradedResources = upgradedResources;
  }

  public List<TsFileResource> getUpgradedResources() {
    return upgradedResources;
  }

  public void setUpgradeTsFileResourceCallBack(
      UpgradeTsFileResourceCallBack upgradeTsFileResourceCallBack) {
    this.upgradeTsFileResourceCallBack = upgradeTsFileResourceCallBack;
  }

  public UpgradeTsFileResourceCallBack getUpgradeTsFileResourceCallBack() {
    return upgradeTsFileResourceCallBack;
  }

  public SettleTsFileCallBack getSettleTsFileCallBack() {
    return settleTsFileCallBack;
  }

  public void setSettleTsFileCallBack(SettleTsFileCallBack settleTsFileCallBack) {
    this.settleTsFileCallBack = settleTsFileCallBack;
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

  public void setModFile(ModificationFile modFile) {
    synchronized (this) {
      this.modFile = modFile;
    }
  }

  /** @return resource map size */
  public long calculateRamSize() {
    ramSize = timeIndex.calculateRamSize();
    return ramSize;
  }

  public void delete() throws IOException {
    if (file.exists()) {
      Files.delete(file.toPath());
      Files.delete(
          FSFactoryProducer.getFSFactory()
              .getFile(file.toPath() + TsFileResource.RESOURCE_SUFFIX)
              .toPath());
    }
    setStatus(TsFileResourceStatus.DELETED);
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
    if (planIndex < minPlanIndex || planIndex > maxPlanIndex) {
      maxPlanIndex = Math.max(maxPlanIndex, planIndex);
      minPlanIndex = Math.min(minPlanIndex, planIndex);
      if (isClosed()) {
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
  }

  public static int getInnerCompactionCount(String fileName) throws IOException {
    TsFileName tsFileName = getTsFileName(fileName);
    return tsFileName.getInnerCompactionCnt();
  }

  /** For merge, the index range of the new file should be the union of all files' in this merge. */
  public void updatePlanIndexes(TsFileResource another) {
    maxPlanIndex = Math.max(maxPlanIndex, another.maxPlanIndex);
    minPlanIndex = Math.min(minPlanIndex, another.minPlanIndex);
  }

  public boolean isPlanIndexOverlap(TsFileResource another) {
    return another.maxPlanIndex > this.minPlanIndex && another.minPlanIndex < this.maxPlanIndex;
  }

  public boolean isPlanRangeCovers(TsFileResource another) {
    return this.minPlanIndex < another.minPlanIndex && another.maxPlanIndex < this.maxPlanIndex;
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

  /**
   * Compare the name of TsFiles corresponding to the two {@link TsFileResource}. Both names should
   * meet the naming specifications.Take the generation time as the first keyword, the version
   * number as the second keyword, the inner merge count as the third keyword, the cross merge as
   * the fourth keyword.
   *
   * @param o1 a {@link TsFileResource}
   * @param o2 a {@link TsFileResource}
   * @return -1, if o1 is smaller than o2, 1 if bigger, 0 means o1 equals to o2
   */
  // ({systemTime}-{versionNum}-{innerMergeNum}-{crossMergeNum}.tsfile)
  public static int compareFileName(TsFileResource o1, TsFileResource o2) {
    String[] items1 =
        o1.getTsFile().getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    String[] items2 =
        o2.getTsFile().getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      int cmpVersion = Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
      if (cmpVersion == 0) {
        int cmpInnerCompact = Long.compare(Long.parseLong(items1[2]), Long.parseLong(items2[2]));
        if (cmpInnerCompact == 0) {
          return Long.compare(Long.parseLong(items1[3]), Long.parseLong(items2[3]));
        }
        return cmpInnerCompact;
      }
      return cmpVersion;
    } else {
      return cmp;
    }
  }

  /**
   * Compare two TsFile's name.This method will first check whether the two names meet the standard
   * naming specifications, and then use the generating time as the first keyword, and use the
   * version number as the second keyword to compare the size of the two names. Notice that this
   * method will not compare the merge count.
   *
   * @param fileName1 a name of TsFile
   * @param fileName2 a name of TsFile
   * @return -1, if fileName1 is smaller than fileNam2, 1 if bigger, 0 means fileName1 equals to
   *     fileName2
   * @throws IOException if fileName1 or fileName2 do not meet the standard naming specifications.
   */
  public static int checkAndCompareFileName(String fileName1, String fileName2) throws IOException {
    TsFileNameGenerator.TsFileName tsFileName1 = TsFileNameGenerator.getTsFileName(fileName1);
    TsFileNameGenerator.TsFileName tsFileName2 = TsFileNameGenerator.getTsFileName(fileName2);
    long timeDiff = tsFileName1.getTime() - tsFileName2.getTime();
    if (timeDiff != 0) {
      return timeDiff < 0 ? -1 : 1;
    }
    long versionDiff = tsFileName1.getVersion() - tsFileName2.getVersion();
    if (versionDiff != 0) {
      return versionDiff < 0 ? -1 : 1;
    }
    return 0;
  }

  /**
   * Compare the name of TsFiles corresponding to the two {@link TsFileResource}.This method will
   * first check whether the two names meet the standard naming specifications, and then compare
   * version of two names.
   *
   * @param o1 a {@link TsFileResource}
   * @param o2 a {@link TsFileResource}
   * @return -1, if o1 is smaller than o2, 1 if bigger, 0 means o1 equals to o2 or do not meet the
   *     naming specifications
   */
  public static int compareFileNameByDesc(TsFileResource o1, TsFileResource o2) {
    try {
      TsFileNameGenerator.TsFileName n1 =
          TsFileNameGenerator.getTsFileName(o1.getTsFile().getName());
      TsFileNameGenerator.TsFileName n2 =
          TsFileNameGenerator.getTsFileName(o2.getTsFile().getName());
      return (int) (n2.getVersion() - n1.getVersion());
    } catch (IOException e) {
      return 0;
    }
  }

  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  public boolean isSeq() {
    return isSeq;
  }

  public int compareIndexDegradePriority(TsFileResource tsFileResource) {
    int cmp = timeIndex.compareDegradePriority(tsFileResource.timeIndex);
    return cmp == 0 ? file.getAbsolutePath().compareTo(tsFileResource.file.getAbsolutePath()) : cmp;
  }

  public byte getTimeIndexType() {
    return timeIndex.getTimeIndexType();
  }

  @TestOnly
  public void setTimeIndexType(byte type) {
    switch (type) {
      case ITimeIndex.DEVICE_TIME_INDEX_TYPE:
        this.timeIndex = new DeviceTimeIndex();
        break;
      case ITimeIndex.FILE_TIME_INDEX_TYPE:
        this.timeIndex = new FileTimeIndex();
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public long getRamSize() {
    return ramSize;
  }

  /** the DeviceTimeIndex degrade to FileTimeIndex and release memory */
  public long degradeTimeIndex() {
    TimeIndexLevel timeIndexLevel = TimeIndexLevel.valueOf(getTimeIndexType());
    // if current timeIndex is FileTimeIndex, no need to degrade
    if (timeIndexLevel == TimeIndexLevel.FILE_TIME_INDEX) {
      return 0;
    }
    // get the minimum startTime
    long startTime = timeIndex.getMinStartTime();
    // get the maximum endTime
    long endTime = timeIndex.getMaxEndTime();
    // replace the DeviceTimeIndex with FileTimeIndex
    timeIndex = new FileTimeIndex(startTime, endTime);
    return ramSize - timeIndex.calculateRamSize();
  }

  private void generatePathToTimeSeriesMetadataMap() throws IOException {
    for (PartialPath path : pathToChunkMetadataListMap.keySet()) {
      pathToTimeSeriesMetadataMap.put(
          path,
          ResourceByPathUtils.getResourceInstance(path)
              .generateTimeSeriesMetadata(
                  pathToReadOnlyMemChunkMap.get(path), pathToChunkMetadataListMap.get(path)));
    }
  }

  public void updateEndTime(Map<String, Long> times) {
    for (Map.Entry<String, Long> entry : times.entrySet()) {
      timeIndex.updateEndTime(entry.getKey(), entry.getValue());
    }
  }

  /** @return is this tsfile resource in a TsFileResourceList */
  public boolean isFileInList() {
    return prev != null || next != null;
  }
}
