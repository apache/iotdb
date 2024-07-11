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

package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.ResourceByPathUtils;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCompactionCandidateStatus;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.FilePathUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

@SuppressWarnings("java:S1135") // ignore todos
public class TsFileResource {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TsFileResource.class)
          + RamUsageEstimator.shallowSizeOfInstance(TsFileRepairStatus.class)
          + RamUsageEstimator.shallowSizeOfInstance(TsFileID.class);

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileResource.class);

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  /** this tsfile */
  private File file;

  public static final String RESOURCE_SUFFIX = ".resource";
  public static final String TEMP_SUFFIX = ".temp";
  public static final String BROKEN_SUFFIX = ".broken";

  /** version number */
  public static final byte VERSION_NUMBER = 1;

  /** Used in {@link TsFileResourceList TsFileResourceList} */
  protected TsFileResource prev;

  protected TsFileResource next;

  /** time index */
  private ITimeIndex timeIndex;

  @SuppressWarnings("squid:S3077")
  private volatile ModificationFile modFile;

  @SuppressWarnings("squid:S3077")
  private volatile ModificationFile compactionModFile;

  protected AtomicReference<TsFileResourceStatus> atomicStatus =
      new AtomicReference<>(TsFileResourceStatus.UNCLOSED);

  /** used for check whether this file has internal unsorted data in compaction selection */
  private TsFileRepairStatus tsFileRepairStatus = TsFileRepairStatus.NORMAL;

  private TsFileLock tsFileLock = new TsFileLock();

  private boolean isSeq;

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private DataRegion.SettleTsFileCallBack settleTsFileCallBack;

  /** Maximum index of plans executed within this TsFile. */
  public long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  public long minPlanIndex = Long.MAX_VALUE;

  private TsFileID tsFileID;

  private long ramSize;

  private AtomicInteger tierLevel;

  private volatile long tsFileSize = -1L;

  private TsFileProcessor processor;

  /**
   * Chunk metadata list of unsealed tsfile. Only be set in a temporal TsFileResource in a read
   * process.
   */
  private Map<PartialPath, List<IChunkMetadata>> pathToChunkMetadataListMap = new HashMap<>();

  /** Mem chunk data. Only be set in a temporal TsFileResource in a read process. */
  private Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap = new HashMap<>();

  /** used for unsealed file to get TimeseriesMetadata */
  private Map<PartialPath, ITimeSeriesMetadata> pathToTimeSeriesMetadataMap = new HashMap<>();

  /**
   * If it is not null, it indicates that the current tsfile resource is a snapshot of the
   * originTsFileResource, and if so, when we want to used the lock, we should try to acquire the
   * lock of originTsFileResource
   */
  private TsFileResource originTsFileResource;

  private ProgressIndex maxProgressIndex;

  private InsertionCompactionCandidateStatus insertionCompactionCandidateStatus =
      InsertionCompactionCandidateStatus.NOT_CHECKED;

  @TestOnly
  public TsFileResource() {
    this.tsFileID = new TsFileID();
  }

  /** for sealed TsFile, call setClosed to close TsFileResource */
  public TsFileResource(File file) {
    this.file = file;
    this.tsFileID = new TsFileID(file.getAbsolutePath());
    this.timeIndex = CONFIG.getTimeIndexLevel().getTimeIndex();
    this.isSeq = FilePathUtils.isSequence(this.file.getAbsolutePath());
    // This method is invoked when DataNode recovers, so the tierLevel should be calculated when
    // restarting
    this.tierLevel = new AtomicInteger(TierManager.getInstance().getFileTierLevel(file));
  }

  /** Used for compaction to create target files. */
  public TsFileResource(File file, TsFileResourceStatus status) {
    this(file);
    this.setAtomicStatus(status);
  }

  /** unsealed TsFile, for writter */
  public TsFileResource(File file, TsFileProcessor processor) {
    this.file = file;
    this.tsFileID = new TsFileID(file.getAbsolutePath());
    this.timeIndex = CONFIG.getTimeIndexLevel().getTimeIndex();
    this.processor = processor;
    this.isSeq = processor.isSequence();
    // this method is invoked when a new TsFile is created and a newly created TsFile's the
    // tierLevel is 0 by default
    this.tierLevel = new AtomicInteger(0);
  }

  /** unsealed TsFile, for read */
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
    this.tsFileID = originTsFileResource.tsFileID;
    this.isSeq = originTsFileResource.isSeq;
    this.tierLevel = originTsFileResource.tierLevel;
  }

  public synchronized void serialize() throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(file + RESOURCE_SUFFIX + TEMP_SUFFIX);
    BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
    try {
      serializeTo(outputStream);
    } finally {
      outputStream.flush();
      fileOutputStream.getFD().sync();
      outputStream.close();
    }
    File src = fsFactory.getFile(file + RESOURCE_SUFFIX + TEMP_SUFFIX);
    File dest = fsFactory.getFile(file + RESOURCE_SUFFIX);
    fsFactory.deleteIfExists(dest);
    fsFactory.moveFile(src, dest);
  }

  private void serializeTo(BufferedOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(VERSION_NUMBER, outputStream);
    timeIndex.serialize(outputStream);

    ReadWriteIOUtils.write(maxPlanIndex, outputStream);
    ReadWriteIOUtils.write(minPlanIndex, outputStream);

    if (modFile != null && modFile.exists()) {
      String modFileName = new File(modFile.getFilePath()).getName();
      ReadWriteIOUtils.write(modFileName, outputStream);
    } else {
      // make the first "inputStream.available() > 0" in deserialize() happy.
      //
      // if modFile not exist, write null (-1). the first "inputStream.available() > 0" in
      // deserialize() and deserializeFromOldFile() detect -1 and deserialize modFileName as null
      // and skip the modFile deserialize.
      //
      // this make sure the first and the second "inputStream.available() > 0" in deserialize()
      // will always be called... which is a bit ugly but allows the following variable
      // maxProgressIndex to be deserialized correctly.
      ReadWriteIOUtils.write((String) null, outputStream);
    }

    if (maxProgressIndex != null) {
      TsFileResourceBlockType.PROGRESS_INDEX.serialize(outputStream);
      maxProgressIndex.serialize(outputStream);
    } else {
      TsFileResourceBlockType.EMPTY_BLOCK.serialize(outputStream);
    }
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

      while (inputStream.available() > 0) {
        final TsFileResourceBlockType blockType =
            TsFileResourceBlockType.deserialize(ReadWriteIOUtils.readByte(inputStream));
        if (blockType == TsFileResourceBlockType.PROGRESS_INDEX) {
          maxProgressIndex = ProgressIndexType.deserializeFrom(inputStream);
        }
      }
    }
  }

  public void updateStartTime(IDeviceID device, long time) {
    timeIndex.updateStartTime(device, time);
  }

  public void updateEndTime(IDeviceID device, long time) {
    timeIndex.updateEndTime(device, time);
  }

  public boolean resourceFileExists() {
    return file != null && fsFactory.getFile(file + RESOURCE_SUFFIX).exists();
  }

  public boolean tsFileExists() {
    return file != null && file.exists();
  }

  public boolean modFileExists() {
    return getModFile().exists();
  }

  public boolean compactionModFileExists() {
    return getCompactionModFile().exists();
  }

  public List<IChunkMetadata> getChunkMetadataList(PartialPath seriesPath) {
    return new ArrayList<>(pathToChunkMetadataListMap.get(seriesPath));
  }

  public List<ReadOnlyMemChunk> getReadOnlyMemChunk(PartialPath seriesPath) {
    return pathToReadOnlyMemChunkMap.get(seriesPath);
  }

  @SuppressWarnings("squid:S2886")
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

  public void resetModFile() throws IOException {
    if (modFile != null) {
      synchronized (this) {
        modFile.close();
        modFile = null;
      }
    }
  }

  public void setFile(File file) {
    this.file = file;
    this.tsFileID = new TsFileID(file.getAbsolutePath());
  }

  public File getTsFile() {
    return file;
  }

  public String getTsFilePath() {
    return file.getPath();
  }

  public void increaseTierLevel() {
    this.tierLevel.addAndGet(1);
  }

  public int getTierLevel() {
    return tierLevel.get();
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

  public long getStartTime(IDeviceID deviceId) {
    try {
      return deviceId == null ? getFileStartTime() : timeIndex.getStartTime(deviceId);
    } catch (Exception e) {
      LOGGER.error(
          "meet error when getStartTime of {} in file {}", deviceId, file.getAbsolutePath(), e);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("TimeIndex = {}", timeIndex);
      }
      throw e;
    }
  }

  /** open file's end time is Long.MIN_VALUE */
  public long getEndTime(IDeviceID deviceId) {
    try {
      return deviceId == null ? getFileEndTime() : timeIndex.getEndTime(deviceId);
    } catch (Exception e) {
      LOGGER.error(
          "meet error when getEndTime of {} in file {}", deviceId, file.getAbsolutePath(), e);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("TimeIndex = {}", timeIndex);
      }
      throw e;
    }
  }

  public long getOrderTime(IDeviceID deviceId, boolean ascending) {
    return ascending ? getStartTime(deviceId) : getEndTime(deviceId);
  }

  public long getFileStartTime() {
    return timeIndex.getMinStartTime();
  }

  /** Open file's end time is Long.MIN_VALUE */
  public long getFileEndTime() {
    return timeIndex.getMaxEndTime();
  }

  public Set<IDeviceID> getDevices() {
    return timeIndex.getDevices(file.getPath(), this);
  }

  public DeviceTimeIndex buildDeviceTimeIndex() throws IOException {
    readLock();
    try {
      if (!resourceFileExists()) {
        throw new IOException("resource file not found");
      }
      try (InputStream inputStream =
          FSFactoryProducer.getFSFactory()
              .getBufferedInputStream(file.getPath() + RESOURCE_SUFFIX)) {
        ReadWriteIOUtils.readByte(inputStream);
        ITimeIndex timeIndexFromResourceFile = ITimeIndex.createTimeIndex(inputStream);
        if (!(timeIndexFromResourceFile instanceof DeviceTimeIndex)) {
          throw new IOException("cannot build DeviceTimeIndex from resource " + file.getPath());
        }
        return (DeviceTimeIndex) timeIndexFromResourceFile;
      } catch (Exception e) {
        throw new IOException(
            "Can't read file " + file.getPath() + RESOURCE_SUFFIX + " from disk", e);
      }
    } finally {
      readUnlock();
    }
  }

  /** Only used for compaction to validate tsfile. */
  public ITimeIndex getTimeIndex() {
    return timeIndex;
  }

  /**
   * Whether this TsFile definitely not contains this device, if ture, it must not contain this
   * device, if false, it may or may not contain this device Notice: using method be CAREFULLY and
   * you really understand the meaning!!!!!
   */
  public boolean definitelyNotContains(IDeviceID device) {
    return timeIndex.definitelyNotContains(device);
  }

  /**
   * Get the min start time and max end time of devices matched by given devicePattern. If there's
   * no device matched by given pattern, return null.
   */
  public Pair<Long, Long> getPossibleStartTimeAndEndTime(
      PartialPath devicePattern, Set<IDeviceID> deviceMatchInfo) {
    return timeIndex.getPossibleStartTimeAndEndTime(devicePattern, deviceMatchInfo);
  }

  public boolean isClosed() {
    return getStatus() != TsFileResourceStatus.UNCLOSED;
  }

  public void close() throws IOException {
    this.setStatus(TsFileResourceStatus.NORMAL);
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

  public TsFileProcessor getProcessor() {
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

  public void removeModFile() throws IOException {
    getModFile().remove();
    modFile = null;
  }

  /**
   * Remove the data file, its resource file, its chunk metadata temp file, and its modification
   * file physically.
   */
  public boolean remove() {
    forceMarkDeleted();
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
      fsFactory.deleteIfExists(
          fsFactory.getFile(file.getPath() + ModificationFile.COMPACTION_FILE_SUFFIX));
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

  public void moveTo(File targetDir) throws IOException {
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
    return String.format("file is %s, status: %s", file.toString(), getStatus());
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
    return getStatus() == TsFileResourceStatus.DELETED;
  }

  public boolean isCompacting() {
    return getStatus() == TsFileResourceStatus.COMPACTING;
  }

  public boolean isCompactionCandidate() {
    return getStatus() == TsFileResourceStatus.COMPACTION_CANDIDATE;
  }

  public boolean onRemote() {
    return !isDeleted() && !file.exists();
  }

  private boolean compareAndSetStatus(
      TsFileResourceStatus expectedValue, TsFileResourceStatus newValue) {
    return atomicStatus.compareAndSet(expectedValue, newValue);
  }

  private void setAtomicStatus(TsFileResourceStatus status) {
    atomicStatus.set(status);
  }

  @TestOnly
  public void setStatusForTest(TsFileResourceStatus status) {
    setAtomicStatus(status);
  }

  public boolean setStatus(TsFileResourceStatus status) {
    if (status == getStatus()) {
      return true;
    }
    return transformStatus(status);
  }

  /** Return false if the status is not changed */
  public boolean transformStatus(TsFileResourceStatus status) {
    switch (status) {
      case NORMAL:
        return compareAndSetStatus(TsFileResourceStatus.UNCLOSED, TsFileResourceStatus.NORMAL)
            || compareAndSetStatus(TsFileResourceStatus.COMPACTING, TsFileResourceStatus.NORMAL)
            || compareAndSetStatus(
                TsFileResourceStatus.COMPACTION_CANDIDATE, TsFileResourceStatus.NORMAL);
      case UNCLOSED:
        // TsFile cannot be set back to UNCLOSED so false is always returned
        return false;
      case DELETED:
        return compareAndSetStatus(TsFileResourceStatus.NORMAL, TsFileResourceStatus.DELETED)
            || compareAndSetStatus(
                TsFileResourceStatus.COMPACTION_CANDIDATE, TsFileResourceStatus.DELETED);
      case COMPACTING:
        return compareAndSetStatus(
            TsFileResourceStatus.COMPACTION_CANDIDATE, TsFileResourceStatus.COMPACTING);
      case COMPACTION_CANDIDATE:
        return compareAndSetStatus(
            TsFileResourceStatus.NORMAL, TsFileResourceStatus.COMPACTION_CANDIDATE);
      default:
        return false;
    }
  }

  public TsFileRepairStatus getTsFileRepairStatus() {
    return this.tsFileRepairStatus;
  }

  public void setTsFileRepairStatus(TsFileRepairStatus fileRepairStatus) {
    this.tsFileRepairStatus = fileRepairStatus;
  }

  public void forceMarkDeleted() {
    atomicStatus.set(TsFileResourceStatus.DELETED);
  }

  public TsFileResourceStatus getStatus() {
    return this.atomicStatus.get();
  }

  /**
   * check if any of the device lives over the given time bound. If the file is not closed, then
   * return true.
   */
  public boolean stillLives(long timeLowerBound) {
    return !isClosed() || timeIndex.stillLives(timeLowerBound);
  }

  public boolean isDeviceIdExist(IDeviceID deviceId) {
    return timeIndex.checkDeviceIdExist(deviceId);
  }

  /**
   * @return true if the device is contained in the TsFile
   */
  public boolean isSatisfied(IDeviceID deviceId, Filter timeFilter, boolean isSeq, boolean debug) {
    if (deviceId != null && definitelyNotContains(deviceId)) {
      if (debug) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of no device!", deviceId, file);
      }
      return false;
    }

    long startTime = getStartTime(deviceId);
    long endTime = isClosed() || !isSeq ? getEndTime(deviceId) : Long.MAX_VALUE;
    if (startTime > endTime) {
      // startTime > endTime indicates that there is something wrong with this TsFile. Return false
      // directly, or it may lead to infinite loop in GroupByMonthFilter#getTimePointPosition.
      LOGGER.warn(
          "startTime[{}] of TsFileResource[{}] is greater than its endTime[{}]",
          startTime,
          this,
          endTime);
      return false;
    }

    if (timeFilter != null) {
      boolean res = timeFilter.satisfyStartEndTime(startTime, endTime);
      if (debug && !res) {
        DEBUG_LOGGER.info(
            "Path: {} file {} is not satisfied because of time filter!",
            deviceId != null ? deviceId : "",
            fsFactory);
      }
      return res;
    }
    return true;
  }

  /**
   * @return whether the given time falls in ttl
   */
  private boolean isAlive(long time, long dataTTL) {
    return dataTTL == Long.MAX_VALUE || (CommonDateTimeUtils.currentTime() - time) <= dataTTL;
  }

  /**
   * Check whether the given device may still alive or not. Return false if the device does not
   * exist or out of dated.
   */
  public boolean isDeviceAlive(IDeviceID device, long ttl) {
    if (definitelyNotContains(device)) {
      return false;
    }
    return !isClosed() || timeIndex.isDeviceAlive(device, ttl);
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

  public DataRegion.SettleTsFileCallBack getSettleTsFileCallBack() {
    return settleTsFileCallBack;
  }

  public void setSettleTsFileCallBack(DataRegion.SettleTsFileCallBack settleTsFileCallBack) {
    this.settleTsFileCallBack = settleTsFileCallBack;
  }

  /** make sure Either the deviceToIndex is not empty Or the path contains a partition folder */
  public long getTimePartition() {
    return tsFileID.timePartitionId;
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

  public void setModFile(ModificationFile modFile) {
    synchronized (this) {
      this.modFile = modFile;
    }
  }

  /**
   * @return resource map size
   */
  public long calculateRamSize() {
    if (ramSize == 0) {
      ramSize = INSTANCE_SIZE + timeIndex.calculateRamSize();
      return ramSize;
    } else {
      return ramSize;
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
    TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(fileName);
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
    this.tsFileID =
        new TsFileID(
            tsFileID.regionId, tsFileID.timePartitionId, version, tsFileID.compactionVersion);
  }

  public long getVersion() {
    return tsFileID.fileVersion;
  }

  public TsFileID getTsFileID() {
    return tsFileID;
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
   * Compare the creation order of the files and sort them according to the version number from
   * largest to smallest.This method will first check whether the two names meet the standard naming
   * specifications, and then compare version of two names. Notice: This method is only used to
   * compare the creation order of files, which is sorted directly according to version. If you want
   * to compare the order of the content of the file, you must first sort by timestamp and then by
   * version.
   *
   * @param o1 a {@link TsFileResource}
   * @param o2 a {@link TsFileResource}
   * @return -1, if o1 is smaller than o2, 1 if bigger, 0 means o1 equals to o2
   */
  public static int compareFileCreationOrderByDesc(TsFileResource o1, TsFileResource o2) {
    try {
      TsFileNameGenerator.TsFileName n1 =
          TsFileNameGenerator.getTsFileName(o1.getTsFile().getName());
      TsFileNameGenerator.TsFileName n2 =
          TsFileNameGenerator.getTsFileName(o2.getTsFile().getName());
      long versionDiff = n2.getVersion() - n1.getVersion();
      if (versionDiff != 0) {
        return versionDiff < 0 ? -1 : 1;
      }
      return 0;
    } catch (IOException e) {
      LOGGER.error("File name may not meet the standard naming specifications.", e);
      throw new RuntimeException(e.getMessage());
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

    long beforeRamSize = ramSize;

    ramSize = INSTANCE_SIZE + timeIndex.calculateRamSize();

    return beforeRamSize - ramSize;
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

  public void deleteRemovedDeviceAndUpdateEndTime(Map<IDeviceID, Long> lastTimeForEachDevice) {
    ITimeIndex newTimeIndex = CONFIG.getTimeIndexLevel().getTimeIndex();
    for (Map.Entry<IDeviceID, Long> entry : lastTimeForEachDevice.entrySet()) {
      newTimeIndex.updateStartTime(entry.getKey(), timeIndex.getStartTime(entry.getKey()));
      newTimeIndex.updateEndTime(entry.getKey(), entry.getValue());
    }
    timeIndex = newTimeIndex;
  }

  public void updateEndTime(Map<IDeviceID, Long> lastTimeForEachDevice) {
    for (Map.Entry<IDeviceID, Long> entry : lastTimeForEachDevice.entrySet()) {
      timeIndex.updateEndTime(entry.getKey(), entry.getValue());
    }
  }

  /**
   * @return is this tsfile resource in a TsFileResourceList
   */
  public boolean isFileInList() {
    return prev != null || next != null;
  }

  public void updateProgressIndex(ProgressIndex progressIndex) {
    if (progressIndex == null) {
      return;
    }

    maxProgressIndex =
        (maxProgressIndex == null
            ? progressIndex
            : maxProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex));
  }

  public void setProgressIndex(ProgressIndex progressIndex) {
    if (progressIndex == null) {
      return;
    }

    maxProgressIndex = progressIndex;
  }

  public ProgressIndex getMaxProgressIndexAfterClose() throws IllegalStateException {
    if (getStatus().equals(TsFileResourceStatus.UNCLOSED)) {
      throw new IllegalStateException(
          "Should not get progress index from a unclosing TsFileResource.");
    }
    return getMaxProgressIndex();
  }

  public ProgressIndex getMaxProgressIndex() {
    return maxProgressIndex == null ? MinimumProgressIndex.INSTANCE : maxProgressIndex;
  }

  public boolean isEmpty() {
    return getFileStartTime() == Long.MAX_VALUE && getFileEndTime() == Long.MIN_VALUE;
  }

  public String getDatabaseName() {
    return file.getParentFile().getParentFile().getParentFile().getName();
  }

  public String getDataRegionId() {
    return file.getParentFile().getParentFile().getName();
  }

  public boolean isInsertionCompactionTaskCandidate() {
    return !isSeq
        && insertionCompactionCandidateStatus != InsertionCompactionCandidateStatus.NOT_VALID;
  }

  public InsertionCompactionCandidateStatus getInsertionCompactionCandidateStatus() {
    return insertionCompactionCandidateStatus;
  }

  public void setInsertionCompactionTaskCandidate(InsertionCompactionCandidateStatus status) {
    insertionCompactionCandidateStatus = status;
  }
}
