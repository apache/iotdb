/*
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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.db.sync.sender.manager.ISyncManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileManager.class);
  private String storageGroupName;
  private String dataRegionId;
  private String storageGroupDir;

  /** Serialize queries, delete resource files, compaction cleanup files */
  private final ReadWriteLock resourceListLock = new ReentrantReadWriteLock();

  private String writeLockHolder;
  // time partition -> double linked list of tsfiles
  private TreeMap<Long, TsFileResourceList> sequenceFiles = new TreeMap<>();
  private TreeMap<Long, TsFileResourceList> unsequenceFiles = new TreeMap<>();

  private List<TsFileResource> sequenceRecoverTsFileResources = new ArrayList<>();
  private List<TsFileResource> unsequenceRecoverTsFileResources = new ArrayList<>();

  private boolean allowCompaction = true;
  private AtomicLong currentCompactionTaskSerialId = new AtomicLong(0);

  public TsFileManager(String storageGroupName, String dataRegionId, String storageGroupDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupDir = storageGroupDir;
    this.dataRegionId = dataRegionId;
  }

  public List<TsFileResource> getTsFileList(boolean sequence) {
    // the iteration of ConcurrentSkipListMap is not concurrent secure
    // so we must add read lock here
    readLock();
    try {
      List<TsFileResource> allResources = new ArrayList<>();
      Map<Long, TsFileResourceList> chosenMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : chosenMap.entrySet()) {
        allResources.addAll(entry.getValue().getArrayList());
      }
      return allResources;
    } finally {
      readUnlock();
    }
  }

  public TsFileResourceList getOrCreateSequenceListByTimePartition(long timePartition) {
    writeLock("getOrCreateSequenceListByTimePartition");
    try {
      return sequenceFiles.computeIfAbsent(timePartition, l -> new TsFileResourceList());
    } finally {
      writeUnlock();
    }
  }

  public TsFileResourceList getOrCreateUnsequenceListByTimePartition(long timePartition) {
    writeLock("getOrCreateUnsequenceListByTimePartition");
    try {
      return unsequenceFiles.computeIfAbsent(timePartition, l -> new TsFileResourceList());
    } finally {
      writeUnlock();
    }
  }

  public Iterator<TsFileResource> getIterator(boolean sequence) {
    readLock();
    try {
      return getTsFileList(sequence).iterator();
    } finally {
      readUnlock();
    }
  }

  public void remove(TsFileResource tsFileResource, boolean sequence) {
    writeLock("remove");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : selectedMap.entrySet()) {
        if (entry.getValue().contains(tsFileResource)) {
          entry.getValue().remove(tsFileResource);
          TsFileResourceManager.getInstance().removeTsFileResource(tsFileResource);
          break;
        }
      }
    } finally {
      writeUnlock();
    }
  }

  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    writeLock("removeAll");
    try {
      for (TsFileResource resource : tsFileResourceList) {
        remove(resource, sequence);
        TsFileResourceManager.getInstance().removeTsFileResource(resource);
      }
    } finally {
      writeLock("removeAll");
    }
  }

  /**
   * insert tsFileResource to a target pos(targetPos = insertPos) e.g. if insertPos = 0, then to the
   * first, if insert Pos = 1, then to the second.
   */
  public void insertToPartitionFileList(
      TsFileResource tsFileResource, long timePartition, boolean sequence, int insertPos) {
    writeLock("add");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      TsFileResourceList tsFileResources =
          selectedMap.computeIfAbsent(timePartition, o -> new TsFileResourceList());
      tsFileResources.set(insertPos, tsFileResource);
    } finally {
      writeUnlock();
    }
  }

  public void add(TsFileResource tsFileResource, boolean sequence) {
    writeLock("add");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      selectedMap
          .computeIfAbsent(tsFileResource.getTimePartition(), o -> new TsFileResourceList())
          .add(tsFileResource);
    } finally {
      writeUnlock();
    }
  }

  public void keepOrderInsert(TsFileResource tsFileResource, boolean sequence) throws IOException {
    writeLock("keepOrderInsert");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      selectedMap
          .computeIfAbsent(tsFileResource.getTimePartition(), o -> new TsFileResourceList())
          .keepOrderInsert(tsFileResource);
    } finally {
      writeUnlock();
    }
  }

  public void addForRecover(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      sequenceRecoverTsFileResources.add(tsFileResource);
    } else {
      unsequenceRecoverTsFileResources.add(tsFileResource);
    }
  }

  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    writeLock("add");
    try {
      for (TsFileResource resource : tsFileResourceList) {
        add(resource, sequence);
      }
    } finally {
      writeUnlock();
    }
  }

  /** This method is called after compaction to update memory. */
  public void replace(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources,
      long timePartition,
      boolean isTargetSequence)
      throws IOException {
    writeLock("replace");
    try {
      for (TsFileResource tsFileResource : seqFileResources) {
        if (sequenceFiles.get(timePartition).remove(tsFileResource)) {
          TsFileResourceManager.getInstance().removeTsFileResource(tsFileResource);
        }
      }
      for (TsFileResource tsFileResource : unseqFileResources) {
        if (unsequenceFiles.get(timePartition).remove(tsFileResource)) {
          TsFileResourceManager.getInstance().removeTsFileResource(tsFileResource);
        }
      }
      if (isTargetSequence) {
        // seq inner space compaction or cross space compaction
        for (TsFileResource resource : targetFileResources) {
          if (!resource.isDeleted()) {
            TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
            sequenceFiles.get(timePartition).keepOrderInsert(resource);
          }
        }
      } else {
        // unseq inner space compaction
        for (TsFileResource resource : targetFileResources) {
          if (!resource.isDeleted()) {
            TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
            unsequenceFiles.get(timePartition).keepOrderInsert(resource);
          }
        }
      }

    } finally {
      writeUnlock();
    }
  }

  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    readLock();
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      TsFileResourceList list = selectedMap.getOrDefault(tsFileResource.getTimePartition(), null);
      return list != null && list.contains(tsFileResource);
    } finally {
      readUnlock();
    }
  }

  public void clear() {
    writeLock("clear");
    try {
      sequenceFiles.clear();
      unsequenceFiles.clear();
    } finally {
      writeUnlock();
    }
  }

  public boolean isEmpty(boolean sequence) {
    readLock();
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : selectedMap.entrySet()) {
        if (!entry.getValue().isEmpty()) {
          return false;
        }
      }
      return true;
    } finally {
      readUnlock();
    }
  }

  public int size(boolean sequence) {
    readLock();
    try {
      int totalSize = 0;
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : selectedMap.entrySet()) {
        totalSize += entry.getValue().size();
      }
      return totalSize;
    } finally {
      readUnlock();
    }
  }

  public void readLock() {
    resourceListLock.readLock().lock();
  }

  public void readUnlock() {
    resourceListLock.readLock().unlock();
  }

  public void writeLock(String holder) {
    resourceListLock.writeLock().lock();
    writeLockHolder = holder;
  }

  /**
   * Acquire write lock with timeout, {@link WriteLockFailedException} will be thrown after timeout.
   * The unit of timeout is ms.
   */
  public void writeLockWithTimeout(String holder, long timeout) throws WriteLockFailedException {
    try {
      if (resourceListLock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
        writeLockHolder = holder;
      } else {
        throw new WriteLockFailedException(
            String.format("cannot get write lock in %d ms", timeout));
      }
    } catch (InterruptedException e) {
      LOGGER.warn(e.getMessage(), e);
      Thread.interrupted();
      throw new WriteLockFailedException("thread is interrupted");
    }
  }

  public void writeUnlock() {
    resourceListLock.writeLock().unlock();
    writeLockHolder = "";
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public String getStorageGroupDir() {
    return storageGroupDir;
  }

  public void setStorageGroupDir(String storageGroupDir) {
    this.storageGroupDir = storageGroupDir;
  }

  public Set<Long> getTimePartitions() {
    readLock();
    try {
      Set<Long> timePartitions = new HashSet<>(sequenceFiles.keySet());
      return timePartitions;
    } finally {
      readUnlock();
    }
  }

  public boolean isAllowCompaction() {
    return allowCompaction;
  }

  public void setAllowCompaction(boolean allowCompaction) {
    this.allowCompaction = allowCompaction;
  }

  public String getDataRegionId() {
    return dataRegionId;
  }

  public void setDataRegionId(String dataRegionId) {
    this.dataRegionId = dataRegionId;
  }

  public List<TsFileResource> getSequenceRecoverTsFileResources() {
    return sequenceRecoverTsFileResources;
  }

  public List<TsFileResource> getUnsequenceRecoverTsFileResources() {
    return unsequenceRecoverTsFileResources;
  }

  public List<File> collectHistoryTsFileForSync(ISyncManager syncManager, long dataStartTime) {
    readLock();
    try {
      List<File> historyTsFiles = new ArrayList<>();
      collectTsFile(historyTsFiles, getTsFileList(true), syncManager, dataStartTime);
      collectTsFile(historyTsFiles, getTsFileList(false), syncManager, dataStartTime);
      return historyTsFiles;
    } finally {
      readUnlock();
    }
  }

  private void collectTsFile(
      List<File> historyTsFiles,
      List<TsFileResource> tsFileResources,
      ISyncManager syncManager,
      long dataStartTime) {

    for (TsFileResource tsFileResource : tsFileResources) {
      if (tsFileResource.getFileEndTime() < dataStartTime) {
        continue;
      }
      TsFileProcessor tsFileProcessor = tsFileResource.getProcessor();
      boolean isRealTimeTsFile = false;
      if (tsFileProcessor != null) {
        isRealTimeTsFile = tsFileProcessor.isMemtableNotNull();
      }
      File tsFile = tsFileResource.getTsFile();
      if (!isRealTimeTsFile) {
        File mods = new File(tsFileResource.getModFile().getFilePath());
        long modsOffset = mods.exists() ? mods.length() : 0L;
        File hardlink = syncManager.createHardlink(tsFile, modsOffset);
        if (hardlink != null) {
          historyTsFiles.add(hardlink);
        }
      }
    }
  }

  // ({systemTime}-{versionNum}-{innerCompactionNum}-{crossCompactionNum}.tsfile)
  public static int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      int cmpVersion = Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
      if (cmpVersion == 0) {
        return Long.compare(Long.parseLong(items1[2]), Long.parseLong(items2[2]));
      }
      return cmpVersion;
    } else {
      return cmp;
    }
  }

  public long getNextCompactionTaskId() {
    return currentCompactionTaskSerialId.getAndIncrement();
  }

  public boolean hasNextTimePartition(long timePartition, boolean sequence) {
    try {
      return sequence
          ? sequenceFiles.higherKey(timePartition) != null
          : unsequenceFiles.higherKey(timePartition) != null;
    } catch (NullPointerException e) {
      return false;
    }
  }

  // determine whether time partition is the latest(largest) or not
  public boolean isLatestTimePartition(long timePartitionId) {
    return (sequenceFiles.higherKey(timePartitionId) == null
        && unsequenceFiles.higherKey(timePartitionId) == null);
  }
}
