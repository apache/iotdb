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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.db.sync.sender.manager.TsFileSyncManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TsFileManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileManager.class);
  private String storageGroupName;
  private String dataRegion;
  private String storageGroupDir;

  /** Serialize queries, delete resource files, compaction cleanup files */
  private final ReadWriteLock resourceListLock = new ReentrantReadWriteLock();

  private String writeLockHolder;
  // time partition -> double linked list of tsfiles
  private Map<Long, TsFileResourceList> sequenceFiles = new TreeMap<>();
  private Map<Long, TsFileResourceList> unsequenceFiles = new TreeMap<>();

  private List<TsFileResource> sequenceRecoverTsFileResources = new ArrayList<>();
  private List<TsFileResource> unsequenceRecoverTsFileResources = new ArrayList<>();

  private boolean allowCompaction = true;

  public TsFileManager(String storageGroupName, String dataRegion, String storageGroupDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupDir = storageGroupDir;
    this.dataRegion = dataRegion;
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

  public TsFileResourceList getSequenceListByTimePartition(long timePartition) {
    readLock();
    try {
      return sequenceFiles.computeIfAbsent(timePartition, l -> new TsFileResourceList());
    } finally {
      readUnlock();
    }
  }

  public TsFileResourceList getUnsequenceListByTimePartition(long timePartition) {
    readLock();
    try {
      return unsequenceFiles.computeIfAbsent(timePartition, l -> new TsFileResourceList());
    } finally {
      readUnlock();
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
      TsFileResource tsFileResource, boolean sequence, int insertPos) {
    writeLock("add");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      TsFileResourceList tsFileResources =
          selectedMap.computeIfAbsent(
              tsFileResource.getTimePartition(), o -> new TsFileResourceList());
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

  public void keepOrderInsert(TsFileResource tsFileResource, boolean sequence) {
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

  /**
   * Add files with same timestamp after target file in order, some files' version field will be
   * renamed when name conflicting occurs.
   *
   * @param addAfterThisFile files will be added after this file, null means add to the beginning of
   *     corresponding TsFileResourceList
   * @param filesToAdd files to add, add order will follow files order in the list. File name format
   *     should like {@link TsFileName#TS_FILE_NAME_PATTERN } and their time fields should be same.
   * @param sequence files in filesToAdd are sequence or not
   * @param timePartition the time partition of files in filesToAdd
   * @return false if target file doesn't exist in the TsFileManger or files to add don't share same
   *     timestamp
   */
  public boolean keepOrderAddAllAndRenameAfter(
      TsFileResource addAfterThisFile,
      List<TsFileResource> filesToAdd,
      boolean sequence,
      long timePartition) {
    if (filesToAdd.isEmpty()) {
      return true;
    }
    // all files to add should share the same timestamp
    long targetTime = filesToAdd.get(0).getCreatedTime();
    for (TsFileResource resource : filesToAdd) {
      if (resource.getCreatedTime() != targetTime) {
        LOGGER.warn(
            "File {} and file {} don't share the same timestamp, please check it.",
            filesToAdd.get(0).getTsFile(),
            resource.getTsFile());
        return false;
      }
    }

    writeLock("keepOrderAddAllAndRenameAfter");
    try {
      Map<Long, TsFileResourceList> targetMap = sequence ? sequenceFiles : unsequenceFiles;
      TsFileResourceList targetList =
          targetMap.computeIfAbsent(timePartition, o -> new TsFileResourceList());
      Map<Long, TsFileResourceList> leftMap = sequence ? unsequenceFiles : sequenceFiles;
      TsFileResourceList leftList =
          leftMap.computeIfAbsent(timePartition, o -> new TsFileResourceList());

      // check target file position
      if (addAfterThisFile == null) {
        // check time, make sure target time <= header.time
        if (!targetList.isEmpty() && targetList.get(0).getCreatedTime() < targetTime) {
          LOGGER.warn(
              "Files to add will destroy the order of TsFileResourceList, please check the add position of file {}",
              filesToAdd.get(0).getTsFile());
          return false;
        }
      } else {
        // target file doesn't exist
        if (!targetList.contains(addAfterThisFile)) {
          LOGGER.warn(
              "TsFileManager doesn't contain file {}, cannot add files after it.",
              addAfterThisFile.getTsFile());
          return false;
        }
        // check time, make sure prev.time <= target time <= next.time
        if ((addAfterThisFile.prev != null && addAfterThisFile.prev.getCreatedTime() > targetTime)
            || (addAfterThisFile.next != null
                && addAfterThisFile.next.getCreatedTime() < targetTime)) {
          LOGGER.warn(
              "Files to add will destroy the order of TsFileResourceList, please check the add position of file {}",
              filesToAdd.get(0).getTsFile());
          return false;
        }
      }

      // filter files need renaming
      List<TsFileResource> filesToRename = new ArrayList<>();
      long startVersion =
          addAfterThisFile != null && addAfterThisFile.getCreatedTime() == targetTime
              ? addAfterThisFile.getVersion() + 1
              : -1;
      // 1. filter files need renaming from target list
      List<TsFileResource> targetSameTimeList = targetList.getFilesByTime(targetTime);
      // e.g., seq: [1-0-0-0, add here with time=2, 2-0-0-0], unseq: [2-1-0-0]
      if (startVersion == -1 && !targetSameTimeList.isEmpty()) {
        startVersion = targetSameTimeList.get(0).getVersion();
      }
      for (TsFileResource resource : targetSameTimeList) {
        if (resource.getVersion() >= startVersion) {
          filesToRename.add(resource);
        }
      }
      // 2. filter files need renaming from another list
      List<TsFileResource> leftSameTimeList = leftList.getFilesByTime(targetTime);
      // e.g., seq: [1-0-0-0, add here with time=2, 3-0-0-0], unseq: [2-0-0-0, 2-1-0-0]
      if (startVersion == -1 && !leftSameTimeList.isEmpty()) {
        startVersion = leftSameTimeList.get(leftSameTimeList.size() - 1).getVersion() + 1;
      }
      for (TsFileResource resource : leftSameTimeList) {
        if (resource.getVersion() >= startVersion) {
          filesToRename.add(resource);
        }
      }

      // rename existing files
      filesToRename.sort(((Comparator<TsFileResource>) (TsFileName::compareFileName)).reversed());
      long offset = filesToAdd.size();
      for (TsFileResource resource : filesToRename) {
        resource.setVersion(resource.getVersion() + offset);
      }

      // add files
      if (startVersion == -1) {
        startVersion = 0;
      }
      int index = 0;
      if (addAfterThisFile == null) {
        addAfterThisFile = filesToAdd.get(0);
        addAfterThisFile.setVersion(startVersion);
        startVersion++;
        index++;
        targetList.set(0, addAfterThisFile);
      }
      TsFileResource prev = addAfterThisFile;
      for (; index < filesToAdd.size(); index++) {
        TsFileResource current = filesToAdd.get(index);
        // update version
        current.setVersion(startVersion);
        startVersion++;
        // add it to manager
        targetList.insertAfter(prev, current);
        prev = current;
      }
    } finally {
      writeUnlock();
    }

    return true;
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
      // timestamp-version -> resourceList
      Map<String, List<TsFileResource>> time2TargetFiles = new HashMap<>();
      for (TsFileResource resource : targetFileResources) {
        time2TargetFiles
            .computeIfAbsent(
                resource.getCreatedTime()
                    + "-"
                    + resource.getVersion()
                        % IoTDBConstant.CROSS_COMPACTION_TMP_FILE_VERSION_INTERVAL,
                k -> new ArrayList<>())
            .add(resource);
      }
      // timestamp-version -> resource
      Map<String, TsFileResource> time2TargetPosition = new HashMap<>();
      // find insert target position
      for (TsFileResource tsFileResource : seqFileResources) {
        String key = tsFileResource.getCreatedTime() + "-" + tsFileResource.getVersion();
        if (time2TargetFiles.containsKey(key)) {
          time2TargetPosition.put(key, tsFileResource);
        }
      }
      Object[] keyArr = time2TargetPosition.keySet().toArray();
      Arrays.sort(keyArr);

      // first add target from the last position
      for (int i = keyArr.length - 1; i >= 0; i--) {
        String key = keyArr[i].toString();
        keepOrderAddAllAndRenameAfter(
            time2TargetPosition.get(key),
            time2TargetFiles.get(key),
            isTargetSequence,
            timePartition);
      }
      // then remove source
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
      // register to TsFileResourceManager
      for (TsFileResource resource : targetFileResources) {
        TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
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

  public String getDataRegion() {
    return dataRegion;
  }

  public void setDataRegion(String dataRegion) {
    this.dataRegion = dataRegion;
  }

  public List<TsFileResource> getSequenceRecoverTsFileResources() {
    return sequenceRecoverTsFileResources;
  }

  public List<TsFileResource> getUnsequenceRecoverTsFileResources() {
    return unsequenceRecoverTsFileResources;
  }

  public List<File> collectHistoryTsFileForSync(long dataStartTime) {
    readLock();
    try {
      List<File> historyTsFiles = new ArrayList<>();
      collectTsFile(historyTsFiles, getTsFileList(true), dataStartTime);
      collectTsFile(historyTsFiles, getTsFileList(false), dataStartTime);
      return historyTsFiles;
    } finally {
      readUnlock();
    }
  }

  private void collectTsFile(
      List<File> historyTsFiles, List<TsFileResource> tsFileResources, long dataStartTime) {
    TsFileSyncManager syncManager = TsFileSyncManager.getInstance();

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
}
