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

package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModFileManagement;
import org.apache.iotdb.db.storageengine.dataregion.modification.PartitionLevelModFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndexCacheRecorder;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.TimeIndexLevel;

import org.apache.tsfile.read.filter.basic.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TsFileManager {
  private final String storageGroupName;
  private String dataRegionId;
  private final String dataRegionSysDir;

  /** Serialize queries, delete resource files, compaction cleanup files */
  private final ReadWriteLock resourceListLock = new ReentrantReadWriteLock();

  private String writeLockHolder;
  // time partition -> double linked list of tsfiles
  private final TreeMap<Long, TsFileResourceList> sequenceFiles = new TreeMap<>();
  private final TreeMap<Long, TsFileResourceList> unsequenceFiles = new TreeMap<>();
  private final TreeMap<Long, ModFileManagement> modFileManagementMap = new TreeMap<>();
  private final TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();

  private volatile boolean allowCompaction = true;
  private final AtomicLong currentCompactionTaskSerialId = new AtomicLong(0);

  public TsFileManager(String storageGroupName, String dataRegionId, String dataRegionSysDir) {
    this.storageGroupName = storageGroupName;
    this.dataRegionSysDir = dataRegionSysDir;
    this.dataRegionId = dataRegionId;
  }

  public List<TsFileResource> getTsFileList(boolean sequence) {
    return getTsFileList(sequence, null, null);
  }

  /**
   * @param sequence {@code true} for sequence, {@code false} for unsequence
   * @param timePartitions {@code null} for all time partitions, empty for zero time partitions
   */
  public List<TsFileResource> getTsFileList(
      boolean sequence, List<Long> timePartitions, Filter timeFilter) {
    // the iteration of ConcurrentSkipListMap is not concurrent secure
    // so we must add read lock here
    readLock();
    try {
      List<TsFileResource> allResources = new ArrayList<>();
      Map<Long, TsFileResourceList> chosenMap = sequence ? sequenceFiles : unsequenceFiles;
      if (timePartitions == null) {
        for (Map.Entry<Long, TsFileResourceList> entry : chosenMap.entrySet()) {
          if (TimePartitionUtils.satisfyTimePartition(timeFilter, entry.getKey())) {
            allResources.addAll(entry.getValue().getArrayList());
          }
        }
      } else {
        for (Long timePartitionId : timePartitions) {
          TsFileResourceList tsFileResources = chosenMap.get(timePartitionId);
          if (tsFileResources != null) {
            allResources.addAll(tsFileResources.getArrayList());
          }
        }
      }
      return allResources;
    } finally {
      readUnlock();
    }
  }

  public List<TsFileResource> getTsFileListSnapshot(long timePartition, boolean sequence) {
    readLock();
    try {
      Map<Long, TsFileResourceList> chosenMap = sequence ? sequenceFiles : unsequenceFiles;
      return new ArrayList<>(chosenMap.getOrDefault(timePartition, new TsFileResourceList()));
    } finally {
      readUnlock();
    }
  }

  public List<TsFileResource> getTsFileList(boolean sequence, long startTime, long endTime) {
    // the iteration of ConcurrentSkipListMap is not concurrent secure
    // so we must add read lock here
    readLock();
    try {
      List<TsFileResource> allResources = new ArrayList<>();
      Map<Long, TsFileResourceList> chosenMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : chosenMap.entrySet()) {
        if (TimePartitionUtils.satisfyPartitionId(startTime, endTime, entry.getKey())) {
          allResources.addAll(entry.getValue().getArrayList());
        }
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
          break;
        }
      }
    } finally {
      writeUnlock();
    }
    tsFileResourceManager.removeTsFileResource(tsFileResource);
  }

  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    writeLock("removeAll");
    try {
      for (TsFileResource resource : tsFileResourceList) {
        remove(resource, sequence);
      }
    } finally {
      writeLock("removeAll");
    }
  }

  /**
   * Insert tsFileResource to a target pos(targetPos = insertPos) e.g. if insertPos = 0, then to the
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
      if (tsFileResource.getModFileManagement() == null) {
        tsFileResource.setModFileManagement(
            modFileManagementMap.computeIfAbsent(
                timePartition, t -> new PartitionLevelModFileManager()));
      }
    } finally {
      writeUnlock();
    }
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
  }

  public void add(TsFileResource tsFileResource, boolean sequence) {
    writeLock("add");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      selectedMap
          .computeIfAbsent(tsFileResource.getTimePartition(), o -> new TsFileResourceList())
          .add(tsFileResource);
      if (tsFileResource.getModFileManagement() == null) {
        tsFileResource.setModFileManagement(
            modFileManagementMap.computeIfAbsent(
                tsFileResource.getTimePartition(), t -> new PartitionLevelModFileManager()));
      }
    } finally {
      writeUnlock();
    }
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
  }

  public void keepOrderInsert(TsFileResource tsFileResource, boolean sequence) throws IOException {
    writeLock("keepOrderInsert");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      selectedMap
          .computeIfAbsent(tsFileResource.getTimePartition(), o -> new TsFileResourceList())
          .keepOrderInsert(tsFileResource);
      if (tsFileResource.getModFileManagement() == null) {
        tsFileResource.setModFileManagement(
            modFileManagementMap.computeIfAbsent(
                tsFileResource.getTimePartition(), t -> new PartitionLevelModFileManager()));
      }
    } finally {
      writeUnlock();
    }
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
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
      long timePartition)
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
      for (TsFileResource resource : targetFileResources) {
        if (!resource.isDeleted()) {
          TsFileResourceManager.getInstance().registerSealedTsFileResource(resource);
          if (resource.isSeq()) {
            sequenceFiles
                .computeIfAbsent(timePartition, t -> new TsFileResourceList())
                .keepOrderInsert(resource);
          } else {
            unsequenceFiles
                .computeIfAbsent(timePartition, t -> new TsFileResourceList())
                .keepOrderInsert(resource);
          }
          FileTimeIndexCacheRecorder.getInstance().logFileTimeIndex(resource);
          if (resource.getModFileManagement() == null) {
            resource.setModFileManagement(
                modFileManagementMap.computeIfAbsent(
                    resource.getTimePartition(), t -> new PartitionLevelModFileManager()));
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

  public void getModFileManagement() {}

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

  public void writeUnlock() {
    resourceListLock.writeLock().unlock();
    writeLockHolder = "";
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public String getDataRegionSysDir() {
    return dataRegionSysDir;
  }

  public Set<Long> getTimePartitions() {
    readLock();
    try {
      Set<Long> timePartitions = new HashSet<>(sequenceFiles.keySet());
      timePartitions.addAll(unsequenceFiles.keySet());
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

  public void compactFileTimeIndexCache() {
    int currentResourceSize = size(true) + size(false);
    readLock();
    try {
      FileTimeIndexCacheRecorder.getInstance()
          .compactFileTimeIndexIfNeeded(
              storageGroupName,
              Integer.parseInt(dataRegionId),
              currentResourceSize,
              sequenceFiles,
              unsequenceFiles);
    } finally {
      readUnlock();
    }
  }

  public long getMaxFileTimestampOfUnSequenceFile() {
    long maxFileTimestamp = -1;
    resourceListLock.readLock().lock();
    try {
      for (TsFileResourceList resourceList : unsequenceFiles.values()) {
        TsFileResource lastResource = resourceList.get(resourceList.size() - 1);
        maxFileTimestamp = Math.max(maxFileTimestamp, lastResource.getTimePartition());
      }
    } finally {
      resourceListLock.readLock().unlock();
    }
    return maxFileTimestamp;
  }

  public static class TsFileResourceManager {
    private static final Logger logger = LoggerFactory.getLogger(TsFileResourceManager.class);

    private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

    /** threshold total memory for all TimeIndex */
    private long timeIndexMemoryThreshold = CONFIG.getAllocateMemoryForTimeIndex();

    /** store the sealed TsFileResource, sorted by priority of TimeIndex */
    private final TreeSet<TsFileResource> sealedTsFileResources =
        new TreeSet<>(TsFileResource::compareIndexDegradePriority);

    /** total used memory for TimeIndex */
    private long totalTimeIndexMemCost;

    // degraded time index number
    private long degradedTimeIndexNum = 0;

    @TestOnly
    public void setTimeIndexMemoryThreshold(long timeIndexMemoryThreshold) {
      this.timeIndexMemoryThreshold = timeIndexMemoryThreshold;
    }

    public long getPriorityQueueSize() {
      return sealedTsFileResources.size();
    }

    /**
     * add the closed TsFileResource into priorityQueue and increase memory cost of timeIndex, once
     * memory cost is larger than threshold, degradation is triggered.
     */
    private synchronized void registerSealedTsFileResource(TsFileResource tsFileResource) {
      if (!sealedTsFileResources.contains(tsFileResource)) {
        sealedTsFileResources.add(tsFileResource);
        totalTimeIndexMemCost += tsFileResource.calculateRamSize();
        chooseTsFileResourceToDegrade();
      }
    }

    /** delete the TsFileResource in PriorityQueue when the source file is deleted */
    private synchronized void removeTsFileResource(TsFileResource tsFileResource) {
      if (sealedTsFileResources.contains(tsFileResource)) {
        sealedTsFileResources.remove(tsFileResource);
        if (TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType())
            == TimeIndexLevel.FILE_TIME_INDEX) {
          totalTimeIndexMemCost -= tsFileResource.calculateRamSize();
          degradedTimeIndexNum--;
        } else {
          totalTimeIndexMemCost -= tsFileResource.getRamSize();
        }
      }
    }

    /** once degradation is triggered, the total memory for timeIndex should reduce */
    private void releaseTimeIndexMemCost(long memCost) {
      totalTimeIndexMemCost -= memCost;
    }

    /**
     * choose the top TsFileResource in priorityQueue to degrade until the memory is smaller than
     * threshold.
     */
    private void chooseTsFileResourceToDegrade() {
      while (totalTimeIndexMemCost > timeIndexMemoryThreshold) {
        TsFileResource tsFileResource = sealedTsFileResources.pollFirst();
        if (tsFileResource == null
            || TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType())
                == TimeIndexLevel.FILE_TIME_INDEX) {
          logger.debug("Can't degrade time index any more because all time index are file level.");
          sealedTsFileResources.add(tsFileResource);
          return;
        }
        long memoryReduce = tsFileResource.degradeTimeIndex();
        logger.debug("Degrade tsfile resource {}", tsFileResource.getTsFilePath());
        degradedTimeIndexNum++;
        releaseTimeIndexMemCost(memoryReduce);
        // add the polled tsFileResource to the priority queue
        sealedTsFileResources.add(tsFileResource);
      }
    }

    public long getDegradedTimeIndexNum() {
      return degradedTimeIndexNum;
    }

    public long getTimeIndexMemoryThreshold() {
      return timeIndexMemoryThreshold;
    }

    public long getTotalTimeIndexMemCost() {
      return totalTimeIndexMemCost;
    }

    /** function for clearing TsFileManager */
    public synchronized void clear() {
      this.sealedTsFileResources.clear();
      this.totalTimeIndexMemCost = 0;
      this.degradedTimeIndexNum = 0;
    }

    public static TsFileResourceManager getInstance() {
      return InstanceHolder.INSTANCE;
    }

    private static class InstanceHolder {
      private InstanceHolder() {}

      private static final TsFileResourceManager INSTANCE = new TsFileResourceManager();
    }
  }
}
