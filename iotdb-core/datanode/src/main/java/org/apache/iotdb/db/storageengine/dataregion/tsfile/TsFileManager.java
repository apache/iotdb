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

import org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TsFileManager {
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
}
