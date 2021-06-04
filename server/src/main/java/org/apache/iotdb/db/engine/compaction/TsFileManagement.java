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

package org.apache.iotdb.db.engine.compaction;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileManagement {

  private static final Logger logger = LoggerFactory.getLogger(TsFileManagement.class);
  public String storageGroupName;
  public String storageGroupSysDir;

  /** Serialize queries, delete resource files, compaction cleanup files */
  private final ReadWriteLock compactionMergeLock = new ReentrantReadWriteLock();

  /**
   * This is the modification file of the result of the current merge. Because the merged file may
   * be invisible at this moment, without this, deletion/update during merge could be lost.
   */
  public ModificationFile mergingModification;

  // First map is partition list; Second list is level list; Third list is file list in level;
  private final Map<Long, List<SortedSet<TsFileResource>>> sequenceTsFileResources =
      new HashMap<>();
  private final Map<Long, List<List<TsFileResource>>> unSequenceTsFileResources = new HashMap<>();

  private final List<TsFileResource> sequenceRecoverTsFileResources = new ArrayList<>();
  private final List<TsFileResource> unSequenceRecoverTsFileResources = new ArrayList<>();

  private final int seqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum(), 1);
  private final int unseqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);

  public TsFileManagement(String storageGroupName, String storageGroupSysDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupSysDir = storageGroupSysDir;
  }

  /**
   * get the TsFile list in sequence, not recommend to use this method, use
   * getTsFileListByTimePartition instead
   */
  @Deprecated
  public List<TsFileResource> getTsFileList(boolean sequence) {
    readLock();
    try {
      List<TsFileResource> result = new ArrayList<>();
      if (sequence) {
        for (long timePartition : sequenceTsFileResources.keySet()) {
          result.addAll(getTsFileListByTimePartition(true, timePartition));
        }
      } else {
        for (long timePartition : unSequenceTsFileResources.keySet()) {
          result.addAll(getTsFileListByTimePartition(false, timePartition));
        }
      }
      return result;
    } finally {
      readUnLock();
    }
  }

  /** get the TsFile list in sequence by time partition and level */
  public List<TsFileResource> getTsFileListByTimePartitionAndLevel(
      boolean sequence, long timePartition, int level) {
    readLock();
    try {
      if (sequence) {
        List<SortedSet<TsFileResource>> sequenceTsFileList =
            sequenceTsFileResources.get(timePartition);
        return new ArrayList<>(sequenceTsFileList.get(level));
      } else {
        List<List<TsFileResource>> unSequenceTsFileList =
            unSequenceTsFileResources.get(timePartition);
        return new ArrayList<>(unSequenceTsFileList.get(level));
      }
    } finally {
      readUnLock();
    }
  }

  /**
   * get the closed TsFile list in the format of tree in sequence by time partition, used by level
   * compaction
   */
  public List<List<TsFileResource>> getClosedTsFileListByTimePartition(
      boolean sequence, long timePartition) {
    readLock();
    try {
      List<List<TsFileResource>> result = new ArrayList<>();
      if (sequence) {
        List<SortedSet<TsFileResource>> sequenceTsFileList =
            sequenceTsFileResources.get(timePartition);
        for (SortedSet<TsFileResource> tsFileResources : sequenceTsFileList) {
          result.add(new ArrayList<>(tsFileResources));
        }
      } else {
        List<List<TsFileResource>> unSequenceTsFileList =
            unSequenceTsFileResources.get(timePartition);
        for (List<TsFileResource> tsFileResources : unSequenceTsFileList) {
          result.add(new ArrayList<>(tsFileResources));
        }
      }
      return result;
    } finally {
      readUnLock();
    }
  }

  /** get the TsFile list in sequence by time partition */
  public List<TsFileResource> getTsFileListByTimePartition(boolean sequence, long timePartition) {
    readLock();
    try {
      List<TsFileResource> result = new ArrayList<>();
      if (sequence) {
        List<SortedSet<TsFileResource>> sequenceTsFileList =
            sequenceTsFileResources.get(timePartition);
        for (int i = sequenceTsFileList.size() - 1; i >= 0; i--) {
          result.addAll(sequenceTsFileList.get(i));
        }
      } else {
        List<List<TsFileResource>> unSequenceTsFileList =
            unSequenceTsFileResources.get(timePartition);
        for (int i = unSequenceTsFileList.size() - 1; i >= 0; i--) {
          result.addAll(unSequenceTsFileList.get(i));
        }
      }
      return result;
    } finally {
      readUnLock();
    }
  }

  /** get the TsFile list iterator in sequence */
  public Iterator<TsFileResource> getIterator(boolean sequence) {
    readLock();
    try {
      return getTsFileList(sequence).iterator();
    } finally {
      readUnLock();
    }
  }

  /** remove one TsFile from list */
  public void remove(TsFileResource tsFileResource, boolean sequence) {
    writeLock();
    try {
      if (sequence) {
        for (SortedSet<TsFileResource> sequenceTsFileResource :
            sequenceTsFileResources.get(tsFileResource.getTimePartition())) {
          sequenceTsFileResource.remove(tsFileResource);
        }
      } else {
        for (List<TsFileResource> unSequenceTsFileResource :
            unSequenceTsFileResources.get(tsFileResource.getTimePartition())) {
          unSequenceTsFileResource.remove(tsFileResource);
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /** remove some TsFiles from list */
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    writeLock();
    try {
      if (sequence) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
            sequenceTsFileResources.values()) {
          for (SortedSet<TsFileResource> levelTsFileResource : partitionSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      } else {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (List<TsFileResource> levelTsFileResource : partitionUnSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /** add one TsFile to list */
  public void add(TsFileResource tsFileResource, boolean sequence) {
    writeLock();
    try {
      long timePartitionId = tsFileResource.getTimePartition();
      int level = getMergeLevel(tsFileResource.getTsFile());
      if (sequence) {
        if (level <= seqLevelNum - 1) {
          // current file has normal level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(seqLevelNum - 1)
              .add(tsFileResource);
        }
      } else {
        if (level <= unseqLevelNum - 1) {
          // current file has normal level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(unseqLevelNum - 1)
              .add(tsFileResource);
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /** add one TsFile to list for recover */
  public void addRecover(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      sequenceRecoverTsFileResources.add(tsFileResource);
    } else {
      unSequenceRecoverTsFileResources.add(tsFileResource);
    }
  }

  /** add some TsFiles to list */
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    writeLock();
    try {
      for (TsFileResource tsFileResource : tsFileResourceList) {
        add(tsFileResource, sequence);
      }
    } finally {
      writeUnlock();
    }
  }

  /** is one TsFile contained in list */
  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    readLock();
    try {
      if (sequence) {
        for (SortedSet<TsFileResource> sequenceTsFileResource :
            sequenceTsFileResources.computeIfAbsent(
                tsFileResource.getTimePartition(), this::newSequenceTsFileResources)) {
          if (sequenceTsFileResource.contains(tsFileResource)) {
            return true;
          }
        }
      } else {
        for (List<TsFileResource> unSequenceTsFileResource :
            unSequenceTsFileResources.computeIfAbsent(
                tsFileResource.getTimePartition(), this::newUnSequenceTsFileResources)) {
          if (unSequenceTsFileResource.contains(tsFileResource)) {
            return true;
          }
        }
      }
      return false;
    } finally {
      readUnLock();
    }
  }

  /** clear list */
  public void clear() {
    writeLock();
    try {
      sequenceTsFileResources.clear();
      unSequenceTsFileResources.clear();
    } finally {
      writeUnlock();
    }
  }

  /** is the list empty */
  @SuppressWarnings("squid:S3776")
  public boolean isEmpty(boolean sequence) {
    readLock();
    try {
      if (sequence) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
            sequenceTsFileResources.values()) {
          for (SortedSet<TsFileResource> sequenceTsFileResource : partitionSequenceTsFileResource) {
            if (!sequenceTsFileResource.isEmpty()) {
              return false;
            }
          }
        }
      } else {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (List<TsFileResource> unSequenceTsFileResource : partitionUnSequenceTsFileResource) {
            if (!unSequenceTsFileResource.isEmpty()) {
              return false;
            }
          }
        }
      }
      return true;
    } finally {
      readUnLock();
    }
  }

  /** return TsFile list size */
  public int size(boolean sequence) {
    readLock();
    try {
      int result = 0;
      if (sequence) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
            sequenceTsFileResources.values()) {
          for (int i = seqLevelNum - 1; i >= 0; i--) {
            result += partitionSequenceTsFileResource.get(i).size();
          }
        }
      } else {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (int i = unseqLevelNum - 1; i >= 0; i--) {
            result += partitionUnSequenceTsFileResource.get(i).size();
          }
        }
      }
      return result;
    } finally {
      readUnLock();
    }
  }

  public void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
    logger.debug("{} [compaction] merge starts to delete real file", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteLevelFile(mergeTsFile);
      logger.info(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  public void deleteLevelFilesInList(
      long timePartitionId, Collection<TsFileResource> mergeTsFiles, int level, boolean sequence) {
    logger.debug("{} [compaction] merge starts to delete file list", storageGroupName);
    if (sequence) {
      if (sequenceTsFileResources.containsKey(timePartitionId)) {
        if (sequenceTsFileResources.get(timePartitionId).size() > level) {
          sequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
        }
      }
    } else {
      if (unSequenceTsFileResources.containsKey(timePartitionId)) {
        if (unSequenceTsFileResources.get(timePartitionId).size() > level) {
          unSequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
        }
      }
    }
  }

  private void deleteLevelFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  public static int getMergeLevel(File file) {
    String mergeLevelStr =
        file.getPath()
            .substring(file.getPath().lastIndexOf(FILE_NAME_SEPARATOR) + 1)
            .replaceAll(TSFILE_SUFFIX, "");
    return Integer.parseInt(mergeLevelStr);
  }

  private List<SortedSet<TsFileResource>> newSequenceTsFileResources(Long k) {
    List<SortedSet<TsFileResource>> newSequenceTsFileResources = new ArrayList<>();
    for (int i = 0; i < seqLevelNum; i++) {
      newSequenceTsFileResources.add(
          new TreeSet<>(
              (o1, o2) -> {
                try {
                  int rangeCompare =
                      Long.compare(
                          Long.parseLong(o1.getTsFile().getParentFile().getName()),
                          Long.parseLong(o2.getTsFile().getParentFile().getName()));
                  return rangeCompare == 0
                      ? compareFileName(o1.getTsFile(), o2.getTsFile())
                      : rangeCompare;
                } catch (NumberFormatException e) {
                  return compareFileName(o1.getTsFile(), o2.getTsFile());
                }
              }));
    }
    return newSequenceTsFileResources;
  }

  private List<List<TsFileResource>> newUnSequenceTsFileResources(Long k) {
    List<List<TsFileResource>> newUnSequenceTsFileResources = new ArrayList<>();
    for (int i = 0; i < unseqLevelNum; i++) {
      newUnSequenceTsFileResources.add(new ArrayList<>());
    }
    return newUnSequenceTsFileResources;
  }

  public TsFileResource getRecoverTsFileResource(String filePath, boolean isSeq)
      throws IOException {
    if (isSeq) {
      for (TsFileResource tsFileResource : sequenceRecoverTsFileResources) {
        if (Files.isSameFile(tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
          return tsFileResource;
        }
      }
    } else {
      for (TsFileResource tsFileResource : unSequenceRecoverTsFileResources) {
        if (Files.isSameFile(tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
          return tsFileResource;
        }
      }
    }
    logger.error("cannot get tsfile resource path: {}", filePath);
    throw new IOException();
  }

  public TsFileResource getTsFileResource(String filePath, boolean isSeq) throws IOException {
    if (isSeq) {
      for (List<SortedSet<TsFileResource>> tsFileResourcesWithLevel :
          sequenceTsFileResources.values()) {
        for (SortedSet<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
          for (TsFileResource tsFileResource : tsFileResources) {
            if (Files.isSameFile(
                tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
              return tsFileResource;
            }
          }
        }
      }
    } else {
      for (List<List<TsFileResource>> tsFileResourcesWithLevel :
          unSequenceTsFileResources.values()) {
        for (List<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
          for (TsFileResource tsFileResource : tsFileResources) {
            if (Files.isSameFile(
                tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
              return tsFileResource;
            }
          }
        }
      }
    }
    logger.error("cannot get tsfile resource path: {}", filePath);
    throw new IOException();
  }

  public void readLock() {
    compactionMergeLock.readLock().lock();
  }

  public void readUnLock() {
    compactionMergeLock.readLock().unlock();
  }

  public void writeLock() {
    compactionMergeLock.writeLock().lock();
  }

  public void writeUnlock() {
    compactionMergeLock.writeLock().unlock();
  }

  public boolean tryWriteLock() {
    return compactionMergeLock.writeLock().tryLock();
  }

  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
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
}
