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
package org.apache.iotdb.db.timeIndex;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.timeIndex.impl.LoadAllDeviceTimeIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage all indexers
 */
public class FileIndexManager {

  private String indexerFilePath;
  private Map<PartialPath, FileTimeIndexer> seqIndexers;
  private Map<PartialPath, FileTimeIndexer> unseqIndexers;
  private ReentrantReadWriteLock lock;
  private static final Logger logger = LoggerFactory.getLogger(FileIndexManager.class);

  private static class IndexerManagerHolder {

    private IndexerManagerHolder() {
      // allowed to do nothing
    }

    private static final FileIndexManager INSTANCE = new FileIndexManager();
  }

  public static FileIndexManager getInstance() {
    return IndexerManagerHolder.INSTANCE;
  }

  private FileIndexManager() {
    indexerFilePath = IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
        + File.pathSeparator + IndexConstants.INDEXER_FILE;
    seqIndexers = new ConcurrentHashMap<>();
    unseqIndexers = new ConcurrentHashMap<>();
    lock = new ReentrantReadWriteLock();
  }

  /**
   * init all indexer
   *
   * @return whether success
   */
  public boolean init() {
    //TODO
    // 1. get all storage group from file
    // 2. init indexer for the storage group
    return true;
  }

  public void addSeqIndexer(PartialPath storageGroup, FileTimeIndexer fileTimeIndexer) {
    lock.writeLock().lock();
    try {
      seqIndexers.put(storageGroup, fileTimeIndexer);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void addUnseqIndexer(PartialPath storageGroup, FileTimeIndexer fileTimeIndexer) {
    lock.writeLock().lock();
    try {
      unseqIndexers.put(storageGroup, fileTimeIndexer);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteSeqIndexer(PartialPath storageGroup) {
    lock.writeLock().lock();
    try {
      seqIndexers.remove(storageGroup);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteUnseqIndexer(PartialPath storageGroup) {
    lock.writeLock().lock();
    try {
      unseqIndexers.remove(storageGroup);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public FileTimeIndexer getSeqIndexer(PartialPath storageGroup) {
    lock.readLock().lock();
    try {
      return seqIndexers.get(storageGroup);
    } finally {
      lock.readLock().unlock();
    }
  }

  public FileTimeIndexer getSeqIndexer(String storageGroup) throws IllegalPathException {
    PartialPath sgName;
    try {
      sgName = new PartialPath(storageGroup);
    } catch (IllegalPathException e) {
      logger.warn("Fail to get TimeIndexer for storage group {}, err:{}", storageGroup,
          e.getMessage());
      throw e;
    }
    lock.readLock().lock();
    try {
      FileTimeIndexer fileTimeIndexer = seqIndexers.get(sgName);
      if(fileTimeIndexer == null) {
        fileTimeIndexer = new LoadAllDeviceTimeIndexer();
      }
      return fileTimeIndexer;
    } finally {
      lock.readLock().unlock();
    }
  }

  public FileTimeIndexer getUnseqIndexer(PartialPath storageGroup) {
    lock.readLock().lock();
    try {
      return unseqIndexers.get(storageGroup);
    } finally {
      lock.readLock().unlock();
    }
  }

  public FileTimeIndexer getUnseqIndexer(String storageGroup) throws IllegalPathException {
    PartialPath sgName;
    try {
      sgName = new PartialPath(storageGroup);
    } catch (IllegalPathException e) {
      logger.warn("Fail to get TimeIndexer for storage group {}, err:{}", storageGroup,
          e.getMessage());
      throw e;
    }
    lock.readLock().lock();
    try {
      FileTimeIndexer fileTimeIndexer = unseqIndexers.get(sgName);
      if(fileTimeIndexer == null) {
        fileTimeIndexer = new LoadAllDeviceTimeIndexer();
      }
      return fileTimeIndexer;
    } finally {
      lock.readLock().unlock();
    }
  }

  public static FileIndexEntries convertFromTsFileResource(TsFileResource resource)
      throws IllegalPathException {
    FileIndexEntries fileIndexEntries = new FileIndexEntries();
    TimeIndexEntry[] timeIndexEntries = new TimeIndexEntry[resource.getDeviceToIndexMap().size()];
    int i = 0;
    for (Map.Entry<String, Integer> entry : resource.getDeviceToIndexMap().entrySet()) {
      TimeIndexEntry timeIndexEntry = new TimeIndexEntry();
      timeIndexEntry.setAllElem(
          new PartialPath(entry.getKey()),
          resource.getStartTime(entry.getValue()),
          resource.getEndTime(entry.getValue()));
      timeIndexEntries[i++] = timeIndexEntry;
    }
    fileIndexEntries.setIndexEntries(timeIndexEntries);
    fileIndexEntries.setTsFilePath(resource.getTsFilePath());

    return fileIndexEntries;
  }
}
