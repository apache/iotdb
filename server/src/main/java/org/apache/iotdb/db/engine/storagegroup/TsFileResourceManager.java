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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TsFileResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileResourceManager.class);
  private String storageGroupName;
  private String storageGroupDir;

  /** Serialize queries, delete resource files, compaction cleanup files */
  private final ReadWriteLock resourceListLock = new ReentrantReadWriteLock();

  private String writeLockHolder;
  // time partition -> double linked list of tsfiles
  private Map<Long, TsFileResourceList> sequenceFiles = new HashMap<>();
  private Map<Long, TsFileResourceList> unsequenceFiles = new HashMap<>();

  public TsFileResourceList getSequenceListByTimePartition(long timePartition) {
    return sequenceFiles.getOrDefault(timePartition, new TsFileResourceList());
  }

  public TsFileResourceList getUnsequenceListByTimePartition(long timePartition) {
    return unsequenceFiles.getOrDefault(timePartition, new TsFileResourceList());
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
}
