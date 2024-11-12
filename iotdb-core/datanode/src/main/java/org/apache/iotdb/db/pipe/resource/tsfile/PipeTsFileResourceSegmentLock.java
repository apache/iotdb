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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.db.storageengine.StorageEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class PipeTsFileResourceSegmentLock {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResourceSegmentLock.class);

  private static final int SEGMENT_LOCK_MIN_SIZE = 32;
  private static final int SEGMENT_LOCK_MAX_SIZE = 128;

  private volatile ReentrantLock[] locks;

  private void initIfNecessary() {
    if (locks == null) {
      synchronized (this) {
        if (locks == null) {
          int lockSegmentSize = SEGMENT_LOCK_MIN_SIZE;
          try {
            lockSegmentSize = StorageEngine.getInstance().getAllDataRegionIds().size();
          } catch (final Exception e) {
            LOGGER.warn(
                "Cannot get data region ids, use default lock segment size: {}", lockSegmentSize);
          }
          lockSegmentSize = Math.min(SEGMENT_LOCK_MAX_SIZE, lockSegmentSize);
          lockSegmentSize = Math.max(SEGMENT_LOCK_MIN_SIZE, lockSegmentSize);

          final ReentrantLock[] tmpLocks = new ReentrantLock[lockSegmentSize];
          for (int i = 0; i < tmpLocks.length; i++) {
            tmpLocks[i] = new ReentrantLock();
          }

          // publish this variable
          locks = tmpLocks;
        }
      }
    }
  }

  public void lock(final File file) {
    initIfNecessary();
    locks[Math.abs(file.hashCode()) % locks.length].lock();
  }

  public boolean tryLock(final File file, final long timeout, final TimeUnit timeUnit)
      throws InterruptedException {
    initIfNecessary();
    return locks[Math.abs(file.hashCode()) % locks.length].tryLock(timeout, timeUnit);
  }

  public boolean tryLockAll(final long timeout, final TimeUnit timeUnit)
      throws InterruptedException {
    initIfNecessary();
    int alreadyLocked = 0;
    for (final ReentrantLock lock : locks) {
      if (lock.tryLock(timeout, timeUnit)) {
        alreadyLocked++;
      } else {
        break;
      }
    }

    if (alreadyLocked == locks.length) {
      return true;
    } else {
      unlockUntil(alreadyLocked);
      return false;
    }
  }

  private void unlockUntil(final int index) {
    for (int i = 0; i < index; i++) {
      locks[i].unlock();
    }
  }

  public void unlock(final File file) {
    initIfNecessary();
    locks[Math.abs(file.hashCode()) % locks.length].unlock();
  }

  public void unlockAll() {
    initIfNecessary();
    unlockUntil(locks.length);
  }
}
