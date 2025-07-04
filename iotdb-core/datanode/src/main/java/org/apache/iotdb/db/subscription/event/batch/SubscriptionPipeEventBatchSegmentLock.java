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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.db.storageengine.StorageEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/** refer to {@link org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceSegmentLock} */
public class SubscriptionPipeEventBatchSegmentLock {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeEventBatchSegmentLock.class);

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

  public void lock(final int regionId) {
    initIfNecessary();
    locks[regionId % locks.length].lock();
  }

  public void unlock(final int regionId) {
    initIfNecessary();
    locks[regionId % locks.length].unlock();
  }
}
