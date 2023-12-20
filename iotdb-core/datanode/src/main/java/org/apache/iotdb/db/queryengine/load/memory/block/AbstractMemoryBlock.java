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

package org.apache.iotdb.db.queryengine.load.memory.block;

import org.apache.iotdb.db.queryengine.load.memory.LoadTsFileMemoryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractMemoryBlock implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMemoryBlock.class);

  protected final LoadTsFileMemoryManager loadMemoryManager = LoadTsFileMemoryManager.getInstance();

  private final AtomicLong memoryUsageInBytes = new AtomicLong(0);

  private final ReentrantLock lock = new ReentrantLock();

  private volatile boolean isReleased = false;

  public AbstractMemoryBlock(long memoryUsageInBytes) {
    this.memoryUsageInBytes.set(memoryUsageInBytes);
  }

  protected abstract void releaseMemory();

  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  public boolean isReleased() {
    return isReleased;
  }

  public void markAsReleased() {
    isReleased = true;
  }

  @Override
  public void close() {
    while (true) {
      try {
        if (lock.tryLock(50, TimeUnit.MICROSECONDS)) {
          try {
            releaseMemory();
            break;
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while waiting for the lock.", e);
      }
    }
  }
}
