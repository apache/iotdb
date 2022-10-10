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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MultiLeaderMemoryManager {
  private static final Logger logger = LoggerFactory.getLogger(MultiLeaderMemoryManager.class);
  private final AtomicLong memorySizeInByte = new AtomicLong(0);
  private Long maxMemorySizeInByte = Runtime.getRuntime().maxMemory() / 10;

  private MultiLeaderMemoryManager() {}

  public boolean reserve(long size) {
    synchronized (this) {
      if (size > maxMemorySizeInByte - memorySizeInByte.get()) {
        logger.debug(
            "consensus memory limited. required: {}, used: {}, total: {}",
            size,
            memorySizeInByte.get(),
            maxMemorySizeInByte);
        return false;
      }
      memorySizeInByte.addAndGet(size);
    }
    logger.debug(
        "{} add {} bytes, total memory size: {} bytes.",
        Thread.currentThread().getName(),
        size,
        memorySizeInByte.get());
    return true;
  }

  public void free(long size) {
    long currentUsedMemory = memorySizeInByte.addAndGet(-size);
    logger.debug(
        "{} free {} bytes, total memory size: {} bytes.",
        Thread.currentThread().getName(),
        size,
        currentUsedMemory);
  }

  public void init(long maxMemorySize) {
    this.maxMemorySizeInByte = maxMemorySize;
  }

  private static final MultiLeaderMemoryManager INSTANCE = new MultiLeaderMemoryManager();

  public static MultiLeaderMemoryManager getInstance() {
    return INSTANCE;
  }
}
