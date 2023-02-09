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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.commons.service.metric.MetricService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IoTConsensusMemoryManager {
  private static final Logger logger = LoggerFactory.getLogger(IoTConsensusMemoryManager.class);
  private final AtomicLong memorySizeInByte = new AtomicLong(0);
  private final AtomicLong queueMemorySizeInByte = new AtomicLong(0);
  private final AtomicLong syncMemorySizeInByte = new AtomicLong(0);
  private Long maxMemorySizeInByte = Runtime.getRuntime().maxMemory() / 10;
  private Long maxMemorySizeForQueueInByte = Runtime.getRuntime().maxMemory() / 100 * 6;

  private IoTConsensusMemoryManager() {
    MetricService.getInstance().addMetricSet(new IoTConsensusMemoryManagerMetrics(this));
  }

  public boolean reserve(long size, boolean fromQueue) {
    AtomicBoolean result = new AtomicBoolean(false);
    memorySizeInByte.updateAndGet(
        (memorySize) -> {
          long remainSize =
              (fromQueue ? maxMemorySizeForQueueInByte : maxMemorySizeInByte) - memorySize;
          if (size > remainSize) {
            logger.debug(
                "consensus memory limited. required: {}, used: {}, total: {}",
                size,
                memorySize,
                maxMemorySizeInByte);
            result.set(false);
            return memorySize;
          } else {
            logger.debug(
                "{} add {} bytes, total memory size: {} bytes.",
                Thread.currentThread().getName(),
                size,
                memorySize + size);
            result.set(true);
            return memorySize + size;
          }
        });
    if (result.get()) {
      if (fromQueue) {
        queueMemorySizeInByte.addAndGet(size);
      } else {
        syncMemorySizeInByte.addAndGet(size);
      }
    }
    return result.get();
  }

  public void free(long size, boolean fromQueue) {
    long currentUsedMemory = memorySizeInByte.addAndGet(-size);
    if (fromQueue) {
      queueMemorySizeInByte.addAndGet(-size);
    } else {
      syncMemorySizeInByte.addAndGet(-size);
    }
    logger.debug(
        "{} free {} bytes, total memory size: {} bytes.",
        Thread.currentThread().getName(),
        size,
        currentUsedMemory);
  }

  public void init(long maxMemorySize, long maxMemorySizeForQueue) {
    this.maxMemorySizeInByte = maxMemorySize;
    this.maxMemorySizeForQueueInByte = maxMemorySizeForQueue;
  }

  long getMemorySizeInByte() {
    return memorySizeInByte.get();
  }

  long getQueueMemorySizeInByte() {
    return queueMemorySizeInByte.get();
  }

  long getSyncMemorySizeInByte() {
    return syncMemorySizeInByte.get();
  }

  private static final IoTConsensusMemoryManager INSTANCE = new IoTConsensusMemoryManager();

  public static IoTConsensusMemoryManager getInstance() {
    return INSTANCE;
  }
}
