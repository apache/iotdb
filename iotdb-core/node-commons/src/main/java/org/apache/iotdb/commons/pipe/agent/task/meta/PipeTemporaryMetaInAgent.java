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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTemporaryMetaInAgent implements PipeTemporaryMeta {

  // Statistics
  private final AtomicLong floatingMemoryUsageInByte = new AtomicLong(0L);

  // Object pool
  private final String pipeNameWithCreationTime;
  private final Map<Integer, CommitterKey> regionId2CommitterKeyMap = new ConcurrentHashMap<>();

  private final AtomicDouble memoryAdjustFactor = new AtomicDouble(1.0);
  private final AtomicInteger stableTransferCount = new AtomicInteger(0);
  private final AtomicLong lastAdjustmentTime = new AtomicLong(0L);

  PipeTemporaryMetaInAgent(final String pipeName, final long creationTime) {
    this.pipeNameWithCreationTime = pipeName + "_" + creationTime;
  }

  /////////////////////////////// DataNode ///////////////////////////////

  public void addFloatingMemoryUsageInByte(final long usage) {
    floatingMemoryUsageInByte.addAndGet(usage);
  }

  public void decreaseFloatingMemoryUsageInByte(final long usage) {
    floatingMemoryUsageInByte.addAndGet(-usage);
  }

  public long getFloatingMemoryUsageInByte() {
    return floatingMemoryUsageInByte.get();
  }

  public String getPipeNameWithCreationTime() {
    return pipeNameWithCreationTime;
  }

  public CommitterKey getCommitterKey(
      final String pipeName, final long creationTime, final int regionId, final int restartTime) {
    final CommitterKey key = regionId2CommitterKeyMap.get(regionId);
    if (Objects.nonNull(key) && key.getRestartTimes() == restartTime) {
      return key;
    }
    final CommitterKey newKey = new CommitterKey(pipeName, creationTime, regionId, restartTime);
    if (Objects.nonNull(key) && restartTime < key.getRestartTimes()) {
      return newKey;
    }
    // restartTime > key.getRestartTimes()
    regionId2CommitterKeyMap.put(regionId, newKey);
    return newKey;
  }

  public void reportHighTransferTime() {
    if (memoryAdjustFactor.get() <= 0.5) {
      return;
    }
    final long currentTime = System.currentTimeMillis();

    if (currentTime - lastAdjustmentTime.get() < 5000) {
      return;
    }
    // Reset stable counter when high transfer time is detected
    stableTransferCount.set(0);
    // Reduce memory threshold factor
    memoryAdjustFactor.set(memoryAdjustFactor.get() - 0.1);
    lastAdjustmentTime.set(currentTime);
  }

  public void reportStableTransferTime() {
    if (memoryAdjustFactor.get() >= 1.0) {
      return;
    }
    if (stableTransferCount.addAndGet(1) >= 5) {
      final long currentTime = System.currentTimeMillis();

      if (currentTime - lastAdjustmentTime.get() < 5000) {
        return;
      }
      memoryAdjustFactor.set(memoryAdjustFactor.get() + 0.1);
      lastAdjustmentTime.set(currentTime);
      stableTransferCount.set(0);
    }
  }

  public double getMemoryAdjustFactor() {
    return memoryAdjustFactor.get();
  }

  /////////////////////////////// Object ///////////////////////////////

  // We assume that the "pipeNameWithCreationTime" does not contain extra information
  // thus we do not consider it here
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeTemporaryMetaInAgent that = (PipeTemporaryMetaInAgent) o;
    return Objects.equals(
            this.floatingMemoryUsageInByte.get(), that.floatingMemoryUsageInByte.get())
        && Objects.equals(this.regionId2CommitterKeyMap, that.regionId2CommitterKeyMap)
        && Objects.equals(this.memoryAdjustFactor.get(), that.memoryAdjustFactor.get())
        && Objects.equals(this.stableTransferCount.get(), that.stableTransferCount.get())
        && Objects.equals(this.lastAdjustmentTime.get(), that.lastAdjustmentTime.get());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        floatingMemoryUsageInByte.get(),
        regionId2CommitterKeyMap,
        memoryAdjustFactor.get(),
        stableTransferCount.get(),
        lastAdjustmentTime.get());
  }

  @Override
  public String toString() {
    return "PipeTemporaryMeta{"
        + "floatingMemoryUsage="
        + floatingMemoryUsageInByte
        + ", regionId2CommitterKeyMap="
        + regionId2CommitterKeyMap
        + ", memoryAdjustFactor="
        + memoryAdjustFactor
        + '}';
  }
}
