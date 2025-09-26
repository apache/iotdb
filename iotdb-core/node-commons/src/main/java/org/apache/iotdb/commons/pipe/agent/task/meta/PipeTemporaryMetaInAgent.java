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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTemporaryMetaInAgent implements PipeTemporaryMeta {

  // Statistics
  private final AtomicLong floatingMemoryUsageInByte = new AtomicLong(0L);

  // Object pool
  private final String pipeNameWithCreationTime;
  private final Map<Integer, CommitterKey> regionId2CommitterKeyMap = new ConcurrentHashMap<>();

  private final AtomicReference<MemoryFactorState> state =
      new AtomicReference<>(new MemoryFactorState(1.0, 0L, 0));

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
    final MemoryFactorState current = state.get();
    final long currentTime = System.currentTimeMillis();
    if (currentTime - current.lastTime < 5000 || current.factor <= 0.5) {
      return;
    }

    state.updateAndGet(
        old -> {
          long updateTime = System.currentTimeMillis();
          if (updateTime - old.lastTime < 5000 || old.factor <= 0.5) {
            return old;
          }
          return new MemoryFactorState(Math.max(0.5, old.factor - 0.1), updateTime, 0);
        });
  }

  public void reportStableTransferTime() {
    final MemoryFactorState current = state.get();
    final long currentTime = System.currentTimeMillis();
    if (current.factor >= 1.0) {
      return;
    }

    if (currentTime - current.lastTime < 5000 && current.stableCount + 1 < 5) {
      state.updateAndGet(
          old -> {
            if (old.factor >= 1.0) {
              return old;
            }
            return new MemoryFactorState(old.factor, old.lastTime, old.stableCount + 1);
          });
      return;
    }

    state.updateAndGet(
        old -> {
          long updateTime = System.currentTimeMillis();
          if (old.factor >= 1.0) {
            return old;
          }

          int newCount = old.stableCount + 1;
          if (newCount < 5) {
            return new MemoryFactorState(old.factor, old.lastTime, newCount);
          }

          if (updateTime - old.lastTime < 5000) {
            return new MemoryFactorState(old.factor, old.lastTime, newCount);
          }

          return new MemoryFactorState(Math.min(1.0, old.factor + 0.1), updateTime, 0);
        });
  }

  public double getMemoryAdjustFactor() {
    return state.get().factor;
  }

  private static class MemoryFactorState {
    final double factor;
    final long lastTime;
    final int stableCount;

    MemoryFactorState(double factor, long lastTime, int stableCount) {
      this.factor = factor;
      this.lastTime = lastTime;
      this.stableCount = stableCount;
    }
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
    final MemoryFactorState thisState = this.state.get();
    final MemoryFactorState thatState = that.state.get();
    return Objects.equals(
            this.floatingMemoryUsageInByte.get(), that.floatingMemoryUsageInByte.get())
        && Objects.equals(this.regionId2CommitterKeyMap, that.regionId2CommitterKeyMap)
        && Objects.equals(thisState.factor, thatState.factor)
        && Objects.equals(thisState.stableCount, thatState.stableCount)
        && Objects.equals(thisState.lastTime, thatState.lastTime);
  }

  @Override
  public int hashCode() {
    final MemoryFactorState currentState = state.get();
    return Objects.hash(
        floatingMemoryUsageInByte.get(),
        regionId2CommitterKeyMap,
        currentState.factor,
        currentState.stableCount,
        currentState.lastTime);
  }

  @Override
  public String toString() {
    final MemoryFactorState currentState = state.get();
    return "PipeTemporaryMeta{"
        + "floatingMemoryUsage="
        + floatingMemoryUsageInByte
        + ", regionId2CommitterKeyMap="
        + regionId2CommitterKeyMap
        + ", memoryAdjustFactor="
        + currentState.factor
        + '}';
  }
}
