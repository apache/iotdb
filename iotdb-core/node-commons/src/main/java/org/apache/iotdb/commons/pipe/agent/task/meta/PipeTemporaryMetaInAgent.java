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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTemporaryMetaInAgent implements PipeTemporaryMeta {

  // Statistics
  private final AtomicLong floatingMemoryUsageInByte = new AtomicLong(0L);
  private final ConcurrentMap<Integer, Boolean> regionId2TsFileEpochDegradedMap =
      new ConcurrentHashMap<>();

  // Object pool
  private final String pipeNameWithCreationTime;
  private final Map<Integer, CommitterKey> regionId2CommitterKeyMap = new ConcurrentHashMap<>();

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

  public void setTsFileEpochDegraded(final int regionId, final boolean isDegraded) {
    regionId2TsFileEpochDegradedMap.put(regionId, isDegraded);
  }

  public void clearTsFileEpochDegraded(final int regionId) {
    regionId2TsFileEpochDegradedMap.remove(regionId);
  }

  public Boolean getGlobalTsFileEpochDegraded() {
    if (regionId2TsFileEpochDegradedMap.values().stream().anyMatch(Boolean.TRUE::equals)) {
      return true;
    }
    return regionId2TsFileEpochDegradedMap.isEmpty() ? null : false;
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
        && Objects.equals(
            this.regionId2TsFileEpochDegradedMap, that.regionId2TsFileEpochDegradedMap)
        && Objects.equals(this.regionId2CommitterKeyMap, that.regionId2CommitterKeyMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        floatingMemoryUsageInByte.get(), regionId2TsFileEpochDegradedMap, regionId2CommitterKeyMap);
  }

  @Override
  public String toString() {
    return "PipeTemporaryMeta{"
        + "floatingMemoryUsage="
        + floatingMemoryUsageInByte
        + ", regionId2TsFileEpochDegradedMap="
        + regionId2TsFileEpochDegradedMap
        + ", regionId2CommitterKeyMap="
        + regionId2CommitterKeyMap
        + '}';
  }
}
