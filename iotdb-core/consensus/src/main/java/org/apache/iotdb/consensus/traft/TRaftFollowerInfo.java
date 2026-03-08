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

package org.apache.iotdb.consensus.traft;

import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class TRaftFollowerInfo {

  private long currentReplicatingPartitionIndex;
  private final Set<Long> currentReplicatingIndices = new HashSet<>();
  private long nextPartitionIndex;
  private long nextPartitionFirstIndex;
  private long memoryReplicationSuccessCount;
  private long diskReplicationSuccessCount;

  // Keep delayed entries grouped by partition so we can replay one-by-one from disk.
  private final NavigableMap<Long, NavigableSet<Long>> delayedIndicesByPartition = new TreeMap<>();

  public TRaftFollowerInfo(long currentReplicatingPartitionIndex, long nextPartitionFirstIndex) {
    this.currentReplicatingPartitionIndex = currentReplicatingPartitionIndex;
    this.nextPartitionIndex = currentReplicatingPartitionIndex;
    this.nextPartitionFirstIndex = nextPartitionFirstIndex;
  }

  public long getCurrentReplicatingPartitionIndex() {
    return currentReplicatingPartitionIndex;
  }

  public void setCurrentReplicatingPartitionIndex(long currentReplicatingPartitionIndex) {
    this.currentReplicatingPartitionIndex = currentReplicatingPartitionIndex;
  }

  public Set<Long> getCurrentReplicatingIndices() {
    return currentReplicatingIndices;
  }

  public long getNextPartitionIndex() {
    return nextPartitionIndex;
  }

  public void setNextPartitionIndex(long nextPartitionIndex) {
    this.nextPartitionIndex = nextPartitionIndex;
  }

  public long getNextPartitionFirstIndex() {
    return nextPartitionFirstIndex;
  }

  public void setNextPartitionFirstIndex(long nextPartitionFirstIndex) {
    this.nextPartitionFirstIndex = nextPartitionFirstIndex;
  }

  public void addCurrentReplicatingIndex(long index) {
    currentReplicatingIndices.add(index);
  }

  public void removeCurrentReplicatingIndex(long index) {
    currentReplicatingIndices.remove(index);
  }

  public boolean hasCurrentReplicatingIndex() {
    return !currentReplicatingIndices.isEmpty();
  }

  public long getMemoryReplicationSuccessCount() {
    return memoryReplicationSuccessCount;
  }

  public long getDiskReplicationSuccessCount() {
    return diskReplicationSuccessCount;
  }

  public void recordMemoryReplicationSuccess() {
    memoryReplicationSuccessCount++;
  }

  public void recordDiskReplicationSuccess() {
    diskReplicationSuccessCount++;
  }

  public void addDelayedIndex(long partitionIndex, long logIndex) {
    delayedIndicesByPartition
        .computeIfAbsent(partitionIndex, key -> new TreeSet<>())
        .add(logIndex);
    refreshNextPartitionPointers();
  }

  public void removeDelayedIndex(long partitionIndex, long logIndex) {
    NavigableSet<Long> indices = delayedIndicesByPartition.get(partitionIndex);
    if (indices == null) {
      return;
    }
    indices.remove(logIndex);
    if (indices.isEmpty()) {
      delayedIndicesByPartition.remove(partitionIndex);
    }
    refreshNextPartitionPointers();
  }

  public Long getFirstDelayedPartitionIndex() {
    return delayedIndicesByPartition.isEmpty() ? null : delayedIndicesByPartition.firstKey();
  }

  public Long getFirstDelayedIndexOfPartition(long partitionIndex) {
    NavigableSet<Long> indices = delayedIndicesByPartition.get(partitionIndex);
    return indices == null || indices.isEmpty() ? null : indices.first();
  }

  public boolean hasDelayedEntries() {
    return !delayedIndicesByPartition.isEmpty();
  }

  public long delayedEntryCount() {
    long count = 0;
    for (Map.Entry<Long, NavigableSet<Long>> entry : delayedIndicesByPartition.entrySet()) {
      count += entry.getValue().size();
    }
    return count;
  }

  private void refreshNextPartitionPointers() {
    if (delayedIndicesByPartition.isEmpty()) {
      nextPartitionIndex = currentReplicatingPartitionIndex;
      return;
    }
    Map.Entry<Long, NavigableSet<Long>> first = delayedIndicesByPartition.firstEntry();
    nextPartitionIndex = first.getKey();
    nextPartitionFirstIndex = first.getValue().first();
  }
}
