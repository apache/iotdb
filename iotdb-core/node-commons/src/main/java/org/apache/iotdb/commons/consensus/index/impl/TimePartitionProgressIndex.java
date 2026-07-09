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

package org.apache.iotdb.commons.consensus.index.impl;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TimePartitionProgressIndex extends ProgressIndex {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TimePartitionProgressIndex.class)
          + RamUsageEstimator.shallowSizeOfInstance(HashMap.class)
          + ProgressIndex.LOCK_SIZE;
  private static final long ENTRY_SIZE =
      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
          + RamUsageEstimator.alignObjectSize(Long.BYTES);

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<Long, ProgressIndex> timePartitionId2ProgressIndex;

  private TimePartitionProgressIndex() {
    this(new HashMap<>());
  }

  public TimePartitionProgressIndex(final Map<Long, ProgressIndex> timePartitionId2ProgressIndex) {
    this.timePartitionId2ProgressIndex = new HashMap<>();
    timePartitionId2ProgressIndex.forEach(
        (timePartitionId, progressIndex) -> {
          if (Objects.nonNull(progressIndex) && !(progressIndex instanceof MinimumProgressIndex)) {
            this.timePartitionId2ProgressIndex.put(timePartitionId, progressIndex);
          }
        });
  }

  public TimePartitionProgressIndex(final long timePartitionId, final ProgressIndex progressIndex) {
    this(Collections.singletonMap(timePartitionId, progressIndex));
  }

  public Map<Long, ProgressIndex> getTimePartitionId2ProgressIndex() {
    lock.readLock().lock();
    try {
      return ImmutableMap.copyOf(timePartitionId2ProgressIndex);
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean isProgressIndexEqualOrAfter(
      final long timePartitionId, final ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      final ProgressIndex timePartitionProgressIndex =
          timePartitionId2ProgressIndex.get(timePartitionId);
      return Objects.nonNull(timePartitionProgressIndex)
          && timePartitionProgressIndex.isEqualOrAfter(progressIndex);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(final ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.TIME_PARTITION_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(timePartitionId2ProgressIndex.size(), byteBuffer);
      for (final Map.Entry<Long, ProgressIndex> entry : timePartitionId2ProgressIndex.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
        entry.getValue().serialize(byteBuffer);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(final OutputStream stream) throws IOException {
    lock.readLock().lock();
    try {
      ProgressIndexType.TIME_PARTITION_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(timePartitionId2ProgressIndex.size(), stream);
      for (final Map.Entry<Long, ProgressIndex> entry : timePartitionId2ProgressIndex.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        entry.getValue().serialize(stream);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean isAfter(@Nonnull final ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (progressIndex instanceof MinimumProgressIndex) {
        return !timePartitionId2ProgressIndex.isEmpty();
      }

      if (progressIndex instanceof HybridProgressIndex) {
        return ((HybridProgressIndex) progressIndex).isGivenProgressIndexAfterSelf(this);
      }

      if (!(progressIndex instanceof TimePartitionProgressIndex)) {
        return false;
      }

      final TimePartitionProgressIndex thatTimePartitionProgressIndex =
          (TimePartitionProgressIndex) progressIndex;
      boolean hasStrictlyAfterTimePartition =
          timePartitionId2ProgressIndex.size()
              > thatTimePartitionProgressIndex.timePartitionId2ProgressIndex.size();
      for (final Map.Entry<Long, ProgressIndex> entry :
          thatTimePartitionProgressIndex.timePartitionId2ProgressIndex.entrySet()) {
        final ProgressIndex thisProgressIndex = timePartitionId2ProgressIndex.get(entry.getKey());
        if (Objects.isNull(thisProgressIndex)
            || !thisProgressIndex.isEqualOrAfter(entry.getValue())) {
          return false;
        }
        if (thisProgressIndex.isAfter(entry.getValue())) {
          hasStrictlyAfterTimePartition = true;
        }
      }
      return hasStrictlyAfterTimePartition;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(final ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof TimePartitionProgressIndex)) {
        return false;
      }

      return timePartitionId2ProgressIndex.equals(
          ((TimePartitionProgressIndex) progressIndex).timePartitionId2ProgressIndex);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TimePartitionProgressIndex)) {
      return false;
    }
    return this.equals((TimePartitionProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timePartitionId2ProgressIndex);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(
      final ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (progressIndex == null || progressIndex instanceof MinimumProgressIndex) {
        return this;
      }

      if (!(progressIndex instanceof TimePartitionProgressIndex)) {
        return ProgressIndex.blendProgressIndex(this, progressIndex);
      }

      final Map<Long, ProgressIndex> updatedTimePartitionId2ProgressIndex =
          new HashMap<>(timePartitionId2ProgressIndex);
      ((TimePartitionProgressIndex) progressIndex)
          .timePartitionId2ProgressIndex.forEach(
              (thatK, thatV) ->
                  updatedTimePartitionId2ProgressIndex.compute(
                      thatK,
                      (thisK, thisV) ->
                          Objects.isNull(thisV)
                              ? thatV
                              : thisV.updateToMinimumEqualOrIsAfterProgressIndex(thatV)));
      return new TimePartitionProgressIndex(updatedTimePartitionId2ProgressIndex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.TIME_PARTITION_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    lock.readLock().lock();
    try {
      final ArrayList<TotalOrderSumTuple> tupleList =
          timePartitionId2ProgressIndex.values().stream()
              .map(ProgressIndex::getTotalOrderSumTuple)
              .collect(Collectors.toCollection(ArrayList::new));
      tupleList.add(new TotalOrderSumTuple((long) timePartitionId2ProgressIndex.size()));
      return ProgressIndex.TotalOrderSumTuple.sum(tupleList);
    } finally {
      lock.readLock().unlock();
    }
  }

  public static TimePartitionProgressIndex deserializeFrom(final ByteBuffer byteBuffer) {
    final TimePartitionProgressIndex timePartitionProgressIndex = new TimePartitionProgressIndex();

    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final long timePartitionId = ReadWriteIOUtils.readLong(byteBuffer);
      final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(byteBuffer);
      timePartitionProgressIndex.timePartitionId2ProgressIndex.put(timePartitionId, progressIndex);
    }
    return timePartitionProgressIndex;
  }

  public static TimePartitionProgressIndex deserializeFrom(final InputStream stream)
      throws IOException {
    final TimePartitionProgressIndex timePartitionProgressIndex = new TimePartitionProgressIndex();

    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; ++i) {
      final long timePartitionId = ReadWriteIOUtils.readLong(stream);
      final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(stream);
      timePartitionProgressIndex.timePartitionId2ProgressIndex.put(timePartitionId, progressIndex);
    }
    return timePartitionProgressIndex;
  }

  @Override
  public String toString() {
    return "TimePartitionProgressIndex{"
        + "timePartitionId2ProgressIndex="
        + timePartitionId2ProgressIndex
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + timePartitionId2ProgressIndex.size() * ENTRY_SIZE
        + timePartitionId2ProgressIndex.values().stream()
            .map(ProgressIndex::ramBytesUsed)
            .reduce(0L, Long::sum);
  }
}
