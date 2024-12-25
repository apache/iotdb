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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class HybridProgressIndex extends ProgressIndex {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HybridProgressIndex.class) + ProgressIndex.LOCK_SIZE;
  private static final long ENTRY_SIZE =
      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
          + RamUsageEstimator.alignObjectSize(Short.BYTES);
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<Short, ProgressIndex> type2Index;

  private HybridProgressIndex() {
    this(Collections.emptyMap());
  }

  public HybridProgressIndex(final ProgressIndex progressIndex) {
    this(Collections.singletonMap(progressIndex.getType().getType(), progressIndex));
  }

  private HybridProgressIndex(final Map<Short, ProgressIndex> type2Index) {
    this.type2Index = new HashMap<>(type2Index);
  }

  public Map<Short, ProgressIndex> getType2Index() {
    return ImmutableMap.copyOf(type2Index);
  }

  @Override
  public void serialize(final ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.HYBRID_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(type2Index.size(), byteBuffer);
      for (final Map.Entry<Short, ProgressIndex> entry : type2Index.entrySet()) {
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
      ProgressIndexType.HYBRID_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(type2Index.size(), stream);
      for (final Map.Entry<Short, ProgressIndex> entry : type2Index.entrySet()) {
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
        return type2Index.size() > 1
            || !type2Index.containsKey(ProgressIndexType.MINIMUM_PROGRESS_INDEX.getType());
      }

      if (!(progressIndex instanceof HybridProgressIndex)) {
        final short type = progressIndex.getType().getType();
        return type2Index.containsKey(type) && type2Index.get(type).isAfter(progressIndex);
      }

      final HybridProgressIndex thisHybridProgressIndex = this;
      final HybridProgressIndex thatHybridProgressIndex = (HybridProgressIndex) progressIndex;
      return thatHybridProgressIndex.type2Index.entrySet().stream()
          .noneMatch(
              entry ->
                  !thisHybridProgressIndex.type2Index.containsKey(entry.getKey())
                      || !thisHybridProgressIndex
                          .type2Index
                          .get(entry.getKey())
                          .isAfter(entry.getValue()));
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean isGivenProgressIndexAfterSelf(final ProgressIndex progressIndex) {
    return type2Index.size() == 1
        && type2Index.containsKey(progressIndex.getType().getType())
        && progressIndex.isAfter(type2Index.get(progressIndex.getType().getType()));
  }

  @Override
  public boolean equals(final ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof HybridProgressIndex)) {
        return false;
      }

      final HybridProgressIndex thisHybridProgressIndex = this;
      final HybridProgressIndex thatHybridProgressIndex = (HybridProgressIndex) progressIndex;
      return thisHybridProgressIndex.type2Index.size() == thatHybridProgressIndex.type2Index.size()
          && thatHybridProgressIndex.type2Index.entrySet().stream()
              .allMatch(
                  entry ->
                      thisHybridProgressIndex.type2Index.containsKey(entry.getKey())
                          && thisHybridProgressIndex
                              .type2Index
                              .get(entry.getKey())
                              .equals(entry.getValue()));
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
    if (!(obj instanceof HybridProgressIndex)) {
      return false;
    }
    return this.equals((HybridProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type2Index);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(
      final ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (progressIndex == null || progressIndex instanceof MinimumProgressIndex) {
        return this;
      }

      if (progressIndex instanceof StateProgressIndex) {
        return progressIndex.updateToMinimumEqualOrIsAfterProgressIndex(this);
      }

      if (!(progressIndex instanceof HybridProgressIndex)) {
        final Map<Short, ProgressIndex> type2Index = new HashMap<>(this.type2Index);
        type2Index.compute(
            progressIndex.getType().getType(),
            (thisK, thisV) ->
                (thisV == null
                    ? progressIndex
                    : thisV.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex)));
        return new HybridProgressIndex(type2Index);
      }

      final HybridProgressIndex thisHybridProgressIndex = this;
      final HybridProgressIndex thatHybridProgressIndex = (HybridProgressIndex) progressIndex;
      final Map<Short, ProgressIndex> type2Index =
          new HashMap<>(thisHybridProgressIndex.type2Index);
      thatHybridProgressIndex.type2Index.forEach(
          (thatK, thatV) ->
              type2Index.compute(
                  thatK,
                  (thisK, thisV) ->
                      (thisV == null
                          ? thatV
                          : thisV.updateToMinimumEqualOrIsAfterProgressIndex(thatV))));
      return new HybridProgressIndex(type2Index);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.HYBRID_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    lock.readLock().lock();
    try {
      return ProgressIndex.TotalOrderSumTuple.sum(
          type2Index.values().stream()
              .map(ProgressIndex::getTotalOrderSumTuple)
              .collect(Collectors.toList()));
    } finally {
      lock.readLock().unlock();
    }
  }

  public static HybridProgressIndex deserializeFrom(final ByteBuffer byteBuffer) {
    final HybridProgressIndex hybridProgressIndex = new HybridProgressIndex();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final short type = ReadWriteIOUtils.readShort(byteBuffer);
      final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(byteBuffer);
      hybridProgressIndex.type2Index.put(type, progressIndex);
    }
    return hybridProgressIndex;
  }

  public static HybridProgressIndex deserializeFrom(final InputStream stream) throws IOException {
    final HybridProgressIndex hybridProgressIndex = new HybridProgressIndex();
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      final short type = ReadWriteIOUtils.readShort(stream);
      final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(stream);
      hybridProgressIndex.type2Index.put(type, progressIndex);
    }
    return hybridProgressIndex;
  }

  @Override
  public String toString() {
    return "HybridProgressIndex{" + "type2Index=" + type2Index + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + type2Index.size() * ENTRY_SIZE
        + type2Index.values().stream().map(ProgressIndex::ramBytesUsed).reduce(0L, Long::sum);
  }
}
