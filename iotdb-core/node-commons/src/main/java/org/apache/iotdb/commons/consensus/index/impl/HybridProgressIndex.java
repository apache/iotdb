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
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class HybridProgressIndex extends ProgressIndex {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<Short, ProgressIndex> type2Index;

  private HybridProgressIndex() {
    this.type2Index = new HashMap<>();
  }

  public HybridProgressIndex(ProgressIndex progressIndex) {
    short type = progressIndex.getType().getType();
    this.type2Index = new HashMap<>();
    if (ProgressIndexType.HYBRID_PROGRESS_INDEX.getType() != type) {
      this.type2Index.put(type, progressIndex.deepCopy());
    } else {
      for (Entry<Short, ProgressIndex> entry :
          ((HybridProgressIndex) progressIndex).type2Index.entrySet()) {
        this.type2Index.put(entry.getKey(), entry.getValue().deepCopy());
      }
    }
  }

  public Map<Short, ProgressIndex> getType2Index() {
    return ImmutableMap.copyOf(((HybridProgressIndex) deepCopy()).type2Index);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
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
  public void serialize(OutputStream stream) throws IOException {
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
  public boolean isAfter(@Nonnull ProgressIndex progressIndex) {
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

  public boolean isGivenProgressIndexAfterSelf(ProgressIndex progressIndex) {
    return type2Index.size() == 1
        && type2Index.containsKey(progressIndex.getType().getType())
        && progressIndex.isAfter(type2Index.get(progressIndex.getType().getType()));
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
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
  public boolean equals(Object obj) {
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
  public ProgressIndex deepCopy() {
    return new HybridProgressIndex(this);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (progressIndex == null || progressIndex instanceof MinimumProgressIndex) {
        return this;
      }

      if (progressIndex instanceof StateProgressIndex) {
        return progressIndex.deepCopy().updateToMinimumEqualOrIsAfterProgressIndex(this);
      }

      if (!(progressIndex instanceof HybridProgressIndex)) {
        type2Index.compute(
            progressIndex.getType().getType(),
            (thisK, thisV) ->
                (thisV == null
                    ? progressIndex.deepCopy()
                    : thisV.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex)));
        return this;
      }

      final HybridProgressIndex thisHybridProgressIndex = this;
      final HybridProgressIndex thatHybridProgressIndex = (HybridProgressIndex) progressIndex;
      thatHybridProgressIndex.type2Index.forEach(
          (thatK, thatV) ->
              thisHybridProgressIndex.type2Index.compute(
                  thatK,
                  (thisK, thisV) ->
                      (thisV == null
                          ? thatV.deepCopy()
                          : thisV.updateToMinimumEqualOrIsAfterProgressIndex(thatV))));
      return this;
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

  public static HybridProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final HybridProgressIndex hybridProgressIndex = new HybridProgressIndex();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final short type = ReadWriteIOUtils.readShort(byteBuffer);
      final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(byteBuffer);
      hybridProgressIndex.type2Index.put(type, progressIndex);
    }
    return hybridProgressIndex;
  }

  public static HybridProgressIndex deserializeFrom(InputStream stream) throws IOException {
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
}
