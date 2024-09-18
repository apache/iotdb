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

public class RecoverProgressIndex extends ProgressIndex {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<Integer, SimpleProgressIndex> dataNodeId2LocalIndex;

  private RecoverProgressIndex() {
    this.dataNodeId2LocalIndex = new HashMap<>();
  }

  public RecoverProgressIndex(int dataNodeId, SimpleProgressIndex simpleProgressIndex) {
    this(ImmutableMap.of(dataNodeId, simpleProgressIndex));
  }

  public RecoverProgressIndex(Map<Integer, SimpleProgressIndex> dataNodeId2LocalIndex) {
    this.dataNodeId2LocalIndex = new HashMap<>();
    for (Entry<Integer, SimpleProgressIndex> entry : dataNodeId2LocalIndex.entrySet()) {
      this.dataNodeId2LocalIndex.put(
          entry.getKey(), (SimpleProgressIndex) entry.getValue().deepCopy());
    }
  }

  public Map<Integer, SimpleProgressIndex> getDataNodeId2LocalIndex() {
    return ImmutableMap.copyOf(((RecoverProgressIndex) deepCopy()).dataNodeId2LocalIndex);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.RECOVER_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(dataNodeId2LocalIndex.size(), byteBuffer);
      for (final Map.Entry<Integer, SimpleProgressIndex> entry : dataNodeId2LocalIndex.entrySet()) {
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
      ProgressIndexType.RECOVER_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(dataNodeId2LocalIndex.size(), stream);
      for (final Map.Entry<Integer, SimpleProgressIndex> entry : dataNodeId2LocalIndex.entrySet()) {
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
        return true;
      }

      if (progressIndex instanceof HybridProgressIndex) {
        return ((HybridProgressIndex) progressIndex).isGivenProgressIndexAfterSelf(this);
      }

      if (!(progressIndex instanceof RecoverProgressIndex)) {
        return false;
      }

      final RecoverProgressIndex thisRecoverProgressIndex = this;
      final RecoverProgressIndex thatRecoverProgressIndex = (RecoverProgressIndex) progressIndex;
      return thatRecoverProgressIndex.dataNodeId2LocalIndex.entrySet().stream()
          .noneMatch(
              entry ->
                  !thisRecoverProgressIndex.dataNodeId2LocalIndex.containsKey(entry.getKey())
                      || !thisRecoverProgressIndex
                          .dataNodeId2LocalIndex
                          .get(entry.getKey())
                          .isAfter(entry.getValue()));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof RecoverProgressIndex)) {
        return false;
      }

      final RecoverProgressIndex thisRecoverProgressIndex = this;
      final RecoverProgressIndex thatRecoverProgressIndex = (RecoverProgressIndex) progressIndex;
      return thisRecoverProgressIndex.dataNodeId2LocalIndex.size()
              == thatRecoverProgressIndex.dataNodeId2LocalIndex.size()
          && thatRecoverProgressIndex.dataNodeId2LocalIndex.entrySet().stream()
              .allMatch(
                  entry ->
                      thisRecoverProgressIndex.dataNodeId2LocalIndex.containsKey(entry.getKey())
                          && thisRecoverProgressIndex
                              .dataNodeId2LocalIndex
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
    if (!(obj instanceof RecoverProgressIndex)) {
      return false;
    }
    return this.equals((RecoverProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataNodeId2LocalIndex);
  }

  @Override
  public ProgressIndex deepCopy() {
    return new RecoverProgressIndex(dataNodeId2LocalIndex);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof RecoverProgressIndex)) {
        return ProgressIndex.blendProgressIndex(this, progressIndex);
      }

      final RecoverProgressIndex thisRecoverProgressIndex = this;
      final RecoverProgressIndex thatRecoverProgressIndex = (RecoverProgressIndex) progressIndex;
      thatRecoverProgressIndex.dataNodeId2LocalIndex.forEach(
          (thatK, thatV) ->
              thisRecoverProgressIndex.dataNodeId2LocalIndex.compute(
                  thatK,
                  (thisK, thisV) ->
                      (thisV == null
                          ? (SimpleProgressIndex) thatV.deepCopy()
                          : (SimpleProgressIndex)
                              thisV.updateToMinimumEqualOrIsAfterProgressIndex(thatV))));
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ProgressIndexType getType() {
    return ProgressIndexType.RECOVER_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    lock.readLock().lock();
    try {
      return ProgressIndex.TotalOrderSumTuple.sum(
          dataNodeId2LocalIndex.values().stream()
              .map(SimpleProgressIndex::getTotalOrderSumTuple)
              .collect(Collectors.toList()));
    } finally {
      lock.readLock().unlock();
    }
  }

  public static RecoverProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final RecoverProgressIndex recoverProgressIndex = new RecoverProgressIndex();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final int dataNodeId = ReadWriteIOUtils.readInt(byteBuffer);
      final SimpleProgressIndex simpleProgressIndex =
          (SimpleProgressIndex) ProgressIndexType.deserializeFrom(byteBuffer);
      recoverProgressIndex.dataNodeId2LocalIndex.put(dataNodeId, simpleProgressIndex);
    }
    return recoverProgressIndex;
  }

  public static RecoverProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final RecoverProgressIndex recoverProgressIndex = new RecoverProgressIndex();
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      final int dataNodeId = ReadWriteIOUtils.readInt(stream);
      final SimpleProgressIndex simpleProgressIndex =
          (SimpleProgressIndex) ProgressIndexType.deserializeFrom(stream);
      recoverProgressIndex.dataNodeId2LocalIndex.put(dataNodeId, simpleProgressIndex);
    }
    return recoverProgressIndex;
  }

  @Override
  public String toString() {
    return "RecoverProgressIndex{" + "dataNodeId2LocalIndex=" + dataNodeId2LocalIndex + '}';
  }
}
