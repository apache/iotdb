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

public class IoTProgressIndex extends ProgressIndex {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(IoTProgressIndex.class) + ProgressIndex.LOCK_SIZE;

  // We assume that the integers are all cached, while the longs are all not
  private static final long ENTRY_SIZE =
      RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + Long.BYTES;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<Integer, Long> peerId2SearchIndex;

  private IoTProgressIndex() {
    this(Collections.emptyMap());
  }

  public IoTProgressIndex(Integer peerId, Long searchIndex) {
    this(Collections.singletonMap(peerId, searchIndex));
  }

  public IoTProgressIndex(Map<Integer, Long> peerId2SearchIndex) {
    this.peerId2SearchIndex = new HashMap<>(peerId2SearchIndex);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.IOT_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(peerId2SearchIndex.size(), byteBuffer);
      for (final Map.Entry<Integer, Long> entry : peerId2SearchIndex.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    lock.readLock().lock();
    try {
      ProgressIndexType.IOT_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(peerId2SearchIndex.size(), stream);
      for (final Map.Entry<Integer, Long> entry : peerId2SearchIndex.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
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

      if (!(progressIndex instanceof IoTProgressIndex)) {
        return false;
      }

      final IoTProgressIndex thisIoTProgressIndex = this;
      final IoTProgressIndex thatIoTProgressIndex = (IoTProgressIndex) progressIndex;
      return thatIoTProgressIndex.peerId2SearchIndex.entrySet().stream()
          .noneMatch(
              entry ->
                  !thisIoTProgressIndex.peerId2SearchIndex.containsKey(entry.getKey())
                      || thisIoTProgressIndex.peerId2SearchIndex.get(entry.getKey())
                          <= entry.getValue());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof IoTProgressIndex)) {
        return false;
      }

      final IoTProgressIndex thisIoTProgressIndex = this;
      final IoTProgressIndex thatIoTProgressIndex = (IoTProgressIndex) progressIndex;
      return thisIoTProgressIndex.peerId2SearchIndex.size()
              == thatIoTProgressIndex.peerId2SearchIndex.size()
          && thatIoTProgressIndex.peerId2SearchIndex.entrySet().stream()
              .allMatch(
                  entry ->
                      thisIoTProgressIndex.peerId2SearchIndex.containsKey(entry.getKey())
                          && thisIoTProgressIndex
                              .peerId2SearchIndex
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
    if (!(obj instanceof IoTProgressIndex)) {
      return false;
    }
    return this.equals((IoTProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(peerId2SearchIndex);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof IoTProgressIndex)) {
        return ProgressIndex.blendProgressIndex(this, progressIndex);
      }

      final IoTProgressIndex thisIoTProgressIndex = this;
      final IoTProgressIndex thatIoTProgressIndex = (IoTProgressIndex) progressIndex;
      final Map<Integer, Long> peerId2SearchIndex =
          new HashMap<>(thisIoTProgressIndex.peerId2SearchIndex);
      thatIoTProgressIndex.peerId2SearchIndex.forEach(
          (thatK, thatV) ->
              peerId2SearchIndex.compute(
                  thatK, (thisK, thisV) -> (thisV == null ? thatV : Math.max(thisV, thatV))));
      return new IoTProgressIndex(peerId2SearchIndex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.IOT_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    lock.readLock().lock();
    try {
      return new TotalOrderSumTuple(
          peerId2SearchIndex.values().stream().mapToLong(Long::longValue).sum());
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getPeerId2SearchIndexSize() {
    lock.readLock().lock();
    try {
      return peerId2SearchIndex.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  public static IoTProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final IoTProgressIndex ioTProgressIndex = new IoTProgressIndex();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final int peerId = ReadWriteIOUtils.readInt(byteBuffer);
      final long searchIndex = ReadWriteIOUtils.readLong(byteBuffer);
      ioTProgressIndex.peerId2SearchIndex.put(peerId, searchIndex);
    }
    return ioTProgressIndex;
  }

  public static IoTProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final IoTProgressIndex ioTProgressIndex = new IoTProgressIndex();
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      final int peerId = ReadWriteIOUtils.readInt(stream);
      final long searchIndex = ReadWriteIOUtils.readLong(stream);
      ioTProgressIndex.peerId2SearchIndex.put(peerId, searchIndex);
    }
    return ioTProgressIndex;
  }

  @Override
  public String toString() {
    return "IoTProgressIndex{" + "peerId2SearchIndex=" + peerId2SearchIndex + '}';
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + peerId2SearchIndex.size() * ENTRY_SIZE;
  }
}
