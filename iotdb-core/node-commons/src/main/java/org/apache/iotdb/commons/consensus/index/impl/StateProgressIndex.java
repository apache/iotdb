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

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StateProgressIndex extends ProgressIndex {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private long version;
  private Map<String, Binary> state;
  private ProgressIndex innerProgressIndex;

  public StateProgressIndex(
      long version, Map<String, Binary> state, ProgressIndex innerProgressIndex) {
    this.version = version;
    this.state = state;
    this.innerProgressIndex = innerProgressIndex;
  }

  public long getVersion() {
    return version;
  }

  public ProgressIndex getInnerProgressIndex() {
    return innerProgressIndex == null ? MinimumProgressIndex.INSTANCE : innerProgressIndex;
  }

  public Map<String, Binary> getState() {
    return state;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.STATE_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(version, byteBuffer);

      ReadWriteIOUtils.write(state.size(), byteBuffer);
      for (final Map.Entry<String, Binary> entry : state.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
      }

      innerProgressIndex.serialize(byteBuffer);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    lock.readLock().lock();
    try {
      ProgressIndexType.STATE_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(version, stream);

      ReadWriteIOUtils.write(state.size(), stream);
      for (final Map.Entry<String, Binary> entry : state.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }

      innerProgressIndex.serialize(stream);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean isAfter(@Nonnull ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (progressIndex instanceof MinimumProgressIndex) {
        return innerProgressIndex.isAfter(progressIndex);
      }

      if (progressIndex instanceof HybridProgressIndex) {
        return ((HybridProgressIndex) progressIndex)
            .isGivenProgressIndexAfterSelf(innerProgressIndex);
      }

      if (!(progressIndex instanceof StateProgressIndex)) {
        return false;
      }

      return innerProgressIndex.isAfter(((StateProgressIndex) progressIndex).innerProgressIndex)
          && version > ((StateProgressIndex) progressIndex).version;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      return progressIndex instanceof StateProgressIndex
          && innerProgressIndex.equals(((StateProgressIndex) progressIndex).innerProgressIndex)
          && version == ((StateProgressIndex) progressIndex).version;
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
    if (!(obj instanceof StateProgressIndex)) {
      return false;
    }
    return this.equals((StateProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(innerProgressIndex, version);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      innerProgressIndex =
          innerProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
              progressIndex instanceof StateProgressIndex
                  ? ((StateProgressIndex) progressIndex).innerProgressIndex
                  : progressIndex);
      if (progressIndex instanceof StateProgressIndex
          && version <= ((StateProgressIndex) progressIndex).version) {
        version = ((StateProgressIndex) progressIndex).version;
        state = ((StateProgressIndex) progressIndex).state;
      }
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.STATE_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    return innerProgressIndex.getTotalOrderSumTuple();
  }

  public static StateProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final long version = ReadWriteIOUtils.readLong(byteBuffer);

    final Map<String, Binary> state = new HashMap<>();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final Binary value = ReadWriteIOUtils.readBinary(byteBuffer);
      state.put(key, value);
    }

    final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(byteBuffer);

    return new StateProgressIndex(version, state, progressIndex);
  }

  public static StateProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final long version = ReadWriteIOUtils.readLong(stream);

    final Map<String, Binary> state = new HashMap<>();
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(stream);
      final Binary value = ReadWriteIOUtils.readBinary(stream);
      state.put(key, value);
    }

    final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(stream);

    return new StateProgressIndex(version, state, progressIndex);
  }

  @Override
  public String toString() {
    return "StateProgressIndex{"
        + "version="
        + version
        + ", state="
        + state
        + ", innerProgressIndex="
        + innerProgressIndex
        + '}';
  }
}
