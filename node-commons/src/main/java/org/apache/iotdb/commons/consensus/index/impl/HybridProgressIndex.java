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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HybridProgressIndex implements ProgressIndex {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<String, ProgressIndex> className2Index;

  public HybridProgressIndex() {
    this.className2Index = new HashMap<>();
  }

  public HybridProgressIndex(String className, ProgressIndex progressIndex) {
    this.className2Index = new HashMap<>();
    className2Index.put(className, progressIndex);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.HYBRID_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(className2Index.size(), byteBuffer);
      for (final Map.Entry<String, ProgressIndex> entry : className2Index.entrySet()) {
        ReadWriteIOUtils.write(new Binary(entry.getKey()), byteBuffer);
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

      ReadWriteIOUtils.write(className2Index.size(), stream);
      for (final Map.Entry<String, ProgressIndex> entry : className2Index.entrySet()) {
        ReadWriteIOUtils.write(new Binary(entry.getKey()), stream);
        entry.getValue().serialize(stream);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean isAfter(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (progressIndex instanceof MinimumProgressIndex) {
        return true;
      }

      if (!(progressIndex instanceof HybridProgressIndex)) {
        String className = progressIndex.getClass().getSimpleName();
        return className2Index.containsKey(className)
            && className2Index.get(className).isAfter(progressIndex);
      }

      final HybridProgressIndex thisHybridProgressIndex = this;
      final HybridProgressIndex thatHybridProgressIndex = (HybridProgressIndex) progressIndex;

      return thatHybridProgressIndex.className2Index.entrySet().stream()
          .noneMatch(
              entry ->
                  !thisHybridProgressIndex.className2Index.containsKey(entry.getKey())
                      || !thisHybridProgressIndex
                          .className2Index
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
      if (!(progressIndex instanceof HybridProgressIndex)) {
        return false;
      }

      final HybridProgressIndex thisHybridProgressIndex = this;
      final HybridProgressIndex thatHybridProgressIndex = (HybridProgressIndex) progressIndex;
      return thisHybridProgressIndex.className2Index.size()
              == thatHybridProgressIndex.className2Index.size()
          && thatHybridProgressIndex.className2Index.entrySet().stream()
              .allMatch(
                  entry ->
                      thisHybridProgressIndex.className2Index.containsKey(entry.getKey())
                          && thisHybridProgressIndex
                              .className2Index
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
    return 0;
  }

  @Override
  public ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (progressIndex instanceof MinimumProgressIndex) {
        return this;
      }

      if (!(progressIndex instanceof HybridProgressIndex)) {
        className2Index.compute(
            progressIndex.getClass().getSimpleName(),
            (thisK, thisV) ->
                (thisV == null
                    ? progressIndex
                    : thisV.updateToMinimumIsAfterProgressIndex(progressIndex)));
        return this;
      }

      final HybridProgressIndex thisHybridProgressIndex = this;
      final HybridProgressIndex thatHybridProgressIndex = (HybridProgressIndex) progressIndex;
      thatHybridProgressIndex.className2Index.forEach(
          (thatK, thatV) ->
              thisHybridProgressIndex.className2Index.compute(
                  thatK,
                  (thisK, thisV) ->
                      (thisV == null ? thatV : thisV.updateToMinimumIsAfterProgressIndex(thatV))));
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static HybridProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final HybridProgressIndex hybridProgressIndex = new HybridProgressIndex();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final String className = ReadWriteIOUtils.readBinary(byteBuffer).getStringValue();
      final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(byteBuffer);
      hybridProgressIndex.className2Index.put(className, progressIndex);
    }
    return hybridProgressIndex;
  }

  public static HybridProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final HybridProgressIndex hybridProgressIndex = new HybridProgressIndex();
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      final String className = ReadWriteIOUtils.readBinary(stream).getStringValue();
      final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(stream);
      hybridProgressIndex.className2Index.put(className, progressIndex);
    }
    return hybridProgressIndex;
  }

  @Override
  public String toString() {
    return "HybridProgressIndex{" + "className2Index=" + className2Index + '}';
  }
}
