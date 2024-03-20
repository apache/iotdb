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
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TimeProgressIndex extends ProgressIndex {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Map<String, Pair<Long, ByteBuffer>> timeSeries2TimeWindowBufferPairMap;

  public TimeProgressIndex(
      @Nonnull Map<String, Pair<Long, ByteBuffer>> timeSeries2TimeWindowBufferPairMap) {
    this.timeSeries2TimeWindowBufferPairMap = timeSeries2TimeWindowBufferPairMap;
  }

  private TimeProgressIndex() {
    // Empty constructor
  }

  public Map<String, Pair<Long, ByteBuffer>> getTimeSeries2TimeWindowBufferPairMap() {
    return timeSeries2TimeWindowBufferPairMap;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.TIME_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(timeSeries2TimeWindowBufferPairMap.size(), byteBuffer);
      for (final Map.Entry<String, Pair<Long, ByteBuffer>> entry :
          timeSeries2TimeWindowBufferPairMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue().getLeft(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue().getRight().limit(), byteBuffer);
        byteBuffer.put(entry.getValue().getRight().array(), 0, entry.getValue().getRight().limit());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    lock.readLock().lock();
    try {
      ProgressIndexType.TIME_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(timeSeries2TimeWindowBufferPairMap.size(), stream);
      for (final Map.Entry<String, Pair<Long, ByteBuffer>> entry :
          timeSeries2TimeWindowBufferPairMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue().getLeft(), stream);
        ReadWriteIOUtils.write(entry.getValue().getRight().limit(), stream);
        stream.write(entry.getValue().getRight().array(), 0, entry.getValue().getRight().limit());
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

      if (!(progressIndex instanceof TimeProgressIndex)) {
        return false;
      }

      final TimeProgressIndex thisTimeProgressIndex = this;
      final TimeProgressIndex thatTimeProgressIndex = (TimeProgressIndex) progressIndex;
      Map<String, Pair<Long, ByteBuffer>> thisMapView =
          new HashMap<>(thisTimeProgressIndex.timeSeries2TimeWindowBufferPairMap);
      thisMapView
          .entrySet()
          .removeIf(
              entry ->
                  !thatTimeProgressIndex.timeSeries2TimeWindowBufferPairMap.containsKey(
                          entry.getKey())
                      || Objects.equals(
                          entry.getValue().getLeft(),
                          thatTimeProgressIndex
                              .timeSeries2TimeWindowBufferPairMap
                              .get(entry.getKey())
                              .getLeft()));
      return !thisMapView.isEmpty()
          && thisMapView.entrySet().stream()
              .noneMatch(
                  entry ->
                      thatTimeProgressIndex
                              .timeSeries2TimeWindowBufferPairMap
                              .get(entry.getKey())
                              .getLeft()
                          > entry.getValue().getLeft());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof TimeProgressIndex)) {
        return false;
      }

      final TimeProgressIndex thisTimeProgressIndex = this;
      final TimeProgressIndex thatTimeProgressIndex = (TimeProgressIndex) progressIndex;
      return thisTimeProgressIndex.timeSeries2TimeWindowBufferPairMap.equals(
          thatTimeProgressIndex.timeSeries2TimeWindowBufferPairMap);
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
    if (!(obj instanceof TimeProgressIndex)) {
      return false;
    }
    return this.equals((TimeProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeSeries2TimeWindowBufferPairMap);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof TimeProgressIndex)) {
        return this;
      }

      this.timeSeries2TimeWindowBufferPairMap
          .entrySet()
          .addAll(
              ((TimeProgressIndex) progressIndex)
                  .timeSeries2TimeWindowBufferPairMap.entrySet().stream()
                      .filter(
                          entry ->
                              !this.timeSeries2TimeWindowBufferPairMap.containsKey(entry.getKey())
                                  || this.timeSeries2TimeWindowBufferPairMap
                                          .get(entry.getKey())
                                          .getLeft()
                                      < entry.getValue().getLeft())
                      .collect(Collectors.toSet()));
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ProgressIndexType getType() {
    return ProgressIndexType.TIME_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    lock.readLock().lock();
    try {
      return new ProgressIndex.TotalOrderSumTuple(
          timeSeries2TimeWindowBufferPairMap.values().stream()
              .map(Pair::getLeft)
              .mapToLong(Long::longValue)
              .sum());
    } finally {
      lock.readLock().unlock();
    }
  }

  public static TimeProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final TimeProgressIndex timeProgressIndex = new TimeProgressIndex();
    timeProgressIndex.timeSeries2TimeWindowBufferPairMap = new HashMap<>();

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String timeSeries = ReadWriteIOUtils.readString(byteBuffer);
      final long timestamp = ReadWriteIOUtils.readLong(byteBuffer);
      final int length = ReadWriteIOUtils.readInt(byteBuffer);
      final byte[] body = new byte[length];
      byteBuffer.get(body);
      ByteBuffer dstBuffer = ByteBuffer.wrap(body);
      timeProgressIndex.timeSeries2TimeWindowBufferPairMap.put(
          timeSeries, new Pair<>(timestamp, dstBuffer));
    }
    return timeProgressIndex;
  }

  public static TimeProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final TimeProgressIndex timeProgressIndex = new TimeProgressIndex();
    timeProgressIndex.timeSeries2TimeWindowBufferPairMap = new HashMap<>();

    int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; ++i) {
      final String timeSeries = ReadWriteIOUtils.readString(stream);
      final long timestamp = ReadWriteIOUtils.readLong(stream);
      final int length = ReadWriteIOUtils.readInt(stream);
      final byte[] body = new byte[length];
      int readLen = stream.read(body);
      if (readLen != length) {
        throw new IOException(
            String.format(
                "The intended read length is %s but %s is actually read when deserializing TimeProgressIndex, ProgressIndex: %s",
                length, readLen, timeProgressIndex));
      }
      ByteBuffer dstBuffer = ByteBuffer.wrap(body);
      timeProgressIndex.timeSeries2TimeWindowBufferPairMap.put(
          timeSeries, new Pair<>(timestamp, dstBuffer));
    }
    return timeProgressIndex;
  }

  @Override
  public String toString() {
    return "TimeProgressIndex{"
        + "timeSeries2TimeWindowBufferPairMap="
        + timeSeries2TimeWindowBufferPairMap
        + '}';
  }
}
