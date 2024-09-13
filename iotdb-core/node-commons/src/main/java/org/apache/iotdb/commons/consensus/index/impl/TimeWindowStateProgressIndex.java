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
import org.apache.tsfile.utils.Pair;
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
import java.util.stream.Collectors;

/**
 * NOTE: Currently, {@link TimeWindowStateProgressIndex} does not perform deep copies of the {@link
 * ByteBuffer} during construction or updates, which may lead to unintended shared state or
 * modifications. This behavior should be reviewed and adjusted as necessary to ensure the integrity
 * and independence of the progress index instances.
 */
public class TimeWindowStateProgressIndex extends ProgressIndex {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // Only the byteBuffer is nullable, the timeSeries, pair and timestamp must not be null
  private Map<String, Pair<Long, ByteBuffer>> timeSeries2TimestampWindowBufferPairMap;

  public TimeWindowStateProgressIndex(
      @Nonnull Map<String, Pair<Long, ByteBuffer>> timeSeries2TimestampWindowBufferPairMap) {
    this.timeSeries2TimestampWindowBufferPairMap =
        new HashMap<>(timeSeries2TimestampWindowBufferPairMap);
  }

  private TimeWindowStateProgressIndex() {
    // Empty constructor
  }

  public Map<String, Pair<Long, ByteBuffer>> getTimeSeries2TimestampWindowBufferPairMap() {
    return ImmutableMap.copyOf(timeSeries2TimestampWindowBufferPairMap);
  }

  public long getMinTime() {
    return timeSeries2TimestampWindowBufferPairMap.values().stream()
        .mapToLong(Pair::getLeft)
        .min()
        .orElse(Long.MIN_VALUE);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    lock.readLock().lock();
    try {
      ProgressIndexType.TIME_WINDOW_STATE_PROGRESS_INDEX.serialize(byteBuffer);

      ReadWriteIOUtils.write(timeSeries2TimestampWindowBufferPairMap.size(), byteBuffer);
      for (final Map.Entry<String, Pair<Long, ByteBuffer>> entry :
          timeSeries2TimestampWindowBufferPairMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue().getLeft(), byteBuffer);
        final ByteBuffer buffer = entry.getValue().getRight();
        if (Objects.nonNull(buffer)) {
          ReadWriteIOUtils.write(buffer.limit(), byteBuffer);
          byteBuffer.put(buffer.array(), 0, buffer.limit());
        } else {
          ReadWriteIOUtils.write(-1, byteBuffer);
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    lock.readLock().lock();
    try {
      ProgressIndexType.TIME_WINDOW_STATE_PROGRESS_INDEX.serialize(stream);

      ReadWriteIOUtils.write(timeSeries2TimestampWindowBufferPairMap.size(), stream);
      for (final Map.Entry<String, Pair<Long, ByteBuffer>> entry :
          timeSeries2TimestampWindowBufferPairMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue().getLeft(), stream);
        final ByteBuffer buffer = entry.getValue().getRight();
        if (Objects.nonNull(buffer)) {
          ReadWriteIOUtils.write(buffer.limit(), stream);
          stream.write(buffer.array(), 0, buffer.limit());
        } else {
          ReadWriteIOUtils.write(-1, stream);
        }
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

      if (!(progressIndex instanceof TimeWindowStateProgressIndex)) {
        return false;
      }

      final TimeWindowStateProgressIndex thisTimeWindowStateProgressIndex = this;
      final TimeWindowStateProgressIndex thatTimeWindowStateProgressIndex =
          (TimeWindowStateProgressIndex) progressIndex;
      return thatTimeWindowStateProgressIndex
          .timeSeries2TimestampWindowBufferPairMap
          .entrySet()
          .stream()
          .noneMatch(
              entry ->
                  !thisTimeWindowStateProgressIndex.timeSeries2TimestampWindowBufferPairMap
                          .containsKey(entry.getKey())
                      || thisTimeWindowStateProgressIndex
                              .timeSeries2TimestampWindowBufferPairMap
                              .get(entry.getKey())
                              .getLeft()
                          <= entry.getValue().getLeft());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    lock.readLock().lock();
    try {
      if (!(progressIndex instanceof TimeWindowStateProgressIndex)) {
        return false;
      }

      final TimeWindowStateProgressIndex thisTimeWindowStateProgressIndex = this;
      final TimeWindowStateProgressIndex thatTimeWindowStateProgressIndex =
          (TimeWindowStateProgressIndex) progressIndex;
      return thisTimeWindowStateProgressIndex.timeSeries2TimestampWindowBufferPairMap.equals(
          thatTimeWindowStateProgressIndex.timeSeries2TimestampWindowBufferPairMap);
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
    if (!(obj instanceof TimeWindowStateProgressIndex)) {
      return false;
    }
    return this.equals((TimeWindowStateProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeSeries2TimestampWindowBufferPairMap);
  }

  @Override
  public ProgressIndex deepCopy() {
    return new TimeWindowStateProgressIndex(timeSeries2TimestampWindowBufferPairMap);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(ProgressIndex progressIndex) {
    lock.writeLock().lock();
    try {
      if (!(progressIndex instanceof TimeWindowStateProgressIndex)) {
        return this;
      }

      this.timeSeries2TimestampWindowBufferPairMap.putAll(
          ((TimeWindowStateProgressIndex) progressIndex)
              .timeSeries2TimestampWindowBufferPairMap.entrySet().stream()
                  .filter(
                      entry ->
                          !this.timeSeries2TimestampWindowBufferPairMap.containsKey(entry.getKey())
                              || this.timeSeries2TimestampWindowBufferPairMap
                                      .get(entry.getKey())
                                      .getLeft()
                                  <= entry.getValue().getLeft())
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
      return this;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.TIME_WINDOW_STATE_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    throw new UnsupportedOperationException(
        "TimeWindowStateProgressIndex does not support topological sorting");
  }

  public static TimeWindowStateProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final TimeWindowStateProgressIndex timeWindowStateProgressIndex =
        new TimeWindowStateProgressIndex();
    timeWindowStateProgressIndex.timeSeries2TimestampWindowBufferPairMap = new HashMap<>();

    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final String timeSeries = ReadWriteIOUtils.readString(byteBuffer);
      final long timestamp = ReadWriteIOUtils.readLong(byteBuffer);
      final int length = ReadWriteIOUtils.readInt(byteBuffer);
      if (length < 0) {
        continue;
      }
      final byte[] body = new byte[length];
      byteBuffer.get(body);
      final ByteBuffer dstBuffer = ByteBuffer.wrap(body);
      timeWindowStateProgressIndex.timeSeries2TimestampWindowBufferPairMap.put(
          timeSeries, new Pair<>(timestamp, dstBuffer));
    }
    return timeWindowStateProgressIndex;
  }

  public static TimeWindowStateProgressIndex deserializeFrom(InputStream stream)
      throws IOException {
    final TimeWindowStateProgressIndex timeWindowStateProgressIndex =
        new TimeWindowStateProgressIndex();
    timeWindowStateProgressIndex.timeSeries2TimestampWindowBufferPairMap = new HashMap<>();

    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; ++i) {
      final String timeSeries = ReadWriteIOUtils.readString(stream);
      final long timestamp = ReadWriteIOUtils.readLong(stream);
      final int length = ReadWriteIOUtils.readInt(stream);
      if (length < 0) {
        continue;
      }
      final byte[] body = new byte[length];
      final int readLen = stream.read(body);
      if (readLen != length) {
        throw new IOException(
            String.format(
                "The intended read length is %s but %s is actually read when deserializing TimeProgressIndex, ProgressIndex: %s",
                length, readLen, timeWindowStateProgressIndex));
      }
      final ByteBuffer dstBuffer = ByteBuffer.wrap(body);
      timeWindowStateProgressIndex.timeSeries2TimestampWindowBufferPairMap.put(
          timeSeries, new Pair<>(timestamp, dstBuffer));
    }
    return timeWindowStateProgressIndex;
  }

  @Override
  public String toString() {
    return "TimeWindowStateProgressIndex{"
        + "timeSeries2TimeWindowBufferPairMap='"
        + timeSeries2TimestampWindowBufferPairMap
        + "'}";
  }
}
