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

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * {@link SegmentProgressIndex} is a usual {@link ProgressIndex} with broken segments allowed. An
 * {@link org.apache.iotdb.pipe.api.event.Event} is sent if its {@link ProgressIndex} <= the {@link
 * #latestProgressIndex} and none of the {@link #brokenProgressIndexes}es has its {@link Pair#left}
 * <= its {@link ProgressIndex} <= {@link Pair#right}. If the {@link #brokenProgressIndexes} {@link
 * List#isEmpty()}, the progress Index behave just like the {@link #latestProgressIndex}. It is only
 * used in the realtime data region extractor's {@link
 * org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta} to handle downgrading, and will never
 * be in the insertNodes or tsFiles.
 */
public class SegmentProgressIndex extends ProgressIndex {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SegmentProgressIndex.class);
  public static final long LIST_SIZE = RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);
  public static final long PAIR_SIZE = RamUsageEstimator.shallowSizeOfInstance(Pair.class);
  private ProgressIndex latestProgressIndex = MinimumProgressIndex.INSTANCE;

  // <startIndex, endIndex> of the downgraded segments
  private final LinkedList<Pair<ProgressIndex, ProgressIndex>> brokenProgressIndexes =
      new LinkedList<>();

  public void recordStart(final ProgressIndex index) {
    brokenProgressIndexes.add(new Pair<>(index, null));
  }

  public void recordEnd(final ProgressIndex index) {
    brokenProgressIndexes.getLast().setRight(index);
  }

  public void eliminate(final ProgressIndex index) {
    final Iterator<Pair<ProgressIndex, ProgressIndex>> iterator = brokenProgressIndexes.iterator();
    while (iterator.hasNext()) {
      if (index.equals(iterator.next().getRight())) {
        iterator.remove();
        return;
      }
    }
  }

  @Override
  public void serialize(final ByteBuffer byteBuffer) {
    ProgressIndexType.SEGMENT_PROGRESS_INDEX.serialize(byteBuffer);

    latestProgressIndex.serialize(byteBuffer);
    ReadWriteIOUtils.write(brokenProgressIndexes.size(), byteBuffer);
    for (final Pair<ProgressIndex, ProgressIndex> index : brokenProgressIndexes) {
      index.getLeft().serialize(byteBuffer);
      index.getRight().serialize(byteBuffer);
    }
  }

  @Override
  public void serialize(final OutputStream stream) throws IOException {
    ProgressIndexType.SEGMENT_PROGRESS_INDEX.serialize(stream);

    latestProgressIndex.serialize(stream);
    ReadWriteIOUtils.write(brokenProgressIndexes.size(), stream);
    for (final Pair<ProgressIndex, ProgressIndex> index : brokenProgressIndexes) {
      index.getLeft().serialize(stream);
      index.getRight().serialize(stream);
    }
  }

  @Override
  public boolean isAfter(final @Nonnull ProgressIndex progressIndex) {
    return latestProgressIndex.isAfter(progressIndex)
        && brokenProgressIndexes.stream()
            .noneMatch(
                pair ->
                    pair.getRight().isAfter(progressIndex)
                        && (progressIndex.isAfter(pair.getLeft())
                            || progressIndex.equals(pair.getLeft())));
  }

  @Override
  public boolean equals(final ProgressIndex progressIndex) {
    if (progressIndex == null) {
      return false;
    }
    if (this == progressIndex) {
      return true;
    }
    if (progressIndex instanceof SegmentProgressIndex) {
      final SegmentProgressIndex that = (SegmentProgressIndex) progressIndex;
      return this.latestProgressIndex.equals(that.latestProgressIndex)
          && this.brokenProgressIndexes.equals(that.brokenProgressIndexes);
    }
    return this.latestProgressIndex.equals(progressIndex);
  }

  @Override
  public ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(
      final ProgressIndex progressIndex) {
    return latestProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
  }

  @Override
  public ProgressIndexType getType() {
    return ProgressIndexType.SEGMENT_PROGRESS_INDEX;
  }

  @Override
  public TotalOrderSumTuple getTotalOrderSumTuple() {
    throw new UnsupportedOperationException(
        "This progressIndex is not for tsFile and shall never be used to sort resources");
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + latestProgressIndex.ramBytesUsed()
        + shallowSizeOfList(brokenProgressIndexes)
        + PAIR_SIZE * brokenProgressIndexes.size()
        + brokenProgressIndexes.stream()
            .mapToLong(index -> index.getLeft().ramBytesUsed() + index.getRight().ramBytesUsed())
            .reduce(0L, Long::sum);
  }

  public static SegmentProgressIndex deserializeFrom(final ByteBuffer byteBuffer) {
    final SegmentProgressIndex segmentProgressIndex = new SegmentProgressIndex();
    segmentProgressIndex.latestProgressIndex = ProgressIndexType.deserializeFrom(byteBuffer);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      segmentProgressIndex.brokenProgressIndexes.add(
          new Pair<>(
              ProgressIndexType.deserializeFrom(byteBuffer),
              ProgressIndexType.deserializeFrom(byteBuffer)));
    }
    return segmentProgressIndex;
  }

  public static SegmentProgressIndex deserializeFrom(final InputStream stream) throws IOException {
    final SegmentProgressIndex segmentProgressIndex = new SegmentProgressIndex();
    segmentProgressIndex.latestProgressIndex = ProgressIndexType.deserializeFrom(stream);
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      segmentProgressIndex.brokenProgressIndexes.add(
          new Pair<>(
              ProgressIndexType.deserializeFrom(stream),
              ProgressIndexType.deserializeFrom(stream)));
    }
    return segmentProgressIndex;
  }

  private long shallowSizeOfList(final List<?> list) {
    return Objects.nonNull(list)
        ? SegmentProgressIndex.LIST_SIZE
            + RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
                    + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * list.size())
        : 0L;
  }
}
