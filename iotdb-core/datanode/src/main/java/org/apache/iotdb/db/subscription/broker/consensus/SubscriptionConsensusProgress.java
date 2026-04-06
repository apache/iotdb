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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persisted commit metadata for a single (consumerGroup, topic, region) combination.
 *
 * <p>This object stores the committed per-writer region frontier plus the persistence throttling
 * counter.
 */
public class SubscriptionConsensusProgress {

  private volatile RegionProgress committedRegionProgress;

  private final AtomicLong commitIndex;

  public SubscriptionConsensusProgress() {
    this(new RegionProgress(Collections.emptyMap()), 0L);
  }

  public SubscriptionConsensusProgress(
      final RegionProgress committedRegionProgress, final long commitIndex) {
    this.committedRegionProgress = normalize(committedRegionProgress);
    this.commitIndex = new AtomicLong(commitIndex);
  }

  public RegionProgress getCommittedRegionProgress() {
    return committedRegionProgress;
  }

  public void setCommittedRegionProgress(final RegionProgress committedRegionProgress) {
    this.committedRegionProgress = normalize(committedRegionProgress);
  }

  public WriterId getCommittedWriterId() {
    return getDerivedCommittedWriterState().writerId;
  }

  public WriterProgress getCommittedWriterProgress() {
    return getDerivedCommittedWriterState().writerProgress;
  }

  public long getCommitIndex() {
    return commitIndex.get();
  }

  public void incrementCommitIndex() {
    commitIndex.incrementAndGet();
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    committedRegionProgress.serialize(stream);
    ReadWriteIOUtils.write(commitIndex.get(), stream);
  }

  public static SubscriptionConsensusProgress deserialize(final ByteBuffer buffer) {
    final RegionProgress committedRegionProgress = RegionProgress.deserialize(buffer);
    final long commitIndex = ReadWriteIOUtils.readLong(buffer);
    return new SubscriptionConsensusProgress(committedRegionProgress, commitIndex);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SubscriptionConsensusProgress that = (SubscriptionConsensusProgress) o;
    return commitIndex.get() == that.commitIndex.get()
        && Objects.equals(committedRegionProgress, that.committedRegionProgress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(committedRegionProgress, commitIndex.get());
  }

  @Override
  public String toString() {
    return "SubscriptionConsensusProgress{"
        + "committedRegionProgress="
        + committedRegionProgress
        + ", commitIndex="
        + commitIndex.get()
        + '}';
  }

  private static RegionProgress normalize(final RegionProgress committedRegionProgress) {
    if (Objects.isNull(committedRegionProgress)
        || committedRegionProgress.getWriterPositions().isEmpty()) {
      return new RegionProgress(Collections.emptyMap());
    }
    final Map<WriterId, WriterProgress> normalized = new LinkedHashMap<>();
    for (final Map.Entry<WriterId, WriterProgress> entry :
        committedRegionProgress.getWriterPositions().entrySet()) {
      if (Objects.nonNull(entry.getKey()) && Objects.nonNull(entry.getValue())) {
        normalized.put(entry.getKey(), entry.getValue());
      }
    }
    return new RegionProgress(normalized);
  }

  private DerivedCommittedWriterState getDerivedCommittedWriterState() {
    WriterId bestWriterId = null;
    WriterProgress bestWriterProgress = null;
    for (final Map.Entry<WriterId, WriterProgress> entry :
        committedRegionProgress.getWriterPositions().entrySet()) {
      if (Objects.isNull(bestWriterProgress)
          || compareWriterProgress(entry.getValue(), bestWriterProgress) > 0
          || (compareWriterProgress(entry.getValue(), bestWriterProgress) == 0
              && compareWriterId(entry.getKey(), bestWriterId) > 0)) {
        bestWriterId = entry.getKey();
        bestWriterProgress = entry.getValue();
      }
    }
    return new DerivedCommittedWriterState(
        bestWriterId,
        Objects.nonNull(bestWriterProgress) ? bestWriterProgress : new WriterProgress(0L, -1L));
  }

  private static int compareWriterProgress(
      final WriterProgress leftProgress, final WriterProgress rightProgress) {
    int cmp = Long.compare(leftProgress.getPhysicalTime(), rightProgress.getPhysicalTime());
    if (cmp != 0) {
      return cmp;
    }
    return Long.compare(leftProgress.getLocalSeq(), rightProgress.getLocalSeq());
  }

  private static int compareWriterId(final WriterId leftWriterId, final WriterId rightWriterId) {
    if (Objects.isNull(leftWriterId) && Objects.isNull(rightWriterId)) {
      return 0;
    }
    if (Objects.isNull(leftWriterId)) {
      return -1;
    }
    if (Objects.isNull(rightWriterId)) {
      return 1;
    }
    int cmp = Integer.compare(leftWriterId.getNodeId(), rightWriterId.getNodeId());
    if (cmp != 0) {
      return cmp;
    }
    return Long.compare(leftWriterId.getWriterEpoch(), rightWriterId.getWriterEpoch());
  }

  private static final class DerivedCommittedWriterState {

    private final WriterId writerId;

    private final WriterProgress writerProgress;

    private DerivedCommittedWriterState(
        final WriterId writerId, final WriterProgress writerProgress) {
      this.writerId = writerId;
      this.writerProgress = writerProgress;
    }
  }
}
