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

import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persisted commit metadata for a single (consumerGroup, topic, region) combination.
 *
 * <p>This object is no longer a scalar region frontier. It only stores a normalized committed
 * writer checkpoint plus the persistence throttling counter.
 */
public class SubscriptionConsensusProgress {

  private volatile CommittedWriterState committedWriterState;

  private final AtomicLong commitIndex;

  public SubscriptionConsensusProgress() {
    this(null, new WriterProgress(0L, -1L), 0L);
  }

  public SubscriptionConsensusProgress(
      final WriterId committedWriterId,
      final WriterProgress committedWriterProgress,
      final long commitIndex) {
    this.committedWriterState =
        new CommittedWriterState(
            committedWriterId,
            Objects.nonNull(committedWriterProgress)
                ? committedWriterProgress
                : new WriterProgress(0L, -1L));
    this.commitIndex = new AtomicLong(commitIndex);
  }

  public WriterId getCommittedWriterId() {
    return committedWriterState.writerId;
  }

  public WriterProgress getCommittedWriterProgress() {
    return committedWriterState.writerProgress;
  }

  public void setCommittedWriter(
      final WriterId committedWriterId, final WriterProgress committedWriterProgress) {
    this.committedWriterState =
        new CommittedWriterState(
            committedWriterId,
            Objects.nonNull(committedWriterProgress)
                ? committedWriterProgress
                : new WriterProgress(0L, -1L));
  }

  public long getCommitIndex() {
    return commitIndex.get();
  }

  public void incrementCommitIndex() {
    commitIndex.incrementAndGet();
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    final CommittedWriterState snapshot = committedWriterState;
    final boolean hasWriterId = Objects.nonNull(snapshot.writerId);
    final boolean hasWriterProgress = Objects.nonNull(snapshot.writerProgress);
    ReadWriteIOUtils.write((byte) (hasWriterId ? 1 : 0), stream);
    if (hasWriterId) {
      snapshot.writerId.serialize(stream);
    }
    ReadWriteIOUtils.write((byte) (hasWriterProgress ? 1 : 0), stream);
    if (hasWriterProgress) {
      snapshot.writerProgress.serialize(stream);
    }
    ReadWriteIOUtils.write(commitIndex.get(), stream);
  }

  public static SubscriptionConsensusProgress deserialize(final ByteBuffer buffer) {
    final boolean hasWriterId = ReadWriteIOUtils.readByte(buffer) != 0;
    final WriterId writerId = hasWriterId ? WriterId.deserialize(buffer) : null;
    final boolean hasWriterProgress = ReadWriteIOUtils.readByte(buffer) != 0;
    final WriterProgress writerProgress =
        hasWriterProgress ? WriterProgress.deserialize(buffer) : null;
    final long commitIndex = ReadWriteIOUtils.readLong(buffer);
    return new SubscriptionConsensusProgress(writerId, writerProgress, commitIndex);
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
    final CommittedWriterState thisSnapshot = committedWriterState;
    final CommittedWriterState thatSnapshot = that.committedWriterState;
    return commitIndex.get() == that.commitIndex.get()
        && Objects.equals(thisSnapshot.writerId, thatSnapshot.writerId)
        && Objects.equals(thisSnapshot.writerProgress, thatSnapshot.writerProgress);
  }

  @Override
  public int hashCode() {
    final CommittedWriterState snapshot = committedWriterState;
    return Objects.hash(snapshot.writerId, snapshot.writerProgress, commitIndex.get());
  }

  @Override
  public String toString() {
    final CommittedWriterState snapshot = committedWriterState;
    return "SubscriptionConsensusProgress{"
        + "committedWriterId="
        + snapshot.writerId
        + ", committedWriterProgress="
        + snapshot.writerProgress
        + ", commitIndex="
        + commitIndex.get()
        + '}';
  }

  private static final class CommittedWriterState {

    private final WriterId writerId;

    private final WriterProgress writerProgress;

    private CommittedWriterState(final WriterId writerId, final WriterProgress writerProgress) {
      this.writerId = writerId;
      this.writerProgress = writerProgress;
    }
  }
}
