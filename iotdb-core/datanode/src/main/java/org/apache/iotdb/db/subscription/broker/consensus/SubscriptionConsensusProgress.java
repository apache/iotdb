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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks consensus subscription consumption progress for a single (consumerGroup, topic, region)
 * combination.
 *
 * <p>Progress is tracked using (physicalTime, localSeq). The local sequence is the original
 * writer's searchIndex, which is identical across all replicas for the same write operation.
 *
 * <ul>
 *   <li><b>physicalTime</b>: The physical time of the latest committed entry.
 *   <li><b>localSeq</b>: The local sequence (original writer's searchIndex) of the latest committed
 *       entry.
 *   <li><b>commitIndex</b>: Monotonically increasing count of committed events. Used for
 *       persistence throttling and diagnostics.
 * </ul>
 */
public class SubscriptionConsensusProgress {

  private final AtomicLong physicalTime;

  private final AtomicLong localSeq;

  private final AtomicLong commitIndex;

  public SubscriptionConsensusProgress() {
    this(0L, -1L, 0L);
  }

  public SubscriptionConsensusProgress(
      final long physicalTime, final long localSeq, final long commitIndex) {
    this.physicalTime = new AtomicLong(physicalTime);
    this.localSeq = new AtomicLong(localSeq);
    this.commitIndex = new AtomicLong(commitIndex);
  }

  public long getPhysicalTime() {
    return physicalTime.get();
  }

  public void setPhysicalTime(final long physicalTime) {
    this.physicalTime.set(physicalTime);
  }

  public long getLocalSeq() {
    return localSeq.get();
  }

  public void setLocalSeq(final long localSeq) {
    this.localSeq.set(localSeq);
  }

  public long getCommitIndex() {
    return commitIndex.get();
  }

  public void setCommitIndex(final long commitIndex) {
    this.commitIndex.set(commitIndex);
  }

  public void incrementCommitIndex() {
    this.commitIndex.incrementAndGet();
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(physicalTime.get(), stream);
    ReadWriteIOUtils.write(localSeq.get(), stream);
    ReadWriteIOUtils.write(commitIndex.get(), stream);
  }

  public static SubscriptionConsensusProgress deserialize(final ByteBuffer buffer) {
    final long physicalTime = ReadWriteIOUtils.readLong(buffer);
    final long localSeq = ReadWriteIOUtils.readLong(buffer);
    final long commitIndex = ReadWriteIOUtils.readLong(buffer);
    return new SubscriptionConsensusProgress(physicalTime, localSeq, commitIndex);
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
    return physicalTime.get() == that.physicalTime.get()
        && localSeq.get() == that.localSeq.get()
        && commitIndex.get() == that.commitIndex.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(physicalTime.get(), localSeq.get(), commitIndex.get());
  }

  @Override
  public String toString() {
    return "SubscriptionConsensusProgress{"
        + "physicalTime="
        + physicalTime.get()
        + ", localSeq="
        + localSeq.get()
        + ", commitIndex="
        + commitIndex.get()
        + '}';
  }
}
