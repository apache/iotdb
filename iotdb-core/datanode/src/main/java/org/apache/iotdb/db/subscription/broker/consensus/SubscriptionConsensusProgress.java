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
 * <p>Progress is tracked using (epoch, syncIndex) instead of local searchIndex, ensuring
 * consistency across leader migrations. The syncIndex is the original writer's searchIndex, which
 * is identical across all replicas for the same write operation.
 *
 * <ul>
 *   <li><b>epoch</b>: The epoch of the latest committed entry.
 *   <li><b>syncIndex</b>: The syncIndex (original writer's searchIndex) of the latest committed
 *       entry within that epoch.
 *   <li><b>commitIndex</b>: Monotonically increasing count of committed events. Used for
 *       persistence throttling and diagnostics.
 * </ul>
 */
public class SubscriptionConsensusProgress {

  private final AtomicLong epoch;

  private final AtomicLong syncIndex;

  private final AtomicLong commitIndex;

  public SubscriptionConsensusProgress() {
    this(0L, 0L, 0L);
  }

  public SubscriptionConsensusProgress(
      final long epoch, final long syncIndex, final long commitIndex) {
    this.epoch = new AtomicLong(epoch);
    this.syncIndex = new AtomicLong(syncIndex);
    this.commitIndex = new AtomicLong(commitIndex);
  }

  public long getEpoch() {
    return epoch.get();
  }

  public void setEpoch(final long epoch) {
    this.epoch.set(epoch);
  }

  public long getSyncIndex() {
    return syncIndex.get();
  }

  public void setSyncIndex(final long syncIndex) {
    this.syncIndex.set(syncIndex);
  }

  /**
   * @deprecated Use {@link #getSyncIndex()} instead. Kept for backward compatibility.
   */
  @Deprecated
  public long getSearchIndex() {
    return syncIndex.get();
  }

  /**
   * @deprecated Use {@link #setSyncIndex(long)} instead. Kept for backward compatibility.
   */
  @Deprecated
  public void setSearchIndex(final long searchIndex) {
    this.syncIndex.set(searchIndex);
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
    ReadWriteIOUtils.write(epoch.get(), stream);
    ReadWriteIOUtils.write(syncIndex.get(), stream);
    ReadWriteIOUtils.write(commitIndex.get(), stream);
  }

  public static SubscriptionConsensusProgress deserialize(final ByteBuffer buffer) {
    final long epoch = ReadWriteIOUtils.readLong(buffer);
    final long syncIndex = ReadWriteIOUtils.readLong(buffer);
    final long commitIndex = ReadWriteIOUtils.readLong(buffer);
    return new SubscriptionConsensusProgress(epoch, syncIndex, commitIndex);
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
    return epoch.get() == that.epoch.get()
        && syncIndex.get() == that.syncIndex.get()
        && commitIndex.get() == that.commitIndex.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch.get(), syncIndex.get(), commitIndex.get());
  }

  @Override
  public String toString() {
    return "SubscriptionConsensusProgress{"
        + "epoch="
        + epoch.get()
        + ", syncIndex="
        + syncIndex.get()
        + ", commitIndex="
        + commitIndex.get()
        + '}';
  }
}
