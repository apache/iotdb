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
 * <p>Since searchIndex is region-local (each DataRegion has its own independent WAL and searchIndex
 * namespace), progress is tracked <b>per-region</b>:
 *
 * <ul>
 *   <li><b>searchIndex</b>: The committed WAL search index — the highest position where all prior
 *       dispatched events have been acknowledged. Used as the recovery start point after crash.
 *   <li><b>commitIndex</b>: Monotonically increasing count of committed events. Used for
 *       persistence throttling and diagnostics.
 * </ul>
 */
public class SubscriptionConsensusProgress {

  private final AtomicLong searchIndex;

  private final AtomicLong commitIndex;

  public SubscriptionConsensusProgress() {
    this(0L, 0L);
  }

  public SubscriptionConsensusProgress(final long searchIndex, final long commitIndex) {
    this.searchIndex = new AtomicLong(searchIndex);
    this.commitIndex = new AtomicLong(commitIndex);
  }

  public long getSearchIndex() {
    return searchIndex.get();
  }

  public void setSearchIndex(final long searchIndex) {
    this.searchIndex.set(searchIndex);
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
    ReadWriteIOUtils.write(searchIndex.get(), stream);
    ReadWriteIOUtils.write(commitIndex.get(), stream);
  }

  public static SubscriptionConsensusProgress deserialize(final ByteBuffer buffer) {
    final long searchIndex = ReadWriteIOUtils.readLong(buffer);
    final long commitIndex = ReadWriteIOUtils.readLong(buffer);
    return new SubscriptionConsensusProgress(searchIndex, commitIndex);
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
    return searchIndex.get() == that.searchIndex.get()
        && commitIndex.get() == that.commitIndex.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(searchIndex.get(), commitIndex.get());
  }

  @Override
  public String toString() {
    return "SubscriptionConsensusProgress{"
        + "searchIndex="
        + searchIndex.get()
        + ", commitIndex="
        + commitIndex.get()
        + '}';
  }
}
