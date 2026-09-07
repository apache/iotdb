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

package org.apache.iotdb.commons.subscription.meta.consumer;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A side-effect-free point-in-time view of a consensus subscription queue.
 *
 * <p>The in-memory counts are exact for the queue lifecycle stages. {@link #rawWalGap} is a
 * search-index distance and therefore is deliberately named as such: it is not the number of
 * topic-filtered events remaining in the WAL.
 */
public final class SubscriptionProgressSnapshot {

  public static final String STATUS_UNINITIALIZED = "UNINITIALIZED";
  public static final String STATUS_INACTIVE = "INACTIVE";
  public static final String STATUS_CAUGHT_UP = "CAUGHT_UP";
  public static final String STATUS_CATCHING_UP = "CATCHING_UP";
  public static final String STATUS_STALLED = "STALLED";
  public static final String STATUS_NO_QUEUE = "NO_QUEUE";

  private final int dataNodeId;
  private final String consumerGroupId;
  private final String topicName;
  private final String regionId;
  private final boolean active;
  private final boolean initialized;
  private final long currentWalSearchIndex;
  private final long nextReadSearchIndex;
  private final long rawWalGap;
  private final long approximateLag;
  private final long prefetchedEventCount;
  private final long inFlightEventCount;
  private final long pendingEventCount;
  private final long realtimeBufferedEventCount;
  private final long lingerEventCount;
  private final long lastPollTimeMs;
  private final long lastProgressTimeMs;
  private final String lastConsumerId;
  private final long seekGeneration;
  private final long walGapSkippedEntries;
  private final long routingEpochChangeCount;
  private final long maxObservedTimestamp;
  private final String status;

  public SubscriptionProgressSnapshot(
      final int dataNodeId,
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final boolean active,
      final boolean initialized,
      final long currentWalSearchIndex,
      final long nextReadSearchIndex,
      final long rawWalGap,
      final long approximateLag,
      final long prefetchedEventCount,
      final long inFlightEventCount,
      final long pendingEventCount,
      final long realtimeBufferedEventCount,
      final long lingerEventCount,
      final long lastPollTimeMs,
      final long lastProgressTimeMs,
      final String lastConsumerId,
      final long seekGeneration,
      final long walGapSkippedEntries,
      final long routingEpochChangeCount,
      final long maxObservedTimestamp,
      final String status) {
    this.dataNodeId = dataNodeId;
    this.consumerGroupId = consumerGroupId;
    this.topicName = topicName;
    this.regionId = regionId;
    this.active = active;
    this.initialized = initialized;
    this.currentWalSearchIndex = currentWalSearchIndex;
    this.nextReadSearchIndex = nextReadSearchIndex;
    this.rawWalGap = rawWalGap;
    this.approximateLag = approximateLag;
    this.prefetchedEventCount = prefetchedEventCount;
    this.inFlightEventCount = inFlightEventCount;
    this.pendingEventCount = pendingEventCount;
    this.realtimeBufferedEventCount = realtimeBufferedEventCount;
    this.lingerEventCount = lingerEventCount;
    this.lastPollTimeMs = lastPollTimeMs;
    this.lastProgressTimeMs = lastProgressTimeMs;
    this.lastConsumerId = lastConsumerId;
    this.seekGeneration = seekGeneration;
    this.walGapSkippedEntries = walGapSkippedEntries;
    this.routingEpochChangeCount = routingEpochChangeCount;
    this.maxObservedTimestamp = maxObservedTimestamp;
    this.status = status;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getRegionId() {
    return regionId;
  }

  public boolean isActive() {
    return active;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public long getCurrentWalSearchIndex() {
    return currentWalSearchIndex;
  }

  public long getNextReadSearchIndex() {
    return nextReadSearchIndex;
  }

  public long getRawWalGap() {
    return rawWalGap;
  }

  public long getApproximateLag() {
    return approximateLag;
  }

  public long getPrefetchedEventCount() {
    return prefetchedEventCount;
  }

  public long getInFlightEventCount() {
    return inFlightEventCount;
  }

  public long getPendingEventCount() {
    return pendingEventCount;
  }

  public long getRealtimeBufferedEventCount() {
    return realtimeBufferedEventCount;
  }

  public long getLingerEventCount() {
    return lingerEventCount;
  }

  public long getRemainingEventCount() {
    return prefetchedEventCount
        + inFlightEventCount
        + pendingEventCount
        + realtimeBufferedEventCount
        + lingerEventCount;
  }

  public long getLastPollTimeMs() {
    return lastPollTimeMs;
  }

  public long getLastProgressTimeMs() {
    return lastProgressTimeMs;
  }

  public String getLastConsumerId() {
    return lastConsumerId;
  }

  public long getSeekGeneration() {
    return seekGeneration;
  }

  public long getWalGapSkippedEntries() {
    return walGapSkippedEntries;
  }

  public long getRoutingEpochChangeCount() {
    return routingEpochChangeCount;
  }

  public long getMaxObservedTimestamp() {
    return maxObservedTimestamp;
  }

  public String getStatus() {
    return status;
  }

  public ByteBuffer serialize() {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final DataOutputStream stream = new DataOutputStream(output)) {
      ReadWriteIOUtils.write(dataNodeId, stream);
      ReadWriteIOUtils.write(consumerGroupId, stream);
      ReadWriteIOUtils.write(topicName, stream);
      ReadWriteIOUtils.write(regionId, stream);
      ReadWriteIOUtils.write(active, stream);
      ReadWriteIOUtils.write(initialized, stream);
      ReadWriteIOUtils.write(currentWalSearchIndex, stream);
      ReadWriteIOUtils.write(nextReadSearchIndex, stream);
      ReadWriteIOUtils.write(rawWalGap, stream);
      ReadWriteIOUtils.write(approximateLag, stream);
      ReadWriteIOUtils.write(prefetchedEventCount, stream);
      ReadWriteIOUtils.write(inFlightEventCount, stream);
      ReadWriteIOUtils.write(pendingEventCount, stream);
      ReadWriteIOUtils.write(realtimeBufferedEventCount, stream);
      ReadWriteIOUtils.write(lingerEventCount, stream);
      ReadWriteIOUtils.write(lastPollTimeMs, stream);
      ReadWriteIOUtils.write(lastProgressTimeMs, stream);
      ReadWriteIOUtils.write(lastConsumerId, stream);
      ReadWriteIOUtils.write(seekGeneration, stream);
      ReadWriteIOUtils.write(walGapSkippedEntries, stream);
      ReadWriteIOUtils.write(routingEpochChangeCount, stream);
      ReadWriteIOUtils.write(maxObservedTimestamp, stream);
      ReadWriteIOUtils.write(status, stream);
      stream.flush();
      return ByteBuffer.wrap(output.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static SubscriptionProgressSnapshot deserialize(final ByteBuffer input) {
    final ByteBuffer buffer = input.asReadOnlyBuffer();
    return new SubscriptionProgressSnapshot(
        ReadWriteIOUtils.readInt(buffer),
        ReadWriteIOUtils.readString(buffer),
        ReadWriteIOUtils.readString(buffer),
        ReadWriteIOUtils.readString(buffer),
        ReadWriteIOUtils.readBool(buffer),
        ReadWriteIOUtils.readBool(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readString(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readLong(buffer),
        ReadWriteIOUtils.readString(buffer));
  }
}
