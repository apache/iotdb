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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;

import org.apache.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * A buffering processor that reorders messages based on watermark semantics. Messages are buffered
 * internally and emitted only when the watermark advances past their maximum timestamp.
 *
 * <p>Watermark = (minimum of latest timestamp per <b>active</b> region) - maxOutOfOrdernessMs
 *
 * <p>A region is identified by topic, DataNode ID and region ID from the subscription commit
 * context. A region is considered "stale" if its latest timestamp has not increased for {@code
 * staleRegionTimeoutMs}. Stale regions are excluded from the watermark calculation, preventing a
 * single slow or idle region from anchoring the global watermark indefinitely.
 *
 * <p>Server-side WATERMARK events (carrying per-region timestamp progress) serve as heartbeats,
 * confirming region liveness. They advance the per-region timestamp only when their timestamp is
 * higher than the previously observed value.
 *
 * <p>A timeout mechanism ensures that buffered messages are eventually flushed even if no new data
 * arrives, preventing unbounded buffering.
 *
 * <p><b>Note:</b> This processor is primarily intended as a reference implementation. For
 * production use with large-scale out-of-order data, consider using a downstream stream processing
 * framework (Flink, Spark) for watermark handling.
 */
public class WatermarkProcessor implements SubscriptionMessageProcessor {

  private static final long DEFAULT_STALE_SOURCE_TIMEOUT_MS = 30_000L;
  private static final long DEFAULT_MAX_BUFFER_BYTES = 64L * 1024 * 1024; // 64 MB

  private final long maxOutOfOrdernessMs;
  private final long timeoutMs;
  private final long staleRegionTimeoutMs;
  private final long maxBufferBytes;

  // Buffer ordered by message max timestamp
  private final PriorityQueue<TimestampedMessage> buffer =
      new PriorityQueue<>((a, b) -> Long.compare(a.maxTimestamp, b.maxTimestamp));

  // topicName -> regionKey -> latest timestamp
  private final Map<String, Map<String, Long>> latestPerRegionByTopic = new HashMap<>();
  // topicName -> regionKey -> wall-clock time when the region timestamp last increased
  private final Map<String, Map<String, Long>> lastAdvancedTimeMsByTopic = new HashMap<>();
  private long lastEmitTimeMs = System.currentTimeMillis();
  private long bufferedBytes = 0;

  // Current watermark value
  private long watermark = Long.MIN_VALUE;

  /**
   * Creates a WatermarkProcessor with default stale region timeout (30 seconds).
   *
   * @param maxOutOfOrdernessMs maximum expected out-of-orderness in milliseconds
   * @param timeoutMs if no data arrives within this duration, force-flush all buffered messages
   */
  public WatermarkProcessor(final long maxOutOfOrdernessMs, final long timeoutMs) {
    this(maxOutOfOrdernessMs, timeoutMs, DEFAULT_STALE_SOURCE_TIMEOUT_MS, DEFAULT_MAX_BUFFER_BYTES);
  }

  /**
   * Creates a WatermarkProcessor.
   *
   * @param maxOutOfOrdernessMs maximum expected out-of-orderness in milliseconds
   * @param timeoutMs if no data arrives within this duration, force-flush all buffered messages
   * @param staleRegionTimeoutMs if a region's timestamp has not increased for this duration, it is
   *     excluded from watermark calculation. Use {@link Long#MAX_VALUE} to disable.
   * @param maxBufferBytes maximum total estimated bytes of buffered messages. When exceeded, all
   *     buffered messages are force-flushed regardless of watermark. Defaults to 64 MB.
   */
  public WatermarkProcessor(
      final long maxOutOfOrdernessMs,
      final long timeoutMs,
      final long staleRegionTimeoutMs,
      final long maxBufferBytes) {
    this.maxOutOfOrdernessMs = maxOutOfOrdernessMs;
    this.timeoutMs = timeoutMs;
    this.staleRegionTimeoutMs = staleRegionTimeoutMs;
    this.maxBufferBytes = maxBufferBytes;
  }

  @Override
  public List<SubscriptionMessage> process(final List<SubscriptionMessage> messages) {
    final long now = System.currentTimeMillis();

    // Buffer incoming messages and update per-region timestamps
    for (final SubscriptionMessage message : messages) {
      // WATERMARK events carry server-side timestamp progress per region.
      // They serve as heartbeats and advance per-region tracking only when the timestamp
      // actually increases.
      if (message.getMessageType() == SubscriptionMessageType.WATERMARK.getType()) {
        final SubscriptionCommitContext commitContext = message.getCommitContext();
        advanceRegionTimestamp(
            commitContext.getTopicName(),
            getRegionKey(commitContext),
            message.getWatermarkTimestamp(),
            now);
        continue; // Do not buffer system events
      }

      final long maxTs = extractMaxTimestamp(message);
      final long estimatedSize = message.estimateSize();
      buffer.add(new TimestampedMessage(message, maxTs, estimatedSize));
      bufferedBytes += estimatedSize;
      updateRegionTimestamp(message, maxTs, now);
    }

    // Compute watermark = min(latest per active region) - maxOutOfOrderness
    // Regions whose timestamp has not increased for staleRegionTimeoutMs are excluded.
    updateWatermarkIfAnyActiveRegion(now);

    // Emit messages whose maxTimestamp <= watermark
    final List<SubscriptionMessage> emitted = emit(watermark);

    // Buffer overflow: force-flush all if buffer exceeds byte limit
    if (bufferedBytes > maxBufferBytes) {
      emitted.addAll(forceFlushAll());
      return emitted;
    }

    // Timeout: if nothing was emitted and timeout exceeded, force-flush all
    if (emitted.isEmpty() && (now - lastEmitTimeMs) >= timeoutMs && !buffer.isEmpty()) {
      return forceFlushAll();
    }

    if (!emitted.isEmpty()) {
      lastEmitTimeMs = now;
    }
    return emitted;
  }

  @Override
  public List<SubscriptionMessage> flush() {
    return forceFlushAll();
  }

  @Override
  public int getBufferedCount() {
    return buffer.size();
  }

  @Override
  public List<SubscriptionCommitContext> getBufferedCommitContexts() {
    final List<SubscriptionCommitContext> result = new ArrayList<>(buffer.size());
    for (final TimestampedMessage timestampedMessage : buffer) {
      final SubscriptionCommitContext commitContext = timestampedMessage.message.getCommitContext();
      if (commitContext != null && commitContext.isCommittable()) {
        result.add(commitContext);
      }
    }
    return result;
  }

  @Override
  public void reset() {
    buffer.clear();
    latestPerRegionByTopic.clear();
    lastAdvancedTimeMsByTopic.clear();
    lastEmitTimeMs = System.currentTimeMillis();
    bufferedBytes = 0;
    watermark = Long.MIN_VALUE;
  }

  @Override
  public boolean supportsTopicScopedReset() {
    return true;
  }

  @Override
  public void reset(final String topicName) {
    final Iterator<TimestampedMessage> iterator = buffer.iterator();
    while (iterator.hasNext()) {
      final TimestampedMessage timestampedMessage = iterator.next();
      final SubscriptionCommitContext commitContext = timestampedMessage.message.getCommitContext();
      if (Objects.nonNull(commitContext)
          && Objects.equals(topicName, commitContext.getTopicName())) {
        bufferedBytes -= timestampedMessage.estimatedSize;
        iterator.remove();
      }
    }
    latestPerRegionByTopic.remove(topicName);
    lastAdvancedTimeMsByTopic.remove(topicName);
    recomputeWatermark(System.currentTimeMillis());
  }

  /** Returns the current watermark value. */
  public long getWatermark() {
    return watermark;
  }

  private List<SubscriptionMessage> emit(final long watermarkValue) {
    final List<SubscriptionMessage> result = new ArrayList<>();
    while (!buffer.isEmpty() && buffer.peek().maxTimestamp <= watermarkValue) {
      final TimestampedMessage tm = buffer.poll();
      bufferedBytes -= tm.estimatedSize;
      result.add(tm.message);
    }
    return result;
  }

  private List<SubscriptionMessage> forceFlushAll() {
    final List<SubscriptionMessage> result = new ArrayList<>(buffer.size());
    while (!buffer.isEmpty()) {
      result.add(buffer.poll().message);
    }
    bufferedBytes = 0;
    lastEmitTimeMs = System.currentTimeMillis();
    return result;
  }

  private static long extractMaxTimestamp(final SubscriptionMessage message) {
    long maxTs = Long.MIN_VALUE;
    if (message.getMessageType() == SubscriptionMessageType.RECORD_HANDLER.getType()) {
      final Iterator<Tablet> it = message.getRecordTabletIterator();
      while (it.hasNext()) {
        final Tablet tablet = it.next();
        final long[] timestamps = tablet.getTimestamps();
        final int rowSize = tablet.getRowSize();
        for (int i = 0; i < rowSize; i++) {
          maxTs = Math.max(maxTs, timestamps[i]);
        }
      }
    }
    // For non-tablet messages or empty messages, use current wall clock
    if (maxTs == Long.MIN_VALUE) {
      maxTs = System.currentTimeMillis();
    }
    return maxTs;
  }

  private void updateRegionTimestamp(
      final SubscriptionMessage message, final long maxTs, final long nowMs) {
    // Use region-based key so data events and WATERMARK events share the same key namespace.
    final SubscriptionCommitContext commitContext = message.getCommitContext();
    advanceRegionTimestamp(commitContext.getTopicName(), getRegionKey(commitContext), maxTs, nowMs);
  }

  /**
   * Updates the per-region timestamp tracking. Only records a new "last advanced" wall-clock time
   * when the timestamp actually increases, so that stale regions (whose timestamps don't advance)
   * are eventually excluded from watermark calculation.
   */
  private void advanceRegionTimestamp(
      final String topicName, final String key, final long newTs, final long nowMs) {
    final Map<String, Long> latestPerRegion =
        latestPerRegionByTopic.computeIfAbsent(topicName, ignored -> new HashMap<>());
    final Map<String, Long> lastAdvancedTimeMs =
        lastAdvancedTimeMsByTopic.computeIfAbsent(topicName, ignored -> new HashMap<>());
    final Long oldTs = latestPerRegion.get(key);
    if (oldTs == null || newTs > oldTs) {
      latestPerRegion.put(key, newTs);
      lastAdvancedTimeMs.put(key, nowMs);
    }
  }

  private void updateWatermarkIfAnyActiveRegion(final long nowMs) {
    final long minLatest = getMinLatestActiveRegionTimestamp(nowMs);
    if (minLatest != Long.MAX_VALUE) {
      watermark = minLatest - maxOutOfOrdernessMs;
    }
    // If all regions are stale, watermark stays unchanged and timeout will handle it.
  }

  private void recomputeWatermark(final long nowMs) {
    watermark = Long.MIN_VALUE;
    updateWatermarkIfAnyActiveRegion(nowMs);
  }

  private long getMinLatestActiveRegionTimestamp(final long nowMs) {
    long minLatest = Long.MAX_VALUE;
    for (final Map.Entry<String, Map<String, Long>> topicEntry :
        latestPerRegionByTopic.entrySet()) {
      final Map<String, Long> lastAdvancedTimeMs =
          lastAdvancedTimeMsByTopic.get(topicEntry.getKey());
      if (Objects.isNull(lastAdvancedTimeMs)) {
        continue;
      }
      for (final Map.Entry<String, Long> regionEntry : topicEntry.getValue().entrySet()) {
        final Long lastAdv = lastAdvancedTimeMs.get(regionEntry.getKey());
        if (lastAdv != null && (nowMs - lastAdv) <= staleRegionTimeoutMs) {
          minLatest = Math.min(minLatest, regionEntry.getValue());
        }
      }
    }
    return minLatest;
  }

  private static String getRegionKey(final SubscriptionCommitContext commitContext) {
    return "region-" + commitContext.getDataNodeId() + "-" + commitContext.getRegionId();
  }

  private static final class TimestampedMessage {
    final SubscriptionMessage message;
    final long maxTimestamp;
    final long estimatedSize;

    TimestampedMessage(
        final SubscriptionMessage message, final long maxTimestamp, final long estimatedSize) {
      this.message = message;
      this.maxTimestamp = maxTimestamp;
      this.estimatedSize = estimatedSize;
    }
  }
}
