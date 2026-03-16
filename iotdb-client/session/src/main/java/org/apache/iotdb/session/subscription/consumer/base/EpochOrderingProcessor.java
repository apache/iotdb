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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A processor that enforces epoch ordering per region. Uses a per-region state machine:
 *
 * <ul>
 *   <li><b>INITIAL</b>: No message seen yet for this region. The first message sets {@code
 *       currentEpoch} and transitions to STABLE.
 *   <li><b>STABLE</b>: All messages share the same epoch. Messages with a different epoch trigger a
 *       transition to BUFFERING.
 *   <li><b>BUFFERING</b>: Messages with {@code epoch == currentEpoch} pass through; others are
 *       buffered. When a sentinel for {@code currentEpoch} arrives, the buffer is released and the
 *       state resets to INITIAL (ready for the next epoch).
 * </ul>
 *
 * <p>A configurable timeout ensures buffered messages are eventually released even if the sentinel
 * is lost (e.g., due to old leader crash).
 *
 * <p>Messages with empty regionId (from non-consensus queues) pass through unchanged.
 */
public class EpochOrderingProcessor implements SubscriptionMessageProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(EpochOrderingProcessor.class);

  private static final long DEFAULT_TIMEOUT_MS = 60_000;
  private static final long DEFAULT_MAX_BUFFER_BYTES = 64L * 1024 * 1024; // 64 MB

  private final long timeoutMs;
  private final long maxBufferBytes;

  private enum RegionState {
    INITIAL,
    STABLE,
    BUFFERING
  }

  /** Per-region tracking state. */
  private static class RegionTracker {
    RegionState state = RegionState.INITIAL;
    long currentEpoch;
    final List<SubscriptionMessage> buffer = new ArrayList<>();
    long bufferedBytes;
    long bufferStartTimeMs;

    /**
     * Set when a sentinel arrives while in STABLE state (before any new-epoch message). When the
     * first new-epoch message arrives and this flag is true, the message is accepted directly
     * (transition to INITIAL then STABLE) instead of entering BUFFERING, avoiding a 60s timeout
     * wait for a sentinel that has already arrived.
     */
    boolean sentinelSeen;

    /** DataNode ID that produced messages of the currentEpoch. Used to detect node crashes. */
    int currentEpochDataNodeId = -1;
  }

  private final Map<String, RegionTracker> regionTrackers = new HashMap<>();

  public EpochOrderingProcessor() {
    this(DEFAULT_TIMEOUT_MS, DEFAULT_MAX_BUFFER_BYTES);
  }

  public EpochOrderingProcessor(final long timeoutMs) {
    this(timeoutMs, DEFAULT_MAX_BUFFER_BYTES);
  }

  /**
   * @param timeoutMs sentinel timeout; buffered messages are force-released after this duration
   * @param maxBufferBytes maximum estimated bytes buffered per region before force-release.
   *     Defaults to 64 MB.
   */
  public EpochOrderingProcessor(final long timeoutMs, final long maxBufferBytes) {
    this.timeoutMs = timeoutMs;
    this.maxBufferBytes = maxBufferBytes;
  }

  @Override
  public List<SubscriptionMessage> process(final List<SubscriptionMessage> messages) {
    final List<SubscriptionMessage> output = new ArrayList<>();

    for (final SubscriptionMessage message : messages) {
      final SubscriptionCommitContext ctx = message.getCommitContext();
      final String regionId = ctx.getRegionId();

      // Non-consensus messages (empty regionId) pass through
      if (regionId == null || regionId.isEmpty()) {
        output.add(message);
        continue;
      }

      // WATERMARK events bypass epoch ordering — always pass through immediately
      if (message.getMessageType() == SubscriptionMessageType.WATERMARK.getType()) {
        output.add(message);
        continue;
      }

      final RegionTracker tracker =
          regionTrackers.computeIfAbsent(regionId, k -> new RegionTracker());

      if (message.getMessageType() == SubscriptionMessageType.EPOCH_SENTINEL.getType()) {
        handleSentinel(tracker, message, regionId, output);
        continue;
      }

      handleNormalMessage(tracker, message, regionId, output);
    }

    // Check timeouts for buffering regions
    checkTimeouts(output);

    return output;
  }

  private void handleSentinel(
      final RegionTracker tracker,
      final SubscriptionMessage sentinel,
      final String regionId,
      final List<SubscriptionMessage> output) {
    final long sentinelEpoch = sentinel.getCommitContext().getEpoch();

    if (tracker.state == RegionState.BUFFERING && sentinelEpoch == tracker.currentEpoch) {
      // The sentinel confirms currentEpoch is complete → release all buffer, reset to INITIAL
      LOGGER.info(
          "EpochOrderingProcessor: sentinel for region {}, epoch={}, releasing {} buffered messages",
          regionId,
          sentinelEpoch,
          tracker.buffer.size());
      output.addAll(tracker.buffer);
      tracker.buffer.clear();
      tracker.bufferedBytes = 0;
      tracker.state = RegionState.INITIAL;
      tracker.sentinelSeen = false;
    } else if (tracker.state == RegionState.STABLE && sentinelEpoch == tracker.currentEpoch) {
      // Sentinel arrived before any new-epoch message; remember it so that the next different-
      // epoch message can be accepted immediately instead of entering BUFFERING.
      tracker.sentinelSeen = true;
      LOGGER.info(
          "EpochOrderingProcessor: sentinel for region {}, epoch={} in STABLE state, marked sentinelSeen",
          regionId,
          sentinelEpoch);
    } else {
      LOGGER.debug(
          "EpochOrderingProcessor: sentinel for region {}, epoch={}, state={}, currentEpoch={} (no-op)",
          regionId,
          sentinelEpoch,
          tracker.state,
          tracker.currentEpoch);
    }

    // Pass sentinel through (will be stripped downstream)
    output.add(sentinel);
  }

  private void handleNormalMessage(
      final RegionTracker tracker,
      final SubscriptionMessage message,
      final String regionId,
      final List<SubscriptionMessage> output) {
    final long msgEpoch = message.getCommitContext().getEpoch();

    switch (tracker.state) {
      case INITIAL:
        // First message for this region (or after sentinel reset): accept and enter STABLE
        tracker.currentEpoch = msgEpoch;
        tracker.currentEpochDataNodeId = message.getCommitContext().getDataNodeId();
        tracker.state = RegionState.STABLE;
        output.add(message);
        break;

      case STABLE:
        if (msgEpoch == tracker.currentEpoch) {
          output.add(message);
        } else if (tracker.sentinelSeen) {
          // Sentinel for currentEpoch already arrived → old epoch is confirmed complete.
          // Accept this new-epoch message directly instead of entering BUFFERING.
          LOGGER.info(
              "EpochOrderingProcessor: region {} epoch {} -> {} with sentinelSeen, skipping BUFFERING",
              regionId,
              tracker.currentEpoch,
              msgEpoch);
          tracker.currentEpoch = msgEpoch;
          tracker.currentEpochDataNodeId = message.getCommitContext().getDataNodeId();
          tracker.sentinelSeen = false;
          output.add(message);
        } else if (message.getCommitContext().getDataNodeId() == tracker.currentEpochDataNodeId) {
          // Same DataNode changed epoch internally (e.g., routing update race where writes
          // arrive before onRegionRouteChanged sets the new epoch). No cross-node ordering
          // is needed — data from the same node is already ordered by commitId.
          LOGGER.info(
              "EpochOrderingProcessor: region {} same-node epoch update ({} -> {}, dataNodeId={}), staying STABLE",
              regionId,
              tracker.currentEpoch,
              msgEpoch,
              tracker.currentEpochDataNodeId);
          tracker.currentEpoch = msgEpoch;
          output.add(message);
        } else {
          // Different DataNode with different epoch → real leader transition, enter BUFFERING
          tracker.state = RegionState.BUFFERING;
          tracker.buffer.add(message);
          tracker.bufferedBytes = message.estimateSize();
          tracker.bufferStartTimeMs = System.currentTimeMillis();
          LOGGER.info(
              "EpochOrderingProcessor: region {} epoch change detected ({} -> {}, dataNodeId {} -> {}), entering BUFFERING",
              regionId,
              tracker.currentEpoch,
              msgEpoch,
              tracker.currentEpochDataNodeId,
              message.getCommitContext().getDataNodeId());
        }
        break;

      case BUFFERING:
        if (msgEpoch == tracker.currentEpoch) {
          // Same as current epoch → pass through (old leader's remaining messages)
          output.add(message);
        } else {
          // Different epoch → buffer
          tracker.buffer.add(message);
          tracker.bufferedBytes += message.estimateSize();
          if (tracker.bufferedBytes > maxBufferBytes) {
            LOGGER.warn(
                "EpochOrderingProcessor: buffer overflow ({} bytes) for region {}, force-releasing",
                tracker.bufferedBytes,
                regionId);
            output.addAll(tracker.buffer);
            tracker.buffer.clear();
            tracker.bufferedBytes = 0;
            tracker.state = RegionState.INITIAL;
            tracker.sentinelSeen = false;
          }
        }
        break;
    }
  }

  @Override
  public List<SubscriptionMessage> flush() {
    final List<SubscriptionMessage> result = new ArrayList<>();
    for (final RegionTracker tracker : regionTrackers.values()) {
      result.addAll(tracker.buffer);
      tracker.buffer.clear();
      tracker.bufferedBytes = 0;
      tracker.state = RegionState.INITIAL;
    }
    return result;
  }

  @Override
  public int getBufferedCount() {
    int count = 0;
    for (final RegionTracker tracker : regionTrackers.values()) {
      count += tracker.buffer.size();
    }
    return count;
  }

  /**
   * Release buffered messages for any region whose currentEpoch was produced by the specified
   * DataNode. Called when the consumer detects that a DataNode has become unavailable, meaning the
   * sentinel from that node will never arrive.
   *
   * @param dataNodeId the ID of the unavailable DataNode
   * @return released messages that should be delivered to the user
   */
  public List<SubscriptionMessage> releaseBufferedForDataNode(final int dataNodeId) {
    final List<SubscriptionMessage> released = new ArrayList<>();
    for (final Map.Entry<String, RegionTracker> entry : regionTrackers.entrySet()) {
      final RegionTracker tracker = entry.getValue();
      if (tracker.state == RegionState.BUFFERING
          && tracker.currentEpochDataNodeId == dataNodeId
          && !tracker.buffer.isEmpty()) {
        LOGGER.info(
            "EpochOrderingProcessor: DataNode {} unavailable, force-releasing {} buffered messages for region {}",
            dataNodeId,
            tracker.buffer.size(),
            entry.getKey());
        released.addAll(tracker.buffer);
        tracker.buffer.clear();
        tracker.bufferedBytes = 0;
        tracker.state = RegionState.INITIAL;
        tracker.sentinelSeen = false;
      }
    }
    return released;
  }

  /**
   * Release buffered messages for any region whose currentEpoch DataNode is NOT in the given set of
   * available DataNode IDs. Appends released messages to the output list.
   *
   * @param availableDataNodeIds set of currently available DataNode IDs
   * @param output list to append released messages to
   */
  public void releaseBufferedForUnavailableNodes(
      final Set<Integer> availableDataNodeIds, final List<SubscriptionMessage> output) {
    for (final Map.Entry<String, RegionTracker> entry : regionTrackers.entrySet()) {
      final RegionTracker tracker = entry.getValue();
      if (tracker.state == RegionState.BUFFERING
          && tracker.currentEpochDataNodeId >= 0
          && !availableDataNodeIds.contains(tracker.currentEpochDataNodeId)
          && !tracker.buffer.isEmpty()) {
        LOGGER.info(
            "EpochOrderingProcessor: DataNode {} unavailable, force-releasing {} buffered messages for region {}",
            tracker.currentEpochDataNodeId,
            tracker.buffer.size(),
            entry.getKey());
        output.addAll(tracker.buffer);
        tracker.buffer.clear();
        tracker.bufferedBytes = 0;
        tracker.state = RegionState.INITIAL;
        tracker.sentinelSeen = false;
      }
    }
  }

  private void checkTimeouts(final List<SubscriptionMessage> output) {
    if (timeoutMs <= 0) {
      return;
    }
    final long now = System.currentTimeMillis();
    for (final Map.Entry<String, RegionTracker> entry : regionTrackers.entrySet()) {
      final RegionTracker tracker = entry.getValue();
      if (tracker.state == RegionState.BUFFERING
          && !tracker.buffer.isEmpty()
          && now - tracker.bufferStartTimeMs >= timeoutMs) {
        LOGGER.warn(
            "EpochOrderingProcessor: timeout ({}ms) for region {}, force-releasing {} buffered messages",
            timeoutMs,
            entry.getKey(),
            tracker.buffer.size());
        output.addAll(tracker.buffer);
        tracker.buffer.clear();
        tracker.bufferedBytes = 0;
        tracker.state = RegionState.INITIAL;
      }
    }
  }
}
