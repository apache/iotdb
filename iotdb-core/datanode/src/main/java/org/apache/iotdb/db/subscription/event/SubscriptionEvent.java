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

package org.apache.iotdb.db.subscription.event;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SubscriptionEvent {

  private static final long INVALID_TIMESTAMP = -1;

  protected final List<EnrichedEvent> enrichedEvents;
  protected final SubscriptionPollResponse response;

  private String lastPolledConsumerId;
  private long lastPolledTimestamp;
  private long committedTimestamp;

  protected ByteBuffer byteBuffer; // serialized SubscriptionPollResponse

  public SubscriptionEvent(
      final List<EnrichedEvent> enrichedEvents, final SubscriptionPollResponse response) {
    this.enrichedEvents = enrichedEvents;
    this.response = response;

    this.lastPolledConsumerId = null;
    this.lastPolledTimestamp = INVALID_TIMESTAMP;
    this.committedTimestamp = INVALID_TIMESTAMP;
  }

  public List<EnrichedEvent> getEnrichedEvents() {
    return enrichedEvents;
  }

  public SubscriptionPollResponse getResponse() {
    return response;
  }

  //////////////////////////// commit ////////////////////////////

  public void recordCommittedTimestamp() {
    committedTimestamp = System.currentTimeMillis();
  }

  public boolean isCommitted() {
    return committedTimestamp != INVALID_TIMESTAMP;
  }

  public void decreaseReferenceCount() {
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  //////////////////////////// pollable ////////////////////////////

  public void recordLastPolledTimestamp() {
    lastPolledTimestamp = Math.max(lastPolledTimestamp, System.currentTimeMillis());
  }

  public boolean pollable() {
    if (isCommitted()) {
      return false;
    }
    if (lastPolledTimestamp == INVALID_TIMESTAMP) {
      return true;
    }
    // Recycle events that may not be able to be committed, i.e., those that have been polled but
    // not committed within a certain period of time.
    return System.currentTimeMillis() - lastPolledTimestamp
        > SubscriptionConfig.getInstance().getSubscriptionRecycleUncommittedEventIntervalMs();
  }

  public void nack() {
    lastPolledConsumerId = null;
    lastPolledTimestamp = INVALID_TIMESTAMP;
  }

  public void recordLastPolledConsumerId(final String consumerId) {
    lastPolledConsumerId = consumerId;
  }

  public String getLastPolledConsumerId() {
    return lastPolledConsumerId;
  }

  //////////////////////////// byte buffer ////////////////////////////

  public ByteBuffer serialize() throws IOException {
    if (Objects.nonNull(byteBuffer)) {
      return byteBuffer;
    }
    return SubscriptionPollResponse.serialize(response);
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public void resetByteBuffer(final boolean recursive) {
    // maybe friendly for gc
    byteBuffer = null;
  }

  /////////////////////////////// object ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionEvent{enrichedEvents="
        + enrichedEvents.stream().map(EnrichedEvent::coreReportMessage).collect(Collectors.toList())
        + ", response="
        + response
        + ", lastPolledConsumerId="
        + lastPolledConsumerId
        + ", lastPolledTimestamp="
        + lastPolledTimestamp
        + ", committedTimestamp="
        + committedTimestamp
        + "}"
        + "(response event byte buffer size: "
        + (Objects.nonNull(byteBuffer) ? byteBuffer.limit() : "<unknown>")
        + ")";
  }
}
