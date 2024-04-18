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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class SerializedEnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializedEnrichedEvent.class);

  private static final long INVALID_TIMESTAMP = -1;

  private final EnrichedTablets enrichedTablets;
  private final List<EnrichedEvent> enrichedEvents;

  private long lastPolledTimestamp;
  private long committedTimestamp;

  private ByteBuffer byteBuffer; // serialized EnrichedTablets

  public SerializedEnrichedEvent(
      final EnrichedTablets enrichedTablets, final List<EnrichedEvent> enrichedEvents) {
    this.enrichedTablets = enrichedTablets;
    this.enrichedEvents = enrichedEvents;
    this.lastPolledTimestamp = INVALID_TIMESTAMP;
    this.committedTimestamp = INVALID_TIMESTAMP;
  }

  //////////////////////////// serialization ////////////////////////////

  public EnrichedTablets getEnrichedTablets() {
    return enrichedTablets;
  }

  /** @return true -> byte buffer is not null */
  public boolean serialize() {
    if (Objects.isNull(byteBuffer)) {
      try {
        byteBuffer = PipeSubscribePollResp.serializeEnrichedTablets(enrichedTablets);
        return true;
      } catch (final IOException e) {
        LOGGER.warn(
            "Subscription: something unexpected happened when serializing EnrichedTablets {}, exception is {}",
            byteBuffer,
            e.getMessage());
      }
      return false;
    }
    return true;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public void resetByteBuffer() {
    // maybe friendly for gc
    byteBuffer = null;
  }

  //////////////////////////// commit ////////////////////////////

  public String getSubscriptionCommitId() {
    return enrichedTablets.getSubscriptionCommitId();
  }

  public void decreaseReferenceCount() {
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(SerializedEnrichedEvent.class.getName(), true);
    }
  }

  public void recordCommittedTimestamp() {
    committedTimestamp = System.currentTimeMillis();
  }

  public boolean isCommitted() {
    return committedTimestamp != INVALID_TIMESTAMP;
  }

  //////////////////////////// pollable ////////////////////////////

  public void recordLastPolledTimestamp() {
    lastPolledTimestamp = Math.max(lastPolledTimestamp, System.currentTimeMillis());
  }

  public boolean pollable() {
    if (lastPolledTimestamp == INVALID_TIMESTAMP) {
      return true;
    }
    // Recycle events that may not be able to be committed, i.e., those that have been polled but
    // not committed within a certain period of time.
    return System.currentTimeMillis() - lastPolledTimestamp
        > SubscriptionConfig.getInstance().getSubscriptionRecycleUncommittedEventIntervalMs();
  }
}
