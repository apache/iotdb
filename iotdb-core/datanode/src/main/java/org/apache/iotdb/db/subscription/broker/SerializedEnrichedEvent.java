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
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class SerializedEnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializedEnrichedEvent.class);

  private final EnrichedTablets enrichedTablets;
  private final List<EnrichedEvent> enrichedEvents;
  private final long creationTime;

  private ByteBuffer byteBuffer; // serialized EnrichedTablets

  public SerializedEnrichedEvent(
      EnrichedTablets enrichedTablets, List<EnrichedEvent> enrichedEvents) {
    this.enrichedTablets = enrichedTablets;
    this.enrichedEvents = enrichedEvents;
    this.creationTime = System.currentTimeMillis();
  }

  /** @return true -> byte buffer is not null */
  public boolean serialize() {
    if (Objects.isNull(byteBuffer)) {
      try {
        byteBuffer = PipeSubscribePollResp.serializeEnrichedTablets(enrichedTablets);
        return true;
      } catch (IOException e) {
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

  public String getSubscriptionCommitId() {
    return enrichedTablets.getSubscriptionCommitId();
  }

  public void decreaseReferenceCount() {
    for (EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  public boolean maybeExpired() {
    return System.currentTimeMillis() - creationTime
        > SubscriptionConfig.getInstance().getSubscriptionUncommittedEventExpireSeconds();
  }
}
