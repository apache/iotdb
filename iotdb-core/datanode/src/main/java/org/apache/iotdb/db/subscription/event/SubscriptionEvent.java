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

import java.util.List;
import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;

public abstract class SubscriptionEvent {

  private static final long INVALID_TIMESTAMP = -1;

  private final List<EnrichedEvent> enrichedEvents;
  private final SubscriptionCommitContext commitContext;

  private long lastPolledTimestamp;
  private long committedTimestamp;

  public SubscriptionEvent(
      final List<EnrichedEvent> enrichedEvents, final SubscriptionCommitContext commitContext) {
    this.enrichedEvents = enrichedEvents;
    this.commitContext = commitContext;

    this.lastPolledTimestamp = INVALID_TIMESTAMP;
    this.committedTimestamp = INVALID_TIMESTAMP;
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
    if (lastPolledTimestamp == INVALID_TIMESTAMP) {
      return true;
    }
    // Recycle events that may not be able to be committed, i.e., those that have been polled but
    // not committed within a certain period of time.
    return System.currentTimeMillis() - lastPolledTimestamp
        > SubscriptionConfig.getInstance().getSubscriptionRecycleUncommittedEventIntervalMs();
  }
}
