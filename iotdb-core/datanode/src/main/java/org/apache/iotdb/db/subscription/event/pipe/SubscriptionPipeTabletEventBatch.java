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

package org.apache.iotdb.db.subscription.event.pipe;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class SubscriptionPipeTabletEventBatch implements SubscriptionPipeEvents {

  private final List<EnrichedEvent> enrichedEvents;

  public SubscriptionPipeTabletEventBatch(final List<EnrichedEvent> enrichedEvents) {
    this.enrichedEvents = enrichedEvents;
  }

  @Override
  public File getTsFile() {
    return null;
  }

  @Override
  public void ack() {
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  @Override
  public void cleanup() {
    // clear the reference count of events
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.clearReferenceCount(this.getClass().getName());
    }
  }

  @Override
  public String toString() {
    return enrichedEvents.stream()
        .map(EnrichedEvent::coreReportMessage)
        .collect(Collectors.toList())
        .toString();
  }
}
