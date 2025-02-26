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
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletIterationSnapshot;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SubscriptionPipeTabletBatchEvents implements SubscriptionPipeEvents {

  private final SubscriptionPipeTabletEventBatch batch;
  private volatile SubscriptionPipeTabletIterationSnapshot iterationSnapshot;

  public SubscriptionPipeTabletBatchEvents(final SubscriptionPipeTabletEventBatch batch) {
    this.batch = batch;
  }

  public void receiveIterationSnapshot(
      final SubscriptionPipeTabletIterationSnapshot iterationSnapshot) {
    this.iterationSnapshot = iterationSnapshot;
  }

  @Override
  public void ack() {
    batch.ack();
    iterationSnapshot.clear(true);
  }

  @Override
  public void cleanUp(final boolean force) {
    batch.cleanUp(force);
    iterationSnapshot.clear(false);
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("batch", batch)
        .add(
            "events",
            Objects.nonNull(iterationSnapshot)
                ? formatEnrichedEvents(iterationSnapshot.getIteratedEnrichedEvents(), 4)
                : "<unknown>")
        .toString();
  }

  private static String formatEnrichedEvents(
      final List<EnrichedEvent> enrichedEvents, final int threshold) {
    if (Objects.isNull(enrichedEvents)) {
      return "[]";
    }
    final List<String> eventMessageList =
        enrichedEvents.stream()
            .limit(threshold)
            .map(EnrichedEvent::coreReportMessage)
            .collect(Collectors.toList());
    if (enrichedEvents.size() > threshold) {
      eventMessageList.add(
          String.format("omit the remaining %s event(s)...", enrichedEvents.size() - threshold));
    }
    return eventMessageList.toString();
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  @Override
  public int getPipeEventCount() {
    return Objects.nonNull(iterationSnapshot)
        ? iterationSnapshot.getIteratedEnrichedEvents().size()
        : 0;
  }
}
