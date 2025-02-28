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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SubscriptionPipeTabletIterationSnapshot {

  private final List<EnrichedEvent> iteratedEnrichedEvents = new ArrayList<>();
  private final List<PipeRawTabletInsertionEvent> parsedEnrichedEvents = new ArrayList<>();

  public List<EnrichedEvent> getIteratedEnrichedEvents() {
    return Collections.unmodifiableList(iteratedEnrichedEvents);
  }

  public void addIteratedEnrichedEvent(final EnrichedEvent enrichedEvent) {
    iteratedEnrichedEvents.add(enrichedEvent);
  }

  public void addParsedEnrichedEvent(final PipeRawTabletInsertionEvent enrichedEvent) {
    parsedEnrichedEvents.add(enrichedEvent);
  }

  public void ack() {
    closeIteratedEnrichedEvents();

    for (final PipeRawTabletInsertionEvent event : parsedEnrichedEvents) {
      // decrease reference count in raw tablet event
      event.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  public void cleanUp() {
    closeIteratedEnrichedEvents();

    for (final PipeRawTabletInsertionEvent event : parsedEnrichedEvents) {
      // clear reference count in raw tablet event
      event.clearReferenceCount(this.getClass().getName());
    }
  }

  private void closeIteratedEnrichedEvents() {
    // TODO: unify close interface
    for (final EnrichedEvent enrichedEvent : iteratedEnrichedEvents) {
      // close data container in tsfile event
      if (enrichedEvent instanceof PipeTsFileInsertionEvent) {
        ((PipeTsFileInsertionEvent) enrichedEvent).close();
      }
      // close memory block in tablet event
      if (enrichedEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        ((PipeInsertNodeTabletInsertionEvent) enrichedEvent).close();
      }
      if (enrichedEvent instanceof PipeRawTabletInsertionEvent) {
        ((PipeRawTabletInsertionEvent) enrichedEvent).close();
      }
    }
  }
}
