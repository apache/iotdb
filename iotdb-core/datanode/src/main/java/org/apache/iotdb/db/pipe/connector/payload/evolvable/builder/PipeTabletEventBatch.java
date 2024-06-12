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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.builder;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.exception.write.WriteProcessException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class PipeTabletEventBatch implements AutoCloseable {
  private final List<Event> events = new ArrayList<>();
  private final List<Long> requestCommitIds = new ArrayList<>();
  private final int maxDelayInMs;
  private long firstEventProcessingTime = Long.MIN_VALUE;

  PipeTabletEventBatch(final int maxDelayInMs) {
    this.maxDelayInMs = maxDelayInMs;
  }

  /**
   * Try offer {@link Event} into batch if the given {@link Event} is not duplicated.
   *
   * @param event the given {@link Event}
   * @return {@code true} if the batch can be transferred
   */
  synchronized boolean onEvent(final TabletInsertionEvent event)
      throws WALPipeException, IOException, WriteProcessException {
    if (!(event instanceof EnrichedEvent)) {
      return false;
    }

    final long requestCommitId = ((EnrichedEvent) event).getCommitId();

    // The deduplication logic here is to avoid the accumulation of the same event in a batch when
    // retrying.
    if ((events.isEmpty() || !events.get(events.size() - 1).equals(event))) {
      // We increase the reference count for this event to determine if the event may be released.
      if (((EnrichedEvent) event)
          .increaseReferenceCount(PipeTransferBatchReqBuilder.class.getName())) {
        events.add(event);
        requestCommitIds.add(requestCommitId);

        constructBatch(event);

        if (firstEventProcessingTime == Long.MIN_VALUE) {
          firstEventProcessingTime = System.currentTimeMillis();
        }
      } else {
        ((EnrichedEvent) event)
            .decreaseReferenceCount(PipeTransferBatchReqBuilder.class.getName(), false);
      }
    }

    return getTotalSize() >= getMaxBatchSizeInBytes()
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  protected abstract void constructBatch(final TabletInsertionEvent event)
      throws WALPipeException, IOException, WriteProcessException;

  public synchronized void onSuccess() throws IOException {
    events.clear();
    requestCommitIds.clear();
    firstEventProcessingTime = Long.MIN_VALUE;
  }

  boolean isEmpty() {
    return events.isEmpty();
  }

  public List<Event> deepCopyEvents() {
    return new ArrayList<>(events);
  }

  public List<Long> deepCopyRequestCommitIds() {
    return new ArrayList<>(requestCommitIds);
  }

  protected abstract long getTotalSize();

  protected abstract long getMaxBatchSizeInBytes();

  @Override
  public synchronized void close() {
    clearEventsReferenceCount(PipeTransferBatchReqBuilder.class.getName());
  }

  public void decreaseEventsReferenceCount(final String holderMessage, final boolean shouldReport) {
    for (final Event event : events) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).decreaseReferenceCount(holderMessage, shouldReport);
      }
    }
  }

  private void clearEventsReferenceCount(final String holderMessage) {
    for (final Event event : events) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(holderMessage);
      }
    }
  }
}
