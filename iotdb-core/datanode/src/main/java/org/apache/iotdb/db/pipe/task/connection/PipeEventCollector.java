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

package org.apache.iotdb.db.pipe.task.connection;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeEventCollector implements EventCollector, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCollector.class);

  private final BoundedBlockingPendingQueue<Event> pendingQueue;

  private final EnrichedDeque<Event> bufferQueue;

  private final long creationTime;

  private final int regionId;

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final AtomicInteger collectInvocationCount = new AtomicInteger(0);

  public PipeEventCollector(
      BoundedBlockingPendingQueue<Event> pendingQueue, long creationTime, int regionId) {
    this.pendingQueue = pendingQueue;
    this.creationTime = creationTime;
    this.regionId = regionId;
    bufferQueue = new EnrichedDeque<>(new LinkedList<>());
  }

  @Override
  public synchronized void collect(Event event) {
    try {
      if (event instanceof PipeInsertNodeTabletInsertionEvent) {
        parseAndCollectEvent((PipeInsertNodeTabletInsertionEvent) event);
      } else if (event instanceof PipeRawTabletInsertionEvent) {
        parseAndCollectEvent((PipeRawTabletInsertionEvent) event);
      } else if (event instanceof PipeTsFileInsertionEvent) {
        parseAndCollectEvent((PipeTsFileInsertionEvent) event);
      } else {
        collectEvent(event);
      }
    } catch (PipeException e) {
      throw e;
    } catch (Exception e) {
      throw new PipeException("Error occurred when collecting events from processor.", e);
    }
  }

  private void parseAndCollectEvent(PipeInsertNodeTabletInsertionEvent sourceEvent) {
    if (sourceEvent.shouldParseTimeOrPattern()) {
      for (PipeRawTabletInsertionEvent parsedEvent : sourceEvent.toRawTabletInsertionEvents()) {
        collectEvent(parsedEvent);
      }
    } else {
      collectEvent(sourceEvent);
    }
  }

  private void parseAndCollectEvent(PipeRawTabletInsertionEvent sourceEvent) {
    if (sourceEvent.shouldParseTimeOrPattern()) {
      final PipeRawTabletInsertionEvent parsedEvent = sourceEvent.parseEventWithPatternOrTime();
      if (!parsedEvent.hasNoNeedParsingAndIsEmpty()) {
        collectEvent(parsedEvent);
      }
    } else {
      collectEvent(sourceEvent);
    }
  }

  private void parseAndCollectEvent(PipeTsFileInsertionEvent sourceEvent) throws Exception {
    if (!sourceEvent.waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          sourceEvent.getTsFile());
      return;
    }

    if (!sourceEvent.shouldParseTimeOrPattern()) {
      collectEvent(sourceEvent);
      return;
    }

    try {
      for (final TabletInsertionEvent parsedEvent : sourceEvent.toTabletInsertionEvents()) {
        collectEvent(parsedEvent);
      }
    } finally {
      sourceEvent.close();
    }
  }

  private void collectEvent(Event event) {
    collectInvocationCount.incrementAndGet();

    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).increaseReferenceCount(PipeEventCollector.class.getName());

      // Assign a commit id for this event in order to report progress in order.
      PipeEventCommitManager.getInstance()
          .enrichWithCommitterKeyAndCommitId((EnrichedEvent) event, creationTime, regionId);
    }
    if (event instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event).recordBufferQueueSize(bufferQueue);
      ((PipeHeartbeatEvent) event).recordConnectorQueueSize(pendingQueue);
    }

    while (!isClosed.get() && !bufferQueue.isEmpty()) {
      final Event bufferedEvent = bufferQueue.peek();
      // Try to put already buffered events into pending queue, if pending queue is full, wait for
      // pending queue to be available with timeout.
      if (pendingQueue.waitedOffer(bufferedEvent)) {
        bufferQueue.poll();
      } else {
        // We can NOT keep too many PipeHeartbeatEvent in bufferQueue because they may cause OOM.
        if (event instanceof PipeHeartbeatEvent
            && bufferQueue.peekLast() instanceof PipeHeartbeatEvent) {
          ((EnrichedEvent) event).decreaseReferenceCount(PipeEventCollector.class.getName(), false);
        } else {
          bufferQueue.offer(event);
        }
        return;
      }
    }

    if (!pendingQueue.waitedOffer(event)) {
      bufferQueue.offer(event);
    }
  }

  public void resetCollectInvocationCount() {
    collectInvocationCount.set(0);
  }

  public boolean hasNoCollectInvocationAfterReset() {
    return collectInvocationCount.get() == 0;
  }

  public boolean isBufferQueueEmpty() {
    return bufferQueue.isEmpty();
  }

  /**
   * Try to collect buffered events into {@link PipeEventCollector#pendingQueue}.
   *
   * @return {@code true} if there are still buffered events after this operation, {@code false}
   *     otherwise.
   */
  public synchronized boolean tryCollectBufferedEvents() {
    while (!isClosed.get() && !bufferQueue.isEmpty()) {
      final Event bufferedEvent = bufferQueue.peek();
      if (pendingQueue.waitedOffer(bufferedEvent)) {
        bufferQueue.poll();
      } else {
        return true;
      }
    }
    return false;
  }

  public void close() {
    isClosed.set(true);
    doClose();
  }

  private synchronized void doClose() {
    bufferQueue.forEach(
        event -> {
          if (event instanceof EnrichedEvent) {
            ((EnrichedEvent) event).clearReferenceCount(PipeEventCollector.class.getName());
          }
        });
    bufferQueue.clear();
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getTabletInsertionEventCount() {
    return bufferQueue.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return bufferQueue.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return bufferQueue.getPipeHeartbeatEventCount();
  }
}
