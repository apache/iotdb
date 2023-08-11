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

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public class PipeEventCollector implements EventCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCollector.class);

  private final BoundedBlockingPendingQueue<Event> pendingQueue;

  private final Queue<Event> bufferQueue;

  public PipeEventCollector(BoundedBlockingPendingQueue<Event> pendingQueue) {
    this.pendingQueue = pendingQueue;
    bufferQueue = new LinkedList<>();
  }

  @Override
  public synchronized void collect(Event event) {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).increaseReferenceCount(PipeEventCollector.class.getName());
    }

    while (!bufferQueue.isEmpty()) {
      final Event bufferedEvent = bufferQueue.peek();
      // Try to put already buffered events into pending queue, if pending queue is full, wait for
      // pending queue to be available with timeout.
      if (pendingQueue.waitedOffer(bufferedEvent)) {
        bufferQueue.poll();
      } else {
        // If timeout, we judge whether the new event is a PipeRawTabletInsertionEvent. If it is,
        // we wait for pending queue to be available without timeout until the pending queue is
        // available. We don't put PipeRawTabletInsertionEvent into buffer queue, because it is
        // memory consuming, holding too many PipeRawTabletInsertionEvent in buffer queue may cause
        // OOM.
        if (event instanceof PipeRawTabletInsertionEvent) {
          if (pendingQueue.put(bufferedEvent)) {
            bufferQueue.poll();
          } else {
            LOGGER.warn("interrupted when putting event into pending queue, event: {}", event);
            bufferQueue.offer(event);
            return;
          }
        } else {
          bufferQueue.offer(event);
          return;
        }
      }
    }

    if (!pendingQueue.waitedOffer(event)) {
      // PipeRawTabletInsertionEvent is memory consuming, so we should not put it into buffer queue
      // when pending queue is full. Otherwise, it may cause OOM.
      if (event instanceof PipeRawTabletInsertionEvent) {
        if (!pendingQueue.put(event)) {
          LOGGER.warn("interrupted when putting event into pending queue, event: {}", event);
          bufferQueue.offer(event);
        }
      } else {
        bufferQueue.offer(event);
      }
    }
  }
}
