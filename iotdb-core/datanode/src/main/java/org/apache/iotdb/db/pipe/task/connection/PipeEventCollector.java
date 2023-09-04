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
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.Deque;
import java.util.LinkedList;

public class PipeEventCollector implements EventCollector {

  private final BoundedBlockingPendingQueue<Event> pendingQueue;

  private final Deque<Event> bufferQueue;

  public PipeEventCollector(BoundedBlockingPendingQueue<Event> pendingQueue) {
    this.pendingQueue = pendingQueue;
    bufferQueue = new LinkedList<>();
  }

  @Override
  public synchronized void collect(Event event) {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).increaseReferenceCount(PipeEventCollector.class.getName());
    }
    if (event instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event).recordBufferQueueSize(bufferQueue);
      ((PipeHeartbeatEvent) event).recordConnectorQueueSize(pendingQueue);
    }

    while (!bufferQueue.isEmpty()) {
      final Event bufferedEvent = bufferQueue.peek();
      // Try to put already buffered events into pending queue, if pending queue is full, wait for
      // pending queue to be available with timeout.
      if (pendingQueue.waitedOffer(bufferedEvent)) {
        bufferQueue.poll();
      } else {
        // We can NOT keep too many PipeHeartbeatEvent in bufferQueue because they may cause OOM.
        if (event instanceof PipeHeartbeatEvent
            && bufferQueue.peekLast() instanceof PipeHeartbeatEvent) {
          ((EnrichedEvent) event).decreaseReferenceCount(PipeEventCollector.class.getName());
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

  /**
   * Try to collect buffered events into pending queue.
   *
   * @return true if there are still buffered events after this operation, false otherwise.
   */
  public synchronized boolean tryCollectBufferedEvents() {
    while (!bufferQueue.isEmpty()) {
      final Event bufferedEvent = bufferQueue.peek();
      if (pendingQueue.waitedOffer(bufferedEvent)) {
        bufferQueue.poll();
      } else {
        return true;
      }
    }
    return false;
  }
}
