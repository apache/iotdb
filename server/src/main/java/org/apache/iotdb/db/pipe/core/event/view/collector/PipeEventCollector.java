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

package org.apache.iotdb.db.pipe.core.event.view.collector;

import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.task.queue.ListenableBoundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.LinkedList;
import java.util.Queue;

public class PipeEventCollector implements EventCollector {

  private final ListenableBoundedBlockingPendingQueue<Event> pendingQueue;

  // buffer queue is used to store events that are not offered to pending queue
  // because the pending queue is full. when pending queue is full, pending queue
  // will notify tasks to stop collecting events, and buffer queue will be used to store
  // events before tasks are stopped. when pending queue is not full and tasks are
  // notified by the pending queue to start collecting events, buffer queue will be used to store
  // events before events in buffer queue are offered to pending queue.
  private final Queue<Event> bufferQueue;

  public PipeEventCollector(ListenableBoundedBlockingPendingQueue<Event> pendingQueue) {
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
      if (pendingQueue.offer(bufferedEvent)) {
        bufferQueue.poll();
      } else {
        bufferQueue.offer(event);
        return;
      }
    }

    if (!pendingQueue.offer(event)) {
      bufferQueue.offer(event);
    }
  }

  public synchronized void tryCollectBufferedEvents() {
    while (!bufferQueue.isEmpty()) {
      final Event bufferedEvent = bufferQueue.peek();
      if (pendingQueue.offer(bufferedEvent)) {
        bufferQueue.poll();
      } else {
        return;
      }
    }
  }
}
