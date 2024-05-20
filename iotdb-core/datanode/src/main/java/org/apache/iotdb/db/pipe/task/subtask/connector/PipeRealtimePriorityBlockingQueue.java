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

package org.apache.iotdb.db.pipe.task.subtask.connector;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.task.connection.PipeEventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class PipeRealtimePriorityBlockingQueue extends UnboundedBlockingPendingQueue<Event> {

  private final BlockingDeque<TsFileInsertionEvent> batchEventDeque = new LinkedBlockingDeque<>();

  public PipeRealtimePriorityBlockingQueue() {
    super(new PipeDataRegionEventCounter());
  }

  // Realtime priority blocking queue must be unbounded
  // Thus we explicitly use directOffer
  @Override
  public boolean directOffer(final Event event) {
    if (event instanceof TsFileInsertionEvent) {
      batchEventDeque.add((TsFileInsertionEvent) event);
    } else {
      if (event instanceof PipeHeartbeatEvent && super.peekLast() instanceof PipeHeartbeatEvent) {
        // We can NOT keep too many PipeHeartbeatEvent in bufferQueue because they may cause OOM.
        ((EnrichedEvent) event).decreaseReferenceCount(PipeEventCollector.class.getName(), false);
      } else {
        // Heartbeat events are put here to record the latency
        super.directOffer(event);
      }
    }
    return true;
  }

  @Override
  public Event waitedPoll() {
    Event event = null;
    // Try to get the event directly
    if (!super.isEmpty()) {
      // Insert is always:
      // Sequential: To ensure the write performance
      // First: Prior to the historical tsFiles
      event = super.directPoll();
    } else if (!batchEventDeque.isEmpty()) {
      // Always poll the last tsfile
      event = batchEventDeque.pollLast();
    }
    // If empty, block until stream event comes
    if (Objects.isNull(event)) {
      event = super.waitedPoll();
      if (Objects.isNull(event)) {
        event = batchEventDeque.pollLast();
      }
    }
    return event;
  }

  @Override
  public void clear() {
    super.clear();
    batchEventDeque.clear();
  }

  @Override
  public void forEach(final Consumer<? super Event> action) {
    super.forEach(action);
    batchEventDeque.forEach(action);
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty() && batchEventDeque.isEmpty();
  }

  @Override
  public int size() {
    return super.size() + batchEventDeque.size();
  }

  @Override
  public int getTsFileInsertionEventCount() {
    return batchEventDeque.size();
  }
}
