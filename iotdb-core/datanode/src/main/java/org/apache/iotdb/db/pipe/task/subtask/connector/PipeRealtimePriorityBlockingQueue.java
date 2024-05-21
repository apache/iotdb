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

  private final BlockingDeque<TsFileInsertionEvent> tsfileInsertEventDeque =
      new LinkedBlockingDeque<>();

  public PipeRealtimePriorityBlockingQueue() {
    super(new PipeDataRegionEventCounter());
  }

  @Override
  public boolean directOffer(final Event event) {
    if (event instanceof TsFileInsertionEvent) {
      tsfileInsertEventDeque.add((TsFileInsertionEvent) event);
      return true;
    }

    if (event instanceof PipeHeartbeatEvent && super.peekLast() instanceof PipeHeartbeatEvent) {
      // We can NOT keep too many PipeHeartbeatEvent in bufferQueue because they may cause OOM.
      ((EnrichedEvent) event).decreaseReferenceCount(PipeEventCollector.class.getName(), false);
    } else {
      super.directOffer(event);
    }
    return true;
  }

  @Override
  public boolean waitedOffer(final Event event) {
    return directOffer(event);
  }

  @Override
  public boolean put(final Event event) {
    directOffer(event);
    return true;
  }

  @Override
  public Event directPoll() {
    Event event = super.directPoll();
    if (Objects.isNull(event)) {
      event = tsfileInsertEventDeque.pollLast();
    }
    return event;
  }

  /**
   * Try to poll the freshest insertion event from the queue. First, try to poll the first offered
   * non-TsFileInsertionEvent. If no such event is available, poll the last offered
   * TsFileInsertionEvent. If no event is available, block until an event is available.
   *
   * @return the freshest insertion event. can be null if no event is available.
   */
  @Override
  public Event waitedPoll() {
    Event event = null;

    if (!super.isEmpty()) {
      // Sequentially poll the first offered non-TsFileInsertionEvent
      event = super.directPoll();
    } else if (!tsfileInsertEventDeque.isEmpty()) {
      // Always poll the last offered event
      event = tsfileInsertEventDeque.pollLast();
    }

    // If no event is available, block until an event is available
    if (Objects.isNull(event)) {
      event = super.waitedPoll();
      if (Objects.isNull(event)) {
        event = tsfileInsertEventDeque.pollLast();
      }
    }

    return event;
  }

  @Override
  public void clear() {
    super.clear();
    tsfileInsertEventDeque.clear();
  }

  @Override
  public void forEach(final Consumer<? super Event> action) {
    super.forEach(action);
    tsfileInsertEventDeque.forEach(action);
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty() && tsfileInsertEventDeque.isEmpty();
  }

  @Override
  public int size() {
    return super.size() + tsfileInsertEventDeque.size();
  }

  @Override
  public int getTsFileInsertionEventCount() {
    return tsfileInsertEventDeque.size();
  }
}
