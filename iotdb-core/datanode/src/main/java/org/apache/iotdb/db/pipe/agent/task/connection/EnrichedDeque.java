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

package org.apache.iotdb.db.pipe.agent.task.connection;

import org.apache.iotdb.db.pipe.metric.PipeDataRegionEventCounter;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.Deque;
import java.util.function.Consumer;

public class EnrichedDeque<E extends Event> {

  private final PipeDataRegionEventCounter eventCounter = new PipeDataRegionEventCounter();
  protected final Deque<E> deque;

  protected EnrichedDeque(Deque<E> deque) {
    this.deque = deque;
  }

  public boolean offer(E event) {
    final boolean offered = deque.offer(event);
    if (offered) {
      eventCounter.increaseEventCount(event);
    }
    return offered;
  }

  public E poll() {
    final E event = deque.poll();
    eventCounter.decreaseEventCount(event);
    return event;
  }

  public void clear() {
    deque.clear();
    eventCounter.reset();
  }

  public int size() {
    return deque.size();
  }

  public void forEach(Consumer<? super E> action) {
    deque.forEach(action);
  }

  public E peek() {
    return deque.peek();
  }

  public E peekLast() {
    return deque.peekLast();
  }

  public boolean isEmpty() {
    return deque.isEmpty();
  }

  public int getTabletInsertionEventCount() {
    return eventCounter.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return eventCounter.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return eventCounter.getPipeHeartbeatEventCount();
  }
}
