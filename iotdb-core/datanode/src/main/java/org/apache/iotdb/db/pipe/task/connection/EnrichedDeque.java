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

import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class EnrichedDeque<E extends Event> {

  private final AtomicInteger tabletInsertionEventCount;
  private final AtomicInteger tsFileInsertionEventCount;
  protected final Deque<E> deque;

  protected EnrichedDeque(Deque<E> deque) {
    this.deque = deque;
    tabletInsertionEventCount = new AtomicInteger(0);
    tsFileInsertionEventCount = new AtomicInteger(0);
  }

  public boolean offer(E event) {
    final boolean offered = deque.offer(event);
    if (offered) {
      if (event instanceof TabletInsertionEvent) {
        tabletInsertionEventCount.incrementAndGet();
      } else if (event instanceof TsFileInsertionEvent) {
        tsFileInsertionEventCount.incrementAndGet();
      }
    }
    return offered;
  }

  public E poll() {
    final E event = deque.poll();
    if (event instanceof TabletInsertionEvent) {
      tabletInsertionEventCount.decrementAndGet();
    }
    if (event instanceof TsFileInsertionEvent) {
      tsFileInsertionEventCount.decrementAndGet();
    }
    return event;
  }

  public void clear() {
    deque.clear();
    tabletInsertionEventCount.set(0);
    tsFileInsertionEventCount.set(0);
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
    return tabletInsertionEventCount.get();
  }

  public int getTsFileInsertionEventCount() {
    return tsFileInsertionEventCount.get();
  }
}
