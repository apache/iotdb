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
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class UnboundedBlockingPendingQueue<E extends Event> extends BlockingPendingQueue<E> {

  private final BlockingDeque<E> pendingDeque;

  private final AtomicInteger tsfileInsertionEventCount;

  public UnboundedBlockingPendingQueue() {
    super(new LinkedBlockingDeque<>());
    pendingDeque = (BlockingDeque<E>) pendingQueue;
    tsfileInsertionEventCount = new AtomicInteger(0);
  }

  @Override
  public boolean waitedOffer(E event) {
    final boolean offered = super.waitedOffer(event);
    if (offered && event instanceof TsFileInsertionEvent) {
      tsfileInsertionEventCount.incrementAndGet();
    }
    return offered;
  }

  @Override
  public boolean directOffer(E event) {
    final boolean offered = super.directOffer(event);
    if (offered && event instanceof TsFileInsertionEvent) {
      tsfileInsertionEventCount.incrementAndGet();
    }
    return offered;
  }

  @Override
  public boolean put(E event) {
    final boolean putSuccessfully = super.put(event);
    if (putSuccessfully && event instanceof TsFileInsertionEvent) {
      tsfileInsertionEventCount.incrementAndGet();
    }
    return putSuccessfully;
  }

  @Override
  public E directPoll() {
    final E event = super.directPoll();
    if (event instanceof TsFileInsertionEvent) {
      tsfileInsertionEventCount.decrementAndGet();
    }
    return event;
  }

  @Override
  public E waitedPoll() {
    final E event = super.waitedPoll();
    if (event instanceof TsFileInsertionEvent) {
      tsfileInsertionEventCount.decrementAndGet();
    }
    return event;
  }

  @Override
  public void clear() {
    super.clear();
    tsfileInsertionEventCount.set(0);
  }

  public E peekLast() {
    return pendingDeque.peekLast();
  }

  public int getTsfileInsertionEventCount() {
    return tsfileInsertionEventCount.get();
  }
}
