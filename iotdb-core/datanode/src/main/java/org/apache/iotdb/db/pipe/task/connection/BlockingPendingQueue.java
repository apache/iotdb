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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public abstract class BlockingPendingQueue<E extends Event> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingPendingQueue.class);

  private static final long MAX_BLOCKING_TIME_MS =
      PipeConfig.getInstance().getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();

  private final AtomicInteger tabletInsertionEventCount;
  private final AtomicInteger tsFileInsertionEventCount;
  protected final BlockingQueue<E> pendingQueue;

  protected BlockingPendingQueue(BlockingQueue<E> pendingQueue) {
    this.pendingQueue = pendingQueue;
    tabletInsertionEventCount = new AtomicInteger(0);
    tsFileInsertionEventCount = new AtomicInteger(0);
  }

  public boolean waitedOffer(E event) {
    try {
      final boolean offered =
          pendingQueue.offer(event, MAX_BLOCKING_TIME_MS, TimeUnit.MILLISECONDS);
      if (offered) {
        if (event instanceof TabletInsertionEvent) {
          tabletInsertionEventCount.incrementAndGet();
        } else if (event instanceof TsFileInsertionEvent) {
          tsFileInsertionEventCount.incrementAndGet();
        }
      }
      return offered;
    } catch (InterruptedException e) {
      LOGGER.info("pending queue offer is interrupted.", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public boolean directOffer(E event) {
    final boolean offered = pendingQueue.offer(event);
    if (offered) {
      if (event instanceof TabletInsertionEvent) {
        tabletInsertionEventCount.incrementAndGet();
      } else if (event instanceof TsFileInsertionEvent) {
        tsFileInsertionEventCount.incrementAndGet();
      }
    }
    return offered;
  }

  public boolean put(E event) {
    try {
      pendingQueue.put(event);
      if (event instanceof TabletInsertionEvent) {
        tabletInsertionEventCount.incrementAndGet();
      } else if (event instanceof TsFileInsertionEvent) {
        tsFileInsertionEventCount.incrementAndGet();
      }
      return true;
    } catch (InterruptedException e) {
      LOGGER.info("pending queue put is interrupted.", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public E directPoll() {
    final E event = pendingQueue.poll();
    if (event instanceof TabletInsertionEvent) {
      tabletInsertionEventCount.decrementAndGet();
    }
    if (event instanceof TsFileInsertionEvent) {
      tsFileInsertionEventCount.decrementAndGet();
    }
    return event;
  }

  public E waitedPoll() {
    E event = null;
    try {
      event = pendingQueue.poll(MAX_BLOCKING_TIME_MS, TimeUnit.MILLISECONDS);
      if (event instanceof TabletInsertionEvent) {
        tabletInsertionEventCount.decrementAndGet();
      } else if (event instanceof TsFileInsertionEvent) {
        tsFileInsertionEventCount.decrementAndGet();
      }
    } catch (InterruptedException e) {
      LOGGER.info("pending queue poll is interrupted.", e);
      Thread.currentThread().interrupt();
    }
    return event;
  }

  public void clear() {
    pendingQueue.clear();
    tabletInsertionEventCount.set(0);
    tsFileInsertionEventCount.set(0);
  }

  public void forEach(Consumer<? super E> action) {
    pendingQueue.forEach(action);
  }

  public boolean isEmpty() {
    return pendingQueue.isEmpty();
  }

  public int size() {
    return pendingQueue.size();
  }

  public int getTabletInsertionEventCount() {
    return tabletInsertionEventCount.get();
  }

  public int getTsFileInsertionEventCount() {
    return tsFileInsertionEventCount.get();
  }
}
