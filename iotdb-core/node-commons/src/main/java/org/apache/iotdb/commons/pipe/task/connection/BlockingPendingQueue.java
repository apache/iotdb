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

package org.apache.iotdb.commons.pipe.task.connection;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.metric.PipeEventCounter;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class BlockingPendingQueue<E extends Event> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingPendingQueue.class);

  private static final long MAX_BLOCKING_TIME_MS =
      PipeConfig.getInstance().getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();

  protected final BlockingQueue<E> pendingQueue;

  private final PipeEventCounter eventCounter;

  protected BlockingPendingQueue(BlockingQueue<E> pendingQueue, PipeEventCounter eventCounter) {
    this.pendingQueue = pendingQueue;
    this.eventCounter = eventCounter;
  }

  public boolean waitedOffer(E event) {
    try {
      final boolean offered =
          pendingQueue.offer(event, MAX_BLOCKING_TIME_MS, TimeUnit.MILLISECONDS);
      if (offered) {
        eventCounter.increaseEventCount(event);
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
      eventCounter.increaseEventCount(event);
    }
    return offered;
  }

  public boolean put(E event) {
    try {
      pendingQueue.put(event);
      eventCounter.increaseEventCount(event);
      return true;
    } catch (InterruptedException e) {
      LOGGER.info("pending queue put is interrupted.", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public E directPoll() {
    final E event = pendingQueue.poll();
    eventCounter.decreaseEventCount(event);
    return event;
  }

  public E waitedPoll() {
    E event = null;
    try {
      event = pendingQueue.poll(MAX_BLOCKING_TIME_MS, TimeUnit.MILLISECONDS);
      eventCounter.decreaseEventCount(event);
    } catch (InterruptedException e) {
      LOGGER.info("pending queue poll is interrupted.", e);
      Thread.currentThread().interrupt();
    }
    return event;
  }

  public void clear() {
    pendingQueue.clear();
    eventCounter.reset();
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
    return eventCounter.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return eventCounter.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return eventCounter.getPipeHeartbeatEventCount();
  }
}
