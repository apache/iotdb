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

package org.apache.iotdb.commons.pipe.agent.task.connection;

import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.metric.PipeEventCounter;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class BlockingPendingQueue<E extends Event> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingPendingQueue.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  protected final BlockingQueue<E> pendingQueue;

  protected final PipeEventCounter eventCounter;

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected final Set<CommitterKey> droppedPipeTaskKeys = ConcurrentHashMap.newKeySet();

  protected BlockingPendingQueue(
      final BlockingQueue<E> pendingQueue, final PipeEventCounter eventCounter) {
    this.pendingQueue = pendingQueue;
    this.eventCounter = eventCounter;
  }

  public boolean offer(final E event) {
    if (!checkBeforeOffer(event)) {
      return false;
    }

    final boolean offered = pendingQueue.offer(event);
    if (offered) {
      eventCounter.increaseEventCount(event);
    }
    return offered;
  }

  public boolean put(final E event) {
    if (!checkBeforeOffer(event)) {
      return false;
    }
    try {
      pendingQueue.put(event);
      eventCounter.increaseEventCount(event);
      return true;
    } catch (final InterruptedException e) {
      LOGGER.info(PipeMessages.PENDING_QUEUE_PUT_INTERRUPTED, e);
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
      event =
          pendingQueue.poll(
              PIPE_CONFIG.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(),
              TimeUnit.MILLISECONDS);
      eventCounter.decreaseEventCount(event);
    } catch (final InterruptedException e) {
      LOGGER.info(PipeMessages.PENDING_QUEUE_POLL_INTERRUPTED, e);
      Thread.currentThread().interrupt();
    }
    return event;
  }

  public E peek() {
    return pendingQueue.peek();
  }

  public void clear() {
    isClosed.set(true);
    pendingQueue.clear();
    eventCounter.reset();
    droppedPipeTaskKeys.clear();
  }

  /** DO NOT FORGET to set eventCounter to new value after invoking this method. */
  public void forEach(final Consumer<? super E> action) {
    pendingQueue.forEach(action);
  }

  public void discardAllEvents() {
    isClosed.set(true);
    pendingQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent) {
            if (((EnrichedEvent) event).clearReferenceCount(BlockingPendingQueue.class.getName())) {
              eventCounter.decreaseEventCount(event);
            }
          }
          return true;
        });
    eventCounter.reset();
    droppedPipeTaskKeys.clear();
  }

  public void discardEventsOfPipe(
      final String pipeNameToDrop, final long creationTimeToDrop, final int regionId) {
    discardEventsOfPipe(new CommitterKey(pipeNameToDrop, creationTimeToDrop, regionId, -1));
  }

  public void discardEventsOfPipe(final CommitterKey committerKey) {
    droppedPipeTaskKeys.add(committerKey);
    pendingQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent && isEventFromPipe((EnrichedEvent) event, committerKey)) {
            if (((EnrichedEvent) event).clearReferenceCount(BlockingPendingQueue.class.getName())) {
              eventCounter.decreaseEventCount(event);
            }
            return true;
          }
          return false;
        });
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

  protected boolean checkBeforeOffer(final E event) {
    final boolean shouldReject = isClosed.get() || isEventFromDroppedPipe(event);
    if (shouldReject && event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).clearReferenceCount(BlockingPendingQueue.class.getName());
    }
    return !shouldReject;
  }

  protected static boolean isEventFromPipe(
      final EnrichedEvent event,
      final String pipeNameToDrop,
      final long creationTimeToDrop,
      final int regionId) {
    return pipeNameToDrop.equals(event.getPipeName())
        && creationTimeToDrop == event.getCreationTime()
        && regionId == event.getRegionId();
  }

  protected static boolean isEventFromPipe(
      final EnrichedEvent event, final CommitterKey committerKey) {
    return committerKey.getPipeName().equals(event.getPipeName())
        && committerKey.getCreationTime() == event.getCreationTime()
        && committerKey.getRegionId() == event.getRegionId()
        && (committerKey.getRestartTimes() < 0
            || committerKey.equals(event.getCommitterKey()));
  }

  protected boolean isEventFromDroppedPipe(final E event) {
    return event instanceof EnrichedEvent
        && ((EnrichedEvent) event).getPipeName() != null
        && isEventFromDroppedPipe((EnrichedEvent) event);
  }

  public boolean isEventFromDroppedPipe(final EnrichedEvent event) {
    return droppedPipeTaskKeys.stream().anyMatch(key -> isEventFromPipe(event, key));
  }

  public boolean isPipeDropped(final String pipeName, final long creationTime, final int regionId) {
    return droppedPipeTaskKeys.stream()
        .anyMatch(
            key ->
                key.getPipeName().equals(pipeName)
                    && key.getCreationTime() == creationTime
                    && key.getRegionId() == regionId);
  }
}
