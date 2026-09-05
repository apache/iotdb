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

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public abstract class BlockingPendingQueue<E extends Event> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingPendingQueue.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  protected final BlockingQueue<E> pendingQueue;

  protected final PipeEventCounter eventCounter;

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected final Set<CommitterKey> droppedPipeTaskKeys = ConcurrentHashMap.newKeySet();

  private final Object pendingEventMemoryLock = new Object();
  private final Map<E, PendingEventMemoryReservation> eventToMemoryReservation =
      new IdentityHashMap<>();
  private final Map<PendingEventMemoryReservation, Boolean> activeMemoryReservations =
      new IdentityHashMap<>();
  private long pendingEventMemoryUsageInBytes;

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

  public PendingEventMemoryReservation waitForMemoryReservation(
      final long eventMemoryInBytes,
      final long maxPendingEventMemoryInBytes,
      final BooleanSupplier shouldContinueWaiting) {
    final long normalizedEventMemoryInBytes = Math.max(0, eventMemoryInBytes);
    final long normalizedMaxPendingEventMemoryInBytes = Math.max(1, maxPendingEventMemoryInBytes);

    synchronized (pendingEventMemoryLock) {
      while (!isClosed.get()
          && shouldContinueWaiting.getAsBoolean()
          && pendingEventMemoryUsageInBytes > 0
          && normalizedEventMemoryInBytes
              > normalizedMaxPendingEventMemoryInBytes - pendingEventMemoryUsageInBytes) {
        try {
          pendingEventMemoryLock.wait(100);
        } catch (final InterruptedException e) {
          LOGGER.info(PipeMessages.PENDING_QUEUE_PUT_INTERRUPTED, e);
          Thread.currentThread().interrupt();
          return null;
        }
      }

      if (isClosed.get() || !shouldContinueWaiting.getAsBoolean()) {
        return null;
      }

      final PendingEventMemoryReservation reservation =
          new PendingEventMemoryReservation(this, normalizedEventMemoryInBytes);
      activeMemoryReservations.put(reservation, Boolean.TRUE);
      pendingEventMemoryUsageInBytes += normalizedEventMemoryInBytes;
      return reservation;
    }
  }

  /** Publishes an event using bytes previously reserved by {@link #waitForMemoryReservation}. */
  public boolean offer(final E event, final PendingEventMemoryReservation reservation) {
    if (reservation == null || reservation.owner != this) {
      throw new IllegalArgumentException("The memory reservation does not belong to this queue.");
    }

    synchronized (pendingEventMemoryLock) {
      if (!checkBeforeOffer(event)) {
        releaseMemoryReservationInternal(reservation);
        return false;
      }
      if (reservation.released || reservation.published) {
        throw new IllegalStateException("The memory reservation is no longer publishable.");
      }
      if (eventToMemoryReservation.containsKey(event)) {
        releaseMemoryReservationInternal(reservation);
        throw new IllegalStateException("The same event is already byte-accounted in the queue.");
      }

      final boolean offered = pendingQueue.offer(event);
      if (!offered) {
        releaseMemoryReservationInternal(reservation);
        return false;
      }

      reservation.published = true;
      reservation.event = event;
      eventToMemoryReservation.put(event, reservation);
      eventCounter.increaseEventCount(event);
      return true;
    }
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
    onEventPolled(event);
    return event;
  }

  public E waitedPoll() {
    E event = null;
    try {
      event =
          pendingQueue.poll(
              PIPE_CONFIG.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(),
              TimeUnit.MILLISECONDS);
      onEventPolled(event);
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
    closeOffers();
    pendingQueue.clear();
    eventCounter.reset();
    droppedPipeTaskKeys.clear();
    releaseAllMemoryReservations();
  }

  /** DO NOT FORGET to set eventCounter to new value after invoking this method. */
  public void forEach(final Consumer<? super E> action) {
    pendingQueue.forEach(action);
  }

  public void discardAllEvents() {
    closeOffers();
    final ArrayList<E> discardedEvents = new ArrayList<>();
    pendingQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent) {
            if (((EnrichedEvent) event).clearReferenceCount(BlockingPendingQueue.class.getName())) {
              eventCounter.decreaseEventCount(event);
            }
          }
          discardedEvents.add(event);
          return true;
        });
    discardedEvents.forEach(this::releasePendingEventMemory);
    eventCounter.reset();
    droppedPipeTaskKeys.clear();
    releaseAllMemoryReservations();
  }

  public void discardEventsOfPipe(
      final String pipeNameToDrop, final long creationTimeToDrop, final int regionId) {
    discardEventsOfPipe(new CommitterKey(pipeNameToDrop, creationTimeToDrop, regionId, -1));
  }

  public void discardEventsOfPipe(final CommitterKey committerKey) {
    droppedPipeTaskKeys.add(committerKey);
    final ArrayList<E> discardedEvents = new ArrayList<>();
    pendingQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent
              && isEventFromPipe((EnrichedEvent) event, committerKey)) {
            if (((EnrichedEvent) event).clearReferenceCount(BlockingPendingQueue.class.getName())) {
              eventCounter.decreaseEventCount(event);
            }
            discardedEvents.add(event);
            return true;
          }
          return false;
        });
    discardedEvents.forEach(this::releasePendingEventMemory);
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

  public long getPendingEventMemoryUsageInBytes() {
    synchronized (pendingEventMemoryLock) {
      return pendingEventMemoryUsageInBytes;
    }
  }

  protected void onEventPolled(final E event) {
    eventCounter.decreaseEventCount(event);
    releasePendingEventMemory(event);
  }

  private void releasePendingEventMemory(final E event) {
    if (event == null) {
      return;
    }
    synchronized (pendingEventMemoryLock) {
      final PendingEventMemoryReservation reservation = eventToMemoryReservation.remove(event);
      if (reservation != null) {
        releaseMemoryReservationInternal(reservation);
      }
    }
  }

  private void releaseAllMemoryReservations() {
    synchronized (pendingEventMemoryLock) {
      for (final PendingEventMemoryReservation reservation :
          new ArrayList<>(activeMemoryReservations.keySet())) {
        releaseMemoryReservationInternal(reservation);
      }
      eventToMemoryReservation.clear();
      pendingEventMemoryLock.notifyAll();
    }
  }

  private void releaseMemoryReservation(final PendingEventMemoryReservation reservation) {
    synchronized (pendingEventMemoryLock) {
      releaseMemoryReservationInternal(reservation);
    }
  }

  private void releaseMemoryReservationInternal(final PendingEventMemoryReservation reservation) {
    if (reservation.released) {
      return;
    }
    reservation.released = true;
    activeMemoryReservations.remove(reservation);
    if (reservation.event != null) {
      eventToMemoryReservation.remove(reservation.event);
    }
    pendingEventMemoryUsageInBytes -= reservation.memoryInBytes;
    pendingEventMemoryLock.notifyAll();
  }

  private void closeOffers() {
    synchronized (pendingEventMemoryLock) {
      isClosed.set(true);
    }
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
        && (committerKey.getRestartTimes() < 0 || committerKey.equals(event.getCommitterKey()));
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

  public static final class PendingEventMemoryReservation implements AutoCloseable {

    private final BlockingPendingQueue<?> owner;
    private final long memoryInBytes;
    private Object event;
    private boolean published;
    private boolean released;

    private PendingEventMemoryReservation(
        final BlockingPendingQueue<?> owner, final long memoryInBytes) {
      this.owner = owner;
      this.memoryInBytes = memoryInBytes;
    }

    @Override
    public void close() {
      owner.releaseMemoryReservation(this);
    }
  }
}
