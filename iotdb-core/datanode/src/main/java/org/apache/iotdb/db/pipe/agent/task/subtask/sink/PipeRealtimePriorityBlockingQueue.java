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

package org.apache.iotdb.db.pipe.agent.task.subtask.sink;

import org.apache.iotdb.commons.pipe.agent.task.connection.BlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeCompactedTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PipeRealtimePriorityBlockingQueue extends UnboundedBlockingPendingQueue<Event> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimePriorityBlockingQueue.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final BlockingDeque<TsFileInsertionEvent> tsfileInsertEventDeque =
      new LinkedBlockingDeque<>();

  private final AtomicInteger pollTsFileCounter = new AtomicInteger(0);

  private final AtomicLong pollHistoricalTsFileCounter = new AtomicLong(0);

  // Need to ensure that NPE does not occur
  private AtomicInteger offerTsFileCounter = new AtomicInteger(0);

  public PipeRealtimePriorityBlockingQueue() {
    super(new PipeDataRegionEventCounter());
  }

  @Override
  public boolean directOffer(final Event event) {
    checkBeforeOffer(event);

    if (event instanceof TsFileInsertionEvent) {
      tsfileInsertEventDeque.add((TsFileInsertionEvent) event);
      return true;
    }

    if (event instanceof PipeHeartbeatEvent && super.peekLast() instanceof PipeHeartbeatEvent) {
      // We can NOT keep too many PipeHeartbeatEvent in bufferQueue because they may cause OOM.
      ((EnrichedEvent) event).decreaseReferenceCount(PipeEventCollector.class.getName(), false);
      return false;
    } else {
      return super.directOffer(event);
    }
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
    Event event = null;
    final int pollHistoricalTsFileThreshold =
        PIPE_CONFIG.getPipeRealTimeQueuePollHistoricalTsFileThreshold();
    final int realTimeQueueMaxWaitingTsFileSize =
        PIPE_CONFIG.getPipeRealTimeQueueMaxWaitingTsFileSize();

    if (pollTsFileCounter.get() >= PIPE_CONFIG.getPipeRealTimeQueuePollTsFileThreshold()
        && offerTsFileCounter.get() < realTimeQueueMaxWaitingTsFileSize) {
      event =
          pollHistoricalTsFileCounter.incrementAndGet() % pollHistoricalTsFileThreshold == 0
              ? tsfileInsertEventDeque.pollFirst()
              : tsfileInsertEventDeque.pollLast();
      pollTsFileCounter.set(0);
    }

    if (Objects.isNull(event)) {
      // Sequentially poll the first offered non-TsFileInsertionEvent
      event = super.directPoll();
      if (Objects.isNull(event) && offerTsFileCounter.get() < realTimeQueueMaxWaitingTsFileSize) {
        event =
            pollHistoricalTsFileCounter.incrementAndGet() % pollHistoricalTsFileThreshold == 0
                ? tsfileInsertEventDeque.pollFirst()
                : tsfileInsertEventDeque.pollLast();
      }
      if (event != null) {
        pollTsFileCounter.incrementAndGet();
      }
    }

    return event;
  }

  /**
   * When the number of polls exceeds the pollHistoryThreshold, the {@link TsFileInsertionEvent} of
   * the earliest write to the queue is returned. if the pollHistoryThreshold is not reached then an
   * attempt is made to poll the queue for the latest insertion {@link Event}. First, it tries to
   * poll the first provided If there is no such {@link Event}, poll the last supplied {@link
   * TsFileInsertionEvent}. If no {@link Event} is available, it blocks until a {@link Event} is
   * available.
   *
   * @return the freshest insertion {@link Event}. can be {@code null} if no {@link Event} is
   *     available.
   */
  @Override
  public Event waitedPoll() {
    Event event = null;
    final int pollHistoricalTsFileThreshold =
        PIPE_CONFIG.getPipeRealTimeQueuePollHistoricalTsFileThreshold();
    final int realTimeQueueMaxWaitingTsFileSize =
        PIPE_CONFIG.getPipeRealTimeQueueMaxWaitingTsFileSize();

    if (pollTsFileCounter.get() >= PIPE_CONFIG.getPipeRealTimeQueuePollTsFileThreshold()
        && offerTsFileCounter.get() < realTimeQueueMaxWaitingTsFileSize) {
      event =
          pollHistoricalTsFileCounter.incrementAndGet() % pollHistoricalTsFileThreshold == 0
              ? tsfileInsertEventDeque.pollFirst()
              : tsfileInsertEventDeque.pollLast();
      pollTsFileCounter.set(0);
    }
    if (event == null) {
      // Sequentially poll the first offered non-TsFileInsertionEvent
      event = super.directPoll();
      if (event == null && !tsfileInsertEventDeque.isEmpty()) {
        event =
            pollHistoricalTsFileCounter.incrementAndGet() % pollHistoricalTsFileThreshold == 0
                ? tsfileInsertEventDeque.pollFirst()
                : tsfileInsertEventDeque.pollLast();
      }
      if (event != null) {
        pollTsFileCounter.incrementAndGet();
      }
    }

    // If no event is available, block until an event is available
    if (Objects.isNull(event) && offerTsFileCounter.get() < realTimeQueueMaxWaitingTsFileSize) {
      event = super.waitedPoll();
      if (Objects.isNull(event)) {
        event =
            pollHistoricalTsFileCounter.incrementAndGet() % pollHistoricalTsFileThreshold == 0
                ? tsfileInsertEventDeque.pollFirst()
                : tsfileInsertEventDeque.pollLast();
      }
      if (event != null) {
        pollTsFileCounter.incrementAndGet();
      }
    }

    return event;
  }

  @Override
  public Event peek() {
    final Event event = pendingQueue.peek();
    if (Objects.nonNull(event)) {
      return event;
    }
    return tsfileInsertEventDeque.peek();
  }

  public synchronized void replace(
      String dataRegionId, Set<TsFileResource> sourceFiles, List<TsFileResource> targetFiles) {

    final int regionId = Integer.parseInt(dataRegionId);
    final Map<CommitterKey, Set<PipeTsFileInsertionEvent>> eventsToBeRemovedGroupByCommitterKey =
        tsfileInsertEventDeque.stream()
            .filter(
                event ->
                    event instanceof PipeTsFileInsertionEvent
                        && ((PipeTsFileInsertionEvent) event).getRegionId() == regionId)
            .map(event -> (PipeTsFileInsertionEvent) event)
            .collect(
                Collectors.groupingBy(
                    PipeTsFileInsertionEvent::getCommitterKey, Collectors.toSet()))
            .entrySet()
            .stream()
            // Replace if all source files are present in the queue
            .filter(entry -> entry.getValue().size() == sourceFiles.size())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (eventsToBeRemovedGroupByCommitterKey.isEmpty()) {
      LOGGER.info(
          "Region {}: No TsFileInsertionEvents to replace for source files {}",
          regionId,
          sourceFiles.stream()
              .map(TsFileResource::getTsFilePath)
              .collect(Collectors.joining(", ")));
      return;
    }

    final Map<CommitterKey, Set<PipeTsFileInsertionEvent>> eventsToBeAddedGroupByCommitterKey =
        new HashMap<>();
    for (final Map.Entry<CommitterKey, Set<PipeTsFileInsertionEvent>> entry :
        eventsToBeRemovedGroupByCommitterKey.entrySet()) {
      final CommitterKey committerKey = entry.getKey();
      final PipeTsFileInsertionEvent anyEvent = entry.getValue().stream().findFirst().orElse(null);
      final Set<PipeTsFileInsertionEvent> newEvents = new HashSet<>();
      for (int i = 0; i < targetFiles.size(); i++) {
        newEvents.add(
            new PipeCompactedTsFileInsertionEvent(
                committerKey,
                entry.getValue(),
                anyEvent,
                targetFiles.get(i),
                i == targetFiles.size() - 1));
      }
      eventsToBeAddedGroupByCommitterKey.put(committerKey, newEvents);
    }

    // Handling new events
    final Set<PipeTsFileInsertionEvent> successfullyReferenceIncreasedEvents = new HashSet<>();
    final AtomicBoolean
        allSuccess = // To track if all events successfully increased the reference count
        new AtomicBoolean(true);
    outerLoop:
    for (final Map.Entry<CommitterKey, Set<PipeTsFileInsertionEvent>> committerKeySetEntry :
        eventsToBeAddedGroupByCommitterKey.entrySet()) {
      for (final PipeTsFileInsertionEvent event : committerKeySetEntry.getValue()) {
        if (event != null) {
          try {
            if (!event.increaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName())) {
              allSuccess.set(false);
              break outerLoop;
            } else {
              successfullyReferenceIncreasedEvents.add(event);
            }
          } catch (final Exception e) {
            allSuccess.set(false);
            break outerLoop;
          }
        }
      }
    }
    if (!allSuccess.get()) {
      // If any event failed to increase the reference count,
      // we need to decrease the reference count for all successfully increased events
      for (final PipeTsFileInsertionEvent event : successfullyReferenceIncreasedEvents) {
        try {
          event.decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to decrease reference count for event {} in PipeRealtimePriorityBlockingQueue",
              event,
              e);
        }
      }
      return; // Exit early if any event failed to increase the reference count
    } else {
      // If all events successfully increased reference count,
      // we can proceed to add them to the deque
      for (final PipeTsFileInsertionEvent event : successfullyReferenceIncreasedEvents) {
        tsfileInsertEventDeque.add(event);
        eventCounter.increaseEventCount(event);
      }
    }

    // Handling old events
    for (final Map.Entry<CommitterKey, Set<PipeTsFileInsertionEvent>> entry :
        eventsToBeRemovedGroupByCommitterKey.entrySet()) {
      for (final PipeTsFileInsertionEvent event : entry.getValue()) {
        if (event != null) {
          try {
            event.decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
          } catch (final Exception e) {
            LOGGER.warn(
                "Failed to decrease reference count for event {} in PipeRealtimePriorityBlockingQueue",
                event,
                e);
          }
          eventCounter.decreaseEventCount(event);
        }
      }
    }
    final Set<PipeTsFileInsertionEvent> eventsToRemove = new HashSet<>();
    for (Set<PipeTsFileInsertionEvent> pipeTsFileInsertionEvents :
        eventsToBeRemovedGroupByCommitterKey.values()) {
      eventsToRemove.addAll(pipeTsFileInsertionEvents);
    }
    tsfileInsertEventDeque.removeIf(eventsToRemove::contains);

    LOGGER.info(
        "Region {}: Replaced TsFileInsertionEvents {} with {}",
        regionId,
        eventsToBeRemovedGroupByCommitterKey.values().stream()
            .flatMap(Set::stream)
            .map(PipeTsFileInsertionEvent::coreReportMessage)
            .collect(Collectors.joining(", ")),
        eventsToBeAddedGroupByCommitterKey.values().stream()
            .flatMap(Set::stream)
            .map(PipeTsFileInsertionEvent::coreReportMessage)
            .collect(Collectors.joining(", ")));
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
  public void discardAllEvents() {
    super.discardAllEvents();
    tsfileInsertEventDeque.removeIf(
        event -> {
          if (event instanceof EnrichedEvent) {
            if (((EnrichedEvent) event).clearReferenceCount(BlockingPendingQueue.class.getName())) {
              eventCounter.decreaseEventCount(event);
            }
          }
          return true;
        });
    eventCounter.reset();
  }

  @Override
  public void discardEventsOfPipe(final String pipeNameToDrop, final int regionId) {
    super.discardEventsOfPipe(pipeNameToDrop, regionId);
    tsfileInsertEventDeque.removeIf(
        event -> {
          if (event instanceof EnrichedEvent
              && pipeNameToDrop.equals(((EnrichedEvent) event).getPipeName())
              && regionId == ((EnrichedEvent) event).getRegionId()) {
            if (((EnrichedEvent) event)
                .clearReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName())) {
              eventCounter.decreaseEventCount(event);
            }
            return true;
          }
          return false;
        });
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

  public synchronized void setOfferTsFileCounter(AtomicInteger offerTsFileCounter) {
    this.offerTsFileCounter = offerTsFileCounter;
  }
}
