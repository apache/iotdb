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

import org.apache.iotdb.commons.pipe.agent.task.connection.BlockingPendingQueue.PendingEventMemoryReservation;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePatternOperations;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class PipeEventCollector implements EventCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCollector.class);

  private final UnboundedBlockingPendingQueue<Event> pendingQueue;

  private final long creationTime;

  private final int regionId;

  private final boolean forceTabletFormat;

  private final boolean skipParsing;

  private final boolean isTsFileParserCollector;

  private final AtomicInteger collectInvocationCount = new AtomicInteger(0);
  private boolean hasNoGeneratedEvent = true;
  private boolean isFailedToIncreaseReferenceCount = false;

  public PipeEventCollector(
      final UnboundedBlockingPendingQueue<Event> pendingQueue,
      final long creationTime,
      final int regionId,
      final boolean forceTabletFormat,
      final boolean skipParsing) {
    this(pendingQueue, creationTime, regionId, forceTabletFormat, skipParsing, false);
  }

  private PipeEventCollector(
      final UnboundedBlockingPendingQueue<Event> pendingQueue,
      final long creationTime,
      final int regionId,
      final boolean forceTabletFormat,
      final boolean skipParsing,
      final boolean isTsFileParserCollector) {
    this.pendingQueue = pendingQueue;
    this.creationTime = creationTime;
    this.regionId = regionId;
    this.forceTabletFormat = forceTabletFormat;
    this.skipParsing = skipParsing;
    this.isTsFileParserCollector = isTsFileParserCollector;
  }

  @Override
  public void collect(final Event event) {
    try {
      if (event instanceof PipeInsertNodeTabletInsertionEvent) {
        parseAndCollectEvent((PipeInsertNodeTabletInsertionEvent) event);
      } else if (event instanceof PipeRawTabletInsertionEvent) {
        parseAndCollectEvent((PipeRawTabletInsertionEvent) event);
      } else if (event instanceof PipeTsFileInsertionEvent) {
        parseAndCollectEvent((PipeTsFileInsertionEvent) event);
      } else if (event instanceof PipeSchemaRegionWritePlanEvent
          && ((PipeSchemaRegionWritePlanEvent) event).getPlanNode().getType()
              == PlanNodeType.DELETE_DATA) {
        // This is only for delete data node in data region since plan nodes in schema regions are
        // already parsed in schema region extractor
        parseAndCollectEvent((PipeSchemaRegionWritePlanEvent) event);
      } else if (!(event instanceof ProgressReportEvent)) {
        collectEvent(event);
      }
    } catch (final PipeException e) {
      throw e;
    } catch (final Exception e) {
      throw new PipeException("Error occurred when collecting events from processor.", e);
    }
  }

  private void parseAndCollectEvent(final PipeInsertNodeTabletInsertionEvent sourceEvent) {
    if (skipParsing) {
      collectEvent(sourceEvent);
      return;
    }

    if (sourceEvent.shouldParseTimeOrPattern()) {
      for (final PipeRawTabletInsertionEvent parsedEvent :
          sourceEvent.toRawTabletInsertionEvents()) {
        collectParsedRawTableEvent(parsedEvent);
      }
    } else {
      collectEvent(sourceEvent);
    }
  }

  private void parseAndCollectEvent(final PipeRawTabletInsertionEvent sourceEvent) {
    if (sourceEvent.shouldParseTimeOrPattern()) {
      collectParsedRawTableEvent(sourceEvent.parseEventWithPatternOrTime());
    } else {
      collectEvent(sourceEvent, isTsFileParserCollector);
    }
  }

  private void parseAndCollectEvent(final PipeTsFileInsertionEvent sourceEvent) throws Exception {
    if (!sourceEvent.waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          sourceEvent.getTsFile());
      return;
    }

    if (skipParsing || !forceTabletFormat && canSkipParsing4TsFileEvent(sourceEvent)) {
      collectEvent(sourceEvent);
      if (sourceEvent.isGeneratedByHistoricalExtractor()) {
        PipeTerminateEvent.markHistoricalTsFileUnsplit(
            sourceEvent.getPipeName(), sourceEvent.getCreationTime(), regionId);
      }
      return;
    }

    sourceEvent.consumeTabletInsertionEventsWithRetry(
        this::collectParsedRawTableEvent, "PipeEventCollector::parseAndCollectEvent");
    sourceEvent.close();
    if (sourceEvent.isGeneratedByHistoricalExtractor()) {
      PipeTerminateEvent.markHistoricalTsFileSplit(
          sourceEvent.getPipeName(), sourceEvent.getCreationTime(), regionId);
    }
  }

  public static boolean canSkipParsing4TsFileEvent(final PipeTsFileInsertionEvent sourceEvent) {
    return !sourceEvent.shouldParseTimeOrPattern();
  }

  public boolean shouldParseTsFileEvent(final PipeTsFileInsertionEvent sourceEvent) {
    return !skipParsing && (forceTabletFormat || !canSkipParsing4TsFileEvent(sourceEvent));
  }

  public void prepareTsFileEventForParallelParsing(final PipeTsFileInsertionEvent sourceEvent) {
    if (sourceEvent.isProgressReportManagedByTsFileParser()) {
      return;
    }
    if (sourceEvent.getCommitId() <= EnrichedEvent.NO_COMMIT_ID) {
      PipeEventCommitManager.getInstance()
          .enrichWithCommitterKeyAndCommitId(sourceEvent, creationTime, regionId);
    }
    if (sourceEvent.getCommitId() > EnrichedEvent.NO_COMMIT_ID) {
      sourceEvent.markProgressReportManagedByTsFileParser();
    }
  }

  public PipeEventCollector forkForTsFileParser() {
    return new PipeEventCollector(
        pendingQueue, creationTime, regionId, forceTabletFormat, skipParsing, true);
  }

  private void collectParsedRawTableEvent(final PipeRawTabletInsertionEvent parsedEvent) {
    if (!parsedEvent.hasNoNeedParsingAndIsEmpty()) {
      hasNoGeneratedEvent = false;
      collectEvent(parsedEvent, isTsFileParserCollector);
    }
  }

  private void parseAndCollectEvent(final PipeSchemaRegionWritePlanEvent deleteDataEvent) {
    // Only used by events containing delete data node, no need to bind progress index here since
    // delete data event does not have progress index currently
    IoTDBSchemaRegionSource.PATTERN_PARSE_VISITOR
        .process(
            deleteDataEvent.getPlanNode(),
            (IoTDBPipePatternOperations) deleteDataEvent.getPipePattern())
        .map(
            planNode ->
                new PipeSchemaRegionWritePlanEvent(
                    planNode,
                    deleteDataEvent.getPipeName(),
                    deleteDataEvent.getCreationTime(),
                    deleteDataEvent.getPipeTaskMeta(),
                    deleteDataEvent.getPipePattern(),
                    deleteDataEvent.isGeneratedByPipe()))
        .ifPresent(
            event -> {
              hasNoGeneratedEvent = false;
              collectEvent(event);
            });
  }

  private void collectEvent(final Event event) {
    collectEvent(event, false);
  }

  private void collectEvent(final Event event, final boolean useParserQueueMemoryBackpressure) {
    PendingEventMemoryReservation memoryReservation = null;
    long tabletSizeInBytes = 0;
    if (useParserQueueMemoryBackpressure && event instanceof PipeRawTabletInsertionEvent) {
      tabletSizeInBytes = ((PipeRawTabletInsertionEvent) event).getTabletSizeInBytes();
      memoryReservation =
          pendingQueue.waitForMemoryReservation(
              tabletSizeInBytes, Math.max(1, PipeConfig.getInstance().getTsFileParserMemory()));
      if (memoryReservation == null) {
        throw new PipeException("Interrupted while waiting for parser output queue memory.");
      }
    }

    boolean isReferenceIncreased = false;
    boolean isOffered = false;
    try {
      if (event instanceof EnrichedEvent) {
        final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
        final boolean increased =
            useParserQueueMemoryBackpressure && event instanceof PipeRawTabletInsertionEvent
                ? ((PipeRawTabletInsertionEvent) event)
                    .increaseReferenceCountWithReservedMemory(
                        PipeEventCollector.class.getName(),
                        Math.max(
                            tabletSizeInBytes > Long.MAX_VALUE / 2
                                ? Long.MAX_VALUE
                                : tabletSizeInBytes * 2,
                            PipeConfig.getInstance().getPipeDataStructureTabletSizeInBytes()))
                : enrichedEvent.increaseReferenceCount(PipeEventCollector.class.getName());
        if (!increased) {
          LOGGER.warn("PipeEventCollector: The event {} is already released, skipping it.", event);
          isFailedToIncreaseReferenceCount = true;
          return;
        }
        isReferenceIncreased = true;

        final PipeTsFileInsertionEvent progressReportSourceTsFile =
            event instanceof PipeRawTabletInsertionEvent
                ? ((PipeRawTabletInsertionEvent) event).getProgressReportSourceTsFile()
                : null;
        if (progressReportSourceTsFile == null) {
          // Assign a commit id for this event in order to report progress in order.
          PipeEventCommitManager.getInstance()
              .enrichWithCommitterKeyAndCommitId(enrichedEvent, creationTime, regionId);
        } else {
          // The source TsFile owns the ordered commit id. Raw tablets retain its committer key but
          // do not create independent commits.
          enrichedEvent.setCommitterKeyAndCommitId(
              progressReportSourceTsFile.getCommitterKey(), EnrichedEvent.NO_COMMIT_ID);
        }

        // Assign a rebootTime for iotConsensusV2
        enrichedEvent.setRebootTimes(PipeDataNodeAgent.runtime().getRebootTimes());

        if (enrichedEvent.getPipeName() != null
            && (pendingQueue.isEventFromDroppedPipe(enrichedEvent)
                || (enrichedEvent.getCommitterKey() == null
                    && pendingQueue.isPipeDropped(
                        enrichedEvent.getPipeName(), creationTime, regionId)))) {
          enrichedEvent.clearReferenceCount(PipeEventCollector.class.getName());
          return;
        }
      }

      if (event instanceof PipeHeartbeatEvent) {
        ((PipeHeartbeatEvent) event).recordConnectorQueueSize(pendingQueue);
      }

      isOffered =
          memoryReservation == null
              ? pendingQueue.offer(event)
              : pendingQueue.offer(event, memoryReservation);
      if (isOffered) {
        memoryReservation = null;
        collectInvocationCount.incrementAndGet();
      }
    } finally {
      if (memoryReservation != null) {
        memoryReservation.close();
      }
      if (!isOffered
          && isReferenceIncreased
          && event instanceof EnrichedEvent
          && !((EnrichedEvent) event).isReleased()) {
        ((EnrichedEvent) event).decreaseReferenceCount(PipeEventCollector.class.getName(), false);
      }
    }
  }

  public void resetFlags() {
    collectInvocationCount.set(0);
    hasNoGeneratedEvent = true;
    isFailedToIncreaseReferenceCount = false;
  }

  public long getCollectInvocationCount() {
    return collectInvocationCount.get();
  }

  public boolean hasNoCollectInvocationAfterReset() {
    return collectInvocationCount.get() == 0;
  }

  public boolean hasNoGeneratedEvent() {
    return hasNoGeneratedEvent;
  }

  public boolean isFailedToIncreaseReferenceCount() {
    return isFailedToIncreaseReferenceCount;
  }
}
