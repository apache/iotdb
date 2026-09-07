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

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.connection.BlockingPendingQueue.PendingEventMemoryReservation;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtaskExecutionGuard;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtaskYieldException;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.db.pipe.source.schemaregion.PipePlanTablePrivilegeParseVisitor;
import org.apache.iotdb.db.pipe.source.schemaregion.PipePlanTreePrivilegeParseVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
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

  private final boolean isUsedForConsensusPipe;
  private final boolean isTsFileParserCollector;
  private PipeProcessorSubtaskExecutionGuard processorExecutionGuard =
      PipeProcessorSubtaskExecutionGuard.disabled();

  private final AtomicInteger collectInvocationCount = new AtomicInteger(0);
  private boolean hasNoGeneratedEvent = true;
  private boolean isFailedToIncreaseReferenceCount = false;

  public PipeEventCollector(
      final UnboundedBlockingPendingQueue<Event> pendingQueue,
      final long creationTime,
      final int regionId,
      final boolean forceTabletFormat,
      final boolean skipParsing,
      final boolean isUsedInConsensusPipe) {
    this(
        pendingQueue,
        creationTime,
        regionId,
        forceTabletFormat,
        skipParsing,
        isUsedInConsensusPipe,
        false);
  }

  private PipeEventCollector(
      final UnboundedBlockingPendingQueue<Event> pendingQueue,
      final long creationTime,
      final int regionId,
      final boolean forceTabletFormat,
      final boolean skipParsing,
      final boolean isUsedInConsensusPipe,
      final boolean isTsFileParserCollector) {
    this.pendingQueue = pendingQueue;
    this.creationTime = creationTime;
    this.regionId = regionId;
    this.forceTabletFormat = forceTabletFormat;
    this.skipParsing = skipParsing;
    this.isUsedForConsensusPipe = isUsedInConsensusPipe;
    this.isTsFileParserCollector = isTsFileParserCollector;
  }

  public void setProcessorExecutionGuard(
      final PipeProcessorSubtaskExecutionGuard processorExecutionGuard) {
    this.processorExecutionGuard = processorExecutionGuard;
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
      } else if (event instanceof PipeDeleteDataNodeEvent) {
        parseAndCollectEvent((PipeDeleteDataNodeEvent) event);
      } else if (!(event instanceof ProgressReportEvent)) {
        collectEvent(event);
      }
    } catch (final PipeProcessorSubtaskYieldException e) {
      throw e;
    } catch (final PipeException e) {
      throw e;
    } catch (final Exception e) {
      throw new PipeException(
          DataNodePipeMessages.ERROR_OCCURRED_WHEN_COLLECTING_EVENTS_FROM_PROCESSOR, e);
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

  private void parseAndCollectEvent(final PipeRawTabletInsertionEvent sourceEvent)
      throws IllegalPathException {
    if (sourceEvent.shouldParseTimeOrPattern()) {
      collectParsedRawTableEvent(sourceEvent.parseEventWithPatternOrTime());
    } else {
      collectEvent(sourceEvent, isTsFileParserCollector);
    }
  }

  private void parseAndCollectEvent(final PipeTsFileInsertionEvent sourceEvent) throws Exception {
    if (!sourceEvent.waitForTsFileClose(processorExecutionGuard)) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_SKIPPING_TEMPORARY_TSFILE_WHICH_SHOULDN_T,
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
        this::collectParsedRawTableEvent,
        "PipeEventCollector::parseAndCollectEvent",
        processorExecutionGuard);
    sourceEvent.close();
    if (sourceEvent.isGeneratedByHistoricalExtractor()) {
      PipeTerminateEvent.markHistoricalTsFileSplit(
          sourceEvent.getPipeName(), sourceEvent.getCreationTime(), regionId);
    }
  }

  public static boolean canSkipParsing4TsFileEvent(final PipeTsFileInsertionEvent sourceEvent) {
    return !sourceEvent.shouldParseTimeOrPattern()
        || (sourceEvent.isTableModelEvent()
            && (sourceEvent.getTablePattern() == null
                || !sourceEvent.getTablePattern().hasTablePattern())
            && !sourceEvent.shouldParseTime());
  }

  public boolean shouldParseTsFileEvent(final PipeTsFileInsertionEvent sourceEvent) {
    return sourceEvent.shouldParse4Privilege()
        || !skipParsing && (forceTabletFormat || !canSkipParsing4TsFileEvent(sourceEvent));
  }

  public void prepareTsFileEventForParallelParsing(final PipeTsFileInsertionEvent sourceEvent) {
    if (sourceEvent.isProgressReportManagedByTsFileParser()
        || !sourceEvent.shouldReportGeneratedEventsOnCommit()) {
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
    final PipeEventCollector collector =
        new PipeEventCollector(
            pendingQueue,
            creationTime,
            regionId,
            forceTabletFormat,
            skipParsing,
            isUsedForConsensusPipe,
            true);
    collector.setProcessorExecutionGuard(processorExecutionGuard);
    return collector;
  }

  private void collectParsedRawTableEvent(final PipeRawTabletInsertionEvent parsedEvent) {
    if (!parsedEvent.hasNoNeedParsingAndIsEmpty()) {
      hasNoGeneratedEvent = false;
      collectEvent(parsedEvent, isTsFileParserCollector);
    }
  }

  private void parseAndCollectEvent(final PipeDeleteDataNodeEvent deleteDataEvent) {
    // For IoTConsensusV2, there is no need to parse. So we can directly transfer deleteDataEvent
    if (isUsedForConsensusPipe) {
      hasNoGeneratedEvent = false;
      collectEvent(deleteDataEvent);
      return;
    }

    // Only used by events containing delete data node, no need to bind progress index here since
    // delete data event does not have progress index currently
    (deleteDataEvent.getDeleteDataNode() instanceof DeleteDataNode
            ? IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
                .process(
                    deleteDataEvent.getDeleteDataNode(),
                    (IoTDBTreePatternOperations) deleteDataEvent.getTreePattern())
                .flatMap(
                    planNode ->
                        new PipePlanTreePrivilegeParseVisitor(
                                deleteDataEvent.isSkipIfNoPrivileges())
                            .process(
                                planNode,
                                new UserEntity(
                                    Long.parseLong(deleteDataEvent.getUserId()),
                                    deleteDataEvent.getUserName(),
                                    deleteDataEvent.getCliHostname())))
            : IoTDBSchemaRegionSource.TABLE_PATTERN_PARSE_VISITOR
                .process(deleteDataEvent.getDeleteDataNode(), deleteDataEvent.getTablePattern())
                .flatMap(
                    planNode ->
                        new PipePlanTablePrivilegeParseVisitor(
                                deleteDataEvent.isSkipIfNoPrivileges())
                            .process(
                                planNode,
                                new UserEntity(
                                    Long.parseLong(deleteDataEvent.getUserId()),
                                    deleteDataEvent.getUserName(),
                                    deleteDataEvent.getCliHostname()))))
        .map(
            planNode ->
                new PipeDeleteDataNodeEvent(
                    (AbstractDeleteDataNode) planNode,
                    deleteDataEvent.getPipeName(),
                    deleteDataEvent.getCreationTime(),
                    deleteDataEvent.getPipeTaskMeta(),
                    deleteDataEvent.getTreePattern(),
                    deleteDataEvent.getTablePattern(),
                    deleteDataEvent.getUserId(),
                    deleteDataEvent.getUserName(),
                    deleteDataEvent.getCliHostname(),
                    deleteDataEvent.isSkipIfNoPrivileges(),
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
              tabletSizeInBytes,
              Math.max(1, PipeConfig.getInstance().getTsFileParserMemory()),
              processorExecutionGuard::isCurrentInvocationValid);
      if (memoryReservation == null) {
        processorExecutionGuard.check();
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
          LOGGER.warn(
              DataNodePipeMessages.PIPEEVENTCOLLECTOR_THE_EVENT_IS_ALREADY_RELEASED_SKIPPING,
              event);
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
          // The source TsFile owns the ordered commit id. Parsed tablets only retain its committer
          // key so downstream queues can still identify the pipe session and region.
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
