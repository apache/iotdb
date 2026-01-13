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
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
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
    this.pendingQueue = pendingQueue;
    this.creationTime = creationTime;
    this.regionId = regionId;
    this.forceTabletFormat = forceTabletFormat;
    this.skipParsing = skipParsing;
    this.isUsedForConsensusPipe = isUsedInConsensusPipe;
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

  private void parseAndCollectEvent(final PipeRawTabletInsertionEvent sourceEvent)
      throws IllegalPathException {
    if (sourceEvent.shouldParseTimeOrPattern()) {
      collectParsedRawTableEvent(sourceEvent.parseEventWithPatternOrTime());
    } else {
      collectEvent(sourceEvent);
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
      return;
    }

    try {
      sourceEvent.consumeTabletInsertionEventsWithRetry(
          this::collectParsedRawTableEvent, "PipeEventCollector::parseAndCollectEvent");
    } finally {
      sourceEvent.close();
    }
  }

  public static boolean canSkipParsing4TsFileEvent(final PipeTsFileInsertionEvent sourceEvent) {
    return !sourceEvent.shouldParseTimeOrPattern()
        || (sourceEvent.isTableModelEvent()
            && (sourceEvent.getTablePattern() == null
                || !sourceEvent.getTablePattern().hasTablePattern())
            && !sourceEvent.shouldParseTime());
  }

  private void collectParsedRawTableEvent(final PipeRawTabletInsertionEvent parsedEvent) {
    if (!parsedEvent.hasNoNeedParsingAndIsEmpty()) {
      hasNoGeneratedEvent = false;
      collectEvent(parsedEvent);
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
                        IoTDBSchemaRegionSource.TABLE_PRIVILEGE_PARSE_VISITOR.process(
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
    if (event instanceof EnrichedEvent) {
      if (!((EnrichedEvent) event).increaseReferenceCount(PipeEventCollector.class.getName())) {
        LOGGER.warn("PipeEventCollector: The event {} is already released, skipping it.", event);
        isFailedToIncreaseReferenceCount = true;
        return;
      }

      // Assign a commit id for this event in order to report progress in order.
      PipeEventCommitManager.getInstance()
          .enrichWithCommitterKeyAndCommitId((EnrichedEvent) event, creationTime, regionId);

      // Assign a rebootTime for pipeConsensus
      ((EnrichedEvent) event).setRebootTimes(PipeDataNodeAgent.runtime().getRebootTimes());
    }

    if (event instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event).recordConnectorQueueSize(pendingQueue);
    }

    pendingQueue.directOffer(event);
    collectInvocationCount.incrementAndGet();
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
