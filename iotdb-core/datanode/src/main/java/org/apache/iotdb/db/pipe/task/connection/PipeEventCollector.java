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

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.pipe.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.schemaregion.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class PipeEventCollector implements EventCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCollector.class);

  private final UnboundedBlockingPendingQueue<Event> pendingQueue;

  private final long creationTime;

  private final int regionId;

  private final AtomicInteger collectInvocationCount = new AtomicInteger(0);

  public PipeEventCollector(
      final UnboundedBlockingPendingQueue<Event> pendingQueue,
      final long creationTime,
      final int regionId) {
    this.pendingQueue = pendingQueue;
    this.creationTime = creationTime;
    this.regionId = regionId;
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
      } else if (event instanceof PipeSchemaRegionWritePlanEvent) {
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
    if (sourceEvent.shouldParseTimeOrPattern()) {
      for (final PipeRawTabletInsertionEvent parsedEvent :
          sourceEvent.toRawTabletInsertionEvents()) {
        collectEvent(parsedEvent);
      }
    } else {
      collectEvent(sourceEvent);
    }
  }

  private void parseAndCollectEvent(final PipeRawTabletInsertionEvent sourceEvent) {
    if (sourceEvent.shouldParseTimeOrPattern()) {
      final PipeRawTabletInsertionEvent parsedEvent = sourceEvent.parseEventWithPatternOrTime();
      if (!parsedEvent.hasNoNeedParsingAndIsEmpty()) {
        collectEvent(parsedEvent);
      }
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

    if (!sourceEvent.shouldParseTimeOrPattern()) {
      collectEvent(sourceEvent);
      return;
    }

    try {
      for (final TabletInsertionEvent parsedEvent : sourceEvent.toTabletInsertionEvents()) {
        collectEvent(parsedEvent);
      }
    } finally {
      sourceEvent.close();
    }
  }

  private void parseAndCollectEvent(final PipeSchemaRegionWritePlanEvent deleteDataEvent) {
    IoTDBSchemaRegionExtractor.PATTERN_PARSE_VISITOR
        .process(deleteDataEvent.getPlanNode(), (IoTDBPipePattern) deleteDataEvent.getPipePattern())
        .map(
            planNode ->
                new PipeSchemaRegionWritePlanEvent(
                    planNode,
                    deleteDataEvent.getPipeName(),
                    deleteDataEvent.getPipeTaskMeta(),
                    deleteDataEvent.getPipePattern(),
                    deleteDataEvent.isGeneratedByPipe()))
        .ifPresent(this::collectEvent);
  }

  private void collectEvent(final Event event) {
    collectInvocationCount.incrementAndGet();

    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).increaseReferenceCount(PipeEventCollector.class.getName());

      // Assign a commit id for this event in order to report progress in order.
      PipeEventCommitManager.getInstance()
          .enrichWithCommitterKeyAndCommitId((EnrichedEvent) event, creationTime, regionId);
    }

    if (event instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) event).recordConnectorQueueSize(pendingQueue);
    }

    pendingQueue.directOffer(event);
  }

  public void resetCollectInvocationCount() {
    collectInvocationCount.set(0);
  }

  public boolean hasNoCollectInvocationAfterReset() {
    return collectInvocationCount.get() == 0;
  }
}
