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

package org.apache.iotdb.db.pipe.event.common.heartbeat;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.metric.PipeDataNodeRemainingEventAndTimeMetrics;
import org.apache.iotdb.db.pipe.metric.PipeHeartbeatEventMetrics;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.event.Event;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class PipeHeartbeatEvent extends EnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatEvent.class);

  private final String dataRegionId;

  private long timePublished;
  private long timeAssigned;
  private long timeProcessed;
  private long timeTransferred;

  // Do not report disruptor tablet or tsFile size separately since
  // The disruptor is usually nearly empty.
  private int disruptorSize;

  private int extractorQueueTabletSize;
  private int extractorQueueTsFileSize;
  private int extractorQueueSize;

  private int connectorQueueTabletSize;
  private int connectorQueueTsFileSize;
  private int connectorQueueSize;

  private final boolean shouldPrintMessage;

  public PipeHeartbeatEvent(final String dataRegionId, final boolean shouldPrintMessage) {
    super(null, 0, null, null, null, Long.MIN_VALUE, Long.MAX_VALUE);
    this.dataRegionId = dataRegionId;
    this.shouldPrintMessage = shouldPrintMessage;
  }

  public PipeHeartbeatEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final String dataRegionId,
      final long timePublished,
      final boolean shouldPrintMessage) {
    super(pipeName, creationTime, pipeTaskMeta, null, null, Long.MIN_VALUE, Long.MAX_VALUE);
    this.dataRegionId = dataRegionId;
    this.timePublished = timePublished;
    this.shouldPrintMessage = shouldPrintMessage;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    if (Objects.nonNull(pipeName)) {
      PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
          .increaseHeartbeatEventCount(pipeName, creationTime);
    }
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    // PipeName == null indicates that the event is the raw event at disruptor,
    // not the event copied and passed to the extractor
    if (Objects.nonNull(pipeName)) {
      PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
          .decreaseHeartbeatEventCount(pipeName, creationTime);
      if (shouldPrintMessage && LOGGER.isDebugEnabled()) {
        LOGGER.debug(this.toString());
      }
    }
    return true;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return MinimumProgressIndex.INSTANCE;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    // Should record PipeTaskMeta, for sometimes HeartbeatEvents should report exceptions.
    // Here we ignore parameters `pattern`, `startTime`, and `endTime`.
    return new PipeHeartbeatEvent(
        pipeName, creationTime, pipeTaskMeta, dataRegionId, timePublished, shouldPrintMessage);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return true;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return true;
  }

  /////////////////////////////// Whether to print ///////////////////////////////

  public boolean isShouldPrintMessage() {
    return shouldPrintMessage;
  }

  /////////////////////////////// Delay Reporting ///////////////////////////////

  public void onPublished() {
    if (shouldPrintMessage) {
      timePublished = System.currentTimeMillis();
    }
  }

  public void onAssigned() {
    if (shouldPrintMessage) {
      timeAssigned = System.currentTimeMillis();
      if (timePublished != 0) {
        PipeHeartbeatEventMetrics.getInstance()
            .recordPublishedToAssignedTime(timeAssigned - timePublished);
      }
    }
  }

  public void onProcessed() {
    if (shouldPrintMessage) {
      timeProcessed = System.currentTimeMillis();
      if (timeAssigned != 0) {
        PipeHeartbeatEventMetrics.getInstance()
            .recordAssignedToProcessedTime(timeProcessed - timeAssigned);
      }
    }
  }

  public void onTransferred() {
    if (shouldPrintMessage) {
      timeTransferred = System.currentTimeMillis();
      if (timeProcessed != 0) {
        PipeHeartbeatEventMetrics.getInstance()
            .recordProcessedToTransferredTime(timeTransferred - timeProcessed);
      }
    }
  }

  /////////////////////////////// Queue size Reporting ///////////////////////////////

  public void recordDisruptorSize(final RingBuffer<?> ringBuffer) {
    if (shouldPrintMessage) {
      disruptorSize = ringBuffer.getBufferSize() - (int) ringBuffer.remainingCapacity();
    }
  }

  public void recordExtractorQueueSize(final UnboundedBlockingPendingQueue<Event> pendingQueue) {
    if (shouldPrintMessage) {
      extractorQueueTabletSize = pendingQueue.getTabletInsertionEventCount();
      extractorQueueTsFileSize = pendingQueue.getTsFileInsertionEventCount();
      extractorQueueSize = pendingQueue.size();
    }
  }

  public void recordConnectorQueueSize(final UnboundedBlockingPendingQueue<Event> pendingQueue) {
    if (shouldPrintMessage) {
      connectorQueueTabletSize = pendingQueue.getTabletInsertionEventCount();
      connectorQueueTsFileSize = pendingQueue.getTsFileInsertionEventCount();
      connectorQueueSize = pendingQueue.size();
    }
  }

  /////////////////////////////// For Commit Ordering ///////////////////////////////

  /** {@link PipeHeartbeatEvent}s do not need to be committed in order. */
  @Override
  public boolean needToCommit() {
    return false;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public String toString() {
    final String unknownMessage = "Unknown";

    final String startTimeMessage =
        (timePublished != 0)
            ? DateTimeUtils.convertLongToDate(timePublished, "ms")
            : unknownMessage;
    final String publishedToAssignedMessage =
        (timeAssigned != 0 && timePublished != 0)
            ? (timeAssigned - timePublished) + "ms"
            : unknownMessage;
    final String assignedToProcessedMessage =
        (timeProcessed != 0 && timeAssigned != 0)
            ? (timeProcessed - timeAssigned) + "ms"
            : unknownMessage;
    final String processedToTransferredMessage =
        (timeTransferred != 0 && timeProcessed != 0)
            ? (timeTransferred - timeProcessed) + "ms"
            : unknownMessage;
    final String totalTimeMessage =
        (timeTransferred != 0 && timePublished != 0)
            ? (timeTransferred - timePublished) + "ms"
            : unknownMessage;

    final String disruptorSizeMessage = Integer.toString(disruptorSize);

    final String extractorQueueTabletSizeMessage =
        timeAssigned != 0 ? Integer.toString(extractorQueueTabletSize) : unknownMessage;
    final String extractorQueueTsFileSizeMessage =
        timeAssigned != 0 ? Integer.toString(extractorQueueTsFileSize) : unknownMessage;
    final String extractorQueueSizeMessage =
        timeAssigned != 0 ? Integer.toString(extractorQueueSize) : unknownMessage;

    final String connectorQueueTabletSizeMessage =
        timeProcessed != 0 ? Integer.toString(connectorQueueTabletSize) : unknownMessage;
    final String connectorQueueTsFileSizeMessage =
        timeProcessed != 0 ? Integer.toString(connectorQueueTsFileSize) : unknownMessage;
    final String connectorQueueSizeMessage =
        timeProcessed != 0 ? Integer.toString(connectorQueueSize) : unknownMessage;

    return "PipeHeartbeatEvent{"
        + "pipeName='"
        + pipeName
        + "', dataRegionId="
        + dataRegionId
        + ", startTime="
        + startTimeMessage
        + ", publishedToAssigned="
        + publishedToAssignedMessage
        + ", assignedToProcessed="
        + assignedToProcessedMessage
        + ", processedToTransferred="
        + processedToTransferredMessage
        + ", totalTimeCost="
        + totalTimeMessage
        + ", disruptorSize="
        + disruptorSizeMessage
        + ", extractorQueueTabletSize="
        + extractorQueueTabletSizeMessage
        + ", extractorQueueTsFileSize="
        + extractorQueueTsFileSizeMessage
        + ", extractorQueueSize="
        + extractorQueueSizeMessage
        + ", connectorQueueTabletSize="
        + connectorQueueTabletSizeMessage
        + ", connectorQueueTsFileSize="
        + connectorQueueTsFileSizeMessage
        + ", connectorQueueSize="
        + connectorQueueSizeMessage
        + "}";
  }
}
