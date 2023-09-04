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
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.event.Event;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;

public class PipeHeartbeatEvent extends EnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatEvent.class);

  private final String dataRegionId;
  private String pipeName;

  private long timePublished;
  private long timeAssigned;
  private long timeProcessed;
  private long timeTransferred;

  private int disruptorSize;
  private int extractorQueueSize;
  private int bufferQueueSize;
  private int connectorQueueSize;

  private final boolean shouldPrintMessage;

  public PipeHeartbeatEvent(String dataRegionId, boolean shouldPrintMessage) {
    super(null, null);
    this.dataRegionId = dataRegionId;
    this.shouldPrintMessage = shouldPrintMessage;
  }

  public PipeHeartbeatEvent(String dataRegionId, long timePublished, boolean shouldPrintMessage) {
    super(null, null);
    this.dataRegionId = dataRegionId;
    this.timePublished = timePublished;
    this.shouldPrintMessage = shouldPrintMessage;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    // PipeName == null indicates that the event is the raw event at disruptor,
    // not the event copied and passed to the extractor
    if (shouldPrintMessage && pipeName != null && LOGGER.isInfoEnabled()) {
      LOGGER.info(this.toString());
    }
    return true;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return new MinimumProgressIndex();
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeHeartbeatEvent(dataRegionId, timePublished, shouldPrintMessage);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  /////////////////////////////// Whether to print ///////////////////////////////

  public boolean isShouldPrintMessage() {
    return shouldPrintMessage;
  }

  /////////////////////////////// Delay Reporting ///////////////////////////////

  public void bindPipeName(String pipeName) {
    if (shouldPrintMessage) {
      this.pipeName = pipeName;
    }
  }

  public void onPublished() {
    if (shouldPrintMessage) {
      timePublished = System.currentTimeMillis();
    }
  }

  public void onAssigned() {
    if (shouldPrintMessage) {
      timeAssigned = System.currentTimeMillis();
    }
  }

  public void onProcessed() {
    if (shouldPrintMessage) {
      timeProcessed = System.currentTimeMillis();
    }
  }

  public void onTransferred() {
    if (shouldPrintMessage) {
      timeTransferred = System.currentTimeMillis();
    }
  }

  /////////////////////////////// Queue size Reporting ///////////////////////////////

  public void recordDisruptorSize(RingBuffer<?> ringBuffer) {
    if (shouldPrintMessage) {
      disruptorSize = ringBuffer.getBufferSize() - (int) ringBuffer.remainingCapacity();
    }
  }

  public void recordExtractorQueueSize(UnboundedBlockingPendingQueue<Event> pendingQueue) {
    if (shouldPrintMessage) {
      extractorQueueSize = pendingQueue.size();
    }
  }

  public void recordBufferQueueSize(Deque<Event> bufferQueue) {
    if (shouldPrintMessage) {
      bufferQueueSize = bufferQueue.size();
    }
  }

  public void recordConnectorQueueSize(BoundedBlockingPendingQueue<Event> pendingQueue) {
    if (shouldPrintMessage) {
      connectorQueueSize = pendingQueue.size();
    }
  }

  @Override
  public String toString() {
    final String unknownMessage = "Unknown";

    final String publishedToAssignedMessage =
        timeAssigned != 0 ? (timeAssigned - timePublished) + "ms" : unknownMessage;
    final String assignedToProcessedMessage =
        timeProcessed != 0 ? (timeProcessed - timeAssigned) + "ms" : unknownMessage;
    final String processedToTransferredMessage =
        timeTransferred != 0 ? (timeTransferred - timeProcessed) + "ms" : unknownMessage;
    final String totalTimeMessage =
        timeTransferred != 0 ? (timeTransferred - timePublished) + "ms" : unknownMessage;

    final String disruptorSizeMessage = Integer.toString(disruptorSize);
    final String extractorQueueSizeMessage =
        timeAssigned != 0 ? Integer.toString(extractorQueueSize) : unknownMessage;
    final String bufferQueueSizeMessage =
        timeProcessed != 0 ? Integer.toString(bufferQueueSize) : unknownMessage;
    final String connectorQueueSizeMessage =
        timeProcessed != 0 ? Integer.toString(connectorQueueSize) : unknownMessage;

    return "PipeHeartbeatEvent{"
        + "pipeName='"
        + pipeName
        + "', dataRegionId="
        + dataRegionId
        + ", startTime="
        + DateTimeUtils.convertLongToDate(timePublished, "ms")
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
        + ", extractorQueueSize="
        + extractorQueueSizeMessage
        + ", bufferQueueSize="
        + bufferQueueSizeMessage
        + ", connectorQueueSize="
        + connectorQueueSizeMessage
        + "}";
  }
}
