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
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.connection.EnrichedDeque;
import org.apache.iotdb.db.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.event.Event;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeHeartbeatEvent extends EnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatEvent.class);

  private final String dataRegionId;
  private String pipeName;

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

  private int bufferQueueTabletSize;
  private int bufferQueueTsFileSize;
  private int bufferQueueSize;

  private int connectorQueueTabletSize;
  private int connectorQueueTsFileSize;
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
    return MinimumProgressIndex.INSTANCE;
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
      extractorQueueTabletSize = pendingQueue.getTabletInsertionEventCount();
      extractorQueueTsFileSize = pendingQueue.getTsFileInsertionEventCount();
      extractorQueueSize = pendingQueue.size();
    }
  }

  public void recordBufferQueueSize(EnrichedDeque<Event> bufferQueue) {
    if (shouldPrintMessage) {
      bufferQueueTabletSize = bufferQueue.getTabletInsertionEventCount();
      bufferQueueTsFileSize = bufferQueue.getTsFileInsertionEventCount();
      bufferQueueSize = bufferQueue.size();
    }
  }

  public void recordConnectorQueueSize(BoundedBlockingPendingQueue<Event> pendingQueue) {
    if (shouldPrintMessage) {
      connectorQueueTabletSize = pendingQueue.getTabletInsertionEventCount();
      connectorQueueTsFileSize = pendingQueue.getTsFileInsertionEventCount();
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

    final String extractorQueueTabletSizeMessage =
        timeAssigned != 0 ? Integer.toString(extractorQueueTabletSize) : unknownMessage;
    final String extractorQueueTsFileSizeMessage =
        timeAssigned != 0 ? Integer.toString(extractorQueueTsFileSize) : unknownMessage;
    final String extractorQueueSizeMessage =
        timeAssigned != 0 ? Integer.toString(extractorQueueSize) : unknownMessage;

    final String bufferQueueTabletSizeMessage =
        timeProcessed != 0 ? Integer.toString(bufferQueueTabletSize) : unknownMessage;
    final String bufferQueueTsFileSizeMessage =
        timeProcessed != 0 ? Integer.toString(bufferQueueTsFileSize) : unknownMessage;
    final String bufferQueueSizeMessage =
        timeProcessed != 0 ? Integer.toString(bufferQueueSize) : unknownMessage;

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
        + CommonDateTimeUtils.convertLongToDate(timePublished, "ms")
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
        + ", bufferQueueTabletSize="
        + bufferQueueTabletSizeMessage
        + ", bufferQueueTsFileSize="
        + bufferQueueTsFileSizeMessage
        + ", bufferQueueSize="
        + bufferQueueSizeMessage
        + ", connectorQueueTabletSize="
        + connectorQueueTabletSizeMessage
        + ", connectorQueueTsFileSize="
        + connectorQueueTsFileSizeMessage
        + ", connectorQueueSize="
        + connectorQueueSizeMessage
        + "}";
  }
}
