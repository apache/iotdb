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
import org.apache.iotdb.db.utils.DateTimeUtils;

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

  public PipeHeartbeatEvent(String dataRegionId) {
    super(null, null);
    this.dataRegionId = dataRegionId;
  }

  public PipeHeartbeatEvent(String dataRegionId, long timePublished) {
    super(null, null);
    this.dataRegionId = dataRegionId;
    this.timePublished = timePublished;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    // PipeName == null indicates that the event is the raw event at disruptor,
    // not the event copied and passed to the extractor
    if (pipeName != null && LOGGER.isInfoEnabled()) {
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
    return new PipeHeartbeatEvent(dataRegionId, timePublished);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  /////////////////////////////// Delay Reporting ///////////////////////////////

  public void bindPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public void onPublished() {
    timePublished = System.currentTimeMillis();
  }

  public void onAssigned() {
    timeAssigned = System.currentTimeMillis();
  }

  public void onProcessed() {
    timeProcessed = System.currentTimeMillis();
  }

  public void onTransferred() {
    timeTransferred = System.currentTimeMillis();
  }

  @Override
  public String toString() {
    final String errorMessage = "error";

    final String publishedToAssignedMessage =
        timeAssigned != 0 ? (timeAssigned - timePublished) + "ms" : errorMessage;
    final String assignedToProcessedMessage =
        timeProcessed != 0 ? (timeProcessed - timeAssigned) + "ms" : errorMessage;
    final String processedToTransferredMessage =
        timeTransferred != 0 ? (timeTransferred - timeProcessed) + "ms" : errorMessage;
    final String totalTimeMessage =
        timeTransferred != 0 ? (timeTransferred - timePublished) + "ms" : errorMessage;

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
        + "}";
  }
}
