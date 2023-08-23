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
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.event.dml.heartbeat.HeartbeatEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeHeartbeatEvent extends EnrichedEvent implements HeartbeatEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHeartbeatEvent.class);

  private final String dataRegionId;
  private String pipeName;

  private long disruptTime;
  private long extractTime;
  private long processTime;
  private long transferTime;

  public PipeHeartbeatEvent(String dataRegionId) {
    super(null, null);
    this.dataRegionId = dataRegionId;
  }

  // The disruptTime has been recorded in the previous event
  public PipeHeartbeatEvent(String dataRegionId, long disruptTime) {
    super(null, null);
    this.dataRegionId = dataRegionId;
    this.disruptTime = disruptTime;
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
    return null;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeHeartbeatEvent(dataRegionId, disruptTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  /////////////////////////////// Report ///////////////////////////////

  @Override
  public void reportDisrupt() {
    disruptTime = System.currentTimeMillis();
  }

  @Override
  public void reportExtract() {
    extractTime = System.currentTimeMillis();
  }

  @Override
  public void reportProcess() {
    processTime = System.currentTimeMillis();
  }

  @Override
  public void reportTransfer() {
    transferTime = System.currentTimeMillis();
  }

  @Override
  public void bindPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  @Override
  public String toString() {
    String errorMsg = "error";

    String disruptToExtractMsg = extractTime != 0 ? (extractTime - disruptTime) + "ms" : errorMsg;
    String extractToProcessMsg = processTime != 0 ? (processTime - extractTime) + "ms" : errorMsg;
    String processToTransferMsg =
        transferTime != 0 ? (transferTime - processTime) + "ms" : errorMsg;
    String totalTimeMsg = transferTime != 0 ? (transferTime - disruptTime) + "ms" : errorMsg;

    return "PipeHeartbeatEvent{"
        + "pipeName='"
        + pipeName
        + "', dataRegionId="
        + dataRegionId
        + ", startTime="
        + DateTimeUtils.convertLongToDate(disruptTime, "ms")
        + ", disruptToExtract="
        + disruptToExtractMsg
        + ", extractToProcess="
        + extractToProcessMsg
        + ", processToTransfer="
        + processToTransferMsg
        + ", totalTime="
        + totalTimeMsg
        + "}";
  }
}
