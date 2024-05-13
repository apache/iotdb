/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.schemaregion.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.metrics.core.uitls.IoTDBMovingAverage;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class PipeRemainingEventAndTimeOperator {
  private String pipeName;
  private long creationTime = 0;

  private final ConcurrentMap<IoTDBDataRegionExtractor, IoTDBDataRegionExtractor>
      dataRegionExtractors = new ConcurrentHashMap<>();
  private final ConcurrentMap<PipeProcessorSubtask, PipeProcessorSubtask> dataRegionProcessors =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<PipeConnectorSubtask, PipeConnectorSubtask> dataRegionConnectors =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<IoTDBSchemaRegionExtractor, IoTDBSchemaRegionExtractor>
      schemaRegionExtractors = new ConcurrentHashMap<>();
  private final Meter dataRegionCommitMeter =
      new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
  private final Meter schemaRegionCommitMeter =
      new Meter(new IoTDBMovingAverage(), Clock.defaultClock());

  private double lastDataRegionCommitSmoothingValue;
  private double lastSchemaRegionCommitSmoothingValue;

  //////////////////////////// Tags ////////////////////////////

  String getPipeName() {
    return pipeName;
  }

  long getCreationTime() {
    return creationTime;
  }

  //////////////////////////// Remaining time calculation ////////////////////////////

  double getRemainingEvents() {
    return dataRegionExtractors.keySet().stream()
            .map(IoTDBDataRegionExtractor::getEventCount)
            .reduce(Integer::sum)
            .orElse(0)
        + dataRegionProcessors.keySet().stream()
            .map(PipeProcessorSubtask::getEventCount)
            .reduce(Integer::sum)
            .orElse(0)
        + dataRegionConnectors.keySet().stream()
            .map(PipeConnectorSubtask::getEventCount)
            .reduce(Integer::sum)
            .orElse(0)
        + schemaRegionExtractors.keySet().stream()
            .map(IoTDBSchemaRegionExtractor::getUnTransferredEventCount)
            .reduce(Long::sum)
            .orElse(0L);
  }

  /**
   * This will calculate the estimated remaining time of pipe.
   *
   * <p>Notes:
   *
   * <p>1. The events in pipe assigner are omitted.
   *
   * <p>2. Other pipes' events sharing the same connectorSubtasks may be over-calculated.
   *
   * @return The estimated remaining time
   */
  double getRemainingTime() {
    final double pipeRemainingTimeCommitRateSmoothingFactor =
        PipeConfig.getInstance().getPipeRemainingTimeCommitRateSmoothingFactor();

    final int totalDataRegionWriteEventCount =
        dataRegionExtractors.keySet().stream()
                .map(IoTDBDataRegionExtractor::getEventCount)
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionProcessors.keySet().stream()
                .map(PipeProcessorSubtask::getEventCount)
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionConnectors.keySet().stream()
                .map(PipeConnectorSubtask::getEventCount)
                .reduce(Integer::sum)
                .orElse(0);

    lastDataRegionCommitSmoothingValue =
        pipeRemainingTimeCommitRateSmoothingFactor
            * dataRegionCommitMeter.getOneMinuteRate()
            * (1 - pipeRemainingTimeCommitRateSmoothingFactor)
            * lastDataRegionCommitSmoothingValue;
    final double dataRegionRemainingTime;
    if (totalDataRegionWriteEventCount == 0) {
      dataRegionRemainingTime = 0;
    } else {
      dataRegionRemainingTime =
          lastDataRegionCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : totalDataRegionWriteEventCount / lastDataRegionCommitSmoothingValue;
    }

    final long totalSchemaRegionWriteEventCount =
        schemaRegionExtractors.keySet().stream()
            .map(IoTDBSchemaRegionExtractor::getUnTransferredEventCount)
            .reduce(Long::sum)
            .orElse(0L);

    lastSchemaRegionCommitSmoothingValue =
        pipeRemainingTimeCommitRateSmoothingFactor
            * schemaRegionCommitMeter.getOneMinuteRate()
            * (1 - pipeRemainingTimeCommitRateSmoothingFactor)
            * lastSchemaRegionCommitSmoothingValue;
    final double schemaRegionRemainingTime;
    if (totalSchemaRegionWriteEventCount == 0) {
      schemaRegionRemainingTime = 0;
    } else {
      schemaRegionRemainingTime =
          lastSchemaRegionCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : totalSchemaRegionWriteEventCount / lastSchemaRegionCommitSmoothingValue;
    }

    final double result = Math.max(dataRegionRemainingTime, schemaRegionRemainingTime);
    return result == Double.MAX_VALUE ? -1 : result;
  }

  //////////////////////////// Register & deregister (pipe integration) ////////////////////////////

  void register(final IoTDBDataRegionExtractor extractor) {
    setNameAndCreationTime(extractor.getPipeName(), extractor.getCreationTime());
    dataRegionExtractors.put(extractor, extractor);
  }

  void register(final PipeProcessorSubtask processorSubtask) {
    setNameAndCreationTime(processorSubtask.getPipeName(), processorSubtask.getCreationTime());
    dataRegionProcessors.put(processorSubtask, processorSubtask);
  }

  void register(
      final PipeConnectorSubtask connectorSubtask, final String pipeName, final long creationTime) {
    setNameAndCreationTime(pipeName, creationTime);
    dataRegionConnectors.put(connectorSubtask, connectorSubtask);
  }

  void register(final IoTDBSchemaRegionExtractor extractor) {
    setNameAndCreationTime(extractor.getPipeName(), extractor.getCreationTime());
    schemaRegionExtractors.put(extractor, extractor);
  }

  private void setNameAndCreationTime(final String pipeName, final long creationTime) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
  }

  //////////////////////////// Rate ////////////////////////////
  void markDataRegionCommit() {
    dataRegionCommitMeter.mark();
  }

  void markSchemaRegionCommit() {
    schemaRegionCommitMeter.mark();
  }
}
