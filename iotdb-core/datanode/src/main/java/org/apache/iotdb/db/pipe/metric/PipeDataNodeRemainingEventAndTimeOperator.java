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

import org.apache.iotdb.commons.enums.PipeRemainingTimeRateAverageTime;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.metric.PipeRemainingOperator;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.schemaregion.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.pipe.api.event.Event;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentialMovingAverages;
import com.codahale.metrics.Meter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

class PipeDataNodeRemainingEventAndTimeOperator extends PipeRemainingOperator {
  private final Set<IoTDBDataRegionExtractor> dataRegionExtractors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<PipeProcessorSubtask> dataRegionProcessors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<PipeConnectorSubtask> dataRegionConnectors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<IoTDBSchemaRegionExtractor> schemaRegionExtractors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final AtomicReference<Meter> dataRegionCommitMeter = new AtomicReference<>(null);
  private final AtomicReference<Meter> schemaRegionCommitMeter = new AtomicReference<>(null);

  private double lastDataRegionCommitSmoothingValue = Long.MAX_VALUE;
  private double lastSchemaRegionCommitSmoothingValue = Long.MAX_VALUE;

  //////////////////////////// Remaining event & time calculation ////////////////////////////

  long getRemainingEvents() {
    return dataRegionExtractors.stream()
            .map(IoTDBDataRegionExtractor::getEventCount)
            .reduce(Integer::sum)
            .orElse(0)
        + dataRegionProcessors.stream()
            .map(processorSubtask -> processorSubtask.getEventCount(false))
            .reduce(Integer::sum)
            .orElse(0)
        + dataRegionConnectors.stream()
            .map(connectorSubtask -> connectorSubtask.getEventCount(pipeName))
            .reduce(Integer::sum)
            .orElse(0)
        + schemaRegionExtractors.stream()
            .map(IoTDBSchemaRegionExtractor::getUnTransferredEventCount)
            .reduce(Long::sum)
            .orElse(0L);
  }

  /**
   * This will calculate the estimated remaining time of pipe.
   *
   * <p>Note: The {@link Event}s in pipe assigner are omitted.
   *
   * @return The estimated remaining time
   */
  double getRemainingTime() {
    final PipeRemainingTimeRateAverageTime pipeRemainingTimeCommitRateAverageTime =
        PipeConfig.getInstance().getPipeRemainingTimeCommitRateAverageTime();

    // Do not take heartbeat event into account
    final int totalDataRegionWriteEventCount =
        dataRegionExtractors.stream()
                .map(IoTDBDataRegionExtractor::getEventCount)
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionProcessors.stream()
                .map(processorSubtask -> processorSubtask.getEventCount(true))
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionConnectors.stream()
                .map(connectorSubtask -> connectorSubtask.getEventCount(pipeName))
                .reduce(Integer::sum)
                .orElse(0)
            - dataRegionExtractors.stream()
                .map(IoTDBDataRegionExtractor::getPipeHeartbeatEventCount)
                .reduce(Integer::sum)
                .orElse(0)
            - dataRegionConnectors.stream()
                .map(PipeConnectorSubtask::getPipeHeartbeatEventCount)
                .reduce(Integer::sum)
                .orElse(0);

    dataRegionCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            lastDataRegionCommitSmoothingValue =
                pipeRemainingTimeCommitRateAverageTime.getMeterRate(meter);
          }
          return meter;
        });
    final double dataRegionRemainingTime;
    if (totalDataRegionWriteEventCount <= 0) {
      dataRegionRemainingTime = 0;
    } else {
      dataRegionRemainingTime =
          lastDataRegionCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : totalDataRegionWriteEventCount / lastDataRegionCommitSmoothingValue;
    }

    final long totalSchemaRegionWriteEventCount =
        schemaRegionExtractors.stream()
            .map(IoTDBSchemaRegionExtractor::getUnTransferredEventCount)
            .reduce(Long::sum)
            .orElse(0L);

    schemaRegionCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            lastSchemaRegionCommitSmoothingValue =
                pipeRemainingTimeCommitRateAverageTime.getMeterRate(meter);
          }
          return meter;
        });
    final double schemaRegionRemainingTime;
    if (totalSchemaRegionWriteEventCount <= 0) {
      schemaRegionRemainingTime = 0;
    } else {
      schemaRegionRemainingTime =
          lastSchemaRegionCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : totalSchemaRegionWriteEventCount / lastSchemaRegionCommitSmoothingValue;
    }

    if (totalDataRegionWriteEventCount + totalSchemaRegionWriteEventCount == 0) {
      notifyEmpty();
    } else {
      notifyNonEmpty();
    }

    final double result = Math.max(dataRegionRemainingTime, schemaRegionRemainingTime);
    return result >= REMAINING_MAX_SECONDS ? REMAINING_MAX_SECONDS : result;
  }

  //////////////////////////// Register & deregister (pipe integration) ////////////////////////////

  void register(final IoTDBDataRegionExtractor extractor) {
    setNameAndCreationTime(extractor.getPipeName(), extractor.getCreationTime());
    dataRegionExtractors.add(extractor);
  }

  void register(final PipeProcessorSubtask processorSubtask) {
    setNameAndCreationTime(processorSubtask.getPipeName(), processorSubtask.getCreationTime());
    dataRegionProcessors.add(processorSubtask);
  }

  void register(
      final PipeConnectorSubtask connectorSubtask, final String pipeName, final long creationTime) {
    setNameAndCreationTime(pipeName, creationTime);
    dataRegionConnectors.add(connectorSubtask);
  }

  void register(final IoTDBSchemaRegionExtractor extractor) {
    setNameAndCreationTime(extractor.getPipeName(), extractor.getCreationTime());
    schemaRegionExtractors.add(extractor);
  }

  //////////////////////////// Rate ////////////////////////////

  void markDataRegionCommit() {
    dataRegionCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            meter.mark();
          }
          return meter;
        });
  }

  void markSchemaRegionCommit() {
    schemaRegionCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            meter.mark();
          }
          return meter;
        });
  }

  //////////////////////////// Switch ////////////////////////////

  // Thread-safe & Idempotent
  @Override
  public synchronized void thawRate(final boolean isStartPipe) {
    super.thawRate(isStartPipe);
    // The stopped pipe's rate should only be thawed by "startPipe" command
    if (isStopped) {
      return;
    }
    dataRegionCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
    schemaRegionCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
  }

  // Thread-safe & Idempotent
  @Override
  public synchronized void freezeRate(final boolean isStopPipe) {
    super.freezeRate(isStopPipe);
    dataRegionCommitMeter.set(null);
    schemaRegionCommitMeter.set(null);
  }
}
