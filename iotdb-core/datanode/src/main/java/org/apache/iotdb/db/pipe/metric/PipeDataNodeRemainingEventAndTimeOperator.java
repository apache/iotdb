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
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.metric.PipeRemainingOperator;
import org.apache.iotdb.db.pipe.agent.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.schemaregion.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.pipe.api.event.Event;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentialMovingAverages;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

class PipeDataNodeRemainingEventAndTimeOperator extends PipeRemainingOperator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeDataNodeRemainingEventAndTimeOperator.class);

  private final Set<IoTDBDataRegionExtractor> dataRegionExtractors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<PipeProcessorSubtask> dataRegionProcessors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<PipeConnectorSubtask> dataRegionConnectors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<IoTDBSchemaRegionExtractor> schemaRegionExtractors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final AtomicReference<Meter> dataRegionHistoricalEventCommitMeter =
      new AtomicReference<>(null);
  private final AtomicReference<Meter> dataRegionRealtimeEventCommitMeter =
      new AtomicReference<>(null);
  private final AtomicReference<Meter> schemaRegionCommitMeter = new AtomicReference<>(null);

  private double lastDataRegionHistoricalEventCommitSmoothingValue = Long.MAX_VALUE;
  private double lastDataRegionRealtimeEventCommitSmoothingValue = Long.MAX_VALUE;
  private double lastSchemaRegionCommitSmoothingValue = Long.MAX_VALUE;

  //////////////////////////// Remaining event & time calculation ////////////////////////////

  private static final Predicate<EnrichedEvent> NEED_TO_COMMIT_RATE =
      EnrichedEvent::needToCommitRate;
  private static final Predicate<EnrichedEvent> IS_DATA_REGION_REALTIME_EVENT =
      EnrichedEvent::isDataRegionRealtimeEvent;
  private static final Predicate<EnrichedEvent> IS_DATA_REGION_HISTORICAL_EVENT =
      event -> !event.isDataRegionRealtimeEvent();

  @SafeVarargs
  private static Predicate<EnrichedEvent> filter(
      final String pipeName, final Predicate<EnrichedEvent>... predicates) {
    return event -> {
      if (Objects.isNull(event)) {
        return false;
      }
      if (!Objects.equals(event.getPipeName(), pipeName)) {
        return false;
      }
      for (final Predicate<EnrichedEvent> predicate : predicates) {
        if (!predicate.test(event)) {
          return false;
        }
      }
      return true;
    };
  }

  long getRemainingEvents() {
    return dataRegionExtractors.stream()
            .map(IoTDBDataRegionExtractor::getHistoricalTsFileInsertionEventCount)
            .reduce(Integer::sum)
            .orElse(0)
        + dataRegionExtractors.stream()
            .map(extractor -> extractor.getRealtimeEventCount(filter(pipeName)))
            .reduce(Integer::sum)
            .orElse(0)
        + dataRegionProcessors.stream()
            .map(processor -> processor.getEventCount(filter(pipeName)))
            .reduce(Integer::sum)
            .orElse(0)
        + dataRegionConnectors.stream()
            .map(connector -> connector.getEventCount(filter(pipeName)))
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

    // data region historical event
    final double totalDataRegionWriteHistoricalEventCount =
        dataRegionExtractors.stream()
                .map(IoTDBDataRegionExtractor::getHistoricalTsFileInsertionEventCount)
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionProcessors.stream()
                .map(
                    processor ->
                        processor.getEventCount(
                            filter(pipeName, NEED_TO_COMMIT_RATE, IS_DATA_REGION_HISTORICAL_EVENT)))
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionConnectors.stream()
                .map(
                    connector ->
                        connector.getEventCount(
                            filter(pipeName, NEED_TO_COMMIT_RATE, IS_DATA_REGION_HISTORICAL_EVENT)))
                .reduce(Integer::sum)
                .orElse(0);

    dataRegionHistoricalEventCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            lastDataRegionHistoricalEventCommitSmoothingValue =
                pipeRemainingTimeCommitRateAverageTime.getMeterRate(meter);
          }
          return meter;
        });

    final double dataRegionHistoricalEventRemainingTime;
    if (totalDataRegionWriteHistoricalEventCount <= 0) {
      dataRegionHistoricalEventRemainingTime = 0;
    } else {
      dataRegionHistoricalEventRemainingTime =
          lastDataRegionHistoricalEventCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : totalDataRegionWriteHistoricalEventCount
                  / lastDataRegionHistoricalEventCommitSmoothingValue;
    }

    // data region realtime event
    final double totalDataRegionWriteRealtimeEventCount =
        dataRegionExtractors.stream()
                .map(
                    extractor ->
                        extractor.getRealtimeEventCount(filter(pipeName, NEED_TO_COMMIT_RATE)))
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionProcessors.stream()
                .map(
                    processor ->
                        processor.getEventCount(
                            filter(pipeName, NEED_TO_COMMIT_RATE, IS_DATA_REGION_REALTIME_EVENT)))
                .reduce(Integer::sum)
                .orElse(0)
            + dataRegionConnectors.stream()
                .map(
                    connector ->
                        connector.getEventCount(
                            filter(pipeName, NEED_TO_COMMIT_RATE, IS_DATA_REGION_REALTIME_EVENT)))
                .reduce(Integer::sum)
                .orElse(0);

    dataRegionRealtimeEventCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            lastDataRegionRealtimeEventCommitSmoothingValue =
                pipeRemainingTimeCommitRateAverageTime.getMeterRate(meter);
          }
          return meter;
        });

    final double dataRegionRealtimeEventRemainingTime;
    if (totalDataRegionWriteRealtimeEventCount <= 0) {
      dataRegionRealtimeEventRemainingTime = 0;
    } else {
      dataRegionRealtimeEventRemainingTime =
          lastDataRegionRealtimeEventCommitSmoothingValue <= 0
              ? 0 // NOTE HERE
              : totalDataRegionWriteRealtimeEventCount
                  / lastDataRegionRealtimeEventCommitSmoothingValue;
    }

    // schema region event
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

    if (totalDataRegionWriteHistoricalEventCount
            + totalDataRegionWriteRealtimeEventCount
            + totalSchemaRegionWriteEventCount
        == 0) {
      notifyEmpty();
    } else {
      notifyNonEmpty();
    }

    final double result =
        Math.max(
            dataRegionHistoricalEventRemainingTime + dataRegionRealtimeEventRemainingTime,
            schemaRegionRemainingTime);
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

  void markDataRegionCommit(final boolean isDataRegionRealtimeEvent) {
    if (isDataRegionRealtimeEvent) {
      dataRegionRealtimeEventCommitMeter.updateAndGet(
          meter -> {
            if (Objects.nonNull(meter)) {
              meter.mark();
            }
            return meter;
          });
    } else {
      dataRegionHistoricalEventCommitMeter.updateAndGet(
          meter -> {
            if (Objects.nonNull(meter)) {
              meter.mark();
            }
            return meter;
          });
    }
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
    dataRegionHistoricalEventCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
    dataRegionRealtimeEventCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
    schemaRegionCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
  }

  // Thread-safe & Idempotent
  @Override
  public synchronized void freezeRate(final boolean isStopPipe) {
    super.freezeRate(isStopPipe);
    dataRegionHistoricalEventCommitMeter.set(null);
    dataRegionRealtimeEventCommitMeter.set(null);
    schemaRegionCommitMeter.set(null);
  }
}
