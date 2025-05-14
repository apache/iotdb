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

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.enums.PipeRemainingTimeRateAverageTime;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.metric.PipeRemainingOperator;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.schemaregion.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.pipe.api.event.Event;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentialMovingAverages;
import com.codahale.metrics.Meter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class PipeDataNodeRemainingEventAndTimeOperator extends PipeRemainingOperator {

  // Calculate from schema region extractors directly for it requires less computation
  private final Set<IoTDBSchemaRegionExtractor> schemaRegionExtractors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final AtomicInteger tabletEventCount = new AtomicInteger(0);
  private final AtomicInteger tsfileEventCount = new AtomicInteger(0);
  private final AtomicLong tsFileEventSize = new AtomicLong(0L);

  private final AtomicReference<Meter> tabletCommitMeter = new AtomicReference<>(null);
  private final AtomicReference<Meter> tsfileSizeCommitMeter = new AtomicReference<>(null);
  private final AtomicReference<Meter> schemaRegionCommitMeter = new AtomicReference<>(null);
  private final Meter dataRegionLatencyMeter =
      new Meter(new ExponentialMovingAverages(), Clock.defaultClock());

  private double lastTabletCommitSmoothingValue = Double.MAX_VALUE;
  private double lastTsFileSizeCommitSmoothingValue = Double.MAX_VALUE;
  private double lastSchemaRegionCommitSmoothingValue = Double.MAX_VALUE;

  PipeDataNodeRemainingEventAndTimeOperator(final String pipeName, final long creationTime) {
    super(pipeName, creationTime);
  }

  //////////////////////////// Remaining event & time calculation ////////////////////////////

  void increaseTabletEventCount() {
    tabletEventCount.incrementAndGet();
  }

  void increaseTsFileEventSize(final long size) {
    tsfileEventCount.incrementAndGet();
    tsFileEventSize.addAndGet(size);
  }

  long getRemainingEvents() {
    final long remainingEvents =
        tsfileEventCount.get()
            + tabletEventCount.get()
            + schemaRegionExtractors.stream()
                .map(IoTDBSchemaRegionExtractor::getUnTransferredEventCount)
                .reduce(Long::sum)
                .orElse(0L);

    // There are cases where the indicator is negative. For example, after the Pipe is restarted,
    // the Processor SubTask is still collecting Events, resulting in a negative count. This
    // situation cannot be avoided because the Pipe may be restarted internally.
    return remainingEvents >= 0 ? remainingEvents : 0;
  }

  long getTsFileRemainingEvents() {
    final long value = tsfileEventCount.get();
    return value > 0 ? value : 0;
  }

  long getTabletRemainingEvents() {
    final long value = tabletEventCount.get();
    return value > 0 ? value : 0;
  }

  long getSchemaRemainingEvents() {
    return schemaRegionExtractors.stream()
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

    tabletCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            lastTabletCommitSmoothingValue =
                pipeRemainingTimeCommitRateAverageTime.getMeterRate(meter);
          }
          return meter;
        });
    final double tabletRemainingTime;
    if (tabletEventCount.get() <= 0) {
      tabletRemainingTime = 0;
    } else {
      tabletRemainingTime =
          lastTabletCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : tabletEventCount.get() / lastTabletCommitSmoothingValue;
    }

    if (tabletRemainingTime != Double.MAX_VALUE) {
      dataRegionLatencyMeter.mark((long) tabletRemainingTime);
    }

    tsfileSizeCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            lastTsFileSizeCommitSmoothingValue =
                pipeRemainingTimeCommitRateAverageTime.getMeterRate(meter);
          }
          return meter;
        });
    final double tsFileRemainingTime;
    if (tsFileEventSize.get() <= 0) {
      tsFileRemainingTime = 0;
    } else {
      tsFileRemainingTime =
          lastTsFileSizeCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : tsFileEventSize.get() / lastTsFileSizeCommitSmoothingValue;
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

    if (tabletEventCount.get() + tsfileEventCount.get() + totalSchemaRegionWriteEventCount <= 0) {
      notifyEmpty();
    } else {
      notifyNonEmpty();
    }

    double result;

    // If tsFile / tablet rate does not exist, use another one
    // If both exist, use the larger one
    if (tsFileRemainingTime == Double.MAX_VALUE) {
      result = tabletRemainingTime;
    } else if (tabletRemainingTime == Double.MAX_VALUE) {
      result = tsFileRemainingTime;
    } else {
      result = Math.max(tabletRemainingTime, tsFileRemainingTime);
    }

    result = Math.max(schemaRegionRemainingTime, result);

    if (result > REMAINING_MAX_SECONDS) {
      result = REMAINING_MAX_SECONDS;
    }

    return result;
  }

  // We use smoothed rate to in hybrid degrading algorithm to prevent outliers from triggering it
  double getLatencySmoothingValue() {
    return PipeConfig.getInstance()
        .getPipeRemainingTimeCommitRateAverageTime()
        .getMeterRate(dataRegionLatencyMeter);
  }

  //////////////////////////// Register & deregister (pipe integration) ////////////////////////////

  void register(final IoTDBSchemaRegionExtractor extractor) {
    schemaRegionExtractors.add(extractor);
  }

  //////////////////////////// Rate ////////////////////////////

  void markRegionCommit(final EnrichedEvent event) {
    if (!event.isDataRegionEvent()) {
      schemaRegionCommitMeter.updateAndGet(
          meter -> {
            if (Objects.nonNull(meter)) {
              meter.mark();
            }
            return meter;
          });
      return;
    }

    final EnrichedEvent rootEvent = event.getRootEvent();
    if (rootEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      tabletEventCount.decrementAndGet();
      tabletCommitMeter.updateAndGet(
          meter -> {
            if (Objects.nonNull(meter)) {
              meter.mark();
            }
            return meter;
          });
    } else if (rootEvent instanceof PipeTsFileInsertionEvent) {
      final long length = ((PipeTsFileInsertionEvent) rootEvent).getFileSize();
      tsfileEventCount.decrementAndGet();
      tsFileEventSize.addAndGet(-length);
      tsfileSizeCommitMeter.updateAndGet(
          meter -> {
            if (Objects.nonNull(meter)) {
              meter.mark(length);
            }
            return meter;
          });
    }
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
    tabletCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
    tsfileSizeCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
    schemaRegionCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
  }

  // Thread-safe & Idempotent
  @Override
  public synchronized void freezeRate(final boolean isStopPipe) {
    super.freezeRate(isStopPipe);
    tabletCommitMeter.set(null);
    tsfileSizeCommitMeter.set(null);
    schemaRegionCommitMeter.set(null);
  }
}
