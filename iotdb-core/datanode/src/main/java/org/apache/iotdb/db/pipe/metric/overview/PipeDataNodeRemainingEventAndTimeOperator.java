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

import org.apache.iotdb.commons.enums.PipeRateAverage;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.metric.PipeRemainingOperator;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.metrics.core.IoTDBMetricManager;
import org.apache.iotdb.metrics.core.type.IoTDBHistogram;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.pipe.api.event.Event;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentialMovingAverages;
import com.codahale.metrics.Meter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PipeDataNodeRemainingEventAndTimeOperator extends PipeRemainingOperator {

  // Calculate from schema region extractors directly for it requires less computation
  private final Set<IoTDBSchemaRegionSource> schemaRegionExtractors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final AtomicInteger insertNodeEventCount = new AtomicInteger(0);
  private final AtomicInteger rawTabletEventCount = new AtomicInteger(0);
  private final AtomicInteger tsfileEventCount = new AtomicInteger(0);
  private final AtomicInteger heartbeatEventCount = new AtomicInteger(0);

  private final AtomicReference<Meter> dataRegionCommitMeter = new AtomicReference<>(null);
  private final AtomicReference<Meter> schemaRegionCommitMeter = new AtomicReference<>(null);
  private final IoTDBHistogram collectInvocationHistogram =
      (IoTDBHistogram) IoTDBMetricManager.getInstance().createHistogram();

  private Timer insertNodeTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tsfileTransferTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private double lastDataRegionCommitSmoothingValue = Long.MAX_VALUE;
  private double lastSchemaRegionCommitSmoothingValue = Long.MAX_VALUE;

  PipeDataNodeRemainingEventAndTimeOperator(final String pipeName, final long creationTime) {
    super(pipeName, creationTime);
  }

  //////////////////////////// Remaining event & time calculation ////////////////////////////

  void increaseInsertNodeEventCount() {
    insertNodeEventCount.incrementAndGet();
  }

  void decreaseInsertNodeEventCount() {
    insertNodeEventCount.decrementAndGet();
  }

  void increaseRawTabletEventCount() {
    rawTabletEventCount.incrementAndGet();
  }

  void decreaseRawTabletEventCount() {
    rawTabletEventCount.decrementAndGet();
  }

  void increaseTsFileEventCount() {
    tsfileEventCount.incrementAndGet();
  }

  void decreaseTsFileEventCount() {
    tsfileEventCount.decrementAndGet();
  }

  void increaseHeartbeatEventCount() {
    heartbeatEventCount.incrementAndGet();
  }

  void decreaseHeartbeatEventCount() {
    heartbeatEventCount.decrementAndGet();
  }

  public long getRemainingNonHeartbeatEvents() {
    final long remainingEvents =
        tsfileEventCount.get()
            + rawTabletEventCount.get()
            + insertNodeEventCount.get()
            + schemaRegionExtractors.stream()
                .map(IoTDBSchemaRegionSource::getUnTransferredEventCount)
                .reduce(Long::sum)
                .orElse(0L);

    // There are cases where the indicator is negative. For example, after the Pipe is restarted,
    // the Processor SubTask is still collecting Events, resulting in a negative count. This
    // situation cannot be avoided because the Pipe may be restarted internally.
    return remainingEvents >= 0 ? remainingEvents : 0;
  }

  public int getInsertNodeEventCount() {
    return insertNodeEventCount.get();
  }

  /**
   * This will calculate the estimated remaining time of pipe.
   *
   * <p>Note: The {@link Event}s in pipe assigner are omitted.
   *
   * @return The estimated remaining time
   */
  public double getRemainingTime() {
    final PipeRateAverage pipeRemainingTimeCommitRateAverageTime =
        PipeConfig.getInstance().getPipeRemainingTimeCommitRateAverageTime();

    final double invocationValue = collectInvocationHistogram.getMean();
    // Do not take heartbeat event into account
    final double totalDataRegionWriteEventCount =
        tsfileEventCount.get() * Math.max(invocationValue, 1)
            + rawTabletEventCount.get()
            + insertNodeEventCount.get();

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
            .map(IoTDBSchemaRegionSource::getUnTransferredEventCount)
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

  void register(final IoTDBSchemaRegionSource extractor) {
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

  void markTsFileCollectInvocationCount(final long collectInvocationCount) {
    // If collectInvocationCount == 0, the event will still be committed once
    collectInvocationHistogram.update(Math.max(collectInvocationCount, 1));
  }

  public void setInsertNodeTransferTimer(Timer insertNodeTransferTimer) {
    this.insertNodeTransferTimer = insertNodeTransferTimer;
  }

  public Timer getInsertNodeTransferTimer() {
    return insertNodeTransferTimer;
  }

  public void setTsFileTransferTimer(Timer tsFileTransferTimer) {
    this.tsfileTransferTimer = tsFileTransferTimer;
  }

  public Timer getTsFileTransferTimer() {
    return tsfileTransferTimer;
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
