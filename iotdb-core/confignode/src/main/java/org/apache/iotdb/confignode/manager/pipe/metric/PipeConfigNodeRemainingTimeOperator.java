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

package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.commons.enums.PipeRemainingTimeRateAverageTime;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.metric.PipeRemainingOperator;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeConfigNodeSubtask;
import org.apache.iotdb.confignode.manager.pipe.extractor.IoTDBConfigRegionExtractor;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentialMovingAverages;
import com.codahale.metrics.Meter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

class PipeConfigNodeRemainingTimeOperator extends PipeRemainingOperator {

  private final Set<IoTDBConfigRegionExtractor> configRegionExtractors =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final AtomicReference<Meter> configRegionCommitMeter = new AtomicReference<>(null);

  private double lastConfigRegionCommitSmoothingValue = Long.MAX_VALUE;

  PipeConfigNodeRemainingTimeOperator(String pipeName, long creationTime) {
    super(pipeName, creationTime);
  }

  //////////////////////////// Remaining time calculation ////////////////////////////

  /**
   * This will calculate the estimated remaining time of the given pipe's {@link
   * PipeConfigNodeSubtask}.
   *
   * @return The estimated remaining time
   */
  double getRemainingTime() {
    final PipeRemainingTimeRateAverageTime pipeRemainingTimeCommitRateAverageTime =
        PipeConfig.getInstance().getPipeRemainingTimeCommitRateAverageTime();

    // Do not calculate heartbeat event
    final long totalConfigRegionWriteEventCount =
        configRegionExtractors.stream()
            .map(IoTDBConfigRegionExtractor::getUnTransferredEventCount)
            .reduce(Long::sum)
            .orElse(0L);

    configRegionCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            lastConfigRegionCommitSmoothingValue =
                pipeRemainingTimeCommitRateAverageTime.getMeterRate(meter);
          }
          return meter;
        });

    final double configRegionRemainingTime;
    if (totalConfigRegionWriteEventCount <= 0) {
      notifyEmpty();
      configRegionRemainingTime = 0;
    } else {
      notifyNonEmpty();
      configRegionRemainingTime =
          lastConfigRegionCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : totalConfigRegionWriteEventCount / lastConfigRegionCommitSmoothingValue;
    }

    return configRegionRemainingTime >= REMAINING_MAX_SECONDS
        ? REMAINING_MAX_SECONDS
        : configRegionRemainingTime;
  }

  //////////////////////////// Register & deregister (pipe integration) ////////////////////////////

  void register(final IoTDBConfigRegionExtractor extractor) {
    configRegionExtractors.add(extractor);
  }

  //////////////////////////// Rate ////////////////////////////

  void markConfigRegionCommit() {
    configRegionCommitMeter.updateAndGet(
        meter -> {
          if (Objects.nonNull(meter)) {
            meter.mark();
          }
          return meter;
        });
  }

  //////////////////////////// Switch ////////////////////////////

  @Override
  public synchronized void thawRate(final boolean isStartPipe) {
    super.thawRate(isStartPipe);
    // The stopped pipe's rate should only be thawed by "startPipe" command
    if (isStopped) {
      return;
    }
    configRegionCommitMeter.compareAndSet(
        null, new Meter(new ExponentialMovingAverages(), Clock.defaultClock()));
  }

  @Override
  public synchronized void freezeRate(final boolean isStopPipe) {
    super.freezeRate(isStopPipe);
    configRegionCommitMeter.set(null);
  }
}
