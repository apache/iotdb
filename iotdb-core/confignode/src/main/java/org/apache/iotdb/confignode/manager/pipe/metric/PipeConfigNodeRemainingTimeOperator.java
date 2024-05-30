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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.manager.pipe.extractor.IoTDBConfigRegionExtractor;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentialMovingAverages;
import com.codahale.metrics.Meter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class PipeConfigNodeRemainingTimeOperator {

  private static final long CONFIG_NODE_REMAINING_MAX_SECONDS = 365 * 24 * 60 * 60L; // 1 year

  private String pipeName;
  private long creationTime = 0;

  private final ConcurrentMap<IoTDBConfigRegionExtractor, IoTDBConfigRegionExtractor>
      configRegionExtractors = new ConcurrentHashMap<>();
  private final Meter configRegionCommitMeter =
      new Meter(new ExponentialMovingAverages(), Clock.defaultClock());

  private double lastConfigRegionCommitSmoothingValue = Long.MIN_VALUE;

  //////////////////////////// Tags ////////////////////////////

  String getPipeName() {
    return pipeName;
  }

  long getCreationTime() {
    return creationTime;
  }

  //////////////////////////// Remaining time calculation ////////////////////////////

  /**
   * This will calculate the estimated remaining time of the given pipe's config region subTask.
   *
   * @return The estimated remaining time
   */
  double getRemainingTime() {
    final double pipeRemainingTimeCommitRateSmoothingFactor =
        PipeConfig.getInstance().getPipeRemainingTimeCommitRateSmoothingFactor();

    // Do not calculate heartbeat event
    final long totalConfigRegionWriteEventCount =
        configRegionExtractors.keySet().stream()
            .map(IoTDBConfigRegionExtractor::getUnTransferredEventCount)
            .reduce(Long::sum)
            .orElse(0L);

    lastConfigRegionCommitSmoothingValue =
        lastConfigRegionCommitSmoothingValue == Long.MIN_VALUE
            ? configRegionCommitMeter.getOneMinuteRate()
            : pipeRemainingTimeCommitRateSmoothingFactor
                    * configRegionCommitMeter.getOneMinuteRate()
                + (1 - pipeRemainingTimeCommitRateSmoothingFactor)
                    * lastConfigRegionCommitSmoothingValue;
    final double configRegionRemainingTime;
    if (totalConfigRegionWriteEventCount <= 0) {
      configRegionRemainingTime = 0;
    } else {
      configRegionRemainingTime =
          lastConfigRegionCommitSmoothingValue <= 0
              ? Double.MAX_VALUE
              : totalConfigRegionWriteEventCount / lastConfigRegionCommitSmoothingValue;
    }

    return configRegionRemainingTime >= CONFIG_NODE_REMAINING_MAX_SECONDS
        ? CONFIG_NODE_REMAINING_MAX_SECONDS
        : configRegionRemainingTime;
  }

  //////////////////////////// Register & deregister (pipe integration) ////////////////////////////

  void register(final IoTDBConfigRegionExtractor extractor) {
    setNameAndCreationTime(extractor.getPipeName(), extractor.getCreationTime());
    configRegionExtractors.put(extractor, extractor);
  }

  private void setNameAndCreationTime(final String pipeName, final long creationTime) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
  }

  //////////////////////////// Rate ////////////////////////////

  void markConfigRegionCommit() {
    configRegionCommitMeter.mark();
  }
}
