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

package org.apache.iotdb.confignode.manager.load.cache.detector;

import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.IFailureDetector;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.tsfile.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The Phi Failure Detector, proposed by Hayashibara, Naohiro, et al. "The/spl phi/accrual failure
 * detector.". It is an accrual approach based on heartbeat history analysis with dynamic
 * sensitivity and tunable threshold. It is adaptive with early failure detection, increased
 * accuracy and improved system stability.
 *
 * <p>Initially, Phi has a cold start period where it will only collect heartbeat samples and
 * fallback decision-making to {@link FixedDetector}. After collecting enough samples, it will start
 * failure detection using the Phi algo.
 */
public class PhiAccrualDetector implements IFailureDetector {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhiAccrualDetector.class);
  private final long threshold;
  private final long acceptableHeartbeatPauseNs;
  private final long minHeartbeatStdNs;
  private final int codeStartSampleCount;
  private final IFailureDetector fallbackDuringColdStart;

  public PhiAccrualDetector(
      long threshold,
      long acceptableHeartbeatPauseNs,
      long minHeartbeatStdNs,
      int minimalSampleCount,
      IFailureDetector fallbackDuringColdStart) {
    this.threshold = threshold;
    this.acceptableHeartbeatPauseNs = acceptableHeartbeatPauseNs;
    this.minHeartbeatStdNs = minHeartbeatStdNs;
    this.codeStartSampleCount = minimalSampleCount;
    this.fallbackDuringColdStart = fallbackDuringColdStart;
  }

  @Override
  public boolean isAvailable(List<AbstractHeartbeatSample> history) {
    if (history.size() < codeStartSampleCount) {
      /* We haven't received enough heartbeat replies.*/
      return fallbackDuringColdStart.isAvailable(history);
    }
    final PhiAccrual phiAccrual = create(history);
    final boolean isAvailable = phiAccrual.phi() < (double) this.threshold;
    if (!isAvailable) {
      // log the status change and dump the heartbeat history for analysis use
      final StringBuilder builder = new StringBuilder();
      builder.append("[");
      for (double interval : phiAccrual.heartbeatIntervals) {
        final long msInterval = (long) interval / 1000_000;
        builder.append(msInterval).append(", ");
      }
      builder.append(phiAccrual.timeElapsedSinceLastHeartbeat / 1000_000);
      builder.append("]");
      LOGGER.info(String.format("Node Down, heartbeat history (ms): %s", builder));
    }

    return isAvailable;
  }

  PhiAccrual create(List<AbstractHeartbeatSample> history) {
    final List<Double> heartbeatIntervals = new ArrayList<>();

    long lastTs = -1;
    for (final AbstractHeartbeatSample sample : history) {
      // ensure getSampleLogicalTimestamp() will return system nano timestamp
      Preconditions.checkArgument(
          sample instanceof NodeHeartbeatSample || sample instanceof RegionHeartbeatSample);
      if (lastTs == -1) {
        lastTs = sample.getSampleLogicalTimestamp();
        continue;
      }
      heartbeatIntervals.add((double) sample.getSampleLogicalTimestamp() - lastTs);
      lastTs = sample.getSampleLogicalTimestamp();
    }
    final long lastHeartbeatTimestamp = history.get(history.size() - 1).getSampleLogicalTimestamp();
    final long timeElapsedSinceLastHeartbeat = System.nanoTime() - lastHeartbeatTimestamp;

    final double[] intervalArray =
        heartbeatIntervals.stream().mapToDouble(Double::doubleValue).toArray();
    return new PhiAccrual(
        intervalArray,
        timeElapsedSinceLastHeartbeat,
        minHeartbeatStdNs,
        acceptableHeartbeatPauseNs);
  }

  /**
   * The φ Accrual Failure Detector implementation. See <a
   * href="https://doc.akka.io/libraries/akka-core/current/typed/failure-detector.html">φ
   * Accrual</a>
   */
  static final class PhiAccrual {
    /*
     * All the heartbeat related intervals within this class should be calculated in unit of nanoseconds
     */
    private final double[] heartbeatIntervals;
    private final long timeElapsedSinceLastHeartbeat;
    private final long minHeartbeatStd;
    private final long acceptableHeartbeatPause;

    PhiAccrual(
        double[] heartbeatIntervals,
        long timeElapsedSinceLastHeartbeat,
        long minHeartbeatStd,
        long acceptableHeartbeatPause) {
      Preconditions.checkArgument(heartbeatIntervals.length > 0);
      Preconditions.checkArgument(timeElapsedSinceLastHeartbeat >= 0);
      this.heartbeatIntervals = heartbeatIntervals;
      this.timeElapsedSinceLastHeartbeat = timeElapsedSinceLastHeartbeat;
      this.minHeartbeatStd = minHeartbeatStd;
      this.acceptableHeartbeatPause = acceptableHeartbeatPause;
    }

    /**
     * @return phi value given the heartbeat interval history
     */
    double phi() {
      final DescriptiveStatistics ds = new DescriptiveStatistics(heartbeatIntervals);
      double mean = ds.getMean();
      double std = ds.getStandardDeviation();

      /* ensure the std is valid */
      std = Math.max(std, minHeartbeatStd);

      /* add tolerance specified by acceptableHeartbeatPause */
      mean += acceptableHeartbeatPause;

      return p(timeElapsedSinceLastHeartbeat, mean, std);
    }

    /**
     * Core method for calculating the phi φ coefficient. It uses a logistic approximation to the
     * cumulative normal distribution.
     *
     * @param elapsedTime the difference of the times (current - last heartbeat timestamp)
     * @param historyMean the mean of the history distribution
     * @param historyStd the standard deviation of the history distribution
     * @return The value of the φ
     */
    private double p(double elapsedTime, double historyMean, double historyStd) {
      final double y = (elapsedTime - historyMean) / historyStd;
      /* Math.exp will return {@link Double.POSITIVE_INFINITY} SAFELY when overflows. */
      double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
      if (elapsedTime > historyMean) {
        return -Math.log10(e / (1.0 + e));
      } else {
        return -Math.log10(1.0 - 1.0 / (1.0 + e));
      }
    }
  }
}
