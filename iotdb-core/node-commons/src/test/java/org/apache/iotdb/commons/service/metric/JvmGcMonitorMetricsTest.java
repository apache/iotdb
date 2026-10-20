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

package org.apache.iotdb.commons.service.metric;

import org.junit.Test;

import static org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics.OBSERVATION_WINDOW_MS;
import static org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics.SLEEP_INTERVAL_MS;
import static org.junit.Assert.assertEquals;

public class JvmGcMonitorMetricsTest {

  private static final long START_TIME = 1_000_000L;

  private final JvmGcMonitorMetrics monitor = JvmGcMonitorMetrics.getInstance();

  /** Each sample spends the given GC time within its interval, returns the latest percentage. */
  private long sample(final long startTime, final int samples, final long gcTimePerSample) {
    long totalGcTime = monitor.getGcData().getAccumulatedGcTime();
    for (int i = 1; i <= samples; i++) {
      totalGcTime += gcTimePerSample;
      monitor.calculateGCTimePercentageWithinObservedInterval(
          startTime + i * SLEEP_INTERVAL_MS, totalGcTime);
    }
    return monitor.getGcData().getGcTimePercentage();
  }

  @Test
  public void testLatestSampleIsCounted() {
    monitor.startMonitoring(START_TIME, 0);
    // 300 ms GC in the first 3 s
    assertEquals(10, sample(START_TIME, 1, SLEEP_INTERVAL_MS / 10));
  }

  @Test
  public void testWindowIsFullyCountedWhileSliding() {
    monitor.startMonitoring(START_TIME, 0);
    final int samplesToFillTheWindow = (int) (OBSERVATION_WINDOW_MS / SLEEP_INTERVAL_MS);
    // Keep spending 10% of the time in GC, after the window is full and while it wraps the buffer
    assertEquals(10, sample(START_TIME, samplesToFillTheWindow, SLEEP_INTERVAL_MS / 10));
    final long windowFullTime = START_TIME + samplesToFillTheWindow * SLEEP_INTERVAL_MS;
    assertEquals(10, sample(windowFullTime, 3 * samplesToFillTheWindow, SLEEP_INTERVAL_MS / 10));
  }

  @Test
  public void testRestartDropsTheSamplesOfThePreviousRun() {
    monitor.startMonitoring(START_TIME, 0);
    // Half of the time in GC before the restart
    sample(START_TIME, 3, SLEEP_INTERVAL_MS / 2);

    final long restartTime = START_TIME + 4 * SLEEP_INTERVAL_MS;
    monitor.startMonitoring(restartTime, monitor.getGcData().getAccumulatedGcTime());
    assertEquals(0, sample(restartTime, 1, 0));
  }
}
