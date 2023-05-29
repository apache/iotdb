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

package org.apache.iotdb.udf.api.customizer.strategy;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

import java.time.ZoneId;

/**
 * Used in {@link UDTF#beforeStart(UDFParameters, UDTFConfigurations)}.
 * <p>
 * When the access strategy of a UDTF is set to an instance of this class, the method {@link
 * UDTF#transform(RowWindow, PointCollector)} of the UDTF will be called to transform the original
 * data. You need to override the method in your own UDTF class.
 * <p>
 * Sliding time window is a kind of time-based window. To partition the raw query data set into
 * sliding time windows, you need to give the following 4 parameters:
 * <p>
 * <li> display window begin: determines the start time of the first window
 * <li> display window end: if the start time of current window + sliding step > display window
 * end, then current window is the last window that your UDTF can process
 * <li> time interval: determines the time range of a window
 * <li> sliding step: the start time of the next window = the start time of current window +
 * sliding step
 * <p>
 * Each call of the method {@link UDTF#transform(RowWindow, PointCollector)} processes one time
 * window and can generate any number of data points. Note that the transform method will still be
 * called when there is no data point in a window. Note that the time range of the last few windows
 * may be less than the specified time interval.
 * <p>
 * Sample code:
 * <p>Style 1:
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
 *   configurations.setAccessStrategy(new SlidingTimeWindowAccessStrategy(
 *       parameters.getLong(100),     // time interval
 *       parameters.getLong(50),      // sliding step
 *       parameters.getLong(0),       // display window begin
 *       parameters.getLong(10000))); // display window end
 * }</pre>
 * <p>Style 2 (deprecated since v0.14):
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
 *   configurations.setAccessStrategy(new SlidingTimeWindowAccessStrategy(
 *       parameters.getLong("7d"),                    // time interval
 *       parameters.getLong("7d"),                    // sliding step
 *       parameters.getLong("2020-01-01T00:00:00"),   // display window begin
 *       parameters.getLong("2020-06-01T00:00:00"))); // display window end
 * }</pre>
 *
 * @see UDTF
 * @see UDTFConfigurations
 */
public class SlidingTimeWindowAccessStrategy implements AccessStrategy {

  private final long timeInterval;
  private final long slidingStep;
  private final long displayWindowBegin;
  private final long displayWindowEnd;

  private ZoneId zoneId;

  /**
   * Deprecated since v0.14.
   *
   * @param timeIntervalString time interval in string. examples: 12d8m9ns, 1y1mo, etc. supported
   *     units: y, mo, w, d, h, m, s, ms, us, ns.
   * @param slidingStepString sliding step in string. examples: 12d8m9ns, 1y1mo, etc. supported
   *     units: y, mo, w, d, h, m, s, ms, us, ns.
   * @param displayWindowBeginString display window begin in string. format: 2011-12-03T10:15:30 or
   *     2011-12-03T10:15:30+01:00.
   * @param displayWindowEndString display window end in string. format: 2011-12-03T10:15:30 or
   *     2011-12-03T10:15:30+01:00.
   * @throws UnsupportedOperationException deprecated since v0.14
   */
  @Deprecated
  public SlidingTimeWindowAccessStrategy(
      String timeIntervalString,
      String slidingStepString,
      String displayWindowBeginString,
      String displayWindowEndString) {
    throw new UnsupportedOperationException("The method is deprecated since v0.14.");
  }

  /**
   * Deprecated since v0.14.
   *
   * <p>Display window begin will be set to the same as the minimum timestamp of the query result
   * set, and display window end will be set to the same as the maximum timestamp of the query
   * result set.
   *
   * @param timeIntervalString time interval in string. examples: 12d8m9ns, 1y1mo, etc. supported
   *     units: y, mo, w, d, h, m, s, ms, us, ns.
   * @param slidingStepString sliding step in string. examples: 12d8m9ns, 1y1mo, etc. supported
   *     units: y, mo, w, d, h, m, s, ms, us, ns.
   * @throws UnsupportedOperationException deprecated since v0.14
   */
  @Deprecated
  public SlidingTimeWindowAccessStrategy(String timeIntervalString, String slidingStepString) {
    throw new UnsupportedOperationException("The method is deprecated since v0.14.");
  }

  /**
   * Deprecated since v0.14.
   *
   * <p>Sliding step will be set to the same as the time interval, display window begin will be set
   * to the same as the minimum timestamp of the query result set, and display window end will be
   * set to the same as the maximum timestamp of the query result set.
   *
   * @param timeIntervalString time interval in string. examples: 12d8m9ns, 1y1mo, etc. supported
   *     units: y, mo, w, d, h, m, s, ms, us, ns.
   * @throws UnsupportedOperationException deprecated since v0.14
   */
  public SlidingTimeWindowAccessStrategy(String timeIntervalString) {
    throw new UnsupportedOperationException("The method is deprecated since v0.14.");
  }

  /**
   * @param timeInterval 0 < timeInterval
   * @param slidingStep 0 < slidingStep
   * @param displayWindowBegin displayWindowBegin < displayWindowEnd
   * @param displayWindowEnd displayWindowBegin < displayWindowEnd
   */
  public SlidingTimeWindowAccessStrategy(
      long timeInterval, long slidingStep, long displayWindowBegin, long displayWindowEnd) {
    this.timeInterval = timeInterval;
    this.slidingStep = slidingStep;
    this.displayWindowBegin = displayWindowBegin;
    this.displayWindowEnd = displayWindowEnd;
  }

  /**
   * Display window begin will be set to the same as the minimum timestamp of the query result set,
   * and display window end will be set to the same as the maximum timestamp of the query result
   * set.
   *
   * @param timeInterval 0 < timeInterval
   * @param slidingStep 0 < slidingStep
   */
  public SlidingTimeWindowAccessStrategy(long timeInterval, long slidingStep) {
    this.timeInterval = timeInterval;
    this.slidingStep = slidingStep;
    this.displayWindowBegin = Long.MIN_VALUE;
    this.displayWindowEnd = Long.MAX_VALUE;
  }

  /**
   * Sliding step will be set to the same as the time interval, display window begin will be set to
   * the same as the minimum timestamp of the query result set, and display window end will be set
   * to the same as the maximum timestamp of the query result set.
   *
   * @param timeInterval 0 < timeInterval
   */
  public SlidingTimeWindowAccessStrategy(long timeInterval) {
    this.timeInterval = timeInterval;
    this.slidingStep = timeInterval;
    this.displayWindowBegin = Long.MIN_VALUE;
    this.displayWindowEnd = Long.MAX_VALUE;
  }

  @Override
  public void check() {
    if (timeInterval <= 0) {
      throw new RuntimeException(
          String.format("Parameter timeInterval(%d) should be positive.", timeInterval));
    }
    if (slidingStep <= 0) {
      throw new RuntimeException(
          String.format("Parameter slidingStep(%d) should be positive.", slidingStep));
    }
    if (displayWindowEnd < displayWindowBegin) {
      throw new RuntimeException(
          String.format(
              "displayWindowEnd(%d) < displayWindowBegin(%d)",
              displayWindowEnd, displayWindowBegin));
    }
  }

  public long getTimeInterval() {
    return timeInterval;
  }

  public long getSlidingStep() {
    return slidingStep;
  }

  public long getDisplayWindowBegin() {
    return displayWindowBegin;
  }

  public long getDisplayWindowEnd() {
    return displayWindowEnd;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  @Override
  public AccessStrategyType getAccessStrategyType() {
    return AccessStrategyType.SLIDING_TIME_WINDOW;
  }
}
