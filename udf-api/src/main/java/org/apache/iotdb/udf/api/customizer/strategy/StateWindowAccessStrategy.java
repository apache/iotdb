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

import java.time.ZoneId;

public class StateWindowAccessStrategy implements AccessStrategy {

  private final long displayWindowBegin;
  private final long displayWindowEnd;
  private final double delta;

  private ZoneId zoneId;

  /**
   * @param displayWindowBegin displayWindowBegin < displayWindowEnd
   * @param displayWindowEnd displayWindowBegin < displayWindowEnd
   * @param delta delta >= 0
   */
  public StateWindowAccessStrategy(long displayWindowBegin, long displayWindowEnd, double delta) {
    this.displayWindowBegin = displayWindowBegin;
    this.displayWindowEnd = displayWindowEnd;
    this.delta = delta;
  }

  /**
   * @param displayWindowBegin displayWindowBegin < displayWindowEnd
   * @param displayWindowEnd displayWindowBegin < displayWindowEnd
   */
  public StateWindowAccessStrategy(long displayWindowBegin, long displayWindowEnd) {
    this.displayWindowBegin = displayWindowBegin;
    this.displayWindowEnd = displayWindowEnd;
    this.delta = 0.0;
  }

  /**
   * Display window begin will be set to the same as the minimum timestamp of the query result set,
   * and display window end will be set to the same as the maximum timestamp of the query result
   * set.
   *
   * @param delta 0 < delta
   */
  public StateWindowAccessStrategy(double delta) {
    this.displayWindowBegin = Long.MIN_VALUE;
    this.displayWindowEnd = Long.MAX_VALUE;
    this.delta = delta;
  }

  /**
   * Display window begin will be set to the same as the minimum timestamp of the query result set,
   * and display window end will be set to the same as the maximum timestamp of the query result
   * set. delta default equals 0.
   */
  public StateWindowAccessStrategy() {
    this.displayWindowBegin = Long.MIN_VALUE;
    this.displayWindowEnd = Long.MAX_VALUE;
    this.delta = 0.0;
  }

  @Override
  public void check() {
    if (delta < 0) {
      throw new RuntimeException(
          String.format("Parameter delta(%f) should be positive or equal to 0.", delta));
    }
    if (displayWindowEnd < displayWindowBegin) {
      throw new RuntimeException(
          String.format(
              "displayWindowEnd(%d) < displayWindowBegin(%d)",
              displayWindowEnd, displayWindowBegin));
    }
  }

  @Override
  public AccessStrategyType getAccessStrategyType() {
    return AccessStrategyType.STATE_WINDOW;
  }

  public long getDisplayWindowBegin() {
    return displayWindowBegin;
  }

  public long getDisplayWindowEnd() {
    return displayWindowEnd;
  }

  public double getDelta() {
    return delta;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }
}
