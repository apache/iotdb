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

package org.apache.iotdb.db.query.udf.api.customizer.strategy;

import java.time.ZoneId;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;

public class SlidingTimeWindowAccessStrategy implements AccessStrategy {

  private final String timeIntervalString;
  private final String slidingStepString;
  private final String displayWindowBeginString;
  private final String displayWindowEndString;

  private long timeInterval;
  private long slidingStep;
  private long displayWindowBegin;
  private long displayWindowEnd;

  private ZoneId zoneId;

  public SlidingTimeWindowAccessStrategy(String timeIntervalString, String slidingStepString,
      String displayWindowBeginString, String displayWindowEndString) {
    this.timeIntervalString = timeIntervalString;
    this.slidingStepString = slidingStepString;
    this.displayWindowBeginString = displayWindowBeginString;
    this.displayWindowEndString = displayWindowEndString;
  }

  @Override
  public void check() throws QueryProcessException {
    timeInterval = DatetimeUtils.convertDurationStrToLong(timeIntervalString);
    slidingStep = DatetimeUtils.convertDurationStrToLong(slidingStepString);
    displayWindowBegin = DatetimeUtils.convertDatetimeStrToLong(displayWindowBeginString, zoneId);
    displayWindowEnd = DatetimeUtils.convertDatetimeStrToLong(displayWindowEndString, zoneId);
    if (timeInterval <= 0) {
      throw new QueryProcessException(
          String.format("Parameter timeInterval(%d) should be positive.", timeInterval));
    }
    if (slidingStep <= 0) {
      throw new QueryProcessException(
          String.format("Parameter slidingStep(%d) should be positive.", slidingStep));
    }
    if (displayWindowEnd < displayWindowBegin) {
      throw new QueryProcessException(String.format("displayWindowEnd(%d) < displayWindowBegin(%d)",
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
