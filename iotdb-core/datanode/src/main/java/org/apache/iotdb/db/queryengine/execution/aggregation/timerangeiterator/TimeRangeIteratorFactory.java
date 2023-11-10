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

package org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator;

import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.tsfile.utils.TimeDuration;

public class TimeRangeIteratorFactory {

  private TimeRangeIteratorFactory() {}

  /**
   * The method returns different implements of ITimeRangeIterator depending on the parameters.
   *
   * <p>Note: interval and slidingStep is always stand for the milliseconds in this method.
   */
  @SuppressWarnings("squid:S107")
  public static ITimeRangeIterator getTimeRangeIterator(
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      boolean isAscending,
      boolean leftCRightO,
      boolean outputPartialTimeWindow) {
    if (outputPartialTimeWindow
        && interval.getTotalDuration(TimestampPrecisionUtils.currPrecision)
            > slidingStep.getTotalDuration(TimestampPrecisionUtils.currPrecision)) {
      if (!interval.containsMonth() && !slidingStep.containsMonth()) {
        return new PreAggrWindowIterator(
            startTime,
            endTime,
            interval.nonMonthDuration,
            slidingStep.nonMonthDuration,
            isAscending,
            leftCRightO);
      } else {
        return new PreAggrWindowWithNaturalMonthIterator(
            startTime, endTime, interval, slidingStep, isAscending, leftCRightO);
      }
    } else {
      return new AggrWindowIterator(
          startTime, endTime, interval, slidingStep, isAscending, leftCRightO);
    }
  }
}
