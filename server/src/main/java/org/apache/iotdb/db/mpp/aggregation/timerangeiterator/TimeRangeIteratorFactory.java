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

package org.apache.iotdb.db.mpp.aggregation.timerangeiterator;

import static org.apache.iotdb.db.utils.DateTimeUtils.MS_TO_MONTH;

public class TimeRangeIteratorFactory {

  private TimeRangeIteratorFactory() {}

  /**
   * The method returns different implements of ITimeRangeIterator depending on the parameters.
   *
   * <p>Note: interval and slidingStep is always stand for the milliseconds in this method.
   */
  public static ITimeRangeIterator getTimeRangeIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isAscending,
      boolean isIntervalByMonth,
      boolean isSlidingStepByMonth,
      boolean leftCRightO,
      boolean outputPartialTimeWindow) {
    long originInterval = interval;
    long originSlidingStep = slidingStep;
    interval = isIntervalByMonth ? interval / MS_TO_MONTH : interval;
    slidingStep = isSlidingStepByMonth ? slidingStep / MS_TO_MONTH : slidingStep;

    if (outputPartialTimeWindow && originInterval > originSlidingStep) {
      if (!isIntervalByMonth && !isSlidingStepByMonth) {
        return new PreAggrWindowIterator(
            startTime, endTime, interval, slidingStep, isAscending, leftCRightO);
      } else {
        return new PreAggrWindowWithNaturalMonthIterator(
            startTime,
            endTime,
            interval,
            slidingStep,
            isAscending,
            isSlidingStepByMonth,
            isIntervalByMonth,
            leftCRightO);
      }
    } else {
      return new AggrWindowIterator(
          startTime,
          endTime,
          interval,
          slidingStep,
          isAscending,
          isSlidingStepByMonth,
          isIntervalByMonth,
          leftCRightO);
    }
  }
}
