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

package org.apache.iotdb.db.utils.timerangeiterator;

import static org.apache.iotdb.db.qp.utils.DateTimeUtils.MS_TO_MONTH;

public class TimeRangeIteratorFactory {

  private TimeRangeIteratorFactory() {}

  /**
   * The method returns different implements of ITimeRangeIterator depending on the parameters.
   *
   * <p>Note: interval and slidingStep stand for the milliseconds if not grouped by month, or the
   * month count if grouped by month.
   */
  public static ITimeRangeIterator getTimeRangeIterator(
      long startTime,
      long endTime,
      long interval,
      long slidingStep,
      boolean isAscending,
      boolean isIntervalByMonth,
      boolean isSlidingStepByMonth,
      boolean isPreAggr) {
    long tmpInterval = isIntervalByMonth ? interval * MS_TO_MONTH : interval;
    long tmpSlidingStep = isSlidingStepByMonth ? slidingStep * MS_TO_MONTH : slidingStep;
    if (isPreAggr && tmpInterval > tmpSlidingStep) {
      if (!isIntervalByMonth && !isSlidingStepByMonth) {
        return new PreAggrWindowIterator(startTime, endTime, interval, slidingStep, isAscending);
      } else {
        return new PreAggrWindowWithNaturalMonthIterator(
            startTime,
            endTime,
            interval,
            slidingStep,
            isAscending,
            isSlidingStepByMonth,
            isIntervalByMonth);
      }
    } else {
      return new AggrWindowIterator(
          startTime,
          endTime,
          interval,
          slidingStep,
          isAscending,
          isSlidingStepByMonth,
          isIntervalByMonth);
    }
  }
}
