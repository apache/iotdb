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

import org.apache.tsfile.read.common.TimeRange;

import java.math.BigInteger;

/**
 * This interface used for iteratively generating aggregated time windows in GROUP BY query.
 *
 * <p>It will return a leftCloseRightClose time window, by decreasing maxTime if leftCloseRightOpen
 * and increasing minTime if leftOpenRightClose.
 */
public interface ITimeRangeIterator {

  /** return the first time range by sorting order. */
  TimeRange getFirstTimeRange();

  /**
   * @return whether current iterator has next time range.
   */
  boolean hasNextTimeRange();

  /**
   * return the next time range according to curStartTime (the start time of the last returned time
   * range).
   */
  TimeRange nextTimeRange();

  boolean isAscending();

  default TimeRange getFinalTimeRange(TimeRange timeRange, boolean leftCRightO) {
    return leftCRightO
        ? new TimeRange(timeRange.getMin(), saturatingAdd(timeRange.getMax(), -1))
        : new TimeRange(saturatingAdd(timeRange.getMin(), 1), timeRange.getMax());
  }

  static long saturatingAdd(long left, long right) {
    if (right > 0 && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    if (right < 0 && left < Long.MIN_VALUE - right) {
      return Long.MIN_VALUE;
    }
    return left + right;
  }

  static boolean canMoveForward(long current, long step, long upperBound) {
    return step > 0 && current <= Long.MAX_VALUE - step && current + step < upperBound;
  }

  static boolean canMoveBackward(long current, long step, long lowerBound) {
    return step > 0 && current >= Long.MIN_VALUE + step && current - step >= lowerBound;
  }

  static long ceilDivTimeRange(long startTime, long endTime, long divisor) {
    BigInteger range =
        BigInteger.valueOf(endTime)
            .subtract(BigInteger.valueOf(startTime))
            .add(BigInteger.valueOf(divisor).subtract(BigInteger.ONE));
    return saturateToLong(range.divide(BigInteger.valueOf(divisor)));
  }

  static long rightmostTimeRangeStart(long startTime, long endTime, long slidingStep) {
    BigInteger distanceMinusOne =
        BigInteger.valueOf(endTime)
            .subtract(BigInteger.valueOf(startTime))
            .subtract(BigInteger.ONE);
    long remainder = distanceMinusOne.mod(BigInteger.valueOf(slidingStep)).longValue();
    return saturatingAdd(saturatingAdd(endTime, -1), -remainder);
  }

  static boolean isTimeRangeDistanceGreaterThan(long startTime, long endTime, long distance) {
    return BigInteger.valueOf(endTime)
            .subtract(BigInteger.valueOf(startTime))
            .compareTo(BigInteger.valueOf(distance))
        > 0;
  }

  static long saturateToLong(BigInteger value) {
    if (value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      return Long.MAX_VALUE;
    }
    if (value.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
      return Long.MIN_VALUE;
    }
    return value.longValue();
  }

  /**
   * As there is only one timestamp can be output for a time range, this method will return the
   * output time based on leftCloseRightOpen or not.
   *
   * @return minTime if leftCloseRightOpen, else maxTime.
   */
  long currentOutputTime();

  long getTotalIntervalNum();
}
