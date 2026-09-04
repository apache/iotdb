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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.read.filter.basic.Filter;

import java.math.BigInteger;

import static com.google.common.math.LongMath.saturatedAdd;
import static org.apache.iotdb.commons.utils.CommonDateTimeUtils.saturateToLong;

public class TimePartitionUtils {

  private static final BigInteger BIG_LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger BIG_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
  private static final BigInteger BIG_ONE = BigInteger.ONE;

  /**
   * Time partition origin for dividing database, the time unit is the same with IoTDB's
   * TimestampPrecision
   */
  private static volatile long timePartitionOrigin =
      CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin();

  /** Time range for dividing database, the time unit is the same with IoTDB's TimestampPrecision */
  private static volatile long timePartitionInterval =
      CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

  private static volatile long timePartitionLowerBoundWithoutOverflow;
  private static volatile long timePartitionUpperBoundWithoutOverflow;
  private static volatile boolean timePartitionLowerBoundOverflow;
  private static volatile boolean timePartitionUpperBoundOverflow;

  static {
    updateTimePartitionBound();
  }

  private static void updateTimePartitionBound() {
    BigInteger minPartition = getTimePartitionIdAsBigInteger(Long.MIN_VALUE);
    BigInteger maxPartition = getTimePartitionIdAsBigInteger(Long.MAX_VALUE);
    timePartitionLowerBoundOverflow = minPartition.compareTo(BIG_LONG_MIN) < 0;
    timePartitionUpperBoundOverflow = maxPartition.compareTo(BIG_LONG_MAX) > 0;

    // The lower/upper bounds are the starts of the first and last regular slots.  If the exact
    // partition id at a timestamp is outside the long id range, the corresponding boundary slot
    // is represented by the nearest long id and starts at the timestamp limit.
    BigInteger firstRepresentablePartition = minPartition.max(BIG_LONG_MIN);
    BigInteger firstRepresentableStart =
        getTimePartitionStartTimeAsBigInteger(firstRepresentablePartition);
    if (timePartitionLowerBoundOverflow || firstRepresentableStart.compareTo(BIG_LONG_MIN) < 0) {
      firstRepresentableStart =
          firstRepresentableStart.add(BigInteger.valueOf(timePartitionInterval));
    }
    timePartitionLowerBoundWithoutOverflow = saturateToLong(firstRepresentableStart);

    BigInteger lastRepresentablePartition = maxPartition.min(BIG_LONG_MAX);
    BigInteger lastRepresentableStart;
    if (timePartitionUpperBoundOverflow) {
      lastRepresentableStart =
          getTimePartitionStartTimeAsBigInteger(lastRepresentablePartition.add(BIG_ONE));
    } else {
      lastRepresentableStart = getTimePartitionStartTimeAsBigInteger(lastRepresentablePartition);
    }
    timePartitionUpperBoundWithoutOverflow = saturateToLong(lastRepresentableStart);
  }

  public static TTimePartitionSlot getTimePartitionSlot(long time) {
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
    timePartitionSlot.setStartTime(getTimePartitionLowerBound(time));
    return timePartitionSlot;
  }

  public static long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public static long getTimePartitionLowerBound(long time) {
    if (time < timePartitionLowerBoundWithoutOverflow) {
      return Long.MIN_VALUE;
    }
    if (time >= timePartitionUpperBoundWithoutOverflow) {
      return timePartitionUpperBoundWithoutOverflow;
    }
    return getTimePartitionStartTime(getTimePartitionId(time));
  }

  public static long getTimePartitionUpperBound(long time) {
    if (time >= timePartitionUpperBoundWithoutOverflow) {
      return Long.MAX_VALUE;
    }
    if (time < timePartitionLowerBoundWithoutOverflow) {
      return timePartitionLowerBoundWithoutOverflow;
    }
    return saturatedAdd(getTimePartitionLowerBound(time), timePartitionInterval);
  }

  public static long getTimePartitionEndTime(long time) {
    long upperBound = getTimePartitionUpperBound(time);
    if (upperBound != Long.MAX_VALUE) {
      return upperBound - 1;
    }
    return getTimePartitionLowerBound(time) == getTimePartitionLowerBound(Long.MAX_VALUE)
        ? Long.MAX_VALUE
        : Long.MAX_VALUE - 1;
  }

  public static boolean isAfterOrEqualToTimePartitionUpperBound(
      long time, long timePartitionStartTime, long timePartitionUpperBound) {
    if (timePartitionUpperBound != Long.MAX_VALUE) {
      return time >= timePartitionUpperBound;
    }
    return time == Long.MAX_VALUE && getTimePartitionLowerBound(time) != timePartitionStartTime;
  }

  public static boolean isTimePartitionStartTime(long time) {
    return getTimePartitionLowerBound(time) == time;
  }

  public static long getTimePartitionId(long time) {
    final long timeFromOrigin;
    try {
      timeFromOrigin = Math.subtractExact(time, timePartitionOrigin);
    } catch (ArithmeticException e) {
      return getTimePartitionIdWithoutOverflow(time);
    }
    return Math.floorDiv(timeFromOrigin, timePartitionInterval);
  }

  public static long getTimePartitionIdWithoutOverflow(long time) {
    BigInteger partitionId = getTimePartitionIdAsBigInteger(time);
    if (partitionId.compareTo(BIG_LONG_MIN) < 0) {
      return Long.MIN_VALUE;
    }
    if (partitionId.compareTo(BIG_LONG_MAX) > 0) {
      return Long.MAX_VALUE;
    }
    return partitionId.longValue();
  }

  private static BigInteger getTimePartitionIdAsBigInteger(long time) {
    BigInteger bigTime = BigInteger.valueOf(time).subtract(BigInteger.valueOf(timePartitionOrigin));
    BigInteger bigTimePartitionInterval = BigInteger.valueOf(timePartitionInterval);
    return bigTime.compareTo(BigInteger.ZERO) > 0
            || bigTime.remainder(bigTimePartitionInterval).equals(BigInteger.ZERO)
        ? bigTime.divide(bigTimePartitionInterval)
        : bigTime.divide(bigTimePartitionInterval).subtract(BigInteger.ONE);
  }

  public static long getStartTimeByPartitionId(long partitionId) {
    return getTimePartitionStartTime(partitionId);
  }

  public static boolean satisfyPartitionId(long startTime, long endTime, long partitionId) {
    long startPartition = getTimePartitionId(startTime);
    long endPartition = getTimePartitionId(endTime);
    return startPartition <= partitionId && endPartition >= partitionId;
  }

  public static boolean satisfyPartitionStartTime(Filter timeFilter, long partitionStartTime) {
    if (timeFilter == null) {
      return true;
    }
    long partitionEndTime = getTimePartitionEndTime(partitionStartTime);
    return timeFilter.satisfyStartEndTime(partitionStartTime, partitionEndTime);
  }

  public static boolean satisfyTimePartition(Filter timeFilter, long partitionId) {
    return satisfyPartitionStartTime(timeFilter, getTimePartitionStartTime(partitionId));
  }

  private static long getTimePartitionStartTime(long partitionId) {
    // A clamped boundary id represents the saturated underflow/overflow partition, whose start is
    // the corresponding timestamp boundary rather than the mathematical start of that id.
    if (partitionId == Long.MIN_VALUE && timePartitionLowerBoundOverflow) {
      return Long.MIN_VALUE;
    }
    if (partitionId == Long.MAX_VALUE && timePartitionUpperBoundOverflow) {
      return timePartitionUpperBoundWithoutOverflow;
    }
    return saturateToLong(getTimePartitionStartTimeAsBigInteger(BigInteger.valueOf(partitionId)));
  }

  private static BigInteger getTimePartitionStartTimeAsBigInteger(BigInteger partitionId) {
    return partitionId
        .multiply(BigInteger.valueOf(timePartitionInterval))
        .add(BigInteger.valueOf(timePartitionOrigin));
  }

  public static void setTimePartitionInterval(long timePartitionInterval) {
    TimePartitionUtils.timePartitionInterval = timePartitionInterval;
    updateTimePartitionBound();
  }

  public static void setTimePartitionOrigin(long timePartitionOrigin) {
    TimePartitionUtils.timePartitionOrigin = timePartitionOrigin;
    updateTimePartitionBound();
  }

  public static long getEstimateTimePartitionSize(long startTime, long endTime) {
    BigInteger estimateSize =
        BigInteger.valueOf(endTime)
            .subtract(BigInteger.valueOf(startTime))
            .divide(BigInteger.valueOf(timePartitionInterval))
            .add(BigInteger.ONE);
    if (estimateSize.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      return Long.MAX_VALUE;
    }
    return estimateSize.longValue();
  }
}
