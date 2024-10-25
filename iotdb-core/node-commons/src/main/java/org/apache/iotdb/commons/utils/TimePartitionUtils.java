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

public class TimePartitionUtils {

  /**
   * Time partition origin for dividing database, the time unit is the same with IoTDB's
   * TimestampPrecision
   */
  private static long timePartitionOrigin =
      CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin();

  /** Time range for dividing database, the time unit is the same with IoTDB's TimestampPrecision */
  private static long timePartitionInterval =
      CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

  private static final BigInteger bigTimePartitionOrigin = BigInteger.valueOf(timePartitionOrigin);
  private static final BigInteger bigTimePartitionInterval =
      BigInteger.valueOf(timePartitionInterval);
  private static final boolean originMayCauseOverflow = (timePartitionOrigin != 0);
  private static final long timePartitionLowerBoundWithoutOverflow;
  private static final long timePartitionUpperBoundWithoutOverflow;

  static {
    long minPartition = getTimePartitionIdWithoutOverflow(Long.MIN_VALUE);
    long maxPartition = getTimePartitionIdWithoutOverflow(Long.MAX_VALUE);
    BigInteger minPartitionStartTime =
        BigInteger.valueOf(minPartition)
            .multiply(bigTimePartitionInterval)
            .add(bigTimePartitionOrigin);
    BigInteger maxPartitionEndTime =
        BigInteger.valueOf(maxPartition)
            .multiply(bigTimePartitionInterval)
            .add(bigTimePartitionInterval)
            .add(bigTimePartitionOrigin);
    if (minPartitionStartTime.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
      timePartitionLowerBoundWithoutOverflow =
          minPartitionStartTime.add(bigTimePartitionInterval).longValue();
    } else {
      timePartitionLowerBoundWithoutOverflow = minPartitionStartTime.longValue();
    }
    if (maxPartitionEndTime.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      timePartitionUpperBoundWithoutOverflow =
          maxPartitionEndTime.subtract(bigTimePartitionInterval).longValue();
    } else {
      timePartitionUpperBoundWithoutOverflow = maxPartitionEndTime.longValue();
    }
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
    if (originMayCauseOverflow) {
      return BigInteger.valueOf(getTimePartitionIdWithoutOverflow(time))
          .multiply(bigTimePartitionInterval)
          .add(bigTimePartitionOrigin)
          .longValue();
    } else {
      return getTimePartitionId(time) * timePartitionInterval + timePartitionOrigin;
    }
  }

  public static long getTimePartitionUpperBound(long time) {
    if (time >= timePartitionUpperBoundWithoutOverflow) {
      return Long.MAX_VALUE;
    }
    long lowerBound = getTimePartitionLowerBound(time);
    return lowerBound == Long.MIN_VALUE
        ? timePartitionLowerBoundWithoutOverflow
        : lowerBound + timePartitionInterval;
  }

  public static long getTimePartitionId(long time) {
    time -= timePartitionOrigin;
    return time > 0 || time % timePartitionInterval == 0
        ? time / timePartitionInterval
        : time / timePartitionInterval - 1;
  }

  public static long getTimePartitionIdWithoutOverflow(long time) {
    BigInteger bigTime = BigInteger.valueOf(time).subtract(bigTimePartitionOrigin);
    BigInteger partitionId =
        bigTime.compareTo(BigInteger.ZERO) > 0
                || bigTime.remainder(bigTimePartitionInterval).equals(BigInteger.ZERO)
            ? bigTime.divide(bigTimePartitionInterval)
            : bigTime.divide(bigTimePartitionInterval).subtract(BigInteger.ONE);
    return partitionId.longValue();
  }

  public static boolean satisfyPartitionId(long startTime, long endTime, long partitionId) {
    long startPartition =
        originMayCauseOverflow
            ? getTimePartitionIdWithoutOverflow(startTime)
            : getTimePartitionId(startTime);
    long endPartition =
        originMayCauseOverflow
            ? getTimePartitionIdWithoutOverflow(endTime)
            : getTimePartitionId(endTime);
    return startPartition <= partitionId && endPartition >= partitionId;
  }

  public static boolean satisfyPartitionStartTime(Filter timeFilter, long partitionStartTime) {
    long partitionEndTime =
        partitionStartTime >= timePartitionLowerBoundWithoutOverflow
            ? Long.MAX_VALUE
            : (partitionStartTime + timePartitionInterval - 1);
    return timeFilter == null
        || timeFilter.satisfyStartEndTime(partitionStartTime, partitionEndTime);
  }

  public static boolean satisfyTimePartition(Filter timeFilter, long partitionId) {
    long partitionStartTime;
    if (originMayCauseOverflow) {
      partitionStartTime =
          BigInteger.valueOf(partitionId)
              .multiply(bigTimePartitionInterval)
              .add(bigTimePartitionOrigin)
              .longValue();
    } else {
      partitionStartTime = partitionId * timePartitionInterval + timePartitionOrigin;
    }
    return satisfyPartitionStartTime(timeFilter, partitionStartTime);
  }

  public static void setTimePartitionInterval(long timePartitionInterval) {
    TimePartitionUtils.timePartitionInterval = timePartitionInterval;
  }

  public static long getEstimateTimePartitionSize(long startTime, long endTime) {
    if (endTime > 0 && startTime < 0) {
      return BigInteger.valueOf(endTime)
              .subtract(BigInteger.valueOf(startTime))
              .divide(bigTimePartitionInterval)
              .longValue()
          + 1;
    }
    return (endTime - startTime) / timePartitionInterval + 1;
  }
}
