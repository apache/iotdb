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

  public static TTimePartitionSlot getTimePartitionSlot(long time) {
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
    timePartitionSlot.setStartTime(getTimePartitionLowerBound(time));
    return timePartitionSlot;
  }

  public static long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public static long getTimePartitionLowerBound(long time) {
    long lowerBoundOfTimePartition;
    lowerBoundOfTimePartition =
        getTimePartitionId(time) * timePartitionInterval + timePartitionOrigin;
    return lowerBoundOfTimePartition;
  }

  public static long getTimePartitionUpperBound(long time) {
    return getTimePartitionLowerBound(time) + timePartitionInterval;
  }

  public static long getTimePartitionId(long time) {
    time -= timePartitionOrigin;
    return time > 0 || time % timePartitionInterval == 0
        ? time / timePartitionInterval
        : time / timePartitionInterval - 1;
  }

  public static boolean satisfyPartitionId(long startTime, long endTime, long partitionId) {
    return getTimePartitionId(startTime) <= partitionId
        && getTimePartitionId(endTime) >= partitionId;
  }

  public static boolean satisfyPartitionStartTime(Filter timeFilter, long partitionStartTime) {
    return timeFilter == null
        || timeFilter.satisfyStartEndTime(
            partitionStartTime, partitionStartTime + timePartitionInterval - 1);
  }

  public static boolean satisfyTimePartition(Filter timeFilter, long partitionId) {
    long partitionStartTime = partitionId * timePartitionInterval + timePartitionOrigin;
    return satisfyPartitionStartTime(timeFilter, partitionStartTime);
  }

  public static void setTimePartitionInterval(long timePartitionInterval) {
    TimePartitionUtils.timePartitionInterval = timePartitionInterval;
  }

  public static long getEstimateTimePartitionSize(long startTime, long endTime) {
    return (endTime - startTime) / timePartitionInterval + 1;
  }
}
