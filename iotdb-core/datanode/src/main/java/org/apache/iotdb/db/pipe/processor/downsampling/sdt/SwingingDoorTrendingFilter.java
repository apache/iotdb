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

package org.apache.iotdb.db.pipe.processor.downsampling.sdt;

import org.apache.iotdb.db.pipe.processor.downsampling.DownSamplingFilter;
import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.time.LocalDate;
import java.util.Objects;

public class SwingingDoorTrendingFilter extends DownSamplingFilter {

  private static final long estimatedMemory =
      RamUsageEstimator.shallowSizeOfInstance(SwingingDoorTrendingFilter.class);

  private final long estimatedSize;

  /**
   * The maximum absolute difference the user set if the data's value is within
   * compressionDeviation, it will be compressed and discarded after compression, it will only store
   * out of range (time, data) to form the trend
   */
  private final double compressionDeviation;

  /**
   * The maximum curUpperSlope between the lastStoredPoint to the current point upperDoor can only
   * open up
   */
  private double upperDoor;

  /**
   * The minimum curLowerSlope between the lastStoredPoint to the current point lowerDoor can only
   * open downward
   */
  private double lowerDoor;

  /**
   * The last read time and value if upperDoor >= lowerDoor meaning out of compressionDeviation
   * range, will store lastReadTimestamp and lastReadValue
   */
  private long lastReadTimestamp;

  private Object lastReadValue;

  private Object lastStoredValue;

  public SwingingDoorTrendingFilter(
      final long arrivalTime,
      final long eventTime,
      final Object firstValue,
      final double compressionDeviation) {
    super(arrivalTime, eventTime);
    this.lastStoredValue = firstValue;
    this.compressionDeviation = compressionDeviation;
    if (lastStoredValue instanceof Binary) {
      estimatedSize = estimatedMemory + SIZE_OF_BINARY;
    } else if (lastStoredValue instanceof LocalDate) {
      estimatedSize = estimatedMemory + SIZE_OF_DATE;
    } else {
      estimatedSize = estimatedMemory + SIZE_OF_LONG;
    }
  }

  private void init(final long arrivalTime, final long firstTimestamp, final Object firstValue) {
    upperDoor = Double.MIN_VALUE;
    lowerDoor = Double.MAX_VALUE;

    lastReadTimestamp = firstTimestamp;
    lastReadValue = firstValue;

    lastPointEventTime = firstTimestamp;
    lastStoredValue = firstValue;

    this.lastPointArrivalTime = arrivalTime;
  }

  public boolean filter(final long arrivalTime, final long timestamp, final Object value) {
    try {
      return tryFilter(arrivalTime, timestamp, value);
    } catch (final Exception e) {
      init(arrivalTime, timestamp, value);
      return true;
    }
  }

  private boolean tryFilter(final long arrivalTime, final long timestamp, final Object value) {
    final long timeDiff = timestamp - lastPointEventTime;

    // For boolean and string type, we only compare the value
    if (value instanceof Boolean
        || value instanceof String
        || value instanceof Binary
        || value instanceof LocalDate) {
      if (Objects.equals(lastStoredValue, value)) {
        return false;
      }

      reset(arrivalTime, timestamp, value);
      return true;
    }

    // For other numerical types, we compare the value and the time difference
    final double doubleValue = Double.parseDouble(value.toString());
    final double lastStoredDoubleValue = Double.parseDouble(lastStoredValue.toString());
    final double valueDiff = doubleValue - lastStoredDoubleValue;

    final double currentUpperSlope = (valueDiff - compressionDeviation) / timeDiff;
    if (currentUpperSlope > upperDoor) {
      upperDoor = currentUpperSlope;
    }

    final double currentLowerSlope = (valueDiff + compressionDeviation) / timeDiff;
    if (currentLowerSlope < lowerDoor) {
      lowerDoor = currentLowerSlope;
    }

    if (upperDoor > lowerDoor) {
      lastPointEventTime = lastReadTimestamp;
      lastStoredValue = lastReadValue;

      upperDoor = currentUpperSlope;
      lowerDoor = currentLowerSlope;

      lastReadValue = value;
      lastReadTimestamp = timestamp;

      return true;
    }

    lastReadValue = value;
    lastReadTimestamp = timestamp;

    return false;
  }

  public void reset(final long arrivalTime, final long timestamp, final Object value) {
    super.reset(arrivalTime, timestamp);
    upperDoor = Double.MIN_VALUE;
    lowerDoor = Double.MAX_VALUE;

    lastStoredValue = value;
  }

  public long ramBytesUsed() {
    return estimatedSize;
  }
}
