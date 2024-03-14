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

import java.util.Objects;

public class SwingingDoorTrendingFilter<T> {

  private final SwingingDoorTrendingSamplingProcessor processor;

  /**
   * the maximum curUpperSlope between the lastStoredPoint to the current point upperDoor can only
   * open up
   */
  private double upperDoor;
  /**
   * the minimum curLowerSlope between the lastStoredPoint to the current point lowerDoor can only
   * open downward
   */
  private double lowerDoor;

  /**
   * the last read time and value if upperDoor >= lowerDoor meaning out of compressionDeviation
   * range, will store lastReadTimestamp and lastReadValue
   */
  private long lastReadTimestamp;

  private T lastReadValue;

  /**
   * the last stored time and value we compare current point against lastReadTimestamp and
   * lastReadValue
   */
  private long lastStoredTimestamp;

  private T lastStoredValue;

  public SwingingDoorTrendingFilter(
      SwingingDoorTrendingSamplingProcessor processor, long firstTimestamp, T firstValue) {
    this.processor = processor;
    init(firstTimestamp, firstValue);
  }

  private void init(long firstTimestamp, T firstValue) {
    upperDoor = Double.MIN_VALUE;
    lowerDoor = Double.MAX_VALUE;

    lastReadTimestamp = firstTimestamp;
    lastReadValue = firstValue;

    lastStoredTimestamp = firstTimestamp;
    lastStoredValue = firstValue;
  }

  public boolean filter(long timestamp, T value) {
    try {
      return tryFilter(timestamp, value);
    } catch (Exception e) {
      init(timestamp, value);
      return true;
    }
  }

  private boolean tryFilter(long timestamp, T value) {
    final long timeDiff = timestamp - lastStoredTimestamp;
    final long absTimeDiff = Math.abs(timeDiff);

    if (absTimeDiff <= processor.getCompressionMinTimeInterval()) {
      return false;
    }

    if (absTimeDiff >= processor.getCompressionMaxTimeInterval()) {
      reset(timestamp, value);
      return true;
    }

    // For boolean and string type, we only compare the value
    if (value instanceof Boolean || value instanceof String) {
      if (Objects.equals(lastStoredValue, value)) {
        return false;
      }

      reset(timestamp, value);
      return true;
    }

    // For other numerical types, we compare the value and the time difference
    final double doubleValue = Double.parseDouble(value.toString());
    final double lastStoredDoubleValue = Double.parseDouble(lastStoredValue.toString());
    final double valueDiff = doubleValue - lastStoredDoubleValue;

    final double currentUpperSlope = (valueDiff - processor.getCompressionDeviation()) / timeDiff;
    if (currentUpperSlope > upperDoor) {
      upperDoor = currentUpperSlope;
    }

    final double currentLowerSlope = (valueDiff + processor.getCompressionDeviation()) / timeDiff;
    if (currentLowerSlope < lowerDoor) {
      lowerDoor = currentLowerSlope;
    }

    if (upperDoor > lowerDoor) {
      lastStoredTimestamp = lastReadTimestamp;
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

  private void reset(long timestamp, T value) {
    upperDoor = Double.MIN_VALUE;
    lowerDoor = Double.MAX_VALUE;

    lastStoredTimestamp = timestamp;
    lastStoredValue = value;
  }
}
