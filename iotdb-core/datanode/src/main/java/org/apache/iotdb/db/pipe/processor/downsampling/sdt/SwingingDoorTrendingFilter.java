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

import org.apache.iotdb.pipe.api.type.Binary;

import java.time.LocalDate;
import java.util.Objects;

public class SwingingDoorTrendingFilter<T> {

  private final SwingingDoorTrendingSamplingProcessor processor;

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

  private T lastReadValue;

  /**
   * The last stored time and value we compare current point against lastReadTimestamp and
   * lastReadValue
   */
  private long lastStoredTimestamp;

  private T lastStoredValue;

  public SwingingDoorTrendingFilter(
      final SwingingDoorTrendingSamplingProcessor processor,
      final long firstTimestamp,
      final T firstValue) {
    this.processor = processor;
    init(firstTimestamp, firstValue);
  }

  private void init(final long firstTimestamp, final T firstValue) {
    upperDoor = Double.MIN_VALUE;
    lowerDoor = Double.MAX_VALUE;

    lastReadTimestamp = firstTimestamp;
    lastReadValue = firstValue;

    lastStoredTimestamp = firstTimestamp;
    lastStoredValue = firstValue;
  }

  public boolean filter(final long timestamp, final T value) {
    try {
      return tryFilter(timestamp, value);
    } catch (final Exception e) {
      init(timestamp, value);
      return true;
    }
  }

  private boolean tryFilter(final long timestamp, final T value) {
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
    if (value instanceof Boolean
        || value instanceof String
        || value instanceof Binary
        || value instanceof LocalDate) {
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

  private void reset(final long timestamp, final T value) {
    upperDoor = Double.MIN_VALUE;
    lowerDoor = Double.MAX_VALUE;

    lastStoredTimestamp = timestamp;
    lastStoredValue = value;
  }
}
