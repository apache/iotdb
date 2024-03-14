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

import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;

public class SwingingDoorTrendingFilter<T> {

  private final SwingingDoorTrendingSamplingProcessor processor;

  private double upperDoor = Integer.MIN_VALUE;
  private double lowerDoor = Integer.MAX_VALUE;

  private long lastStoredTimestamp;
  private T lastStoredValue;

  private long lastReadTimestamp;
  private T lastReadValue;

  public SwingingDoorTrendingFilter(
      SwingingDoorTrendingSamplingProcessor processor, long firstTimestamp, T firstValue) {
    this.processor = processor;

    this.lastStoredTimestamp = firstTimestamp;
    this.lastStoredValue = firstValue;

    this.lastReadTimestamp = firstTimestamp;
    this.lastReadValue = firstValue;
  }

  public boolean filter(long timestamp, T value) {
    if (processor.getCompressionMinTimeInterval()
            != PipeProcessorConstant.PROCESSOR_SDT_MIN_TIME_INTERVAL_DEFAULT_VALUE
        && (timestamp - lastStoredTimestamp <= processor.getCompressionMinTimeInterval())) {
      return false;
    }

    if (processor.getCompressionMaxTimeInterval()
            != PipeProcessorConstant.PROCESSOR_SDT_MAX_TIME_INTERVAL_DEFAULT_VALUE
        && (timestamp - lastStoredTimestamp >= processor.getCompressionMaxTimeInterval())) {
      reset(timestamp, value);
      return true;
    }

    if ((value instanceof Boolean || value instanceof String)) {
      if (!lastStoredValue.equals(value)) {
        reset(timestamp, value);
        return true;
      }
      return false;
    }

    final Double doubleValue = Double.valueOf(value.toString());
    final Double lastStoredDoubleValue = Double.valueOf(lastStoredValue.toString());
    final double currentUpperSlope =
        (doubleValue - lastStoredDoubleValue - processor.getCompressionDeviation())
            / (timestamp - lastStoredTimestamp);
    if (currentUpperSlope > upperDoor) {
      upperDoor = currentUpperSlope;
    }

    final double currentLowerSlope =
        (doubleValue - lastStoredDoubleValue + processor.getCompressionDeviation())
            / (timestamp - lastStoredTimestamp);
    if (currentLowerSlope < lowerDoor) {
      lowerDoor = currentLowerSlope;
    }

    if (upperDoor >= lowerDoor) {
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
    lastStoredTimestamp = timestamp;
    lastStoredValue = value;

    upperDoor = Integer.MIN_VALUE;
    lowerDoor = Integer.MAX_VALUE;
  }
}
