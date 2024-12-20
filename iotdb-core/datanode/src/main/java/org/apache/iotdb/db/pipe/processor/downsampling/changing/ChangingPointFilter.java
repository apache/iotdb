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

package org.apache.iotdb.db.pipe.processor.downsampling.changing;

import org.apache.iotdb.db.pipe.processor.downsampling.DownSamplingFilter;
import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.time.LocalDate;
import java.util.Objects;

public class ChangingPointFilter extends DownSamplingFilter {

  private static final long estimatedMemory =
      RamUsageEstimator.shallowSizeOfInstance(ChangingPointFilter.class);

  private final long estimatedSize;

  /**
   * The maximum absolute difference the user set if the data's value is within
   * compressionDeviation, it will be compressed and discarded after compression
   */
  private final double compressionDeviation;

  private Object lastStoredValue;

  public ChangingPointFilter(
      final long arrivalTime,
      final long firstTimestamp,
      final Object firstValue,
      final double compressionDeviation,
      final boolean isFilterArrivalTime) {
    super(arrivalTime, firstTimestamp, isFilterArrivalTime);
    lastStoredValue = firstValue;
    this.compressionDeviation = compressionDeviation;

    if (lastStoredValue instanceof Binary) {
      estimatedSize = estimatedMemory + SIZE_OF_BINARY;
    } else if (lastStoredValue instanceof LocalDate) {
      estimatedSize = estimatedMemory + SIZE_OF_DATE;
    } else {
      estimatedSize = estimatedMemory + SIZE_OF_LONG;
    }
  }

  public void reset(final long arrivalTime, long firstTimestamp, final Object firstValue) {
    reset(arrivalTime, firstTimestamp);
    lastStoredValue = firstValue;
  }

  public boolean filter(final long arrivalTime, final long timestamp, final Object value) {
    try {
      return tryFilter(arrivalTime, timestamp, value);
    } catch (final Exception e) {
      reset(arrivalTime, timestamp, value);
      return true;
    }
  }

  private boolean tryFilter(final long arrivalTime, final long timestamp, final Object value) {
    // For non-numerical types, we only compare the value
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

    // For other numerical types, we compare the value difference
    if (Math.abs(
            Double.parseDouble(lastStoredValue.toString()) - Double.parseDouble(value.toString()))
        > compressionDeviation) {
      reset(arrivalTime, timestamp, value);
      return true;
    }

    return false;
  }

  public long estimatedMemory() {
    return estimatedSize;
  }
}
