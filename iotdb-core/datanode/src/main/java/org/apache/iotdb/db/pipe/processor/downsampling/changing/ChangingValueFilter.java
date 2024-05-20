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

import org.apache.iotdb.pipe.api.type.Binary;

import java.util.Objects;

public class ChangingValueFilter<T> {

  private final ChangingValueSamplingProcessor processor;

  /**
   * The last stored time and value we compare current point against lastReadTimestamp and
   * lastReadValue
   */
  private long lastStoredTimestamp;

  private T lastStoredValue;

  public ChangingValueFilter(
      ChangingValueSamplingProcessor processor, long firstTimestamp, T firstValue) {
    this.processor = processor;
    init(firstTimestamp, firstValue);
  }

  private void init(long firstTimestamp, T firstValue) {
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
    final long timeDiff = Math.abs(timestamp - lastStoredTimestamp);

    if (timeDiff <= processor.getCompressionMinTimeInterval()) {
      return false;
    }

    if (timeDiff >= processor.getCompressionMaxTimeInterval()) {
      reset(timestamp, value);
      return true;
    }

    // For boolean and string type, we only compare the value
    if (value instanceof Boolean || value instanceof String || value instanceof Binary) {
      if (Objects.equals(lastStoredValue, value)) {
        return false;
      }

      reset(timestamp, value);
      return true;
    }

    // For other numerical types, we compare the value difference
    if (Math.abs(
            Double.parseDouble(lastStoredValue.toString()) - Double.parseDouble(value.toString()))
        > processor.getCompressionDeviation()) {
      reset(timestamp, value);
      return true;
    }

    return false;
  }

  private void reset(long timestamp, T value) {
    lastStoredTimestamp = timestamp;
    lastStoredValue = value;
  }
}
