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

package org.apache.iotdb.db.pipe.processor.downsampling.lastpoint;

import java.util.Objects;

public class LastPointFilter<T> {

  /** The last point time and value we compare current point against timestamp and value */
  private volatile long lastPointTimestamp;

  private volatile T lastPointValue;

  // Record whether it is consumed by sink
  private volatile boolean isConsumed;

  public LastPointFilter(final long firstTimestamp, final T firstValue) {
    this.lastPointTimestamp = firstTimestamp;
    this.lastPointValue = firstValue;
  }

  public boolean filter(final long timestamp, final T value) {
    return filter(timestamp, value, false);
  }

  public boolean filterAndMarkAsConsumed(final long timestamp, final T value) {
    return filter(timestamp, value, true);
  }

  /**
   * Filters timestamps and values to determine if the given timestamp is the most recent, and if
   * the value has changed since the last recorded timestamp.
   *
   * <p>This method checks if the provided timestamp is newer than the last recorded timestamp. If
   * the timestamp is the same, it further checks whether the value differs from the last recorded
   * value. If the timestamp is not newer and the value has not changed, the method returns false.
   *
   * <p>Additionally, if the value is consumed by a sink (as indicated by the isConsumed flag being
   * true), it marks the value as consumed in the internal state.
   *
   * @param timestamp The timestamp to check against the most recent timestamp.
   * @param value The value to check for changes against the last recorded value.
   * @param isConsumed A flag indicating whether the value has been consumed by a sink. If true, and
   *     the timestamp and value pass the filter conditions, the value is marked as consumed
   *     internally.
   * @return {@code true} if the timestamp is newer or the value has changed since the last recorded
   *     timestamp; {@code false} otherwise.
   */
  private boolean filter(final long timestamp, final T value, final boolean isConsumed) {
    synchronized (this) {
      if (this.lastPointTimestamp > timestamp) {
        return false;
      }

      // Check for the same timestamp and unchanged value, and whether it was previously consumed.
      if (this.lastPointTimestamp == timestamp
          && Objects.equals(lastPointValue, value)
          && this.isConsumed) {
        return false;
      }

      // Reset the last recorded timestamp and value with the new ones.
      reset(timestamp, value);

      // Mark as consumed if the value is consumed by a sink
      if (isConsumed) {
        this.isConsumed = true;
      }
    }
    return true;
  }

  private void reset(final long timestamp, final T value) {
    this.lastPointTimestamp = timestamp;
    this.lastPointValue = value;
    isConsumed = false;
  }
}
