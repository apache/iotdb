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

  /** The last point time and value we compare current point against Timestamp and Value */
  private long lastPointTimestamp;

  private T lastPointValue;

  // Record whether it is consumed by Sink
  private boolean isConsumed;

  public LastPointFilter(final long firstTimestamp, final T firstValue) {
    this.lastPointTimestamp = firstTimestamp;
    this.lastPointValue = firstValue;
    this.isConsumed = false;
  }

  public boolean filter(final long timestamp, final T value) {
    if (this.lastPointTimestamp > timestamp) {
      return false;
    }

    if (this.lastPointTimestamp == timestamp && Objects.equals(lastPointValue, value)) {
      return false;
    }

    reset(timestamp, value);
    return true;
  }

  private void reset(final long timestamp, final T value) {
    this.lastPointTimestamp = timestamp;
    this.lastPointValue = value;
    this.isConsumed = false;
  }

  public void consume() {
    this.isConsumed = true;
  }
}
