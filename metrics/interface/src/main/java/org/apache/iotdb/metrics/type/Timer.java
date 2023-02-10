/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.type;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface Timer extends IMetric {

  /** Update time of timer. */
  void update(long duration, TimeUnit unit);

  /** Update timer by millisecond. */
  default void updateMillis(long durationMillis) {
    update(durationMillis, TimeUnit.MILLISECONDS);
  }

  /** Update timer by microseconds. */
  default void updateMicros(long durationMicros) {
    update(durationMicros, TimeUnit.MICROSECONDS);
  }

  /** Update timer by nanoseconds. */
  default void updateNanos(long durationNanos) {
    update(durationNanos, TimeUnit.NANOSECONDS);
  }

  /** Take snapshot of timer. */
  HistogramSnapshot takeSnapshot();

  /** It's not safe to use the update interface of this rate. */
  Rate getImmutableRate();

  @Override
  default void constructValueMap(Map<String, Object> result) {
    takeSnapshot().constructValueMap(result);
    getImmutableRate().constructValueMap(result);
  }
}
