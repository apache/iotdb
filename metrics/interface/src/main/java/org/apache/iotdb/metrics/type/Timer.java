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

import java.util.concurrent.TimeUnit;

public abstract class Timer extends IMetric {

  /** update time of timer */
  public abstract void update(long duration, TimeUnit unit);

  /** update timer by millisecond */
  public void updateMillis(long durationMillis) {
    update(durationMillis, TimeUnit.MILLISECONDS);
  }

  /** update timer by microseconds */
  public void updateMicros(long durationMicros) {
    update(durationMicros, TimeUnit.MICROSECONDS);
  }

  /** update timer by nanoseconds */
  public void updateNanos(long durationNanos) {
    update(durationNanos, TimeUnit.NANOSECONDS);
  }

  /** take snapshot of timer */
  public abstract HistogramSnapshot takeSnapshot();

  /** It's not safe to use the update interface of this rate */
  public abstract Rate getImmutableRate();
}
