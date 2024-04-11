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

package org.apache.iotdb.db.pipe.extractor.dataregion;

import org.apache.iotdb.db.pipe.event.common.watermark.PipeWatermarkEvent;

public class DataRegionWatermarkInjector {

  public static final long MIN_INJECTION_INTERVAL_IN_MS = 1000 * 60 * 5; // 5 minutes

  private final long injectionIntervalInMs;
  private long nextInjectionTime;

  public DataRegionWatermarkInjector(long injectionIntervalInMs) {
    this.injectionIntervalInMs =
        Math.max(injectionIntervalInMs, MIN_INJECTION_INTERVAL_IN_MS)
            / MIN_INJECTION_INTERVAL_IN_MS
            * MIN_INJECTION_INTERVAL_IN_MS;
    this.nextInjectionTime = calculateNextInjectionTime(this.injectionIntervalInMs);
  }

  public long getInjectionIntervalInMs() {
    return injectionIntervalInMs;
  }

  public long getNextInjectionTime() {
    return nextInjectionTime;
  }

  public PipeWatermarkEvent inject() {
    if (System.currentTimeMillis() < nextInjectionTime) {
      return null;
    }

    final PipeWatermarkEvent watermarkEvent = new PipeWatermarkEvent(nextInjectionTime);
    nextInjectionTime = calculateNextInjectionTime(injectionIntervalInMs);
    return watermarkEvent;
  }

  private static long calculateNextInjectionTime(long injectionIntervalInMs) {
    final long currentTime = System.currentTimeMillis();
    return currentTime / injectionIntervalInMs * injectionIntervalInMs + injectionIntervalInMs;
  }
}
