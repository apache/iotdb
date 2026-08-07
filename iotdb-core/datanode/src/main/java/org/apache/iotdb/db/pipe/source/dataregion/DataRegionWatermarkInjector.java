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

package org.apache.iotdb.db.pipe.source.dataregion;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.watermark.PipeWatermarkEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataRegionWatermarkInjector {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataRegionWatermarkInjector.class);

  public static final long MIN_INJECTION_INTERVAL_IN_MS = 30 * 1000L; // 30s

  private final int regionId;

  private final long injectionIntervalInMs;
  private long nextInjectionTime;

  public DataRegionWatermarkInjector(int regionId, long injectionIntervalInMs) {
    this.regionId = regionId;
    this.injectionIntervalInMs =
        Math.max(injectionIntervalInMs, MIN_INJECTION_INTERVAL_IN_MS)
            / MIN_INJECTION_INTERVAL_IN_MS
            * MIN_INJECTION_INTERVAL_IN_MS;
    this.nextInjectionTime = calculateNextInjectionTime(this.injectionIntervalInMs);
  }

  public long getInjectionIntervalInMs() {
    return injectionIntervalInMs;
  }

  public PipeWatermarkEvent inject() {
    if (System.currentTimeMillis() < nextInjectionTime) {
      return null;
    }

    try {
      final PipeWatermarkEvent watermarkEvent = new PipeWatermarkEvent(nextInjectionTime);
      nextInjectionTime = calculateNextInjectionTime(injectionIntervalInMs);
      return watermarkEvent;
    } finally {
      LOGGER.info(
          DataNodePipeMessages.DATA_REGION_INJECTED_WATERMARK_EVENT_WITH_TIMESTAMP,
          regionId,
          nextInjectionTime);
    }
  }

  private static long calculateNextInjectionTime(long injectionIntervalInMs) {
    return calculateNextInjectionTime(System.currentTimeMillis(), injectionIntervalInMs);
  }

  static long calculateNextInjectionTime(long currentTime, long injectionIntervalInMs) {
    return saturatingAdd(
        currentTime / injectionIntervalInMs * injectionIntervalInMs, injectionIntervalInMs);
  }

  private static long saturatingAdd(final long left, final long right) {
    if (right > 0 && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    if (right < 0 && left < Long.MIN_VALUE - right) {
      return Long.MIN_VALUE;
    }
    return left + right;
  }
}
