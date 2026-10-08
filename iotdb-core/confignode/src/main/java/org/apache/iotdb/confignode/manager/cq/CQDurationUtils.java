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

package org.apache.iotdb.confignode.manager.cq;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TCQDuration;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;

import org.apache.tsfile.utils.TimeDuration;

import java.time.ZoneId;

/** Shared utilities for converting CQ Thrift types to structured durations and boundaries. */
public final class CQDurationUtils {

  private CQDurationUtils() {}

  /**
   * Converts a Thrift TCQDuration to a structured TimeDuration, falling back to a legacy fixed
   * duration if the request predates calendar-aware encoding.
   */
  public static TimeDuration toTimeDuration(TCreateCQReq req, TCQDuration d, long legacyFixed) {
    if (req.isSetDurationEncodingVersion() && req.getDurationEncodingVersion() == 1 && d != null) {
      return new TimeDuration(Math.toIntExact(d.getMonthPart()), d.getNonMonthDuration());
    }
    return new TimeDuration(0, legacyFixed);
  }

  /**
   * Resolves the calendar anchor for a CQ. If the request explicitly sets BOUNDARY, that value is
   * used as-is; if omitted and the CQ is calendar-aware, the anchor becomes the local epoch
   * (1970-01-01 00:00 in the persisted zone).
   */
  public static long resolveBoundary(TCreateCQReq req, ZoneId zone, TimeDuration everyDuration) {
    if (everyDuration.monthDuration != 0
        && req.isSetBoundaryExplicit()
        && !req.isBoundaryExplicit()) {
      return CQCalendarUtils.localEpochBoundary(zone);
    }
    return req.boundaryTime;
  }

  /**
   * Scales the current system time to the configured timestamp precision, producing a long suitable
   * for CQ scheduling arithmetic.
   */
  public static long currentTimeInPrecision() {
    String precision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    long multiplier;
    if ("ns".equals(precision)) {
      multiplier = 1_000_000L;
    } else if ("us".equals(precision)) {
      multiplier = 1_000L;
    } else {
      multiplier = 1L;
    }
    return System.currentTimeMillis() * multiplier;
  }
}
