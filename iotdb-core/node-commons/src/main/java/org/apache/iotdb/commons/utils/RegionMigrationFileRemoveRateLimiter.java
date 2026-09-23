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

package org.apache.iotdb.commons.utils;

import com.google.common.util.concurrent.RateLimiter;

public class RegionMigrationFileRemoveRateLimiter {

  private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

  private RegionMigrationFileRemoveRateLimiter() {}

  public void init(long regionMigrationFileRemoveSpeedLimitBytesPerSecond) {
    rateLimiter.setRate(
        regionMigrationFileRemoveSpeedLimitBytesPerSecond <= 0
            ? Double.MAX_VALUE
            : regionMigrationFileRemoveSpeedLimitBytesPerSecond);
  }

  public void acquire(long estimatedRemoveCostInBytes) {
    while (estimatedRemoveCostInBytes > 0) {
      if (estimatedRemoveCostInBytes > Integer.MAX_VALUE) {
        rateLimiter.acquire(Integer.MAX_VALUE);
        estimatedRemoveCostInBytes -= Integer.MAX_VALUE;
      } else {
        rateLimiter.acquire((int) estimatedRemoveCostInBytes);
        return;
      }
    }
  }

  @TestOnly
  public double getRate() {
    return rateLimiter.getRate();
  }

  private static final RegionMigrationFileRemoveRateLimiter INSTANCE =
      new RegionMigrationFileRemoveRateLimiter();

  public static RegionMigrationFileRemoveRateLimiter getInstance() {
    return INSTANCE;
  }
}
