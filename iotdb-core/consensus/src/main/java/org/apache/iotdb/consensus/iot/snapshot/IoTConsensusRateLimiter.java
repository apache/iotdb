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

package org.apache.iotdb.consensus.iot.snapshot;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTConsensusRateLimiter {
  private static final Logger logger = LoggerFactory.getLogger(IoTConsensusRateLimiter.class);

  private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

  private IoTConsensusRateLimiter() {}

  public void init(long regionMigrationSpeedLimitBytesPerSecond) {
    rateLimiter.setRate(
        regionMigrationSpeedLimitBytesPerSecond <= 0
            ? Double.MAX_VALUE
            : regionMigrationSpeedLimitBytesPerSecond);
  }

  /**
   * Acquire the size of the data to be sent.
   *
   * @param transitDataSize the size of the data to be sent
   */
  public void acquireTransitDataSizeWithRateLimiter(long transitDataSize) {
    while (transitDataSize > 0) {
      if (transitDataSize > Integer.MAX_VALUE) {
        rateLimiter.acquire(Integer.MAX_VALUE);
        transitDataSize -= Integer.MAX_VALUE;
      } else {
        rateLimiter.acquire((int) transitDataSize);
        return;
      }
    }
  }

  private static final IoTConsensusRateLimiter INSTANCE = new IoTConsensusRateLimiter();

  public static IoTConsensusRateLimiter getInstance() {
    return INSTANCE;
  }
}
