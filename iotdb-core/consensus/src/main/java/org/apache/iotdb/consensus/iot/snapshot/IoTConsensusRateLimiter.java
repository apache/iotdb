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

import org.apache.iotdb.commons.quotas.AverageIntervalRateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTConsensusRateLimiter {
  private static final Logger logger = LoggerFactory.getLogger(IoTConsensusRateLimiter.class);

  private final AverageIntervalRateLimiter rateLimiter = new AverageIntervalRateLimiter();

  private IoTConsensusRateLimiter() {}

  public void init(long regionMigrationSpeedLimitBytesPerSecond) {
    rateLimiter.set(regionMigrationSpeedLimitBytesPerSecond, 1000);
  }

  /**
   * Acquire the size of the data to be sent.
   *
   * @param size the size of the data to be sent
   * @return true if the data can be sent, false if the data cannot be sent
   */
  public boolean reserve(long size) {
    synchronized (this) {
      if (rateLimiter.canExecute(size)) {
        rateLimiter.consume(size);
        return true;
      } else {
        logger.debug(
            "The rate limiter is limited. Required: {}, available: {}",
            size,
            rateLimiter.getAvailable());
        return false;
      }
    }
  }

  private static final IoTConsensusRateLimiter INSTANCE = new IoTConsensusRateLimiter();

  public static IoTConsensusRateLimiter getInstance() {
    return INSTANCE;
  }
}
