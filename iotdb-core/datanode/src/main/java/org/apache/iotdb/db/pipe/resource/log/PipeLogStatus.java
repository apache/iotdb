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

package org.apache.iotdb.db.pipe.resource.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

class PipeLogStatus {

  private final Logger logger;

  private final int maxAverageScale;
  private final int maxLogInterval;
  private final AtomicLong currentRounds = new AtomicLong(0);

  PipeLogStatus(Class<?> logClass, int maxAverageScale, int maxLogInterval) {
    logger = LoggerFactory.getLogger(logClass);

    this.maxAverageScale = maxAverageScale;
    this.maxLogInterval = maxLogInterval;
  }

  synchronized Optional<Logger> schedule(int scale) {
    if (currentRounds.incrementAndGet()
        >= Math.min((int) Math.ceil((double) scale / maxAverageScale), maxLogInterval)) {
      currentRounds.set(0);
      return Optional.of(logger);
    }

    return Optional.empty();
  }
}
