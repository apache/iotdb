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

package org.apache.iotdb.session.subscription.consumer;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class EmptyPollLogThrottler {

  private static final long DEFAULT_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

  private final long logIntervalNanos;
  private final LongSupplier ticker;

  private long consecutiveEmptyPollCount;
  private long lastLogTimeNanos;
  private boolean hasLoggedEmptyPoll;

  EmptyPollLogThrottler() {
    this(DEFAULT_LOG_INTERVAL_NANOS, System::nanoTime);
  }

  EmptyPollLogThrottler(final long logIntervalNanos, final LongSupplier ticker) {
    this.logIntervalNanos = Math.max(logIntervalNanos, 1);
    this.ticker = ticker;
  }

  synchronized OptionalLong markEmptyPollAndMaybeGetCount() {
    consecutiveEmptyPollCount++;
    final long currentTimeNanos = ticker.getAsLong();
    if (!hasLoggedEmptyPoll || currentTimeNanos - lastLogTimeNanos >= logIntervalNanos) {
      hasLoggedEmptyPoll = true;
      lastLogTimeNanos = currentTimeNanos;
      return OptionalLong.of(consecutiveEmptyPollCount);
    }
    return OptionalLong.empty();
  }

  synchronized void reset() {
    consecutiveEmptyPollCount = 0;
    lastLogTimeNanos = 0;
    hasLoggedEmptyPoll = false;
  }
}
