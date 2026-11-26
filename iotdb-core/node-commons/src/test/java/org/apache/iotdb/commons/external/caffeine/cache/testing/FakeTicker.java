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

package org.apache.iotdb.commons.external.caffeine.cache.testing;

import org.apache.iotdb.commons.external.caffeine.cache.Ticker;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Controllable monotonic ticker for deterministic expiration tests. */
public final class FakeTicker implements Ticker {

  private final AtomicLong nanos = new AtomicLong();

  public FakeTicker() {}

  public FakeTicker(long initialNanos) {
    nanos.set(initialNanos);
  }

  public FakeTicker advance(long time, TimeUnit unit) {
    nanos.addAndGet(unit.toNanos(time));
    return this;
  }

  public FakeTicker advance(Duration duration) {
    nanos.addAndGet(duration.toNanos());
    return this;
  }

  public FakeTicker set(long timeNanos) {
    nanos.set(timeNanos);
    return this;
  }

  @Override
  public long read() {
    return nanos.get();
  }
}
