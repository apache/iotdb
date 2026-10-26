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

package org.apache.iotdb.session.subscription.consumer.base;

import org.junit.Assert;
import org.junit.Test;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

public class EmptyPollLogThrottlerTest {

  @Test
  public void testThrottleConsecutiveEmptyPollLogs() {
    final AtomicLong ticker = new AtomicLong();
    final EmptyPollLogThrottler throttler = new EmptyPollLogThrottler(100, ticker::get);

    OptionalLong logCount = throttler.markEmptyPollAndMaybeGetCount();
    Assert.assertTrue(logCount.isPresent());
    Assert.assertEquals(1, logCount.getAsLong());

    ticker.addAndGet(99);
    logCount = throttler.markEmptyPollAndMaybeGetCount();
    Assert.assertFalse(logCount.isPresent());

    ticker.incrementAndGet();
    logCount = throttler.markEmptyPollAndMaybeGetCount();
    Assert.assertTrue(logCount.isPresent());
    Assert.assertEquals(3, logCount.getAsLong());
  }

  @Test
  public void testResetMakesNextEmptyPollLoggable() {
    final AtomicLong ticker = new AtomicLong();
    final EmptyPollLogThrottler throttler = new EmptyPollLogThrottler(100, ticker::get);

    Assert.assertTrue(throttler.markEmptyPollAndMaybeGetCount().isPresent());
    Assert.assertFalse(throttler.markEmptyPollAndMaybeGetCount().isPresent());

    throttler.reset();

    final OptionalLong logCount = throttler.markEmptyPollAndMaybeGetCount();
    Assert.assertTrue(logCount.isPresent());
    Assert.assertEquals(1, logCount.getAsLong());
  }
}
