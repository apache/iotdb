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

package org.apache.iotdb.commons.pipe.resource;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class PipeRecentFailureCounterTest {

  @Test
  public void testCountsOnlyFailuresWithinOneMinute() {
    final PipeRecentFailureCounter counter = new PipeRecentFailureCounter();
    final long now = 100_000L;

    counter.record(
        PipeResourceFailureType.NETWORK_TIMEOUT, now - PipeRecentFailureCounter.WINDOW_MILLIS - 1);
    counter.record(
        PipeResourceFailureType.NETWORK_TIMEOUT, now - PipeRecentFailureCounter.WINDOW_MILLIS);
    counter.record(PipeResourceFailureType.NETWORK_TIMEOUT, now);
    counter.record(PipeResourceFailureType.MEMORY_TIMEOUT, now);

    final Map<String, Long> failures = counter.getRecentFailures(now);
    Assert.assertEquals(Long.valueOf(2), failures.get("network_timeout"));
    Assert.assertEquals(Long.valueOf(1), failures.get("memory_timeout"));

    Assert.assertTrue(
        counter.getRecentFailures(now + PipeRecentFailureCounter.WINDOW_MILLIS + 1).isEmpty());
  }
}
