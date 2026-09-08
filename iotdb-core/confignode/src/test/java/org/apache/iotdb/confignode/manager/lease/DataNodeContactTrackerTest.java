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

package org.apache.iotdb.confignode.manager.lease;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class DataNodeContactTrackerTest {

  private static final int DN = 3;

  @Test
  public void reportsMillisSinceLastSuccessfulResponse() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final DataNodeContactTracker tracker = new DataNodeContactTracker(nowNanos::get);
    tracker.recordSuccessfulResponse(DN);
    nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1234));
    assertEquals(1234L, tracker.getMillisSinceLastSuccessfulResponse(DN));
  }

  @Test
  public void ageKeepsGrowingWithoutSuccessfulResponse() {
    // Failures must NOT refresh the contact time. This is enforced structurally: only
    // recordSuccessfulResponse updates it, so with no further success the age keeps growing.
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final DataNodeContactTracker tracker = new DataNodeContactTracker(nowNanos::get);
    tracker.recordSuccessfulResponse(DN);
    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(30));
    assertEquals(30_000L, tracker.getMillisSinceLastSuccessfulResponse(DN));
  }

  @Test
  public void leadershipAcquisitionResetsContactToNow() {
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final DataNodeContactTracker tracker = new DataNodeContactTracker(nowNanos::get);
    tracker.recordSuccessfulResponse(DN);
    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(30)); // would otherwise look stale
    tracker.onLeadershipAcquired(Arrays.asList(DN, 4));
    assertEquals(0L, tracker.getMillisSinceLastSuccessfulResponse(DN));
    assertEquals(0L, tracker.getMillisSinceLastSuccessfulResponse(4));
  }

  @Test
  public void neverContactedReadsAsZeroSoVerdictTreatsAsRecent() {
    // Conservative: an unknown DataNode must NOT look fenced (else the verdict would wrongly
    // proceed past it), so its age reads as 0 until a real success/expiry is observed.
    final AtomicLong nowNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
    final DataNodeContactTracker tracker = new DataNodeContactTracker(nowNanos::get);
    assertEquals(0L, tracker.getMillisSinceLastSuccessfulResponse(999));
  }
}
