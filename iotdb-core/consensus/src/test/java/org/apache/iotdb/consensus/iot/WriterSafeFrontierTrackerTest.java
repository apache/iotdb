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

package org.apache.iotdb.consensus.iot;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WriterSafeFrontierTrackerTest {

  @Test
  public void testPendingSafeHlcPromotesWhenBarrierIsApplied() {
    final WriterSafeFrontierTracker tracker = new WriterSafeFrontierTracker();

    tracker.recordAppliedProgress(7, 2L, 100L, 10L);
    assertEquals(100L, tracker.getEffectiveSafePt(7, 2L));

    tracker.observePendingSafeHlc(7, 2L, 130L, 20L);
    assertEquals(100L, tracker.getEffectiveSafePt(7, 2L));
    assertEquals(130L, tracker.getPendingSafeHlc(7, 2L).getSafePhysicalTime());

    tracker.recordAppliedProgress(7, 2L, 125L, 19L);
    assertEquals(125L, tracker.getEffectiveSafePt(7, 2L));

    tracker.recordAppliedProgress(7, 2L, 126L, 20L);
    assertEquals(130L, tracker.getEffectiveSafePt(7, 2L));
    assertNull(tracker.getPendingSafeHlc(7, 2L));
  }

  @Test
  public void testSameWriterKeepsOnlyNewestPendingSafeHlc() {
    final WriterSafeFrontierTracker tracker = new WriterSafeFrontierTracker();

    tracker.observePendingSafeHlc(9, 3L, 200L, 30L);
    tracker.observePendingSafeHlc(9, 3L, 220L, 35L);

    assertEquals(220L, tracker.getPendingSafeHlc(9, 3L).getSafePhysicalTime());
    assertEquals(35L, tracker.getPendingSafeHlc(9, 3L).getBarrierLocalSeq());

    tracker.observePendingSafeHlc(9, 3L, 210L, 32L);
    assertEquals(220L, tracker.getPendingSafeHlc(9, 3L).getSafePhysicalTime());
    assertEquals(35L, tracker.getPendingSafeHlc(9, 3L).getBarrierLocalSeq());
  }
}
