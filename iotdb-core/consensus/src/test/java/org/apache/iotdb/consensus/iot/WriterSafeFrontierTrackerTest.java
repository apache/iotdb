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

public class WriterSafeFrontierTrackerTest {

  @Test
  public void testPendingWriterSafeTimeBarrierPromotesWhenBarrierIsApplied() {
    final WriterSafeFrontierTracker tracker = new WriterSafeFrontierTracker();

    tracker.recordAppliedProgress(100L, 7, 10L);
    assertEquals(100L, tracker.getEffectiveSafePt(7));

    tracker.observePendingWriterSafeTimeBarrier(130L, 7, 20L);
    assertEquals(100L, tracker.getEffectiveSafePt(7));

    tracker.recordAppliedProgress(125L, 7, 19L);
    assertEquals(125L, tracker.getEffectiveSafePt(7));

    tracker.recordAppliedProgress(126L, 7, 20L);
    assertEquals(130L, tracker.getEffectiveSafePt(7));
  }

  @Test
  public void testSameWriterKeepsOnlyNewestPendingWriterSafeTimeBarrier() {
    final WriterSafeFrontierTracker tracker = new WriterSafeFrontierTracker();

    tracker.observePendingWriterSafeTimeBarrier(200L, 9, 30L);
    tracker.observePendingWriterSafeTimeBarrier(220L, 9, 35L);
    tracker.observePendingWriterSafeTimeBarrier(210L, 9, 32L);

    tracker.recordAppliedProgress(0L, 9, 35L);
    assertEquals(220L, tracker.getEffectiveSafePt(9));
  }
}
