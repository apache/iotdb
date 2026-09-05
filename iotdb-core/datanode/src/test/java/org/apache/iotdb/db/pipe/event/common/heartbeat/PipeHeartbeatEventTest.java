/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.event.common.heartbeat;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PipeHeartbeatEventTest {

  @Test
  public void testOnlyBoundCompletionBarrierNeedsOrderedCommit() {
    final PipeHeartbeatEvent rawBarrier = new PipeHeartbeatEvent(1, false, true);
    rawBarrier.bindCompletionBarrier(2L, 3L);
    assertFalse(rawBarrier.needToCommit());

    final PipeTaskMeta taskMeta = mock(PipeTaskMeta.class);
    final EnrichedEvent copiedEvent =
        rawBarrier.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            "test_pipe",
            1L,
            taskMeta,
            null,
            null,
            null,
            null,
            null,
            true,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    final PipeHeartbeatEvent boundBarrier = (PipeHeartbeatEvent) copiedEvent;

    assertTrue(boundBarrier.isCompletionBarrier());
    assertFalse(boundBarrier.needToCommit());
    assertTrue(boundBarrier.getOnCommittedHooks().isEmpty());
    boundBarrier.bindCompletionSource(4L);
    assertTrue(boundBarrier.needToCommit());
    assertEquals(2L, boundBarrier.getAssignerEpoch());
    assertEquals(3L, boundBarrier.getDataGeneration());
    assertEquals(4L, boundBarrier.getCompletionSourceId());
    assertEquals(1, boundBarrier.getOnCommittedHooks().size());

    final PipeHeartbeatEvent periodicHeartbeat = new PipeHeartbeatEvent(1, false);
    final EnrichedEvent boundPeriodicHeartbeat =
        periodicHeartbeat.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            "test_pipe",
            1L,
            taskMeta,
            null,
            null,
            null,
            null,
            null,
            true,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    assertFalse(boundPeriodicHeartbeat.needToCommit());
    assertTrue(boundPeriodicHeartbeat.getOnCommittedHooks().isEmpty());
  }
}
