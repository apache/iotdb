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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ConsensusSubscriptionCommitStateTest {

  @Test
  public void testCommitAdvancesContiguousWriterProgress() {
    final WriterId writerId = new WriterId("1_1", 7, 2L);
    final Map<WriterId, WriterProgress> initialCommitted = new LinkedHashMap<>();
    initialCommitted.put(writerId, new WriterProgress(100L, 0L));
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "1_1", new SubscriptionConsensusProgress(new RegionProgress(initialCommitted), 0L));

    state.recordMapping(writerId, new WriterProgress(101L, 1L));
    state.recordMapping(writerId, new WriterProgress(102L, 2L));
    state.recordMapping(writerId, new WriterProgress(103L, 3L));

    assertTrue(state.commit(writerId, new WriterProgress(102L, 2L)));
    assertEquals(100L, state.getCommittedPhysicalTime());
    assertEquals(0L, state.getCommittedLocalSeq());
    assertEquals(
        new WriterProgress(100L, 0L),
        state.getCommittedRegionProgress().getWriterPositions().get(writerId));

    assertTrue(state.commit(writerId, new WriterProgress(101L, 1L)));
    assertEquals(102L, state.getCommittedPhysicalTime());
    assertEquals(2L, state.getCommittedLocalSeq());
    assertEquals(7, state.getCommittedWriterNodeId());
    assertEquals(2L, state.getCommittedWriterEpoch());
    assertEquals(writerId, state.getCommittedWriterId());
    assertEquals(
        new WriterProgress(102L, 2L),
        state.getCommittedRegionProgress().getWriterPositions().get(writerId));

    assertTrue(state.commit(writerId, new WriterProgress(103L, 3L)));
    assertEquals(103L, state.getCommittedPhysicalTime());
    assertEquals(3L, state.getCommittedLocalSeq());
    assertEquals(7, state.getCommittedWriterNodeId());
    assertEquals(2L, state.getCommittedWriterEpoch());
  }

  @Test
  public void testSerializeDeserializeWriterProgress() throws Exception {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "2_5", new SubscriptionConsensusProgress());
    final Map<WriterId, WriterProgress> seekProgress = new LinkedHashMap<>();
    final WriterId writerA = new WriterId("2_5", 4, 9L);
    final WriterId writerB = new WriterId("2_5", 5, 3L);
    seekProgress.put(writerA, new WriterProgress(222L, 11L));
    seekProgress.put(writerB, new WriterProgress(230L, 4L));
    state.resetForSeek(new RegionProgress(seekProgress));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      state.serialize(dos);
    }

    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState restored =
        ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState.deserialize(
            "2_5", ByteBuffer.wrap(baos.toByteArray()));

    assertEquals(new RegionProgress(seekProgress), restored.getCommittedRegionProgress());
    assertEquals(230L, restored.getCommittedPhysicalTime());
    assertEquals(4L, restored.getCommittedLocalSeq());
    assertEquals(5, restored.getCommittedWriterNodeId());
    assertEquals(3L, restored.getCommittedWriterEpoch());
    assertEquals(writerB, restored.getCommittedWriterId());
    assertEquals(new WriterProgress(230L, 4L), restored.getCommittedWriterProgress());
  }

  @Test
  public void testDirectCommitWithoutOutstandingRequiresOutstandingMapping() {
    final WriterId writerId = new WriterId("3_1", 9, 4L);
    final Map<WriterId, WriterProgress> initialCommitted = new LinkedHashMap<>();
    initialCommitted.put(writerId, new WriterProgress(100L, 0L));
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "3_1", new SubscriptionConsensusProgress(new RegionProgress(initialCommitted), 0L));

    assertFalse(state.commitWithoutOutstanding(writerId, new WriterProgress(103L, 3L)));
    assertEquals(100L, state.getCommittedPhysicalTime());
    assertEquals(0L, state.getCommittedLocalSeq());
  }

  @Test
  public void testDirectCommitWithoutOutstandingRespectsOutstandingGap() {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "3_2", new SubscriptionConsensusProgress());

    final WriterId writerId = new WriterId("3_2", 8, 1L);
    state.recordMapping(writerId, new WriterProgress(101L, 1L));
    state.recordMapping(writerId, new WriterProgress(102L, 2L));
    state.recordMapping(writerId, new WriterProgress(103L, 3L));

    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(103L, 3L)));
    assertEquals(new WriterProgress(0L, -1L), state.getCommittedWriterProgress());

    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(101L, 1L)));
    assertEquals(new WriterProgress(101L, 1L), state.getCommittedWriterProgress());

    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(102L, 2L)));
    assertEquals(new WriterProgress(103L, 3L), state.getCommittedWriterProgress());
  }

  @Test
  public void testBroadcastThrottleKeyIsPerWriter() {
    final String baseKey = "cg##topic##1_1";
    final WriterId writerA = new WriterId("1_1", 7, 1L);
    final WriterId writerB = new WriterId("1_1", 8, 1L);

    assertNotEquals(
        ConsensusSubscriptionCommitManager.buildBroadcastKey(baseKey, writerA),
        ConsensusSubscriptionCommitManager.buildBroadcastKey(baseKey, writerB));
  }
}
