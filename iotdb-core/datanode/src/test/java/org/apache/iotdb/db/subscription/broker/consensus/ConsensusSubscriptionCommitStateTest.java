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

import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConsensusSubscriptionCommitStateTest {

  @Test
  public void testCommitAdvancesContiguousWriterProgress() {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "1_1", new SubscriptionConsensusProgress(100L, 0L, 0L));

    state.recordMapping(new WriterId("1_1", 7, 2L), new WriterProgress(101L, 1L));
    state.recordMapping(new WriterId("1_1", 7, 2L), new WriterProgress(102L, 2L));
    state.recordMapping(new WriterId("1_1", 7, 2L), new WriterProgress(103L, 3L));

    assertTrue(state.commit(new WriterId("1_1", 7, 2L), new WriterProgress(102L, 2L)));
    assertEquals(100L, state.getCommittedPhysicalTime());
    assertEquals(0L, state.getCommittedLocalSeq());

    assertTrue(state.commit(new WriterId("1_1", 7, 2L), new WriterProgress(101L, 1L)));
    assertEquals(102L, state.getCommittedPhysicalTime());
    assertEquals(2L, state.getCommittedLocalSeq());
    assertEquals(7, state.getCommittedWriterNodeId());
    assertEquals(2L, state.getCommittedWriterEpoch());
    assertEquals(new WriterId("1_1", 7, 2L), state.getCommittedWriterId());

    assertTrue(state.commit(new WriterId("1_1", 7, 2L), new WriterProgress(103L, 3L)));
    assertEquals(103L, state.getCommittedPhysicalTime());
    assertEquals(3L, state.getCommittedLocalSeq());
    assertEquals(7, state.getCommittedWriterNodeId());
    assertEquals(2L, state.getCommittedWriterEpoch());
  }

  @Test
  public void testSerializeDeserializeWriterProgress() throws Exception {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "2_5", new SubscriptionConsensusProgress(0L, -1L, 0L));
    state.resetForSeek(new WriterId("2_5", 4, 9L), new WriterProgress(222L, 11L));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      state.serialize(dos);
    }

    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState restored =
        ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState.deserialize(
            "2_5", ByteBuffer.wrap(baos.toByteArray()));

    assertEquals(222L, restored.getCommittedPhysicalTime());
    assertEquals(11L, restored.getCommittedLocalSeq());
    assertEquals(4, restored.getCommittedWriterNodeId());
    assertEquals(9L, restored.getCommittedWriterEpoch());
    assertEquals(new WriterId("2_5", 4, 9L), restored.getCommittedWriterId());
    assertEquals(222L, restored.getCommittedWriterProgress().getPhysicalTime());
    assertEquals(11L, restored.getCommittedWriterProgress().getLocalSeq());
  }

  @Test
  public void testDirectCommitWithoutOutstandingActsAsWriterCheckpoint() {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "3_1", new SubscriptionConsensusProgress(100L, 0L, 0L));

    final WriterId writerId = new WriterId("3_1", 9, 4L);
    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(103L, 3L)));
    assertEquals(103L, state.getCommittedPhysicalTime());
    assertEquals(3L, state.getCommittedLocalSeq());

    assertTrue(state.commitWithoutOutstanding(writerId, new WriterProgress(101L, 1L)));
    assertEquals(103L, state.getCommittedPhysicalTime());
    assertEquals(3L, state.getCommittedLocalSeq());
  }

  @Test
  public void testDirectCommitWithoutOutstandingIsIndependentPerWriter() {
    final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState state =
        new ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState(
            "3_2", new SubscriptionConsensusProgress(100L, 0L, 0L));

    final WriterId writerA = new WriterId("3_2", 7, 1L);
    final WriterId writerB = new WriterId("3_2", 8, 1L);

    assertTrue(state.commitWithoutOutstanding(writerA, new WriterProgress(110L, 10L)));
    assertTrue(state.commitWithoutOutstanding(writerB, new WriterProgress(105L, 5L)));

    assertEquals(
        new WriterProgress(110L, 10L),
        state.getCommittedRegionProgress().getWriterPositions().get(writerA));
    assertEquals(
        new WriterProgress(105L, 5L),
        state.getCommittedRegionProgress().getWriterPositions().get(writerB));

    assertTrue(state.commitWithoutOutstanding(writerB, new WriterProgress(103L, 3L)));
    assertEquals(
        new WriterProgress(105L, 5L),
        state.getCommittedRegionProgress().getWriterPositions().get(writerB));
  }
}
