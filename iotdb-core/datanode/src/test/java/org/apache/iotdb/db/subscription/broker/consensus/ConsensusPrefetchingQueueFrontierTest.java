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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue.OrderingKey;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue.WriterFrontier;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue.WriterFrontierQueue;

import org.junit.Test;

import java.util.PriorityQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsensusPrefetchingQueueFrontierTest {

  @Test
  public void testIncrementalHeadInsertionPreservesOrdering() {
    final PriorityQueue<WriterFrontier> initialFrontiers = new PriorityQueue<>();
    initialFrontiers.add(WriterFrontier.forHead(1, new OrderingKey(100L, 1, 1L)));
    initialFrontiers.add(WriterFrontier.forHead(2, new OrderingKey(90L, 2, 1L)));
    final WriterFrontierQueue frontiers = new WriterFrontierQueue(initialFrontiers);

    assertEquals(2, frontiers.poll().getWriterNodeId());
    frontiers.add(WriterFrontier.forHead(2, new OrderingKey(110L, 2, 2L)));

    assertEquals(1, frontiers.poll().getWriterNodeId());
    assertEquals(2, frontiers.poll().getWriterNodeId());
    assertNull(frontiers.peek());
  }

  @Test
  public void testUnknownActiveWriterBarrierBlocksKnownHeadsAndResetClearsIt() {
    final PriorityQueue<WriterFrontier> initialFrontiers = new PriorityQueue<>();
    initialFrontiers.add(WriterFrontier.forHead(1, new OrderingKey(1L, 1, 1L)));
    initialFrontiers.add(WriterFrontier.forUnknownActiveWriter(2));
    final WriterFrontierQueue frontiers = new WriterFrontierQueue(initialFrontiers);

    assertTrue(frontiers.hasUnknownActiveWriterBarrier());
    assertTrue(frontiers.peek().isBarrier());
    assertEquals(2, frontiers.peek().getWriterNodeId());

    frontiers.reset(new PriorityQueue<>());

    assertFalse(frontiers.hasUnknownActiveWriterBarrier());
  }
}
