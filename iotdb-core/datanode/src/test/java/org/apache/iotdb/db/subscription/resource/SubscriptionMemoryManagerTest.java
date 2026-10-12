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

package org.apache.iotdb.db.subscription.resource;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SubscriptionMemoryManagerTest {

  @Test
  public void testAllocateAndReleaseWithinBudget() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(10L);

    assertTrue(memoryManager.tryAllocate(6L));
    assertFalse(memoryManager.tryAllocate(5L));
    assertEquals(6L, memoryManager.getUsedMemorySizeInBytes());
    assertEquals(4L, memoryManager.getFreeMemorySizeInBytes());

    memoryManager.release(6L);
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    assertEquals(10L, memoryManager.getFreeMemorySizeInBytes());
  }

  @Test
  public void testAllowOnlyOneOversizedEntryWhenBudgetIsEmpty() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(10L);

    assertTrue(memoryManager.tryAllocate(11L));
    assertFalse(memoryManager.tryAllocate(1L));
    assertEquals(11L, memoryManager.getUsedMemorySizeInBytes());

    memoryManager.release(11L);
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    assertTrue(memoryManager.tryAllocate(10L));
  }

  @Test
  public void testZeroBudgetRejectsAllocation() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(0L);

    assertFalse(memoryManager.tryAllocate(1L));
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testQueueCanBorrowIdleMemoryWithoutConsumingAnotherQueuesProtectedShare() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(10L);
    final SubscriptionMemoryManager.MemoryHandle queueA = memoryManager.registerQueue();
    final SubscriptionMemoryManager.MemoryHandle queueB = memoryManager.registerQueue();

    assertEquals(5L, queueA.getMemoryQuotaInBytes());
    assertEquals(
        SubscriptionMemoryManager.AllocationRejectionReason.OVERSIZED_ENTRY,
        queueA.inspectRejection(9L));
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());

    assertTrue(queueA.tryAllocate(6L).isAccepted());
    assertTrue(queueA.tryAllocate(2L).isAccepted());
    assertEquals(
        SubscriptionMemoryManager.AllocationRejectionReason.MEMORY_QUOTA,
        queueA.inspectRejection(1L));
    assertTrue(queueB.tryAllocate(2L).isAccepted());
    assertFalse(queueB.tryAllocate(1L).isAccepted());
    assertEquals(10L, memoryManager.getUsedMemorySizeInBytes());

    queueA.close();
    assertEquals(10L, queueB.getMemoryQuotaInBytes());
    assertTrue(queueB.tryAllocate(8L).isAccepted());
    assertEquals(10L, memoryManager.getUsedMemorySizeInBytes());

    queueB.close();
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testBorrowedMemoryWaitsForAckWhenAnotherQueueNeedsMoreThanItsReserve() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(100L);
    try (SubscriptionMemoryManager.MemoryHandle largeQueue = memoryManager.registerQueue();
        SubscriptionMemoryManager.MemoryHandle smallQueue = memoryManager.registerQueue()) {
      assertTrue(largeQueue.tryAllocate(75L).isAccepted());
      assertEquals(25L, smallQueue.getFreeMemorySizeInBytes());
      assertEquals(
          SubscriptionMemoryManager.AllocationRejectionReason.MEMORY_LIMIT,
          smallQueue.inspectRejection(30L));
      assertTrue(smallQueue.tryAllocate(25L).isAccepted());
      assertEquals(0L, smallQueue.getFreeMemorySizeInBytes());
      largeQueue.release(50L);
      assertTrue(smallQueue.tryAllocate(30L).isAccepted());
      assertEquals(80L, memoryManager.getUsedMemorySizeInBytes());
    }
  }

  @Test
  public void testNewQueueWaitsForExistingBorrowedAllocationsToDrain() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(100L);
    try (SubscriptionMemoryManager.MemoryHandle first = memoryManager.registerQueue()) {
      assertTrue(first.tryAllocate(100L).isAccepted());
      try (SubscriptionMemoryManager.MemoryHandle second = memoryManager.registerQueue()) {
        assertEquals(
            SubscriptionMemoryManager.AllocationRejectionReason.MEMORY_LIMIT,
            second.inspectRejection(1L));
        first.release(25L);
        assertTrue(second.tryAllocate(25L).isAccepted());
      }
    }
  }

  @Test
  public void testOversizedQueueEntryIsExplicitlyRejectedEvenWhenNodeIsEmpty() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(10L);
    try (SubscriptionMemoryManager.MemoryHandle queue = memoryManager.registerQueue()) {
      assertEquals(
          SubscriptionMemoryManager.AllocationRejectionReason.OVERSIZED_ENTRY,
          queue.tryAllocate(11L).getRejectionReason());
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
      assertTrue(queue.tryAllocate(10L).isAccepted());
    }
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testInactiveQueueKeepsShareUntilRetainedDataIsReleased() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(10L);
    try (SubscriptionMemoryManager.MemoryHandle queueA = memoryManager.registerQueue();
        SubscriptionMemoryManager.MemoryHandle queueB = memoryManager.registerQueue()) {
      assertTrue(queueA.tryAllocate(5L).isAccepted());
      queueA.setActive(false);
      assertEquals(5L, queueB.getMemoryQuotaInBytes());
      assertTrue(queueB.tryAllocate(5L).isAccepted());
      queueA.release(5L);
      assertEquals(10L, queueB.getMemoryQuotaInBytes());
      assertTrue(queueB.tryAllocate(5L).isAccepted());
    }
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testAllocationProbeDoesNotReserveMemory() {
    final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(10L);
    try (SubscriptionMemoryManager.MemoryHandle queue = memoryManager.registerQueue()) {
      for (int i = 0; i < 100; i++) {
        assertTrue(queue.canAllocate(10L));
        assertEquals(
            SubscriptionMemoryManager.AllocationRejectionReason.NONE, queue.inspectRejection(10L));
      }
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
      assertTrue(queue.tryAllocate(10L).isAccepted());
      queue.release(100L);
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    }
  }
}
