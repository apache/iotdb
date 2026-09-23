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
}
