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
package org.apache.iotdb.commons.memory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryManagerTest {
  private final MemoryManager GLOBAL_MEMORY_MANAGER = MemoryConfig.global();

  @Before
  public void reset() {
    GLOBAL_MEMORY_MANAGER.clearAll();
    GLOBAL_MEMORY_MANAGER.setTotalMemorySizeInBytes(100);
  }

  @Test
  public void testAttributes() {
    Assert.assertEquals("GlobalMemoryManager", GLOBAL_MEMORY_MANAGER.getName());
    Assert.assertFalse(GLOBAL_MEMORY_MANAGER.isEnable());
    Assert.assertEquals(100, GLOBAL_MEMORY_MANAGER.getTotalMemorySizeInBytes());
    Assert.assertEquals(100, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    Assert.assertEquals(0, GLOBAL_MEMORY_MANAGER.getAllocatedMemorySizeInBytes());
    Assert.assertEquals(0, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());
  }

  @Test
  public void testAllocate() throws InterruptedException {
    Assert.assertEquals("GlobalMemoryManager", GLOBAL_MEMORY_MANAGER.getName());
    Assert.assertFalse(GLOBAL_MEMORY_MANAGER.isEnable());
    Assert.assertEquals(100, GLOBAL_MEMORY_MANAGER.getTotalMemorySizeInBytes());

    // create memoryBlock1 in the size of 20 from globalMemoryManager
    IMemoryBlock memoryBlock1 =
        GLOBAL_MEMORY_MANAGER.forceAllocate("Block1", 20, MemoryBlockType.FUNCTION);
    Assert.assertEquals(80, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    Assert.assertEquals(20, GLOBAL_MEMORY_MANAGER.getAllocatedMemorySizeInBytes());
    Assert.assertEquals("Block1", memoryBlock1.getName());
    Assert.assertEquals(20, memoryBlock1.getTotalMemorySizeInBytes());
    Assert.assertEquals(MemoryBlockType.FUNCTION, memoryBlock1.getMemoryBlockType());

    // create memoryBlock2 in the size of 10 from globalMemoryManager
    IMemoryBlock memoryBlock2 =
        GLOBAL_MEMORY_MANAGER.forceAllocateIfSufficient(
            "Block2", 10, 0.9f, MemoryBlockType.PERFORMANCE);
    Assert.assertEquals(70, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    Assert.assertEquals(30, GLOBAL_MEMORY_MANAGER.getAllocatedMemorySizeInBytes());
    Assert.assertEquals("Block2", memoryBlock2.getName());
    Assert.assertEquals(10, memoryBlock2.getTotalMemorySizeInBytes());
    Assert.assertEquals(MemoryBlockType.PERFORMANCE, memoryBlock2.getMemoryBlockType());

    // create subMemoryManager in the size of 50 from globalMemoryManager
    MemoryManager subMemoryManager =
        GLOBAL_MEMORY_MANAGER.getOrCreateMemoryManager("SubManager", 50, true);
    Assert.assertEquals(20, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    Assert.assertEquals(80, GLOBAL_MEMORY_MANAGER.getAllocatedMemorySizeInBytes());
    Assert.assertEquals(50, subMemoryManager.getTotalMemorySizeInBytes());

    // create memoryBlock3 in the size of 20 from globalMemoryManager
    IMemoryBlock memoryBlock3 = GLOBAL_MEMORY_MANAGER.forceAllocate("Block3", MemoryBlockType.NONE);
    Assert.assertEquals(0, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    Assert.assertEquals(100, GLOBAL_MEMORY_MANAGER.getAllocatedMemorySizeInBytes());
    Assert.assertEquals("Block3", memoryBlock3.getName());
    Assert.assertEquals(20, memoryBlock3.getTotalMemorySizeInBytes());
    Assert.assertEquals(MemoryBlockType.NONE, memoryBlock3.getMemoryBlockType());

    // create memoryBlock4 in the size of 50 from subMemoryManager
    IMemoryBlock memoryBlock4 =
        subMemoryManager.tryAllocate("Block4", 100, size -> size / 2, MemoryBlockType.FUNCTION);
    Assert.assertEquals(0, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    Assert.assertEquals("Block4", memoryBlock4.getName());
    Assert.assertEquals(50, memoryBlock4.getTotalMemorySizeInBytes());
    Assert.assertEquals(MemoryBlockType.FUNCTION, memoryBlock4.getMemoryBlockType());

    Assert.assertEquals(0, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());

    // allocate memory
    Assert.assertTrue(memoryBlock1.allocate(5));
    Assert.assertFalse(memoryBlock1.allocate(100));
    Assert.assertEquals(5, memoryBlock1.getUsedMemoryInBytes());
    Assert.assertEquals(5, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());
    Assert.assertTrue(memoryBlock2.allocateUntilAvailable(10, 100));
    Assert.assertEquals(10, memoryBlock2.getUsedMemoryInBytes());
    Assert.assertEquals(15, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());
    Assert.assertTrue(memoryBlock3.allocate(15));
    Assert.assertEquals(15, memoryBlock3.getUsedMemoryInBytes());
    Assert.assertEquals(30, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());
    Assert.assertTrue(memoryBlock4.allocate(40));
    Assert.assertEquals(40, memoryBlock4.getUsedMemoryInBytes());
    Assert.assertEquals(40, subMemoryManager.getUsedMemorySizeInBytes());
    Assert.assertEquals(70, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());

    // release memory
    memoryBlock1.release(100);
    Assert.assertEquals(0, memoryBlock1.getUsedMemoryInBytes());
    Assert.assertEquals(65, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());
    memoryBlock2.release(10);
    Assert.assertEquals(0, memoryBlock2.getUsedMemoryInBytes());
    Assert.assertEquals(55, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());
    memoryBlock3.release(10);
    Assert.assertEquals(5, memoryBlock3.getUsedMemoryInBytes());
    Assert.assertEquals(45, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());
    memoryBlock4.release(1);
    Assert.assertEquals(39, memoryBlock4.getUsedMemoryInBytes());
    Assert.assertEquals(39, subMemoryManager.getUsedMemorySizeInBytes());
    Assert.assertEquals(44, GLOBAL_MEMORY_MANAGER.getUsedMemorySizeInBytes());

    // release memoryBlock3
    GLOBAL_MEMORY_MANAGER.release(memoryBlock3);
    Assert.assertEquals(20, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    // release memoryBlock4
    subMemoryManager.release(memoryBlock4);
    Assert.assertEquals(50, subMemoryManager.getAvailableMemorySizeInBytes());
    Assert.assertNull(subMemoryManager.getOrCreateMemoryManager("SubManager2", 100));

    MemoryManager subMemoryManager2 = subMemoryManager.getOrCreateMemoryManager("SubManager2", 20);
    Assert.assertFalse(subMemoryManager2.isEnable());
    Assert.assertEquals(20, subMemoryManager2.getTotalMemorySizeInBytes());
    Assert.assertEquals(30, subMemoryManager.getAvailableMemorySizeInBytes());

    Assert.assertNull(GLOBAL_MEMORY_MANAGER.getMemoryManager("123"));
    Assert.assertEquals(subMemoryManager, GLOBAL_MEMORY_MANAGER.getMemoryManager("SubManager"));
    Assert.assertEquals(subMemoryManager2, subMemoryManager.getMemoryManager("SubManager2"));
    Assert.assertEquals(
        subMemoryManager2, GLOBAL_MEMORY_MANAGER.getMemoryManager("SubManager", "SubManager2"));

    // create memoryBlock when enable
    IMemoryBlock memoryBlock5 =
        subMemoryManager.forceAllocate("Block5", 10, MemoryBlockType.FUNCTION);
    Assert.assertEquals(10, memoryBlock5.getTotalMemorySizeInBytes());
    Assert.assertEquals(20, subMemoryManager.getAvailableMemorySizeInBytes());
    Assert.assertNull(
        subMemoryManager.forceAllocateIfSufficient("Block6", 5, 0.6f, MemoryBlockType.FUNCTION));
    IMemoryBlock memoryBlock6 =
        subMemoryManager.forceAllocateIfSufficient("Block6", 5, 0.8f, MemoryBlockType.FUNCTION);
    Assert.assertEquals(5, memoryBlock6.getTotalMemorySizeInBytes());
    Assert.assertEquals(15, subMemoryManager.getAvailableMemorySizeInBytes());
    IMemoryBlock memoryBlock7 =
        subMemoryManager2.tryAllocate("Block7", 5, size -> size / 2, MemoryBlockType.FUNCTION);
    Assert.assertEquals(5, memoryBlock7.getTotalMemorySizeInBytes());
    Assert.assertEquals(15, subMemoryManager2.getAvailableMemorySizeInBytes());

    // release subMemoryManager
    GLOBAL_MEMORY_MANAGER.releaseChildMemoryManager("SubManager");
    Assert.assertEquals(70, GLOBAL_MEMORY_MANAGER.getAvailableMemorySizeInBytes());
    Assert.assertFalse(memoryBlock1.isReleased());
    Assert.assertFalse(memoryBlock2.isReleased());
    Assert.assertTrue(memoryBlock3.isReleased());
    Assert.assertTrue(memoryBlock4.isReleased());
    Assert.assertTrue(memoryBlock5.isReleased());
    Assert.assertTrue(memoryBlock6.isReleased());
    Assert.assertTrue(memoryBlock7.isReleased());
  }
}
