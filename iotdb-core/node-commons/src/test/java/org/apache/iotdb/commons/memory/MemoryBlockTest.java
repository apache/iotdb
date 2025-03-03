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
import org.junit.Test;

public class MemoryBlockTest {

  @Test
  public void test() throws Exception {
    IMemoryBlock memoryBlock1 = new AtomicLongMemoryBlock("Block1", null, 20);
    IMemoryBlock memoryBlock2 =
        new AtomicLongMemoryBlock("Block2", null, 100, MemoryBlockType.DYNAMIC);
    // Test Attributes
    Assert.assertEquals("Block1", memoryBlock1.getName());
    Assert.assertEquals(0, memoryBlock1.getUsedMemoryInBytes());
    Assert.assertEquals(20, memoryBlock1.getFreeMemoryInBytes());
    Assert.assertEquals(20, memoryBlock1.getTotalMemorySizeInBytes());
    Assert.assertEquals(MemoryBlockType.NONE, memoryBlock1.getMemoryBlockType());
    Assert.assertFalse(memoryBlock1.isReleased());

    Assert.assertEquals("Block2", memoryBlock2.getName());
    Assert.assertEquals(0, memoryBlock2.getUsedMemoryInBytes());
    Assert.assertEquals(100, memoryBlock2.getFreeMemoryInBytes());
    Assert.assertEquals(100, memoryBlock2.getTotalMemorySizeInBytes());
    Assert.assertEquals(MemoryBlockType.DYNAMIC, memoryBlock2.getMemoryBlockType());
    Assert.assertFalse(memoryBlock2.isReleased());

    // Test Allocation
    Assert.assertTrue(memoryBlock1.allocate(10));
    Assert.assertEquals(10, memoryBlock1.getUsedMemoryInBytes());
    Assert.assertEquals(10, memoryBlock1.getFreeMemoryInBytes());
    Assert.assertFalse(memoryBlock1.allocate(20));
    Assert.assertEquals(10, memoryBlock1.getUsedMemoryInBytes());
    Assert.assertEquals(10, memoryBlock1.getFreeMemoryInBytes());
    Assert.assertFalse(memoryBlock1.allocateIfSufficient(10, 0.5));
    Assert.assertEquals(0, memoryBlock1.release(10));
    Assert.assertEquals(0, memoryBlock1.getUsedMemoryInBytes());
    Assert.assertEquals(20, memoryBlock1.getFreeMemoryInBytes());
    Assert.assertTrue(memoryBlock1.allocateIfSufficient(10, 0.5));
    Assert.assertEquals(10, memoryBlock1.getUsedMemoryInBytes());
    Assert.assertEquals(10, memoryBlock1.getFreeMemoryInBytes());

    memoryBlock2.markAsReleased();
    Assert.assertTrue(memoryBlock2.isReleased());

    memoryBlock1.close();
    memoryBlock2.close();
  }
}
