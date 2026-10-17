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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ReadOnlyMemoryBlockTest {
  @Test
  public void externalUsageAndBudgetHaveSeparateLifetimes() throws Exception {
    MemoryManager manager = new MemoryManager(100);
    IMemoryBlock other = manager.exactAllocate("other", 20, MemoryBlockType.DYNAMIC);
    other.allocate(7);
    AtomicLong owner = new AtomicLong(11);
    IMemoryBlock view = manager.exactAllocateReadOnly("view", MemoryBlockType.DYNAMIC, owner::get);
    assertEquals(100, manager.getAllocatedMemorySizeInBytes());
    assertEquals(80, view.getTotalMemorySizeInBytes());
    assertEquals(69, view.getFreeMemoryInBytes());
    assertEquals(18, manager.getUsedMemorySizeInBytes());
    owner.set(37);
    assertEquals(37, view.getUsedMemoryInBytes());
    assertEquals(44, manager.getUsedMemorySizeInBytes());
    view.close();
    view.close();
    assertTrue(view.isReleased());
    assertEquals(37, owner.get());
    assertEquals(20, manager.getAllocatedMemorySizeInBytes());
    assertEquals(7, manager.getUsedMemorySizeInBytes());
    other.close();
    assertEquals(0, manager.getAllocatedMemorySizeInBytes());
  }

  @Test
  public void writesCannotChangeAnExternallyOwnedUsage() throws Exception {
    AtomicLong owner = new AtomicLong(11);
    IMemoryBlock view =
        new MemoryManager(100).exactAllocateReadOnly("view", MemoryBlockType.DYNAMIC, owner::get);
    assertThrows(UnsupportedOperationException.class, () -> view.allocate(1));
    assertThrows(UnsupportedOperationException.class, () -> view.forceAllocateWithoutLimitation(1));
    assertThrows(UnsupportedOperationException.class, () -> view.allocateIfSufficient(1, .5));
    assertThrows(UnsupportedOperationException.class, () -> view.allocateUntilAvailable(1, 1));
    assertThrows(UnsupportedOperationException.class, () -> view.release(1));
    assertThrows(UnsupportedOperationException.class, () -> view.setUsedMemoryInBytes(1));
    assertEquals(11, view.getUsedMemoryInBytes());
    view.close();
  }

  @Test
  public void mutableOwnerCannotSilentlyBecomeAReadOnlyView() {
    MemoryManager manager = new MemoryManager(100);
    IMemoryBlock existing = manager.exactAllocate("owner", 20, MemoryBlockType.DYNAMIC);
    existing.allocate(7);
    assertThrows(
        MemoryException.class,
        () -> manager.exactAllocateReadOnly("owner", MemoryBlockType.DYNAMIC, () -> 11));
    assertEquals(20, manager.getAllocatedMemorySizeInBytes());
    assertEquals(7, manager.getUsedMemorySizeInBytes());
  }
}
