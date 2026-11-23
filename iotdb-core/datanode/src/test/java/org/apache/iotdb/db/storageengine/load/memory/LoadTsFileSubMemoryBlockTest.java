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

package org.apache.iotdb.db.storageengine.load.memory;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LoadTsFileSubMemoryBlockTest {

  private static final long PARENT_MEMORY_SIZE_IN_BYTES = 1024 * 1024L;

  @Test
  public void testAccountsTheUsageOnTheParent() {
    final LoadTsFileDataCacheMemoryBlock parent = newParent();
    final LoadTsFileAbstractMemoryBlock subBlock = parent.createSubBlock(128L);

    subBlock.addMemoryUsage(64L);

    Assert.assertEquals(64L, subBlock.getMemoryUsageInBytes());
    Assert.assertEquals(64L, parent.getMemoryUsageInBytes());

    subBlock.reduceMemoryUsage(64L);

    Assert.assertEquals(0L, subBlock.getMemoryUsageInBytes());
    Assert.assertEquals(0L, parent.getMemoryUsageInBytes());
  }

  @Test
  public void testCapsItsOwnUsage() {
    final LoadTsFileDataCacheMemoryBlock parent = newParent();
    final LoadTsFileAbstractMemoryBlock subBlock = parent.createSubBlock(128L);

    subBlock.addMemoryUsage(120L);

    assertTrue(subBlock.hasEnoughMemory(8L));
    assertFalse(subBlock.hasEnoughMemory(9L));
  }

  @Test
  public void testCapsTheUsageOfTheParentToo() {
    // A sub-block may be allowed to hold more than its parent has free: the parent still decides.
    final LoadTsFileDataCacheMemoryBlock parent = newParent();
    final LoadTsFileAbstractMemoryBlock subBlock =
        parent.createSubBlock(PARENT_MEMORY_SIZE_IN_BYTES * 2);
    parent.addMemoryUsage(PARENT_MEMORY_SIZE_IN_BYTES - 32L);

    assertTrue(subBlock.hasEnoughMemory(32L));
    assertFalse(subBlock.hasEnoughMemory(33L));
  }

  @Test
  public void testCloseReturnsTheUsageToTheParentOnce() {
    final LoadTsFileDataCacheMemoryBlock parent = newParent();
    final LoadTsFileAbstractMemoryBlock subBlock = parent.createSubBlock(128L);
    subBlock.addMemoryUsage(96L);

    subBlock.close();
    subBlock.close();

    assertTrue(subBlock.isClosed());
    Assert.assertEquals(0L, subBlock.getMemoryUsageInBytes());
    Assert.assertEquals(0L, parent.getMemoryUsageInBytes());
  }

  @Test
  public void testRejectsACapThatIsNotPositive() {
    final LoadTsFileDataCacheMemoryBlock parent = newParent();

    try {
      parent.createSubBlock(0L);
      fail("A sub-block that may hold nothing has to be rejected");
    } catch (final IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be positive"));
    }
  }

  @Test
  public void testDoesNotSupportResizing() {
    final LoadTsFileAbstractMemoryBlock subBlock = newParent().createSubBlock(128L);

    try {
      subBlock.forceResize(64L);
      fail("The cap of a sub-block is fixed: resizing it has to be rejected");
    } catch (final UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("not supported"));
    }
  }

  private static LoadTsFileDataCacheMemoryBlock newParent() {
    return new LoadTsFileDataCacheMemoryBlock(PARENT_MEMORY_SIZE_IN_BYTES);
  }
}
