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

import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class LoadTsFileMemoryManagerTest {

  @Test
  public void testAllocateDataCacheMemoryBlockDoesNotDoubleCountMemory() throws Exception {
    final long allocatedMemoryInBytes = 2L * 1024 * 1024;
    final LoadTsFileMemoryManager manager = spy(newMemoryManager());

    doAnswer(
            invocation -> {
              setUsedMemorySize(manager, allocatedMemoryInBytes);
              return allocatedMemoryInBytes;
            })
        .when(manager)
        .tryAllocateFromQuery(anyLong());

    manager.allocateDataCacheMemoryBlock();

    Assert.assertEquals(allocatedMemoryInBytes, manager.getUsedMemorySizeInBytes());
    Assert.assertNotNull(getDataCacheMemoryBlock(manager));
  }

  @Test
  public void testAllocateDataCacheMemoryBlockRollsBackPartialAllocationOnFailure()
      throws Exception {
    final long allocatedMemoryInBytes = 512L;
    final LoadTsFileMemoryManager manager = spy(newMemoryManager());

    doAnswer(
            invocation -> {
              setUsedMemorySize(manager, allocatedMemoryInBytes);
              return allocatedMemoryInBytes;
            })
        .when(manager)
        .tryAllocateFromQuery(anyLong());
    doAnswer(
            invocation -> {
              setUsedMemorySize(manager, 0L);
              return null;
            })
        .when(manager)
        .releaseToQuery(anyLong());

    try {
      manager.allocateDataCacheMemoryBlock();
      Assert.fail("Expected LoadRuntimeOutOfMemoryException");
    } catch (LoadRuntimeOutOfMemoryException e) {
      Assert.assertEquals(0L, manager.getUsedMemorySizeInBytes());
      Assert.assertNull(getDataCacheMemoryBlock(manager));
    }
  }

  private static LoadTsFileMemoryManager newMemoryManager() throws Exception {
    final Constructor<LoadTsFileMemoryManager> constructor =
        LoadTsFileMemoryManager.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static void setUsedMemorySize(
      final LoadTsFileMemoryManager manager, final long usedMemorySizeInBytes) throws Exception {
    final Field field = LoadTsFileMemoryManager.class.getDeclaredField("usedMemorySizeInBytes");
    field.setAccessible(true);
    ((AtomicLong) field.get(manager)).set(usedMemorySizeInBytes);
  }

  private static LoadTsFileDataCacheMemoryBlock getDataCacheMemoryBlock(
      final LoadTsFileMemoryManager manager) throws Exception {
    final Field field = LoadTsFileMemoryManager.class.getDeclaredField("dataCacheMemoryBlock");
    field.setAccessible(true);
    return (LoadTsFileDataCacheMemoryBlock) field.get(manager);
  }
}
