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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PipeMemoryManagerResizeTest {

  private static final long TOTAL_MEMORY_SIZE_IN_BYTES = 2000;
  private static final long TABLET_MEMORY_SIZE_IN_BYTES = 901;
  private static final long SINK_MEMORY_SIZE_IN_BYTES = 100;

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();

  private boolean originalMemoryManagementEnabled;
  private int originalAllocateMaxRetries;
  private long originalAllocateRetryIntervalInMs;
  private double originalFloatingMemoryProportion;
  private double originalTabletRejectThreshold;
  private double originalTsFileRejectThreshold;

  @Before
  public void setUp() {
    originalMemoryManagementEnabled = config.getPipeMemoryManagementEnabled();
    originalAllocateMaxRetries = config.getPipeMemoryAllocateMaxRetries();
    originalAllocateRetryIntervalInMs = config.getPipeMemoryAllocateRetryIntervalInMs();
    originalFloatingMemoryProportion = config.getPipeTotalFloatingMemoryProportion();
    originalTabletRejectThreshold =
        config.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold();
    originalTsFileRejectThreshold =
        config.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold();

    config.setPipeMemoryManagementEnabled(true);
    config.setPipeMemoryAllocateMaxRetries(1);
    config.setPipeMemoryAllocateRetryIntervalInMs(1);
    config.setPipeTotalFloatingMemoryProportion(0.5);
    config.setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(0.3);
    config.setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(0.3);
  }

  @After
  public void tearDown() {
    config.setPipeMemoryManagementEnabled(originalMemoryManagementEnabled);
    config.setPipeMemoryAllocateMaxRetries(originalAllocateMaxRetries);
    config.setPipeMemoryAllocateRetryIntervalInMs(originalAllocateRetryIntervalInMs);
    config.setPipeTotalFloatingMemoryProportion(originalFloatingMemoryProportion);
    config.setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(
        originalTabletRejectThreshold);
    config.setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(
        originalTsFileRejectThreshold);
  }

  @Test
  public void testTabletResizeCannotCrossTabletHardLimit() {
    final PipeMemoryManager manager =
        new PipeMemoryManager(
            new AtomicLongMemoryBlock(
                "PipeMemoryManagerResizeTest",
                null,
                TOTAL_MEMORY_SIZE_IN_BYTES,
                MemoryBlockType.DYNAMIC));
    final PipeTabletMemoryBlock tablet = manager.forceAllocateForTabletWithRetry("tablet", 0);

    try {
      Assert.assertThrows(
          PipeRuntimeOutOfMemoryCriticalException.class,
          () -> manager.forceResize(tablet, TABLET_MEMORY_SIZE_IN_BYTES));
      Assert.assertEquals(0, tablet.getMemoryUsageInBytes());
      Assert.assertEquals(0, manager.getUsedMemorySizeInBytes());
      Assert.assertEquals(0, manager.getUsedMemorySizeInBytesOfTablets());
    } finally {
      manager.release(tablet);
    }
  }

  @Test
  public void testTabletResizeLeavesMemoryForSinkForwardProgress() {
    final PipeMemoryManager manager =
        new PipeMemoryManager(
            new AtomicLongMemoryBlock(
                "PipeMemoryManagerResizeTest",
                null,
                TOTAL_MEMORY_SIZE_IN_BYTES,
                MemoryBlockType.DYNAMIC));
    final PipeTabletMemoryBlock retainedTablet =
        manager.forceAllocateForTabletWithRetry("retainedTablet", TABLET_MEMORY_SIZE_IN_BYTES);
    final PipeTabletMemoryBlock pendingTablet =
        manager.forceAllocateForTabletWithRetry("pendingTablet", 0);
    final PipeMemoryBlock sinkBatch = manager.forceAllocate("sinkBatch", 0);

    try {
      Assert.assertThrows(
          PipeRuntimeOutOfMemoryCriticalException.class,
          () -> manager.forceResize(pendingTablet, 1));
      Assert.assertEquals(TABLET_MEMORY_SIZE_IN_BYTES, manager.getUsedMemorySizeInBytes());
      Assert.assertEquals(TABLET_MEMORY_SIZE_IN_BYTES, manager.getUsedMemorySizeInBytesOfTablets());

      manager.forceResize(sinkBatch, SINK_MEMORY_SIZE_IN_BYTES);
      Assert.assertEquals(
          TABLET_MEMORY_SIZE_IN_BYTES + SINK_MEMORY_SIZE_IN_BYTES,
          manager.getUsedMemorySizeInBytes());

      manager.release(retainedTablet);
      manager.forceResize(pendingTablet, 1);
      Assert.assertEquals(1, manager.getUsedMemorySizeInBytesOfTablets());
    } finally {
      manager.release(retainedTablet);
      manager.release(pendingTablet);
      manager.release(sinkBatch);
    }

    Assert.assertEquals(0, manager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testFloatingAndNonFloatingMemoryShareTheSamePool() {
    final AtomicLong floatingMemoryUsageInBytes = new AtomicLong(0);
    final PipeMemoryManager manager =
        new PipeMemoryManager(
            new AtomicLongMemoryBlock(
                "PipeMemoryManagerResizeTest",
                null,
                TOTAL_MEMORY_SIZE_IN_BYTES,
                MemoryBlockType.DYNAMIC),
            floatingMemoryUsageInBytes::get);

    Assert.assertEquals(TOTAL_MEMORY_SIZE_IN_BYTES, manager.getTotalNonFloatingMemorySizeInBytes());
    Assert.assertEquals(
        TOTAL_MEMORY_SIZE_IN_BYTES / 2, manager.getTotalFloatingMemorySizeInBytes());

    final PipeTsFileMemoryBlock nonFloatingMemory =
        manager.forceAllocateForTsFileWithRetry("tsFile", 1200);
    try {
      // Non-floating memory can borrow the unused half that was previously reserved for InsertNode
      // queues. Its usage also reduces the current floating-memory limit symmetrically.
      Assert.assertEquals(1200, manager.getUsedMemorySizeInBytes());
      Assert.assertEquals(800, manager.getTotalFloatingMemorySizeInBytes());

      floatingMemoryUsageInBytes.set(500);
      Assert.assertEquals(1500, manager.getTotalNonFloatingMemorySizeInBytes());
      Assert.assertEquals(300, manager.getFreeMemorySizeInBytes());

      Assert.assertThrows(
          PipeRuntimeOutOfMemoryCriticalException.class,
          () -> manager.forceAllocate("normal", 301));
    } finally {
      manager.release(nonFloatingMemory);
    }
  }

  @Test
  public void testMemoryBlockInfoIncludesNamesAndSeparatesFloatingMemory() {
    final AtomicLong floatingMemoryUsageInBytes = new AtomicLong(0);
    final PipeMemoryManager manager =
        new PipeMemoryManager(
            new AtomicLongMemoryBlock(
                "PipeMemoryManagerResizeTest",
                null,
                TOTAL_MEMORY_SIZE_IN_BYTES,
                MemoryBlockType.DYNAMIC),
            floatingMemoryUsageInBytes::get);
    final PipeMemoryBlock normalMemory = manager.forceAllocate("normal", 100);
    final PipeMemoryBlock zeroSizedMemory = manager.forceAllocate("zero", 0);

    try {
      floatingMemoryUsageInBytes.set(250);
      final List<PipeMemoryManager.PipeMemoryBlockInfo> memoryBlockInfoList =
          manager.getPipeMemoryBlockInfoList();

      Assert.assertEquals(3, memoryBlockInfoList.size());
      Assert.assertEquals("FloatingMemory", memoryBlockInfoList.get(0).getName());
      Assert.assertEquals(250, memoryBlockInfoList.get(0).getMemoryUsageInBytes());
      Assert.assertEquals("normal", memoryBlockInfoList.get(1).getName());
      Assert.assertEquals(100, memoryBlockInfoList.get(1).getMemoryUsageInBytes());
      Assert.assertEquals("zero", memoryBlockInfoList.get(2).getName());
      Assert.assertEquals(0, memoryBlockInfoList.get(2).getMemoryUsageInBytes());
      Assert.assertEquals(100, manager.getUsedMemorySizeInBytes());

      manager.forceResize(normalMemory, 0);
      final List<PipeMemoryManager.PipeMemoryBlockInfo> memoryBlockInfoListAfterResize =
          manager.getPipeMemoryBlockInfoList();
      Assert.assertEquals(3, memoryBlockInfoListAfterResize.size());
      Assert.assertEquals("normal", memoryBlockInfoListAfterResize.get(1).getName());
      Assert.assertEquals(0, memoryBlockInfoListAfterResize.get(1).getMemoryUsageInBytes());
      Assert.assertEquals(0, manager.getUsedMemorySizeInBytes());
    } finally {
      manager.release(normalMemory);
      manager.release(zeroSizedMemory);
    }

    final List<PipeMemoryManager.PipeMemoryBlockInfo> memoryBlockInfoListAfterRelease =
        manager.getPipeMemoryBlockInfoList();
    Assert.assertEquals(1, memoryBlockInfoListAfterRelease.size());
    Assert.assertEquals("FloatingMemory", memoryBlockInfoListAfterRelease.get(0).getName());
    Assert.assertEquals(250, memoryBlockInfoListAfterRelease.get(0).getMemoryUsageInBytes());
  }
}
