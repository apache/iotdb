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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class PipeMemoryManagerResizeTest {

  private static final long TOTAL_MEMORY_SIZE_IN_BYTES = 2000;
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
    final PipeMemoryManager manager = new PipeMemoryManager();
    final PipeTabletMemoryBlock tablet = manager.forceAllocateForTabletWithRetry(0);
    final long tabletMemorySizeInBytes =
        (long)
                (manager.getTotalNonFloatingMemorySizeInBytes()
                    * (config.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
                        + config.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()
                            / 2))
            + 1;

    try {
      Assert.assertThrows(
          PipeRuntimeOutOfMemoryCriticalException.class,
          () -> manager.forceResize(tablet, tabletMemorySizeInBytes));
      Assert.assertEquals(0, tablet.getMemoryUsageInBytes());
      Assert.assertEquals(0, manager.getUsedMemorySizeInBytes());
      Assert.assertEquals(0, manager.getUsedMemorySizeInBytesOfTablets());
    } finally {
      manager.release(tablet);
    }
  }

  @Test
  public void testTabletResizeLeavesMemoryForSinkForwardProgress() {
    final PipeMemoryManager manager = new PipeMemoryManager();
    final long totalNonFloatingMemorySizeInBytes = manager.getTotalNonFloatingMemorySizeInBytes();
    final long tabletMemorySizeInBytes =
        (long)
                (totalNonFloatingMemorySizeInBytes
                    * (config.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
                        + config.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()
                            / 2))
            + 1;
    final long sinkMemorySizeInBytes = totalNonFloatingMemorySizeInBytes / 10;
    final PipeTabletMemoryBlock retainedTablet =
        manager.forceAllocateForTabletWithRetry(tabletMemorySizeInBytes);
    final PipeTabletMemoryBlock pendingTablet = manager.forceAllocateForTabletWithRetry(0);
    final PipeMemoryBlock sinkBatch = manager.forceAllocate(0);

    try {
      Assert.assertThrows(
          PipeRuntimeOutOfMemoryCriticalException.class,
          () -> manager.forceResize(pendingTablet, 1));
      Assert.assertEquals(tabletMemorySizeInBytes, manager.getUsedMemorySizeInBytes());
      Assert.assertEquals(tabletMemorySizeInBytes, manager.getUsedMemorySizeInBytesOfTablets());

      manager.forceResize(sinkBatch, sinkMemorySizeInBytes);
      Assert.assertEquals(
          tabletMemorySizeInBytes + sinkMemorySizeInBytes, manager.getUsedMemorySizeInBytes());

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
        new PipeMemoryManager(TOTAL_MEMORY_SIZE_IN_BYTES, floatingMemoryUsageInBytes::get);

    Assert.assertEquals(TOTAL_MEMORY_SIZE_IN_BYTES, manager.getTotalNonFloatingMemorySizeInBytes());
    Assert.assertEquals(
        TOTAL_MEMORY_SIZE_IN_BYTES / 2, manager.getTotalFloatingMemorySizeInBytes());

    final PipeTsFileMemoryBlock nonFloatingMemory = manager.forceAllocateForTsFileWithRetry(1200);
    try {
      // Non-floating memory can borrow the unused half that was previously reserved for InsertNode
      // queues. Its usage also reduces the current floating-memory limit symmetrically.
      Assert.assertEquals(1200, manager.getUsedMemorySizeInBytes());
      Assert.assertEquals(800, manager.getTotalFloatingMemorySizeInBytes());

      floatingMemoryUsageInBytes.set(500);
      Assert.assertEquals(1500, manager.getTotalNonFloatingMemorySizeInBytes());
      Assert.assertEquals(300, manager.getFreeMemorySizeInBytes());

      Assert.assertThrows(
          PipeRuntimeOutOfMemoryCriticalException.class, () -> manager.forceAllocate(301));
    } finally {
      manager.release(nonFloatingMemory);
    }
  }
}
