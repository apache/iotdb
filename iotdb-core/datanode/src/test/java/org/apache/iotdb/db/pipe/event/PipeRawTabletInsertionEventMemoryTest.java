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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class PipeRawTabletInsertionEventMemoryTest {

  private static final String PIPE_NAME = "pipe_raw_tablet_memory_test";
  private static final String HOLDER = PipeRawTabletInsertionEventMemoryTest.class.getName();

  /** Pipe budget smaller than the test tablet, so forceResize fails without pre-filling memory. */
  private static final long INSUFFICIENT_PIPE_BUDGET_BYTES = 32 * 1024;

  /** Expanded budget large enough for one Raw tablet event and its tablet admission headroom. */
  private static final long SUFFICIENT_PIPE_BUDGET_BYTES = 1024 * 1024;

  private static boolean originalPipeMemoryManagementEnabled;
  private static boolean originalConfigPipeMemoryManagementEnabled;

  private int originalMaxRetries;
  private long originalRetryIntervalInMs;
  private double originalFloatingMemoryProportion;
  private double originalTabletRejectThreshold;
  private double originalTsFileRejectThreshold;
  private long originalPipeMemoryTotalSize;
  private IMemoryBlock pipeMemoryBlock;

  @BeforeClass
  public static void enablePipeMemoryManagementForTest() throws Exception {
    originalConfigPipeMemoryManagementEnabled =
        CommonDescriptor.getInstance().getConfig().getPipeMemoryManagementEnabled();
    CommonDescriptor.getInstance().getConfig().setPipeMemoryManagementEnabled(true);

    // PipeMemoryManager caches this flag at class-load time; other tests may have loaded it as
    // false first, so flip it back on for this test class.
    originalPipeMemoryManagementEnabled = getPipeMemoryManagementEnabledStatic();
    setPipeMemoryManagementEnabledStatic(true);
  }

  @AfterClass
  public static void restorePipeMemoryManagementForTest() throws Exception {
    CommonDescriptor.getInstance()
        .getConfig()
        .setPipeMemoryManagementEnabled(originalConfigPipeMemoryManagementEnabled);
    setPipeMemoryManagementEnabledStatic(originalPipeMemoryManagementEnabled);
  }

  @Before
  public void setUp() throws Exception {
    pipeMemoryBlock = getPipeMemoryBlock();
    originalPipeMemoryTotalSize = pipeMemoryBlock.getTotalMemorySizeInBytes();
    setPipeMemoryBudget(INSUFFICIENT_PIPE_BUDGET_BYTES);

    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    originalMaxRetries = config.getPipeMemoryAllocateMaxRetries();
    originalRetryIntervalInMs = config.getPipeMemoryAllocateRetryIntervalInMs();
    originalFloatingMemoryProportion = config.getPipeTotalFloatingMemoryProportion();
    originalTabletRejectThreshold =
        config.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold();
    originalTsFileRejectThreshold =
        config.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold();
    config.setPipeMemoryAllocateMaxRetries(1);
    config.setPipeMemoryAllocateRetryIntervalInMs(1);
    config.setPipeTotalFloatingMemoryProportion(0.5);
    config.setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(0.3);
    config.setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(0.3);
  }

  @After
  public void tearDown() {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    config.setPipeMemoryAllocateMaxRetries(originalMaxRetries);
    config.setPipeMemoryAllocateRetryIntervalInMs(originalRetryIntervalInMs);
    config.setPipeTotalFloatingMemoryProportion(originalFloatingMemoryProportion);
    config.setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(
        originalTabletRejectThreshold);
    config.setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(
        originalTsFileRejectThreshold);
    if (pipeMemoryBlock != null) {
      pipeMemoryBlock.setTotalMemorySizeInBytes(originalPipeMemoryTotalSize);
    }
  }

  @Test
  public void testIncreaseReferenceCountRetriesAfterMemoryInsufficient() {
    final Tablet tablet = createTabletWithMixedSizePoints();
    final PipeRawTabletInsertionEvent event =
        new PipeRawTabletInsertionEvent(
            false, "root.db", "db", "root.db", tablet, false, PIPE_NAME, 1L, null, null, false);

    try {
      event.increaseReferenceCount(HOLDER);
      Assert.fail("Expected PipeRuntimeOutOfMemoryCriticalException");
    } catch (final PipeRuntimeOutOfMemoryCriticalException ignored) {
      // Raw event should propagate insufficient-memory failure for upper-layer retry.
    }
    Assert.assertEquals(0, event.getReferenceCount());

    setPipeMemoryBudget(SUFFICIENT_PIPE_BUDGET_BYTES);

    Assert.assertTrue(event.increaseReferenceCount(HOLDER));
    Assert.assertEquals(1, event.getReferenceCount());

    Assert.assertTrue(event.decreaseReferenceCount(HOLDER, false));
    Assert.assertEquals(0, event.getReferenceCount());
  }

  private void setPipeMemoryBudget(final long extraBudgetBytes) {
    pipeMemoryBlock.setTotalMemorySizeInBytes(
        pipeMemoryBlock.getUsedMemoryInBytes() + extraBudgetBytes);
  }

  private static IMemoryBlock getPipeMemoryBlock() throws ReflectiveOperationException {
    final Field field = PipeMemoryManager.class.getDeclaredField("memoryBlock");
    field.setAccessible(true);
    return (IMemoryBlock) field.get(PipeDataNodeResourceManager.memory());
  }

  private static boolean getPipeMemoryManagementEnabledStatic()
      throws ReflectiveOperationException {
    final Field field = PipeMemoryManager.class.getDeclaredField("PIPE_MEMORY_MANAGEMENT_ENABLED");
    field.setAccessible(true);
    return field.getBoolean(null);
  }

  private static void setPipeMemoryManagementEnabledStatic(final boolean enabled)
      throws ReflectiveOperationException {
    final Field field = PipeMemoryManager.class.getDeclaredField("PIPE_MEMORY_MANAGEMENT_ENABLED");
    field.setAccessible(true);
    try {
      final Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
      field.setBoolean(null, enabled);
      return;
    } catch (final NoSuchFieldException ignored) {
      // JDK 12+
    }

    try {
      final Field unsafeField = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      final Object unsafe = unsafeField.get(null);
      final long offset =
          (long)
              Class.forName("sun.misc.Unsafe")
                  .getMethod("staticFieldOffset", Field.class)
                  .invoke(unsafe, field);
      final Object base =
          Class.forName("sun.misc.Unsafe")
              .getMethod("staticFieldBase", Field.class)
              .invoke(unsafe, field);
      Class.forName("sun.misc.Unsafe")
          .getMethod("putBoolean", Object.class, long.class, boolean.class)
          .invoke(unsafe, base, offset, enabled);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Failed to toggle PipeMemoryManager.PIPE_MEMORY_MANAGEMENT_ENABLED for test", e);
    }
  }

  private static Tablet createTabletWithMixedSizePoints() {
    final List<IMeasurementSchema> schemaList =
        Arrays.asList(
            new MeasurementSchema("small", TSDataType.INT32),
            new MeasurementSchema("payload", TSDataType.BLOB));
    final Tablet tablet = new Tablet("root.db.d1", schemaList, 3);

    tablet.addTimestamp(0, 1L);
    tablet.addValue("small", 0, 1);
    tablet.addValue("payload", 0, new Binary(new byte[4 * 1024]));

    tablet.addTimestamp(1, 2L);
    tablet.addValue("small", 1, 2);

    tablet.addTimestamp(2, 3L);
    tablet.addValue("small", 2, 3);
    tablet.addValue("payload", 2, new Binary(new byte[128 * 1024]));

    return tablet;
  }
}
