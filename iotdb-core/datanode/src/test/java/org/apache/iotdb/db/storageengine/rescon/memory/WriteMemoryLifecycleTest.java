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
package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.MemoryControlledWALEntryQueue;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WriteMemoryLifecycleTest {
  @Test
  public void testFlushReservationsSurviveReloadAndCloseOnce() {
    SystemInfo system = SystemInfo.getInstance();
    double threshold = system.getFlushThreshold();
    long before = system.getFlushMemoryManager().getMemoryUsageInBytes();
    try (SystemInfo.FlushMemoryReservation first =
            system.reserveTemporaryMemoryForFlushing("first", 1024);
        SystemInfo.FlushMemoryReservation second =
            system.reserveTemporaryMemoryForFlushing("second", 2048)) {
      assertEquals(before + 3072, system.getFlushMemoryManager().getMemoryUsageInBytes());
      double reduced = system.getFlushThreshold();
      assertTrue(reduced < threshold);
      system.loadWriteMemory();
      assertEquals(reduced, system.getFlushThreshold(), 0);
      first.close();
      first.close();
      assertEquals(before + 2048, system.getFlushMemoryManager().getMemoryUsageInBytes());
    }
    assertEquals(before, system.getFlushMemoryManager().getMemoryUsageInBytes());
    assertEquals(threshold, system.getFlushThreshold(), 0);
  }

  @Test
  public void testWalUsesOriginalChargeWhenEntryChanges() throws Exception {
    MemoryControlledWALEntryQueue queue = new MemoryControlledWALEntryQueue();
    WALEntry entry = mock(WALEntry.class);
    when(entry.getMemorySize()).thenReturn(128L, 4096L);
    long before = SystemInfo.getInstance().getWalBufferQueueMemoryBlock().getUsedMemoryInBytes();
    queue.put(entry);
    assertEquals(
        before + 128,
        SystemInfo.getInstance().getWalBufferQueueMemoryBlock().getUsedMemoryInBytes());
    assertSame(entry, queue.take());
    assertEquals(
        before, SystemInfo.getInstance().getWalBufferQueueMemoryBlock().getUsedMemoryInBytes());
    verify(entry, times(1)).getMemorySize();
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testInterruptedWalPutReturnsCharge() {
    MemoryControlledWALEntryQueue queue = new MemoryControlledWALEntryQueue();
    WALEntry entry = mock(WALEntry.class);
    when(entry.getMemorySize()).thenReturn(128L);
    long before = SystemInfo.getInstance().getWalBufferQueueMemoryBlock().getUsedMemoryInBytes();
    try {
      Thread.currentThread().interrupt();
      assertThrows(InterruptedException.class, () -> queue.put(entry));
    } finally {
      Thread.interrupted();
    }
    assertTrue(queue.isEmpty());
    assertEquals(
        before, SystemInfo.getInstance().getWalBufferQueueMemoryBlock().getUsedMemoryInBytes());
  }

  @Test
  public void testArrayPoolCountsOnlyIdleArrays() {
    PrimitiveArrayManager.close();
    try {
      for (TSDataType type :
          new TSDataType[] {
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT
          }) {
        Object array = PrimitiveArrayManager.allocate(type);
        if (array instanceof Binary[]) {
          ((Binary[]) array)[0] = new Binary(new byte[32]);
        }
        assertEquals(0, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
        PrimitiveArrayManager.release(array);
        assertEquals(
            RamUsageEstimator.shallowSizeOf(array),
            PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
        if (array instanceof Binary[]) {
          assertNull(((Binary[]) array)[0]);
        }
        assertSame(array, PrimitiveArrayManager.allocate(type));
        assertEquals(0, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
        PrimitiveArrayManager.release(array);
        PrimitiveArrayManager.close();
        assertEquals(0, PrimitiveArrayManager.getMemoryBlock().getUsedMemoryInBytes());
      }
    } finally {
      PrimitiveArrayManager.close();
    }
  }
}
