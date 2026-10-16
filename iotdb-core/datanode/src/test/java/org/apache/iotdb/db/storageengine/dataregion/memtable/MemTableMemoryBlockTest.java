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
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.rescon.memory.WriteMemoryBlock;
import org.apache.iotdb.db.storageengine.rescon.memory.WriteMemoryManager;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MemTableMemoryBlockTest {
  @Test
  public void queryOwnedTvListSurvivesWriteBlockRelease() throws Exception {
    IMemTable table = new PrimitiveMemTable("root.sg", "0");
    WriteMemoryManager manager = new WriteMemoryManager();
    table.setWriteMemoryManager(manager);
    MemTableTestUtils.produceData(
        table,
        1,
        10,
        MemTableTestUtils.deviceId0,
        MemTableTestUtils.measurementId0,
        MemTableTestUtils.dataType0);
    TVList list =
        table
            .getMemTableMap()
            .values()
            .iterator()
            .next()
            .getMemChunkMap(true)
            .values()
            .iterator()
            .next()
            .getWorkingTVList();
    long bytes = list.calculateRamSize().getRamSize();
    table.getArrayMemoryBlock().add(bytes);
    MemoryReservationManager readMemory = mock(MemoryReservationManager.class);
    FragmentInstanceContext query = mock(FragmentInstanceContext.class);
    when(query.getMemoryReservationContext()).thenReturn(readMemory);
    list.getQueryContextSet().add(query);
    try {
      int rows = list.rowCount();
      table.release();
      verify(readMemory).reserveMemoryCumulatively(bytes);
      assertSame(query, list.getOwnerQuery());
      assertEquals(bytes, list.getReservedMemoryBytes());
      assertEquals(rows, list.rowCount());
      assertEquals(0, manager.getMemoryUsageInBytes());
      assertEquals(0, manager.getMemoryBlocks().size());
    } finally {
      list.getQueryContextSet().clear();
      list.clear();
    }
  }

  @Test
  public void flushingDoesNotDoubleCountAndReleaseKeepsMetadata() {
    WriteMemoryManager manager = new WriteMemoryManager();
    WriteMemoryBlock metadata =
        manager.createBlock("metadata", WriteMemoryBlock.Category.TSFILE_METADATA);
    metadata.add(11);
    IMemTable memTable = new PrimitiveMemTable("root.sg", "0");
    memTable.setWriteMemoryManager(manager);
    memTable.getArrayMemoryBlock().add(70);
    memTable.getBinaryMemoryBlock().add(20);
    memTable.getStructureMemoryBlock().add(10);
    assertEquals(100, memTable.getTVListsRamCost());
    assertEquals(111, manager.getMemoryUsageInBytes());
    memTable.setFlushStatus(FlushStatus.FLUSHING);
    assertEquals(111, manager.getMemoryUsageInBytes());
    memTable.release();
    memTable.setFlushStatus(FlushStatus.FLUSHED);
    memTable.release();
    assertEquals(0, memTable.getTVListsRamCost());
    assertEquals(11, manager.getMemoryUsageInBytes());
    assertEquals(1, manager.getMemoryBlocks().size());
    metadata.close();
    assertEquals(0, manager.getMemoryUsageInBytes());
  }

  @Test
  public void walSnapshotRetainsTotalAndFormatWithoutInventingCategories() throws Exception {
    IMemTable original = new PrimitiveMemTable("root.sg", "0");
    original.getArrayMemoryBlock().add(70);
    original.getBinaryMemoryBlock().add(20);
    original.getStructureMemoryBlock().add(10);
    ByteBuffer bytes = ByteBuffer.allocate(original.serializedSize());
    original.serializeToWAL(new WALByteBufferForTest(bytes));
    assertEquals(0, bytes.remaining());
    // The persisted total remains at the same offset after marker, series count and memSize.
    assertEquals(100, bytes.getLong(Byte.BYTES + Integer.BYTES + Long.BYTES));
    for (boolean oldSnapshot : new boolean[] {false, true}) {
      DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.array()));
      IMemTable restored =
          oldSnapshot
              ? AbstractMemTable.Factory.createFromOldMemTableSnapshot(input)
              : AbstractMemTable.Factory.create(input);
      assertEquals(0, input.available());
      assertEquals(100, restored.getTVListsRamCost());
      assertEquals(0, restored.getArrayMemoryBlock().getUsedMemoryInBytes());
      assertEquals(0, restored.getBinaryMemoryBlock().getUsedMemoryInBytes());
      assertEquals(0, restored.getStructureMemoryBlock().getUsedMemoryInBytes());
      WriteMemoryManager manager = new WriteMemoryManager();
      restored.setWriteMemoryManager(manager);
      assertEquals(100, manager.getMemoryUsageInBytes());
      assertEquals(
          100,
          manager.getMemoryBlocks().stream()
              .filter(b -> b.getCategory() == WriteMemoryBlock.Category.MEMTABLE_RECOVERED)
              .mapToLong(WriteMemoryBlock::getUsedMemoryInBytes)
              .sum());
      restored.release();
      assertEquals(0, manager.getMemoryUsageInBytes());
    }
    original.release();
  }

  @Test
  public void clearResetsAccountingWithoutChangingLogicalSizeSemantics() {
    IMemTable table = new PrimitiveMemTable("root.sg", "0");
    WriteMemoryManager manager = new WriteMemoryManager();
    table.setWriteMemoryManager(manager);
    table.addTVListRamCost(70);
    table.getStructureMemoryBlock().add(10);
    table.getBinaryMemoryBlock().add(20);
    table.addTextDataSize(20);
    assertEquals(20, table.memSize());
    assertEquals(100, table.getTVListsRamCost());
    table.clear();
    assertEquals(0, manager.getMemoryUsageInBytes());
    assertEquals(0, table.memSize());
    assertEquals(0, table.getStructureMemoryBlock().getUsedMemoryInBytes());
    table.addTVListRamCost(5);
    assertEquals(5, manager.getMemoryUsageInBytes());
    table.release();
  }
}
