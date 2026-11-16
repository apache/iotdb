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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WriteMemoryBlockTest {
  @Test
  public void reservationCountsAgainstRegionButNotFlushableMemory() {
    WriteMemoryManager manager = new WriteMemoryManager();
    WriteMemoryBlock block = manager.createBlock("array", WriteMemoryBlock.Category.MEMTABLE_ARRAY);
    block.add(8);
    try (WriteMemoryBlock.Reservation reservation = block.reserve(10)) {
      assertEquals(18, manager.getMemoryUsageInBytes());
      assertEquals(8, block.getUsedMemoryInBytes());
      assertEquals(10, block.getReservedMemoryInBytes());
      reservation.commit();
      reservation.commit();
      assertEquals(18, manager.getMemoryUsageInBytes());
      assertEquals(18, block.getUsedMemoryInBytes());
      assertEquals(0, block.getReservedMemoryInBytes());
    }
    block.close();
    block.close();
    assertEquals(0, manager.getMemoryUsageInBytes());
    assertTrue(manager.getMemoryBlocks().isEmpty());
  }

  @Test
  public void rejectedReservationPreservesOtherOwners() {
    WriteMemoryManager manager = new WriteMemoryManager();
    WriteMemoryBlock first = manager.createBlock("same", WriteMemoryBlock.Category.MEMTABLE_ARRAY);
    WriteMemoryBlock second = manager.createBlock("same", WriteMemoryBlock.Category.MEMTABLE_ARRAY);
    first.add(7);
    second.add(11);
    assertNotEquals(first.getId(), second.getId());
    assertEquals(2, manager.getMemoryBlocks().size());
    try (WriteMemoryBlock.Reservation ignored = first.reserve(13)) {
      assertEquals(31, manager.getMemoryUsageInBytes());
    }
    assertEquals(18, manager.getMemoryUsageInBytes());
    assertThrows(IllegalArgumentException.class, () -> first.release(8));
    assertEquals(18, manager.getMemoryUsageInBytes());
    first.close();
    assertEquals(11, manager.getMemoryUsageInBytes());
    second.close();
  }

  @Test
  public void ownerCloseCancelsPendingReservationOnlyOnce() {
    WriteMemoryManager manager = new WriteMemoryManager();
    WriteMemoryBlock block = manager.createBlock("array", WriteMemoryBlock.Category.MEMTABLE_ARRAY);
    WriteMemoryBlock.Reservation reservation = block.reserve(10);
    block.close();
    assertThrows(IllegalStateException.class, reservation::commit);
    reservation.close();
    reservation.close();
    assertEquals(0, manager.getMemoryUsageInBytes());
    assertThrows(IllegalStateException.class, () -> block.add(1));
    assertThrows(IllegalStateException.class, () -> block.reserve(1));
  }

  @Test
  public void attachingExistingUsageIsIdempotent() {
    WriteMemoryBlock block =
        new WriteMemoryBlock("recovery", WriteMemoryBlock.Category.MEMTABLE_RECOVERED);
    block.add(30);
    WriteMemoryManager manager = new WriteMemoryManager();
    block.attachTo(manager);
    block.attachTo(manager);
    assertEquals(30, manager.getMemoryUsageInBytes());
    assertThrows(IllegalStateException.class, () -> block.attachTo(new WriteMemoryManager()));
    assertThrows(IllegalArgumentException.class, () -> block.add(-1));
    block.clear();
    assertEquals(0, manager.getMemoryUsageInBytes());
    block.add(5);
    assertEquals(5, manager.getMemoryUsageInBytes());
    block.close();
  }

  @Test
  public void concurrentReservationsOnOneBlockKeepExactUsage() throws Exception {
    WriteMemoryManager manager = new WriteMemoryManager();
    WriteMemoryBlock block =
        manager.createBlock("shared", WriteMemoryBlock.Category.MEMTABLE_ARRAY);
    block.add(17);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> tasks = new ArrayList<>();
    try {
      for (int i = 0; i < 4; i++) {
        tasks.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int j = 0; j < 1000; j++) {
                    try (WriteMemoryBlock.Reservation reservation = block.reserve(5)) {
                      reservation.commit();
                    }
                    try (WriteMemoryBlock.Reservation ignored = block.reserve(11)) {
                      block.release(3);
                    }
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> task : tasks) {
        task.get(30, TimeUnit.SECONDS);
      }
      assertEquals(8017, block.getUsedMemoryInBytes());
      assertEquals(0, block.getReservedMemoryInBytes());
      assertEquals(8017, manager.getMemoryUsageInBytes());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      block.close();
    }
    assertEquals(0, manager.getMemoryUsageInBytes());
    assertTrue(manager.getMemoryBlocks().isEmpty());
  }

  @Test
  public void concurrentWritersAndReleasesKeepRegionTotal() throws Exception {
    WriteMemoryManager manager = new WriteMemoryManager();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<Future<?>> tasks = new ArrayList<>();
    try {
      for (int i = 0; i < 4; i++) {
        tasks.add(
            executor.submit(
                () -> {
                  try (WriteMemoryBlock block =
                      manager.createBlock("array", WriteMemoryBlock.Category.MEMTABLE_ARRAY)) {
                    for (int j = 0; j < 1000; j++) {
                      try (WriteMemoryBlock.Reservation reservation = block.reserve(5)) {
                        reservation.commit();
                      }
                      block.release(3);
                    }
                    assertEquals(2000, block.getUsedMemoryInBytes());
                  }
                }));
      }
      for (Future<?> task : tasks) {
        task.get();
      }
      assertEquals(0, manager.getMemoryUsageInBytes());
      assertTrue(manager.getMemoryBlocks().isEmpty());
    } finally {
      executor.shutdownNow();
    }
  }
}
