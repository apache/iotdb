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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Assert;
import org.junit.Test;

public class LazyBlockListTest {

  private static final long PAGE_RAM_COST =
      RamUsageEstimator.sizeOfObjectArray(LazyBlockList.PAGE_SIZE);

  @Test
  public void testInliningSingleBlockAllocatesNoArray() {
    LazyBlockList<Object> container = new LazyBlockList<>(0, 0);
    container.add("v0");

    Assert.assertEquals(1, container.size());
    Assert.assertEquals("v0", container.get(0));
    // One written block lives in the container itself: no array at all.
    Assert.assertEquals(LazyBlockList.instanceRamCost(), container.ramCost());
    Assert.assertEquals(1, container.materializedLength());
  }

  @Test
  public void testNullSlotsStayImplicit() {
    LazyBlockList<Object> container = new LazyBlockList<>(0, 0);
    for (int block = 0; block < 100; block++) {
      container.add(null);
    }

    Assert.assertEquals(100, container.size());
    Assert.assertEquals(0, container.materializedLength());
    Assert.assertEquals(LazyBlockList.instanceRamCost(), container.ramCost());
    for (int block = 0; block < 100; block++) {
      Assert.assertNull(container.get(block));
    }
  }

  @Test
  public void testWriteFarAheadAllocatesNoPrefix() {
    // A column that is first written at block 1000 must not materialize the skipped prefix; a
    // single block is inlined regardless of its index.
    LazyBlockList<Object> container = new LazyBlockList(2_000, 0);
    container.set(1_000, "far");

    Assert.assertEquals(2_000, container.size());
    Assert.assertEquals("far", container.get(1_000));
    Assert.assertEquals(LazyBlockList.instanceRamCost(), container.ramCost());
    Assert.assertEquals(1_001, container.materializedLength());
    for (int block = 0; block < 1_000; block++) {
      Assert.assertNull(container.get(block));
    }
  }

  @Test
  public void testCrossPagePromotionReusesLeafPages() {
    LazyBlockList<Object> container = new LazyBlockList(100, 0);
    container.set(1, "page0");
    // A single block is inlined, so far there is no array at all.
    Assert.assertEquals(LazyBlockList.instanceRamCost(), container.ramCost());

    // A second block on the same page keeps one leaf page and reuses the inlined element.
    container.set(2, "page0b");
    Assert.assertEquals(LazyBlockList.instanceRamCost() + PAGE_RAM_COST, container.ramCost());

    // Moving to another page turns the container into a directory of two leaf pages.
    container.set(20, "page1");
    long twoPages = LazyBlockList.instanceRamCost() + 2L * PAGE_RAM_COST + directoryRamCost(4);
    Assert.assertEquals(twoPages, container.ramCost());
    Assert.assertEquals("page0", container.get(1));
    Assert.assertEquals("page0b", container.get(2));
    Assert.assertEquals("page1", container.get(20));
  }

  @Test
  public void testDenseGrowthKeepsEverySlot() {
    LazyBlockList<Object> container = new LazyBlockList(0, 0);
    int lastMaterialized = 0;
    for (int block = 0; block < 1_000; block++) {
      container.add("v" + block);
      Assert.assertTrue(container.materializedLength() >= lastMaterialized);
      lastMaterialized = container.materializedLength();
    }

    for (int block = 0; block < 1_000; block++) {
      Assert.assertEquals("v" + block, container.get(block));
    }
    Assert.assertEquals(1_000, container.size());
  }

  @Test
  public void testPreallocationReservesOnePage() {
    LazyBlockList<Object> container = new LazyBlockList(0, 16);

    Assert.assertEquals(0, container.size());
    Assert.assertEquals(16, container.materializedLength());
    Assert.assertEquals(LazyBlockList.instanceRamCost() + PAGE_RAM_COST, container.ramCost());

    long ramCost = container.ramCost();
    container.add("v0");
    Assert.assertEquals(1, container.size());
    Assert.assertEquals(ramCost, container.ramCost());
  }

  @Test
  public void testCopyStructureKeepsLayoutWithoutValues() {
    LazyBlockList<Object> container = new LazyBlockList(100, 0);
    container.set(1, "a");
    container.set(40, "b");

    LazyBlockList<Object> copy = container.copyStructure();
    Assert.assertEquals(container.size(), copy.size());
    Assert.assertEquals(container.materializedLength(), copy.materializedLength());
    Assert.assertEquals(container.ramCost(), copy.ramCost());
    for (int block = 0; block < container.size(); block++) {
      Assert.assertNull(copy.get(block));
    }
  }

  @Test
  public void testClearReleasesTheStorage() {
    LazyBlockList<Object> container = new LazyBlockList(31, 0);
    container.set(0, "v0");
    container.set(30, "v30");

    container.clear();
    Assert.assertEquals(0, container.size());
    Assert.assertEquals(0, container.materializedLength());
    Assert.assertEquals(LazyBlockList.instanceRamCost(), container.ramCost());
  }

  @Test
  public void testDirectoryGrowthDoesNotOverflow() {
    Assert.assertEquals(4, LazyBlockList.grownDirectoryLength(0, 1));
    Assert.assertEquals(64, LazyBlockList.grownDirectoryLength(8, 64));
    int nearlyFull = LazyBlockList.MAX_CAPACITY;
    Assert.assertEquals(nearlyFull, LazyBlockList.grownDirectoryLength(nearlyFull, nearlyFull));
    Assert.assertEquals(
        LazyBlockList.MAX_CAPACITY,
        LazyBlockList.grownDirectoryLength(nearlyFull, LazyBlockList.MAX_CAPACITY));
    // At the limit the exact minimum is requested so the allocation fails instead of shrinking.
    Assert.assertEquals(
        Integer.MAX_VALUE,
        LazyBlockList.grownDirectoryLength(LazyBlockList.MAX_CAPACITY, Integer.MAX_VALUE));
  }

  @Test
  public void testInsertInsideTheListIsRejected() {
    LazyBlockList<Object> container = new LazyBlockList(0, 0);
    container.add("v0");
    Assert.assertThrows(UnsupportedOperationException.class, () -> container.add(0, "x"));
  }

  private static long directoryRamCost(int length) {
    return RamUsageEstimator.sizeOfObjectArray(length);
  }
}
