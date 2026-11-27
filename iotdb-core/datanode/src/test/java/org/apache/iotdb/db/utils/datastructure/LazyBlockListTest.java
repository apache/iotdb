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
  public void testGeometricGrowthKeepsEverySlot() {
    LazyBlockList<Object> container = new LazyBlockList<>(0, 0);
    int lastCapacity = 0;
    for (int block = 0; block < 1_000; block++) {
      container.add("v" + block);
      // The backing array always covers every slot the container reported so far, and its length
      // never shrinks, so an already materialized slot never disappears.
      Assert.assertTrue(container.materializedLength() >= container.size());
      Assert.assertTrue(container.materializedLength() >= lastCapacity);
      lastCapacity = container.materializedLength();
    }

    for (int block = 0; block < 1_000; block++) {
      Assert.assertEquals("v" + block, container.get(block));
    }
    Assert.assertEquals(
        LazyBlockList.instanceRamCost() + RamUsageEstimator.sizeOfObjectArray(lastCapacity),
        container.ramCost());
  }

  @Test
  public void testWriteFarAheadCoversTheWholePrefix() {
    LazyBlockList<Object> container = new LazyBlockList(2_000, 0);
    container.set(1_999, "tail");

    Assert.assertEquals(2_000, container.size());
    Assert.assertTrue(container.materializedLength() >= 2_000);
    Assert.assertEquals("tail", container.get(1_999));
    for (int block = 0; block < 1_999; block++) {
      Assert.assertNull(container.get(block));
    }
  }

  @Test
  public void testPreallocationIsReservedButEmpty() {
    LazyBlockList<Object> container = new LazyBlockList<>(0, 16);

    Assert.assertEquals(0, container.size());
    Assert.assertEquals(16, container.materializedLength());
    Assert.assertEquals(
        LazyBlockList.instanceRamCost() + RamUsageEstimator.sizeOfObjectArray(16),
        container.ramCost());

    // Filling the reserved slots does not allocate again.
    long ramCost = container.ramCost();
    container.add("v0");
    Assert.assertEquals(1, container.size());
    Assert.assertEquals(ramCost, container.ramCost());
  }

  @Test
  public void testCopyStructureKeepsCapacityWithoutValues() {
    LazyBlockList<Object> container = new LazyBlockList(3, 0);
    container.set(2, "value");

    LazyBlockList<Object> copy = container.copyStructure();
    Assert.assertEquals(container.size(), copy.size());
    Assert.assertEquals(container.materializedLength(), copy.materializedLength());
    Assert.assertEquals(container.ramCost(), copy.ramCost());
    for (int block = 0; block < container.size(); block++) {
      Assert.assertNull(copy.get(block));
    }
  }

  @Test
  public void testClearReleasesTheSlots() {
    LazyBlockList<Object> container = new LazyBlockList<>(0, 0);
    container.add("v0");
    Assert.assertTrue(container.materializedLength() > 0);

    container.clear();
    Assert.assertEquals(0, container.size());
    Assert.assertEquals(0, container.materializedLength());
    Assert.assertEquals(LazyBlockList.instanceRamCost(), container.ramCost());
  }
}
