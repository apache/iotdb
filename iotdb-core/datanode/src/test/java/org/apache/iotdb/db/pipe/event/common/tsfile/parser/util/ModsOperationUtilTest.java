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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.util;

import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;

import org.apache.tsfile.read.common.TimeRange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ModsOperationUtilTest {

  @Test
  public void testIsDeleteWithBinarySearch() {
    // Create test mods with time ranges: [10, 20], [30, 40], [50, 60], [70, 80]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}, {70, 80}});

    ModsOperationUtil.ModsInfo modsInfo = new ModsOperationUtil.ModsInfo(mods, 0);

    // Test cases
    // Time 5: before first mod, should return false
    assertFalse(ModsOperationUtil.isDelete(5, modsInfo));
    assertEquals(0, modsInfo.getCurrentIndex());

    // Time 15: within first mod [10, 20], should return true
    assertTrue(ModsOperationUtil.isDelete(15, modsInfo));
    assertEquals(0, modsInfo.getCurrentIndex());

    // Time 25: between first and second mod, should return false
    assertFalse(ModsOperationUtil.isDelete(25, modsInfo));
    assertEquals(1, modsInfo.getCurrentIndex());

    // Time 35: within second mod [30, 40], should return true
    assertTrue(ModsOperationUtil.isDelete(35, modsInfo));
    assertEquals(1, modsInfo.getCurrentIndex());

    // Time 45: between second and third mod, should return false
    assertFalse(ModsOperationUtil.isDelete(45, modsInfo));
    assertEquals(2, modsInfo.getCurrentIndex());

    // Time 55: within third mod [50, 60], should return true
    assertTrue(ModsOperationUtil.isDelete(55, modsInfo));
    assertEquals(2, modsInfo.getCurrentIndex());

    // Time 65: between third and fourth mod, should return false
    assertFalse(ModsOperationUtil.isDelete(65, modsInfo));
    assertEquals(3, modsInfo.getCurrentIndex());

    // Time 75: within fourth mod [70, 80], should return true
    assertTrue(ModsOperationUtil.isDelete(75, modsInfo));
    assertEquals(3, modsInfo.getCurrentIndex());

    // Time 85: after last mod, should return false and clear mods
    assertFalse(ModsOperationUtil.isDelete(85, modsInfo));
    assertTrue(modsInfo.getMods().isEmpty());
    assertEquals(0, modsInfo.getCurrentIndex());
  }

  @Test
  public void testIsDeleteWithEmptyMods() {
    ModsOperationUtil.ModsInfo modsInfo = new ModsOperationUtil.ModsInfo(new ArrayList<>(), 0);

    // Should return false for any time when mods is empty
    assertFalse(ModsOperationUtil.isDelete(100, modsInfo));
  }

  @Test
  public void testIsDeleteWithNullModsInfo() {
    // Should return false when modsInfo is null
    assertFalse(ModsOperationUtil.isDelete(100, null));
  }

  @Test
  public void testIsDeleteWithNegativeIndex() {
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}});
    ModsOperationUtil.ModsInfo modsInfo = new ModsOperationUtil.ModsInfo(mods, -1);

    // Should return false when currentIndex is negative
    assertFalse(ModsOperationUtil.isDelete(15, modsInfo));
  }

  @Test
  public void testIsDeleteWithSingleMod() {
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}});
    ModsOperationUtil.ModsInfo modsInfo = new ModsOperationUtil.ModsInfo(mods, 0);

    // Time before mod
    assertFalse(ModsOperationUtil.isDelete(5, modsInfo));
    assertEquals(0, modsInfo.getCurrentIndex());

    // Time within mod
    assertTrue(ModsOperationUtil.isDelete(15, modsInfo));
    assertEquals(0, modsInfo.getCurrentIndex());

    // Time after mod
    assertFalse(ModsOperationUtil.isDelete(25, modsInfo));
    assertTrue(modsInfo.getMods().isEmpty());
    assertEquals(0, modsInfo.getCurrentIndex());
  }

  @Test
  public void testIsDeleteWithOverlappingMods() {
    // Create overlapping mods: [10, 30], [20, 40], [30, 50]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 30}, {20, 40}, {30, 50}});

    ModsOperationUtil.ModsInfo modsInfo = new ModsOperationUtil.ModsInfo(mods, 0);

    // Time 15: within first mod
    assertTrue(ModsOperationUtil.isDelete(15, modsInfo));
    assertEquals(0, modsInfo.getCurrentIndex());

    // Time 25: within both first and second mod, should find first one
    assertTrue(ModsOperationUtil.isDelete(25, modsInfo));
    assertEquals(0, modsInfo.getCurrentIndex());

    // Time 35: within second and third mod, should find second one
    assertTrue(ModsOperationUtil.isDelete(35, modsInfo));
    assertEquals(1, modsInfo.getCurrentIndex());

    // Time 45: within third mod
    assertTrue(ModsOperationUtil.isDelete(45, modsInfo));
    assertEquals(2, modsInfo.getCurrentIndex());
  }

  @Test
  public void testBinarySearchMods() {
    // Create test mods with time ranges: [10, 20], [30, 40], [50, 60], [70, 80]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}, {70, 80}});

    // Test binary search from start index 0
    // Time 5: before all mods, should return 0 (first mod)
    assertEquals(0, binarySearchMods(mods, 5, 0));

    // Time 15: within first mod [10, 20], should return 0
    assertEquals(0, binarySearchMods(mods, 15, 0));

    // Time 25: between first and second mod, should return 1
    assertEquals(1, binarySearchMods(mods, 25, 0));

    // Time 35: within second mod [30, 40], should return 1
    assertEquals(1, binarySearchMods(mods, 35, 0));

    // Time 45: between second and third mod, should return 2
    assertEquals(2, binarySearchMods(mods, 45, 0));

    // Time 55: within third mod [50, 60], should return 2
    assertEquals(2, binarySearchMods(mods, 55, 0));

    // Time 65: between third and fourth mod, should return 3
    assertEquals(3, binarySearchMods(mods, 65, 0));

    // Time 75: within fourth mod [70, 80], should return 3
    assertEquals(3, binarySearchMods(mods, 75, 0));

    // Time 85: after all mods, should return 4 (mods.size())
    assertEquals(4, binarySearchMods(mods, 85, 0));
  }

  @Test
  public void testBinarySearchModsWithStartIndex() {
    // Create test mods with time ranges: [10, 20], [30, 40], [50, 60], [70, 80]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}, {70, 80}});

    // Test binary search starting from index 2
    // Time 15: before start index, should return 2 (start index)
    assertEquals(2, binarySearchMods(mods, 15, 2));

    // Time 35: before start index, should return 2 (start index)
    assertEquals(2, binarySearchMods(mods, 35, 2));

    // Time 55: within mod at index 2, should return 2
    assertEquals(2, binarySearchMods(mods, 55, 2));

    // Time 65: between mods at index 2 and 3, should return 3
    assertEquals(3, binarySearchMods(mods, 65, 2));

    // Time 75: within mod at index 3, should return 3
    assertEquals(3, binarySearchMods(mods, 75, 2));

    // Time 85: after all mods, should return 4 (mods.size())
    assertEquals(4, binarySearchMods(mods, 85, 2));
  }

  @Test
  public void testBinarySearchModsWithOverlappingRanges() {
    // Create overlapping mods: [10, 30], [20, 40], [30, 50]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 30}, {20, 40}, {30, 50}});

    // Time 15: within first mod, should return 0
    assertEquals(0, binarySearchMods(mods, 15, 0));

    // Time 25: within first and second mod, should return 0 (first match)
    assertEquals(0, binarySearchMods(mods, 25, 0));

    // Time 35: within second and third mod, should return 1 (first match from start)
    assertEquals(1, binarySearchMods(mods, 35, 0));

    // Time 45: within third mod, should return 2
    assertEquals(2, binarySearchMods(mods, 45, 0));
  }

  @Test
  public void testBinarySearchModsWithEmptyList() {
    List<ModEntry> mods = new ArrayList<>();

    // Should return 0 for any time when mods is empty
    assertEquals(0, binarySearchMods(mods, 100, 0));
  }

  @Test
  public void testBinarySearchModsWithSingleMod() {
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}});

    // Time 5: before mod, should return 0
    assertEquals(0, binarySearchMods(mods, 5, 0));

    // Time 15: within mod, should return 0
    assertEquals(0, binarySearchMods(mods, 15, 0));

    // Time 25: after mod, should return 1
    assertEquals(1, binarySearchMods(mods, 25, 0));
  }

  @Test
  public void testBinarySearchModsWithExactBoundaries() {
    // Create mods with exact boundaries: [10, 20], [20, 30], [30, 40]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {20, 30}, {30, 40}});

    // Time 20: at boundary, first mod [10, 20] has endTime=20 >= 20, should return 0
    assertEquals(0, binarySearchMods(mods, 20, 0));

    // Time 30: at boundary, second mod [20, 30] has endTime=30 >= 30, should return 1
    assertEquals(1, binarySearchMods(mods, 30, 0));
  }

  @Test
  public void testBinarySearchModsWithMinBoundaries() {
    // Create mods: [10, 20], [30, 40], [50, 60]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}});

    // Time 10: exactly at first mod's min, should return 0
    assertEquals(0, binarySearchMods(mods, 10, 0));

    // Time 30: exactly at second mod's min, should return 1
    assertEquals(1, binarySearchMods(mods, 30, 0));

    // Time 50: exactly at third mod's min, should return 2
    assertEquals(2, binarySearchMods(mods, 50, 0));
  }

  @Test
  public void testBinarySearchModsWithMaxBoundaries() {
    // Create mods: [10, 20], [30, 40], [50, 60]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}});

    // Time 20: exactly at first mod's max, should return 0
    assertEquals(0, binarySearchMods(mods, 20, 0));

    // Time 40: exactly at second mod's max, should return 1
    assertEquals(1, binarySearchMods(mods, 40, 0));

    // Time 60: exactly at third mod's max, should return 2
    assertEquals(2, binarySearchMods(mods, 60, 0));
  }

  @Test
  public void testBinarySearchModsWithJustBeforeMin() {
    // Create mods: [10, 20], [30, 40], [50, 60]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}});

    // Time 9: just before first mod's min, should return 0 (first mod)
    assertEquals(0, binarySearchMods(mods, 9, 0));

    // Time 29: just before second mod's min, should return 1 (second mod)
    assertEquals(1, binarySearchMods(mods, 29, 0));

    // Time 49: just before third mod's min, should return 2 (third mod)
    assertEquals(2, binarySearchMods(mods, 49, 0));
  }

  @Test
  public void testBinarySearchModsWithJustAfterMax() {
    // Create mods: [10, 20], [30, 40], [50, 60]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}});

    // Time 21: just after first mod's max, should return 1 (second mod)
    assertEquals(1, binarySearchMods(mods, 21, 0));

    // Time 41: just after second mod's max, should return 2 (third mod)
    assertEquals(2, binarySearchMods(mods, 41, 0));

    // Time 61: just after third mod's max, should return 3 (mods.size())
    assertEquals(3, binarySearchMods(mods, 61, 0));
  }

  @Test
  public void testBinarySearchModsWithLargeGaps() {
    // Create mods with large gaps: [10, 20], [100, 200], [1000, 2000]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {100, 200}, {1000, 2000}});

    // Time 50: in large gap, should return 1 (second mod)
    assertEquals(1, binarySearchMods(mods, 50, 0));

    // Time 500: in large gap, should return 2 (third mod)
    assertEquals(2, binarySearchMods(mods, 500, 0));

    // Time 5000: after all mods, should return 3 (mods.size())
    assertEquals(3, binarySearchMods(mods, 5000, 0));
  }

  @Test
  public void testBinarySearchModsWithNegativeTime() {
    // Create mods: [10, 20], [30, 40]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}});

    // Time -10: negative time, should return 0 (first mod)
    assertEquals(0, binarySearchMods(mods, -10, 0));

    // Time 0: zero time, should return 0 (first mod)
    assertEquals(0, binarySearchMods(mods, 0, 0));
  }

  @Test
  public void testBinarySearchModsWithVeryLargeTime() {
    // Create mods: [10, 20], [30, 40]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}});

    // Time Long.MAX_VALUE: very large time, should return 2 (mods.size())
    assertEquals(2, binarySearchMods(mods, Long.MAX_VALUE, 0));

    // Time Long.MIN_VALUE: very small time, should return 0 (first mod)
    assertEquals(0, binarySearchMods(mods, Long.MIN_VALUE, 0));
  }

  @Test
  public void testBinarySearchModsWithDuplicateTimeRanges() {
    // Create mods with duplicate time ranges: [10, 20], [10, 20], [30, 40]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {10, 20}, {30, 40}});

    // Time 15: within duplicate ranges, should return 0 (first match)
    assertEquals(0, binarySearchMods(mods, 15, 0));

    // Time 20: at max of duplicate ranges, should return 0 (first match)
    assertEquals(0, binarySearchMods(mods, 20, 0));
  }

  @Test
  public void testBinarySearchModsWithStartIndexAtEnd() {
    // Create mods: [10, 20], [30, 40], [50, 60]
    List<ModEntry> mods = createTestMods(new long[][] {{10, 20}, {30, 40}, {50, 60}});

    // Start from index 3 (beyond array), should return 3
    assertEquals(3, binarySearchMods(mods, 100, 3));

    // Start from index 2, search for time 55, should return 2
    assertEquals(2, binarySearchMods(mods, 55, 2));

    // Start from index 2, search for time 25, should return 2 (start index)
    assertEquals(2, binarySearchMods(mods, 25, 2));
  }

  // Helper method to access the private binarySearchMods method for testing
  private int binarySearchMods(List<ModEntry> mods, long time, int startIndex) {
    // Use reflection to access the private method
    try {
      java.lang.reflect.Method method =
          ModsOperationUtil.class.getDeclaredMethod(
              "binarySearchMods", List.class, long.class, int.class);
      method.setAccessible(true);
      return (Integer) method.invoke(null, mods, time, startIndex);
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke binarySearchMods method", e);
    }
  }

  private List<ModEntry> createTestMods(long[][] timeRanges) {
    List<ModEntry> mods = new ArrayList<>();
    for (long[] range : timeRanges) {
      TreeDeletionEntry mod = new TreeDeletionEntry(null, new TimeRange(range[0], range[1]));
      mods.add(mod);
    }
    return mods;
  }
}
