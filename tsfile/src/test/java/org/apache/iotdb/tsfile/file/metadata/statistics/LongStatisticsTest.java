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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LongStatisticsTest {

  @Test
  public void testUpdate() {
    Statistics<Long> longStats = new LongStatistics();
    assertTrue(longStats.isEmpty());
    long firstValue = -120985402913209L;
    long secondValue = 1251465332132513L;
    longStats.updateStats(firstValue);
    assertFalse(longStats.isEmpty());
    longStats.updateStats(secondValue);
    assertFalse(longStats.isEmpty());
    assertEquals(secondValue, (long) longStats.getMaxValue());
    assertEquals(firstValue, (long) longStats.getMinValue());
    assertEquals(firstValue, (long) longStats.getFirstValue());
    assertEquals(firstValue + secondValue, (long) longStats.getSumDoubleValue());
    assertEquals(secondValue, (long) longStats.getLastValue());
  }

  @Test
  public void testMerge() {
    Statistics<Long> longStats1 = new LongStatistics();
    longStats1.setStartTime(0);
    longStats1.setEndTime(2);
    Statistics<Long> longStats2 = new LongStatistics();
    longStats2.setStartTime(3);
    longStats2.setEndTime(5);
    assertTrue(longStats1.isEmpty());
    assertTrue(longStats2.isEmpty());
    long max1 = 100000000000L;
    long max2 = 200000000000L;
    longStats1.updateStats(1L);
    longStats1.updateStats(max1);
    longStats2.updateStats(max2);

    Statistics<Long> longStats3 = new LongStatistics();
    longStats3.mergeStatistics(longStats1);
    assertFalse(longStats3.isEmpty());
    assertEquals(max1, (long) longStats3.getMaxValue());
    assertEquals(1, (long) longStats3.getMinValue());
    assertEquals(max1 + 1, (long) longStats3.getSumDoubleValue());
    assertEquals(1, (long) longStats3.getFirstValue());
    assertEquals(max1, (long) longStats3.getLastValue());

    longStats3.mergeStatistics(longStats2);
    assertEquals(max2, (long) longStats3.getMaxValue());
    assertEquals(1, (long) longStats3.getMinValue());
    assertEquals(max2 + max1 + 1, (long) longStats3.getSumDoubleValue());
    assertEquals(1, (long) longStats3.getFirstValue());
    assertEquals(max2, (long) longStats3.getLastValue());

    // Test mismatch
    IntegerStatistics intStats5 = new IntegerStatistics();
    intStats5.updateStats(-10000);
    try {
      longStats3.mergeStatistics(intStats5);
    } catch (StatisticsClassException e) {
      // that's true route
    } catch (Exception e) {
      fail();
    }

    assertEquals(max2, (long) longStats3.getMaxValue());
    // if not merge, the min value will not be changed by smaller value in
    // intStats5
    assertEquals(1, (long) longStats3.getMinValue());
    assertEquals(max2 + max1 + 1, (long) longStats3.getSumDoubleValue());
    assertEquals(1, (long) longStats3.getFirstValue());
    assertEquals(max2, (long) longStats3.getLastValue());

    // Unseq Merge
    LongStatistics longStats4 = new LongStatistics();
    longStats4.setStartTime(0);
    longStats4.setEndTime(5);
    LongStatistics longStats5 = new LongStatistics();
    longStats5.setStartTime(1);
    longStats5.setEndTime(4);

    longStats4.updateStats(111L);
    longStats4.updateStats(114L);

    longStats5.updateStats(116L);

    longStats3.mergeStatistics(longStats4);
    assertEquals(111L, (long) longStats3.getFirstValue());
    assertEquals(114L, (long) longStats3.getLastValue());

    longStats3.mergeStatistics(longStats5);
    assertEquals(111L, (long) longStats3.getFirstValue());
    assertEquals(114L, (long) longStats3.getLastValue());
  }
}
