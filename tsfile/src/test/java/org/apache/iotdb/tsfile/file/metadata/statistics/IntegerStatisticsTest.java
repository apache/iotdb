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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IntegerStatisticsTest {

  @Test
  public void testUpdate() {
    Statistics<Integer> intStats = new IntegerStatistics();
    intStats.updateStats(1);
    assertFalse(intStats.isEmpty());
    intStats.updateStats(2);
    assertFalse(intStats.isEmpty());
    assertEquals(2, (int) intStats.getMaxValue());
    assertEquals(1, (int) intStats.getMinValue());
    assertEquals(1, (int) intStats.getFirstValue());
    assertEquals(3, (int) intStats.getSumLongValue());
    assertEquals(2, (int) intStats.getLastValue());
  }

  @Test
  public void testMerge() {
    Statistics<Integer> intStats1 = new IntegerStatistics();
    intStats1.setStartTime(0);
    intStats1.setEndTime(2);
    Statistics<Integer> intStats2 = new IntegerStatistics();
    intStats2.setStartTime(3);
    intStats2.setEndTime(5);

    intStats1.updateStats(1);
    intStats1.updateStats(100);

    intStats2.updateStats(200);

    Statistics<Integer> intStats3 = new IntegerStatistics();
    intStats3.mergeStatistics(intStats1);
    assertFalse(intStats3.isEmpty());
    assertEquals(100, (int) intStats3.getMaxValue());
    assertEquals(1, (int) intStats3.getMinValue());
    assertEquals(1, (int) intStats3.getFirstValue());
    assertEquals(1 + 100, (int) intStats3.getSumLongValue());
    assertEquals(100, (int) intStats3.getLastValue());

    intStats3.mergeStatistics(intStats2);
    assertEquals(200, (int) intStats3.getMaxValue());
    assertEquals(1, (int) intStats3.getMinValue());
    assertEquals(1, (int) intStats3.getFirstValue());
    assertEquals(101 + 200, (int) intStats3.getSumLongValue());
    assertEquals(200, (int) intStats3.getLastValue());

    // Unseq merge
    Statistics<Integer> intStats4 = new IntegerStatistics();
    intStats4.setStartTime(0);
    intStats4.setEndTime(5);
    Statistics<Integer> intStats5 = new IntegerStatistics();
    intStats5.setStartTime(1);
    intStats5.setEndTime(4);

    intStats4.updateStats(11);
    intStats4.updateStats(15);

    intStats5.updateStats(20);

    intStats3.mergeStatistics(intStats4);
    assertEquals(11, (int) intStats3.getFirstValue());
    assertEquals(15, (int) intStats3.getLastValue());

    intStats3.mergeStatistics(intStats5);
    assertEquals(11, (int) intStats3.getFirstValue());
    assertEquals(15, (int) intStats3.getLastValue());
  }
}
