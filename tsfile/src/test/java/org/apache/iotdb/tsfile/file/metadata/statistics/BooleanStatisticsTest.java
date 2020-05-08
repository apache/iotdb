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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BooleanStatisticsTest {

  private static final double maxError = 0.0001d;

  @Test
  public void testUpdate() {
    Statistics<Boolean> booleanStatistics = new BooleanStatistics();
    booleanStatistics.updateStats(true);
    assertFalse(booleanStatistics.isEmpty());
    booleanStatistics.updateStats(false);
    assertFalse(booleanStatistics.isEmpty());
    assertTrue(booleanStatistics.getFirstValue());
    assertFalse(booleanStatistics.getLastValue());
  }

  @Test
  public void testMerge() {
    Statistics<Boolean> booleanStats1 = new BooleanStatistics();
    booleanStats1.setStartTime(0);
    booleanStats1.setEndTime(2);
    Statistics<Boolean> booleanStats2 = new BooleanStatistics();
    booleanStats2.setStartTime(3);
    booleanStats2.setEndTime(5);

    booleanStats1.updateStats(false);
    booleanStats1.updateStats(false);

    booleanStats2.updateStats(true);

    Statistics<Boolean> booleanStats3 = new BooleanStatistics();
    booleanStats3.mergeStatistics(booleanStats1);
    assertFalse(booleanStats3.isEmpty());
    assertFalse(booleanStats3.getFirstValue());
    assertFalse(booleanStats3.getLastValue());

    booleanStats3.mergeStatistics(booleanStats2);
    assertFalse(booleanStats3.getFirstValue());
    assertTrue(booleanStats3.getLastValue());

    // unseq merge
    Statistics<Boolean> booleanStats4 = new BooleanStatistics();
    booleanStats4.setStartTime(0);
    booleanStats4.setEndTime(5);
    booleanStats4.updateStats(true);

    booleanStats3.mergeStatistics(booleanStats4);
    assertTrue(booleanStats3.getFirstValue());
    assertTrue(booleanStats3.getLastValue());

    Statistics<Boolean> booleanStats5 = new BooleanStatistics();
    booleanStats5.setStartTime(1);
    booleanStats5.setEndTime(4);
    booleanStats5.updateStats(false);
    booleanStats3.mergeStatistics(booleanStats5);
    assertTrue(booleanStats3.getFirstValue());
    assertTrue(booleanStats3.getLastValue());
  }
}
