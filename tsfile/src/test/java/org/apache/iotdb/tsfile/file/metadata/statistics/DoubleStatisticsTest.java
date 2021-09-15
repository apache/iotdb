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

public class DoubleStatisticsTest {

  private static final double maxError = 0.0001d;

  @Test
  public void testUpdate() {
    Statistics<Double> doubleStats = new DoubleStatistics();
    doubleStats.updateStats(1.34d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.updateStats(2.32d);
    assertFalse(doubleStats.isEmpty());
    assertEquals(2.32d, doubleStats.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats.getMinValue(), maxError);
    assertEquals(2.32d + 1.34d, doubleStats.getSumDoubleValue(), maxError);
    assertEquals(1.34d, doubleStats.getFirstValue(), maxError);
    assertEquals(2.32d, doubleStats.getLastValue(), maxError);
  }

  @Test
  public void testMerge() {
    Statistics<Double> doubleStats1 = new DoubleStatistics();
    doubleStats1.setStartTime(0);
    doubleStats1.setEndTime(2);
    Statistics<Double> doubleStats2 = new DoubleStatistics();
    doubleStats2.setStartTime(3);
    doubleStats2.setEndTime(5);

    doubleStats1.updateStats(1.34d);
    doubleStats1.updateStats(100.13453d);

    doubleStats2.updateStats(200.435d);

    Statistics<Double> doubleStats3 = new DoubleStatistics();
    doubleStats3.mergeStatistics(doubleStats1);
    assertFalse(doubleStats3.isEmpty());
    assertEquals(100.13453d, doubleStats3.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats3.getMinValue(), maxError);
    assertEquals(100.13453d + 1.34d, doubleStats3.getSumDoubleValue(), maxError);
    assertEquals(1.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(100.13453d, doubleStats3.getLastValue(), maxError);

    doubleStats3.mergeStatistics(doubleStats2);
    assertEquals(200.435d, doubleStats3.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats3.getMinValue(), maxError);
    assertEquals(100.13453d + 1.34d + 200.435d, doubleStats3.getSumDoubleValue(), maxError);
    assertEquals(1.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(200.435d, doubleStats3.getLastValue(), maxError);

    // Unseq merge
    Statistics<Double> doubleStats4 = new DoubleStatistics();
    doubleStats4.setStartTime(0);
    doubleStats4.setEndTime(5);
    Statistics<Double> doubleStats5 = new DoubleStatistics();
    doubleStats5.setStartTime(1);
    doubleStats5.setEndTime(4);

    doubleStats4.updateStats(122.34d);
    doubleStats4.updateStats(125.34d);
    doubleStats5.updateStats(111.1d);

    doubleStats3.mergeStatistics(doubleStats4);
    assertEquals(122.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(125.34d, doubleStats3.getLastValue(), maxError);

    doubleStats3.mergeStatistics(doubleStats5);
    assertEquals(122.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(125.34d, doubleStats3.getLastValue(), maxError);
  }
}
