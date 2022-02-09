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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;

import static org.junit.Assert.*;

public class DoubleStatisticsTest {

  private static final double maxError = 0.0001d;

  @Test
  public void testUpdate() {
    Statistics<Double> doubleStats = new DoubleStatistics();
    doubleStats.update(1, 1.34d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(2, 2.32d);
    assertFalse(doubleStats.isEmpty());
    assertEquals(0.5, doubleStats.getValidity(), maxError);
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
    doubleStats1.setEndTime(1);
    Statistics<Double> doubleStats2 = new DoubleStatistics();
    doubleStats2.setStartTime(2);
    doubleStats2.setEndTime(5);

    doubleStats1.update(0, 1.34d);
    doubleStats1.update(1, 100.13453d);

    doubleStats2.update(2, 200.435d);
    doubleStats2.update(3, 200.435d);

    Statistics<Double> doubleStats3 = new DoubleStatistics();
    doubleStats3.mergeStatistics(doubleStats1);
    assertFalse(doubleStats3.isEmpty());
    assertEquals(0.5, doubleStats3.getValidity(), maxError);
    assertEquals(100.13453d, doubleStats3.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats3.getMinValue(), maxError);
    assertEquals(100.13453d + 1.34d, doubleStats3.getSumDoubleValue(), maxError);
    assertEquals(1.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(100.13453d, doubleStats3.getLastValue(), maxError);

    doubleStats3.mergeStatistics(doubleStats2);

    assertEquals(0.25, doubleStats3.getValidity(), maxError);
    assertEquals(200.435d, doubleStats3.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats3.getMinValue(), maxError);
    assertEquals(
        100.13453d + 1.34d + 200.435d + 200.435d, doubleStats3.getSumDoubleValue(), maxError);
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

  @Test
  public void testUpdateValidity() {
    Statistics<Double> doubleStats = new DoubleStatistics();
    Statistics<Double> doubleStatsMerge = new DoubleStatistics();

    DescriptiveStatistics stats = new DescriptiveStatistics();

    System.out.println(Runtime.getRuntime().totalMemory() / 1024 / 1024);
    for (int i = 1; i < 3000; i++) {
      doubleStats.update(1623311071000L - 3000 * 1000 + i * 1000, 2.32d);
      stats.addValue(0d / 1000);
    }
    doubleStats.updateDP();
    doubleStats.updateReverseDP();
    doubleStats.update(1623311071000L, 2.32d);
    stats.addValue(0d / 1000);
    doubleStats.update(1623311072000L, 12.32d);
    stats.addValue(10d / 1000);
    doubleStats.update(1623311073000L, 2.32d);
    stats.addValue(-10d / 1000);
    doubleStats.update(1623311074000L, 2.32d);
    stats.addValue(0d / 1000);
    doubleStats.update(1623311075000L, 2.32d);

    System.out.println(Runtime.getRuntime().freeMemory() / 1024 / 1024);
    System.out.println(stats.getMean() + 3 * stats.getStandardDeviation());
    System.out.println(stats.getMean() - 3 * stats.getStandardDeviation());
    double smax = doubleStats.getSpeedAVG() + 3 * doubleStats.getSpeedSTD();
    double smin = doubleStats.getSpeedAVG() - 3 * doubleStats.getSpeedSTD();

    doubleStats.updateDP();
    doubleStats.updateReverseDP();
    assertEquals(1.0, doubleStats.getValidity(), maxError);
  }
}
