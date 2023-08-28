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

public class FloatStatisticsTest {

  private static final float maxError = 0.0001f;

  @Test
  public void testUpdate() {
    Statistics<Float> floatStats = new FloatStatistics();
    floatStats.updateStats(1.34f);
    assertFalse(floatStats.isEmpty());
    floatStats.updateStats(2.32f);
    assertFalse(floatStats.isEmpty());
    assertEquals(2.32f, (double) floatStats.getMaxValue(), maxError);
    assertEquals(1.34f, (double) floatStats.getMinValue(), maxError);
    assertEquals(2.32f + 1.34f, (double) floatStats.getSumDoubleValue(), maxError);
    assertEquals(1.34f, (double) floatStats.getFirstValue(), maxError);
    assertEquals(2.32f, (double) floatStats.getLastValue(), maxError);
  }

  @Test
  public void testMerge() {
    Statistics<Float> floatStats1 = new FloatStatistics();
    floatStats1.setStartTime(0);
    floatStats1.setEndTime(2);
    Statistics<Float> floatStats2 = new FloatStatistics();
    floatStats2.setStartTime(3);
    floatStats2.setEndTime(5);

    floatStats1.updateStats(1.34f);
    floatStats1.updateStats(100.13453f);

    floatStats2.updateStats(200.435f);

    Statistics<Float> floatStats3 = new FloatStatistics();
    floatStats3.mergeStatistics(floatStats1);
    assertFalse(floatStats3.isEmpty());
    assertEquals(100.13453f, floatStats3.getMaxValue(), maxError);
    assertEquals(1.34f, floatStats3.getMinValue(), maxError);
    assertEquals(100.13453f + 1.34f, (float) floatStats3.getSumDoubleValue(), maxError);
    assertEquals(1.34f, floatStats3.getFirstValue(), maxError);
    assertEquals(100.13453f, floatStats3.getLastValue(), maxError);

    floatStats3.mergeStatistics(floatStats2);
    assertEquals(200.435f, floatStats3.getMaxValue(), maxError);
    assertEquals(1.34f, floatStats3.getMinValue(), maxError);
    assertEquals(100.13453f + 1.34f + 200.435f, (float) floatStats3.getSumDoubleValue(), maxError);
    assertEquals(1.34f, floatStats3.getFirstValue(), maxError);
    assertEquals(200.435f, floatStats3.getLastValue(), maxError);

    // Unseq merge
    Statistics<Float> floatStats4 = new FloatStatistics();
    floatStats4.setStartTime(0);
    floatStats4.setEndTime(5);
    Statistics<Float> floatStats5 = new FloatStatistics();
    floatStats5.setStartTime(1);
    floatStats5.setEndTime(4);

    floatStats4.updateStats(122.34f);
    floatStats4.updateStats(125.34f);
    floatStats5.updateStats(111.1f);

    floatStats3.mergeStatistics(floatStats4);
    assertEquals(122.34f, floatStats3.getFirstValue(), maxError);
    assertEquals(125.34f, floatStats3.getLastValue(), maxError);

    floatStats3.mergeStatistics(floatStats5);
    assertEquals(122.34f, floatStats3.getFirstValue(), maxError);
    assertEquals(125.34f, floatStats3.getLastValue(), maxError);
  }
}
