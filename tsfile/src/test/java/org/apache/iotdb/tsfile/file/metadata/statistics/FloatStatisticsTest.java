/**
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FloatStatisticsTest {

  private static final float maxError = 0.0001f;

  @Test
  public void testUpdate() {
    Statistics<Float> floatStats = new FloatStatistics();
    floatStats.updateStats(1.34f);
    assertEquals(false, floatStats.isEmpty());
    floatStats.updateStats(2.32f);
    assertEquals(false, floatStats.isEmpty());
    assertEquals(2.32f, (double) floatStats.getMax(), maxError);
    assertEquals(1.34f, (double) floatStats.getMin(), maxError);
    assertEquals(2.32f + 1.34f, (double) floatStats.getSum(), maxError);
    assertEquals(1.34f, (double) floatStats.getFirst(), maxError);
    assertEquals(2.32f, (double) floatStats.getLast(), maxError);
  }

  @Test
  public void testMerge() {
    Statistics<Float> floatStats1 = new FloatStatistics();
    Statistics<Float> floatStats2 = new FloatStatistics();

    floatStats1.updateStats(1.34f);
    floatStats1.updateStats(100.13453f);

    floatStats2.updateStats(200.435f);

    Statistics<Float> floatStats3 = new FloatStatistics();
    floatStats3.mergeStatistics(floatStats1);
    assertEquals(false, floatStats3.isEmpty());
    assertEquals(100.13453f, (float) floatStats3.getMax(), maxError);
    assertEquals(1.34f, (float) floatStats3.getMin(), maxError);
    assertEquals(100.13453f + 1.34f, (float) floatStats3.getSum(), maxError);
    assertEquals(1.34f, (float) floatStats3.getFirst(), maxError);
    assertEquals(100.13453f, (float) floatStats3.getLast(), maxError);

    floatStats3.mergeStatistics(floatStats2);
    assertEquals(200.435d, (float) floatStats3.getMax(), maxError);
    assertEquals(1.34d, (float) floatStats3.getMin(), maxError);
    assertEquals(100.13453f + 1.34f + 200.435d, (float) floatStats3.getSum(), maxError);
    assertEquals(1.34f, (float) floatStats3.getFirst(), maxError);
    assertEquals(200.435f, (float) floatStats3.getLast(), maxError);

  }

}
