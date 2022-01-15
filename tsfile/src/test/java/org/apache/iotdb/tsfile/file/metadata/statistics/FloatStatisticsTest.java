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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FloatStatisticsTest {

  private static final float maxError = 0.01f;

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123650 81932492 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 70839329 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testInOrderUpdate() {
    Statistics<Float> floatStatistics = new FloatStatistics();
    assertTrue(floatStatistics.isEmpty());

    float[] vals =
        new float[] {
          7607.41005f,
          4027.54405f,
          8193.24925f,
          1380.64375f,
          7813.17305f,
          5999.96185f,
          7083.93295f,
          351.58025f
        };
    long[] times =
        new long[] {
          2783647123649L,
          2783647123650L,
          2783647123651L,
          2783647123652L,
          2783647123653L,
          2783647123654L,
          2783647123655L,
          2783647123656L
        };

    floatStatistics.updateStats(7607.41005f, 2783647123649L);
    floatStatistics.setStartTime(2783647123649L);
    floatStatistics.setEndTime(2783647123649L);
    assertFalse(floatStatistics.isEmpty());
    assertEquals(7607.41005f, floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(2783647123649L), floatStatistics.getMaxInfo().timestamps);
    assertEquals(7607.41005f, floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(2783647123649L), floatStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, floatStatistics.getStartTime());
    //    assertEquals(2783647123649L, floatStatistics.getEndTime());
    assertEquals(7607.41005f, floatStatistics.getFirstValue(), maxError);
    assertEquals(7607.41005f, floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(4027.54405f, 2783647123650L);
    floatStatistics.setEndTime(2783647123650L);
    assertFalse(floatStatistics.isEmpty());
    assertEquals(7607.41005f, floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(2783647123649L), floatStatistics.getMaxInfo().timestamps);
    assertEquals(4027.54405f, floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(2783647123650L), floatStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, floatStatistics.getStartTime());
    //    assertEquals(2783647123650L, floatStatistics.getEndTime());
    assertEquals(7607.41005f, floatStatistics.getFirstValue(), maxError);
    assertEquals(4027.54405f, floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(8193.24925f, 2783647123651L);
    floatStatistics.updateStats(1380.64375f, 2783647123652L);
    floatStatistics.updateStats(7813.17305f, 2783647123653L);
    floatStatistics.updateStats(5999.96185f, 2783647123654L);
    floatStatistics.updateStats(7083.93295f, 2783647123655L);
    floatStatistics.updateStats(351.58025f, 2783647123656L);
    floatStatistics.setEndTime(2783647123656L);

    assertEquals(8193.24925f, floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(2783647123651L), floatStatistics.getMaxInfo().timestamps);
    assertEquals(351.58025f, floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(2783647123656L), floatStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, floatStatistics.getStartTime());
    //    assertEquals(2783647123656L, floatStatistics.getEndTime());
    assertEquals(7607.41005f, floatStatistics.getFirstValue(), maxError);
    assertEquals(351.58025f, floatStatistics.getLastValue(), maxError);

    float sum = 0;
    for (float i : vals) {
      sum += i;
    }
    assertEquals(sum, floatStatistics.getSumDoubleValue(), maxError);
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 76074100 2783647123650 76074100 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 3515802 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testSameValueUpdate() {
    Statistics<Float> floatStatistics = new FloatStatistics();
    assertTrue(floatStatistics.isEmpty());

    float[] vals =
        new float[] {
          7607.41005f,
          7607.41005f,
          7607.41005f,
          1380.64375f,
          7813.17305f,
          5999.96185f,
          351.58025f,
          351.58025f
        };
    long[] times =
        new long[] {
          2783647123649L,
          2783647123650L,
          2783647123651L,
          2783647123652L,
          2783647123653L,
          2783647123654L,
          2783647123655L,
          2783647123656L
        };

    floatStatistics.updateStats(vals[0], times[0]);
    floatStatistics.setStartTime(times[0]);
    floatStatistics.setEndTime(times[0]);
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[0], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[0]), floatStatistics.getMaxInfo().timestamps);
    assertEquals(vals[0], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[0]), floatStatistics.getMinInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[0], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[0], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[1], times[1]);
    floatStatistics.setEndTime(times[1]);
    assertFalse(floatStatistics.isEmpty());
    assertEquals(7607.41005f, floatStatistics.getMaxInfo().val, maxError);
    Set<Long> expectedTimestamps = new HashSet<>();
    expectedTimestamps.add(times[0]);
    expectedTimestamps.add(times[1]);
    assertEquals(expectedTimestamps, floatStatistics.getMaxInfo().timestamps);
    assertEquals(7607.41005f, floatStatistics.getMinInfo().val, maxError);
    assertEquals(expectedTimestamps, floatStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, floatStatistics.getStartTime());
    //    assertEquals(2783647123650L, floatStatistics.getEndTime());
    assertEquals(7607.41005f, floatStatistics.getFirstValue(), maxError);
    assertEquals(7607.41005f, floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[2], times[2]);
    floatStatistics.updateStats(vals[3], times[3]);
    floatStatistics.setEndTime(times[3]);
    assertEquals(7607.41005f, floatStatistics.getMaxInfo().val, maxError);
    expectedTimestamps.add(times[2]);
    assertEquals(expectedTimestamps, floatStatistics.getMaxInfo().timestamps);
    assertEquals(1380.64375f, floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[3]), floatStatistics.getMinInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[3], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[3], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[4], times[4]);
    floatStatistics.updateStats(vals[5], times[5]);
    floatStatistics.updateStats(vals[6], times[6]);
    floatStatistics.updateStats(vals[7], times[7]);
    floatStatistics.setEndTime(times[7]);

    assertEquals(7813.17305f, floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(2783647123653L), floatStatistics.getMaxInfo().timestamps);

    expectedTimestamps = new HashSet<>();
    expectedTimestamps.add(times[6]);
    expectedTimestamps.add(times[7]);
    assertEquals(351.58025f, floatStatistics.getMinInfo().val, maxError);
    assertEquals(expectedTimestamps, floatStatistics.getMinInfo().timestamps);

    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[7], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[7], floatStatistics.getLastValue(), maxError);

    float sum = 0;
    for (float i : vals) {
      sum += i;
    }
    assertEquals(sum, (float) floatStatistics.getSumDoubleValue(), maxError);
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123653 81932492 2783647123656 13806437
   *     2783647123652 78131730 2783647123650 59999618 2783647123651 70839329 2783647123655 3515802
   *     2783647123654
   */
  @Test
  public void testOutOfOrderUpdate() {
    Statistics<Float> floatStatistics = new FloatStatistics();
    assertTrue(floatStatistics.isEmpty());

    float[] vals =
        new float[] {
          7607.41005f,
          4027.54405f,
          8193.24925f,
          1380.64375f,
          7813.17305f,
          5999.96185f,
          7083.93295f,
          351.58025f
        };
    long[] times =
        new long[] {
          2783647123649L,
          2783647123653L,
          2783647123656L,
          2783647123652L,
          2783647123650L,
          2783647123651L,
          2783647123655L,
          2783647123654L
        };

    floatStatistics.updateStats(vals[0], times[0]);
    floatStatistics.setStartTime(times[0]);
    floatStatistics.setEndTime(times[0]);
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[0], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[0]), floatStatistics.getMaxInfo().timestamps);
    assertEquals(vals[0], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[0]), floatStatistics.getMinInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[0], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[0], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[1], times[1]);
    floatStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 2));
    floatStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 2));
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[1], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[1]), floatStatistics.getMinInfo().timestamps);
    assertEquals(vals[0], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[0]), floatStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[1], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[1], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[2], times[2]);
    floatStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 3));
    floatStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 3));
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[1], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[1]), floatStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[2]), floatStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[2], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[2], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[3], times[3]);
    floatStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 4));
    floatStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 4));
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[3], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[3]), floatStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[2]), floatStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[3], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[3], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[4], times[4]);
    floatStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 5));
    floatStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 5));
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[3], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[3]), floatStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[2]), floatStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[4], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[4], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[5], times[5]);
    floatStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 6));
    floatStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 6));
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[3], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[3]), floatStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[2]), floatStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[5], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[5], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[6], times[6]);
    floatStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 7));
    floatStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 7));
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[3], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[3]), floatStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[2]), floatStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[6], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[6], floatStatistics.getLastValue(), maxError);

    floatStatistics.updateStats(vals[7], times[7]);
    floatStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 8));
    floatStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 8));
    assertFalse(floatStatistics.isEmpty());
    assertEquals(vals[7], floatStatistics.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(times[7]), floatStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], floatStatistics.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(times[2]), floatStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], floatStatistics.getStartTime());
    //    assertEquals(times[7], floatStatistics.getEndTime());
    assertEquals(vals[0], floatStatistics.getFirstValue(), maxError);
    assertEquals(vals[7], floatStatistics.getLastValue(), maxError);

    float sum = 0;
    for (float i : vals) {
      sum += i;
    }
    assertEquals(sum, (float) floatStatistics.getSumDoubleValue(), maxError);
  }

  /** @author Yuyuan Kang */
  @Test
  public void testMergeNoOverlap() {
    Statistics<Float> floatStatistics1 = new FloatStatistics();
    floatStatistics1.setStartTime(1000L);
    floatStatistics1.setEndTime(5000L);
    floatStatistics1.updateStats(100.5f, 1000L);
    floatStatistics1.updateStats(10000.5f, 5000L);

    Statistics<Float> floatStatistics2 = new FloatStatistics();
    floatStatistics2.setStartTime(6000L);
    floatStatistics2.setEndTime(7000L);
    floatStatistics2.updateStats(600.5f, 6000L);
    floatStatistics2.updateStats(8000.5f, 7000L);

    floatStatistics1.mergeStatistics(floatStatistics2);
    assertFalse(floatStatistics1.isEmpty());
    assertEquals(100.5f, floatStatistics1.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(1000L), floatStatistics1.getMinInfo().timestamps);
    assertEquals(10000.5f, floatStatistics1.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(5000L), floatStatistics1.getMaxInfo().timestamps);
    //    assertEquals(1000L, floatStatistics1.getStartTime());
    //    assertEquals(7000L, floatStatistics1.getEndTime());
    assertEquals(100.5f, floatStatistics1.getFirstValue(), maxError);
    assertEquals(8000.5f, floatStatistics1.getLastValue(), maxError);

    floatStatistics1 = new FloatStatistics();
    floatStatistics1.updateStats(100.5f, 1000L);
    floatStatistics1.updateStats(10000.5f, 5000L);
    floatStatistics1.setStartTime(1000L);
    floatStatistics1.setStartTime(5000L);
    floatStatistics2 = new FloatStatistics();
    floatStatistics2.updateStats(600.5f, 6000L);
    floatStatistics2.updateStats(80000.5f, 7000L);
    floatStatistics2.setStartTime(6000L);
    floatStatistics2.setStartTime(7000L);
    floatStatistics1.mergeStatistics(floatStatistics2);
    assertFalse(floatStatistics1.isEmpty());
    assertEquals(100.5f, floatStatistics1.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(1000L), floatStatistics1.getMinInfo().timestamps);
    assertEquals(80000.5f, floatStatistics1.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(7000L), floatStatistics1.getMaxInfo().timestamps);
    //    assertEquals(1000L, floatStatistics1.getStartTime());
    //    assertEquals(7000L, floatStatistics1.getEndTime());
    assertEquals(100.5f, floatStatistics1.getFirstValue(), maxError);
    assertEquals(80000.5f, floatStatistics1.getLastValue(), maxError);

    floatStatistics1 = new FloatStatistics();
    floatStatistics1.updateStats(100.5f, 1000L);
    floatStatistics1.updateStats(10000.5f, 5000L);
    floatStatistics1.setStartTime(1000L);
    floatStatistics1.setEndTime(5000L);
    floatStatistics2 = new FloatStatistics();
    floatStatistics2.updateStats(10.5f, 6000L);
    floatStatistics2.updateStats(1000.5f, 7000L);
    floatStatistics2.setStartTime(6000L);
    floatStatistics2.setEndTime(7000L);
    floatStatistics1.mergeStatistics(floatStatistics2);
    assertFalse(floatStatistics1.isEmpty());
    assertEquals(10.5f, floatStatistics1.getMinInfo().val, maxError);
    assertEquals(Collections.singleton(6000L), floatStatistics1.getMinInfo().timestamps);
    assertEquals(10000.5f, floatStatistics1.getMaxInfo().val, maxError);
    assertEquals(Collections.singleton(5000L), floatStatistics1.getMaxInfo().timestamps);
    //    assertEquals(1000L, floatStatistics1.getStartTime());
    //    assertEquals(7000L, floatStatistics1.getEndTime());
    assertEquals(100.5f, floatStatistics1.getFirstValue(), maxError);
    assertEquals(1000.5f, floatStatistics1.getLastValue(), maxError);
  }

  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap1() {
  //    Statistics<Float> floatStatistics1 = new FloatStatistics();
  //    floatStatistics1.updateStats(100.5f, 1000L);
  //    floatStatistics1.updateStats(10000.5f, 5000L);
  //    floatStatistics1.setStartTime(1000L);
  //    floatStatistics1.setEndTime(5000L);
  //    Statistics<Float> floatStatistics2 = new FloatStatistics();
  //    floatStatistics2.updateStats(600.5f, 3000L);
  //    floatStatistics2.updateStats(8000.5f, 7000L);
  //    floatStatistics2.setStartTime(3000L);
  //    floatStatistics2.setEndTime(7000L);
  //    floatStatistics1.mergeStatistics(floatStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap2() {
  //    Statistics<Float> floatStatistics1 = new FloatStatistics();
  //    floatStatistics1.updateStats(100.5f, 1000L);
  //    floatStatistics1.updateStats(10000.5f, 5000L);
  //    floatStatistics1.setStartTime(1000L);
  //    floatStatistics1.setEndTime(5000L);
  //    Statistics<Float> floatStatistics2 = new FloatStatistics();
  //    floatStatistics2.updateStats(600.5f, 10L);
  //    floatStatistics2.updateStats(8000.5f, 7000L);
  //    floatStatistics2.setStartTime(10L);
  //    floatStatistics2.setEndTime(7000L);
  //    floatStatistics1.mergeStatistics(floatStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap3() {
  //    Statistics<Float> floatStatistics1 = new FloatStatistics();
  //    floatStatistics1.updateStats(100.5f, 1000L);
  //    floatStatistics1.updateStats(10000.5f, 5000L);
  //    floatStatistics1.setStartTime(1000L);
  //    floatStatistics1.setEndTime(5000L);
  //    Statistics<Float> floatStatistics2 = new FloatStatistics();
  //    floatStatistics2.updateStats(600.5f, 10L);
  //    floatStatistics2.updateStats(8000.5f, 2000L);
  //    floatStatistics2.setStartTime(10L);
  //    floatStatistics2.setEndTime(2000L);
  //    floatStatistics1.mergeStatistics(floatStatistics2);
  //  }
}
