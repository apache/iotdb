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
import static org.junit.Assert.assertTrue;

public class DoubleStatisticsTest {

  private static final double maxError = 0.0001d;

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123650 81932492 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 70839329 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testInOrderUpdate() {
    Statistics<Double> doubleStatistics = new DoubleStatistics();
    assertTrue(doubleStatistics.isEmpty());

    double[] vals =
        new double[] {
          7607.41005d,
          4027.54405d,
          8193.24925d,
          1380.64375d,
          7813.17305d,
          5999.96185d,
          7083.93295d,
          351.58025d
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

    doubleStatistics.updateStats(7607.41005d, 2783647123649L);
    doubleStatistics.setStartTime(2783647123649L);
    doubleStatistics.setEndTime(2783647123649L);
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(7607.41005d, doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(2783647123649L, (long) doubleStatistics.getMaxInfo().timestamp);
    assertEquals(7607.41005d, doubleStatistics.getMinInfo().val, maxError);
    assertEquals(2783647123649L, (long) doubleStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, doubleStatistics.getStartTime());
    //    assertEquals(2783647123649L, doubleStatistics.getEndTime());
    assertEquals(7607.41005d, doubleStatistics.getFirstValue(), maxError);
    assertEquals(7607.41005d, doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(4027.54405d, 2783647123650L);
    doubleStatistics.setEndTime(2783647123650L);
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(7607.41005d, doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(2783647123649L, (long) doubleStatistics.getMaxInfo().timestamp);
    assertEquals(4027.54405d, doubleStatistics.getMinInfo().val, maxError);
    assertEquals(2783647123650L, (long) doubleStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, doubleStatistics.getStartTime());
    //    assertEquals(2783647123650L, doubleStatistics.getEndTime());
    assertEquals(7607.41005d, doubleStatistics.getFirstValue(), maxError);
    assertEquals(4027.54405d, doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(8193.24925d, 2783647123651L);
    doubleStatistics.updateStats(1380.64375d, 2783647123652L);
    doubleStatistics.updateStats(7813.17305d, 2783647123653L);
    doubleStatistics.updateStats(5999.96185d, 2783647123654L);
    doubleStatistics.updateStats(7083.93295d, 2783647123655L);
    doubleStatistics.updateStats(351.58025d, 2783647123656L);
    doubleStatistics.setEndTime(2783647123656L);

    assertEquals(8193.24925d, doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(2783647123651L, (long) doubleStatistics.getMaxInfo().timestamp);
    assertEquals(351.58025d, doubleStatistics.getMinInfo().val, maxError);
    assertEquals(2783647123656L, (long) doubleStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, doubleStatistics.getStartTime());
    //    assertEquals(2783647123656L, doubleStatistics.getEndTime());
    assertEquals(7607.41005d, doubleStatistics.getFirstValue(), maxError);
    assertEquals(351.58025d, doubleStatistics.getLastValue(), maxError);

    double sum = 0;
    for (double i : vals) {
      sum += i;
    }
    assertEquals(sum, doubleStatistics.getSumDoubleValue(), maxError);
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 76074100 2783647123650 76074100 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 3515802 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testSameValueUpdate() {
    Statistics<Double> doubleStatistics = new DoubleStatistics();
    assertTrue(doubleStatistics.isEmpty());

    double[] vals =
        new double[] {
          7607.41005d,
          7607.41005d,
          7607.41005d,
          1380.64375d,
          7813.17305d,
          5999.96185d,
          351.58025d,
          351.58025d
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

    doubleStatistics.updateStats(vals[0], times[0]);
    doubleStatistics.setStartTime(times[0]);
    doubleStatistics.setEndTime(times[0]);
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[0], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[0], (long) doubleStatistics.getMaxInfo().timestamp);
    assertEquals(vals[0], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[0], (long) doubleStatistics.getMinInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[0], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[0], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[1], times[1]);
    doubleStatistics.setEndTime(times[1]);
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(7607.41005d, doubleStatistics.getMaxInfo().val, maxError);
    long expectedTimestamp = times[0];
    assertEquals(expectedTimestamp, (long) doubleStatistics.getMaxInfo().timestamp);
    assertEquals(7607.41005d, doubleStatistics.getMinInfo().val, maxError);
    assertEquals(expectedTimestamp, (long) doubleStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, doubleStatistics.getStartTime());
    //    assertEquals(2783647123650L, doubleStatistics.getEndTime());
    assertEquals(7607.41005d, doubleStatistics.getFirstValue(), maxError);
    assertEquals(7607.41005d, doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[2], times[2]);
    doubleStatistics.updateStats(vals[3], times[3]);
    doubleStatistics.setEndTime(times[3]);
    assertEquals(7607.41005d, doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(expectedTimestamp, (long) doubleStatistics.getMaxInfo().timestamp);
    assertEquals(1380.64375d, doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[3], (long) doubleStatistics.getMinInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[3], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[3], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[4], times[4]);
    doubleStatistics.updateStats(vals[5], times[5]);
    doubleStatistics.updateStats(vals[6], times[6]);
    doubleStatistics.updateStats(vals[7], times[7]);
    doubleStatistics.setEndTime(times[7]);

    assertEquals(7813.17305d, doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(2783647123653L, (long) doubleStatistics.getMaxInfo().timestamp);
    expectedTimestamp = times[6];
    assertEquals(351.58025d, doubleStatistics.getMinInfo().val, maxError);
    assertEquals(expectedTimestamp, (long) doubleStatistics.getMinInfo().timestamp);

    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[7], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[7], doubleStatistics.getLastValue(), maxError);

    double sum = 0;
    for (double i : vals) {
      sum += i;
    }
    assertEquals(sum, doubleStatistics.getSumDoubleValue(), maxError);
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123653 81932492 2783647123656 13806437
   *     2783647123652 78131730 2783647123650 59999618 2783647123651 70839329 2783647123655 3515802
   *     2783647123654
   */
  @Test
  public void testOutOfOrderUpdate() {
    Statistics<Double> doubleStatistics = new DoubleStatistics();
    assertTrue(doubleStatistics.isEmpty());

    double[] vals =
        new double[] {
          7607.41005d,
          4027.54405d,
          8193.24925d,
          1380.64375d,
          7813.17305d,
          5999.96185d,
          7083.93295d,
          351.58025d
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

    doubleStatistics.updateStats(vals[0], times[0]);
    doubleStatistics.setStartTime(times[0]);
    doubleStatistics.setEndTime(times[0]);
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[0], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[0], (long) doubleStatistics.getMaxInfo().timestamp);
    assertEquals(vals[0], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[0], (long) doubleStatistics.getMinInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[0], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[0], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[1], times[1]);
    doubleStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 2));
    doubleStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 2));
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[1], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[1], (long) doubleStatistics.getMinInfo().timestamp);
    assertEquals(vals[0], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[0], (long) doubleStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[1], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[1], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[2], times[2]);
    doubleStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 3));
    doubleStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 3));
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[1], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[1], (long) doubleStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[2], (long) doubleStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[2], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[2], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[3], times[3]);
    doubleStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 4));
    doubleStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 4));
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[3], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[3], (long) doubleStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[2], (long) doubleStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[3], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[3], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[4], times[4]);
    doubleStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 5));
    doubleStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 5));
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[3], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[3], (long) doubleStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[2], (long) doubleStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[4], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[4], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[5], times[5]);
    doubleStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 6));
    doubleStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 6));
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[3], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[3], (long) doubleStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[2], (long) doubleStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[5], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[5], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[6], times[6]);
    doubleStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 7));
    doubleStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 7));
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[3], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[3], (long) doubleStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[2], (long) doubleStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[6], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[6], doubleStatistics.getLastValue(), maxError);

    doubleStatistics.updateStats(vals[7], times[7]);
    doubleStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 8));
    doubleStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 8));
    assertFalse(doubleStatistics.isEmpty());
    assertEquals(vals[7], doubleStatistics.getMinInfo().val, maxError);
    assertEquals(times[7], (long) doubleStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], doubleStatistics.getMaxInfo().val, maxError);
    assertEquals(times[2], (long) doubleStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], doubleStatistics.getStartTime());
    //    assertEquals(times[7], doubleStatistics.getEndTime());
    assertEquals(vals[0], doubleStatistics.getFirstValue(), maxError);
    assertEquals(vals[7], doubleStatistics.getLastValue(), maxError);

    double sum = 0;
    for (double i : vals) {
      sum += i;
    }
    assertEquals(sum, doubleStatistics.getSumDoubleValue(), maxError);
  }

  /** @author Yuyuan Kang */
  @Test
  public void testMergeNoOverlap() {
    Statistics<Double> doubleStatistics1 = new DoubleStatistics();
    doubleStatistics1.setStartTime(1000L);
    doubleStatistics1.setEndTime(5000L);
    doubleStatistics1.updateStats(100.5d, 1000L);
    doubleStatistics1.updateStats(10000.5d, 5000L);

    Statistics<Double> doubleStatistics2 = new DoubleStatistics();
    doubleStatistics2.setStartTime(6000L);
    doubleStatistics2.setEndTime(7000L);
    doubleStatistics2.updateStats(600.5d, 6000L);
    doubleStatistics2.updateStats(8000.5d, 7000L);

    doubleStatistics1.mergeStatistics(doubleStatistics2);
    assertFalse(doubleStatistics1.isEmpty());
    assertEquals(100.5d, doubleStatistics1.getMinInfo().val, maxError);
    assertEquals(1000L, (long) doubleStatistics1.getMinInfo().timestamp);
    assertEquals(10000.5d, doubleStatistics1.getMaxInfo().val, maxError);
    assertEquals(5000L, (long) doubleStatistics1.getMaxInfo().timestamp);
    //    assertEquals(1000L, doubleStatistics1.getStartTime());
    //    assertEquals(7000L, doubleStatistics1.getEndTime());
    assertEquals(100.5d, doubleStatistics1.getFirstValue(), maxError);
    assertEquals(8000.5d, doubleStatistics1.getLastValue(), maxError);

    doubleStatistics1 = new DoubleStatistics();
    doubleStatistics1.updateStats(100.5d, 1000L);
    doubleStatistics1.updateStats(10000.5d, 5000L);
    doubleStatistics1.setStartTime(1000L);
    doubleStatistics1.setStartTime(5000L);
    doubleStatistics2 = new DoubleStatistics();
    doubleStatistics2.updateStats(600.5d, 6000L);
    doubleStatistics2.updateStats(80000.5d, 7000L);
    doubleStatistics2.setStartTime(6000L);
    doubleStatistics2.setStartTime(7000L);
    doubleStatistics1.mergeStatistics(doubleStatistics2);
    assertFalse(doubleStatistics1.isEmpty());
    assertEquals(100.5d, doubleStatistics1.getMinInfo().val, maxError);
    assertEquals(1000L, (long) doubleStatistics1.getMinInfo().timestamp);
    assertEquals(80000.5d, doubleStatistics1.getMaxInfo().val, maxError);
    assertEquals(7000L, (long) doubleStatistics1.getMaxInfo().timestamp);
    //    assertEquals(1000L, doubleStatistics1.getStartTime());
    //    assertEquals(7000L, doubleStatistics1.getEndTime());
    assertEquals(100.5d, doubleStatistics1.getFirstValue(), maxError);
    assertEquals(80000.5d, doubleStatistics1.getLastValue(), maxError);

    doubleStatistics1 = new DoubleStatistics();
    doubleStatistics1.updateStats(100.5d, 1000L);
    doubleStatistics1.updateStats(10000.5d, 5000L);
    doubleStatistics1.setStartTime(1000L);
    doubleStatistics1.setEndTime(5000L);
    doubleStatistics2 = new DoubleStatistics();
    doubleStatistics2.updateStats(10.5d, 6000L);
    doubleStatistics2.updateStats(1000.5d, 7000L);
    doubleStatistics2.setStartTime(6000L);
    doubleStatistics2.setEndTime(7000L);
    doubleStatistics1.mergeStatistics(doubleStatistics2);
    assertFalse(doubleStatistics1.isEmpty());
    assertEquals(10.5d, doubleStatistics1.getMinInfo().val, maxError);
    assertEquals(6000L, (long) doubleStatistics1.getMinInfo().timestamp);
    assertEquals(10000.5d, doubleStatistics1.getMaxInfo().val, maxError);
    assertEquals(5000L, (long) doubleStatistics1.getMaxInfo().timestamp);
    //    assertEquals(1000L, doubleStatistics1.getStartTime());
    //    assertEquals(7000L, doubleStatistics1.getEndTime());
    assertEquals(100.5d, doubleStatistics1.getFirstValue(), maxError);
    assertEquals(1000.5d, doubleStatistics1.getLastValue(), maxError);
  }

  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap1() {
  //    Statistics<Double> doubleStatistics1 = new DoubleStatistics();
  //    doubleStatistics1.updateStats(100.5d, 1000L);
  //    doubleStatistics1.updateStats(10000.5d, 5000L);
  //    doubleStatistics1.setStartTime(1000L);
  //    doubleStatistics1.setEndTime(5000L);
  //    Statistics<Double> doubleStatistics2 = new DoubleStatistics();
  //    doubleStatistics2.updateStats(600.5d, 3000L);
  //    doubleStatistics2.updateStats(8000.5d, 7000L);
  //    doubleStatistics2.setStartTime(3000L);
  //    doubleStatistics2.setEndTime(7000L);
  //    doubleStatistics1.mergeStatistics(doubleStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap2() {
  //    Statistics<Double> doubleStatistics1 = new DoubleStatistics();
  //    doubleStatistics1.updateStats(100.5d, 1000L);
  //    doubleStatistics1.updateStats(10000.5d, 5000L);
  //    doubleStatistics1.setStartTime(1000L);
  //    doubleStatistics1.setEndTime(5000L);
  //    Statistics<Double> doubleStatistics2 = new DoubleStatistics();
  //    doubleStatistics2.updateStats(600.5d, 10L);
  //    doubleStatistics2.updateStats(8000.5d, 7000L);
  //    doubleStatistics2.setStartTime(10L);
  //    doubleStatistics2.setEndTime(7000L);
  //    doubleStatistics1.mergeStatistics(doubleStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap3() {
  //    Statistics<Double> doubleStatistics1 = new DoubleStatistics();
  //    doubleStatistics1.updateStats(100.5d, 1000L);
  //    doubleStatistics1.updateStats(10000.5d, 5000L);
  //    doubleStatistics1.setStartTime(1000L);
  //    doubleStatistics1.setEndTime(5000L);
  //    Statistics<Double> doubleStatistics2 = new DoubleStatistics();
  //    doubleStatistics2.updateStats(600.5d, 10L);
  //    doubleStatistics2.updateStats(8000.5d, 2000L);
  //    doubleStatistics2.setStartTime(10L);
  //    doubleStatistics2.setEndTime(2000L);
  //    doubleStatistics1.mergeStatistics(doubleStatistics2);
  //  }
}
