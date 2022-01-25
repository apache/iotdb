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

public class IntegerStatisticsTest {

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123650 81932492 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 70839329 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testInOrderUpdate() {
    Statistics<Integer> integerStatistics = new IntegerStatistics();
    assertTrue(integerStatistics.isEmpty());

    int[] vals =
        new int[] {76074100, 40275440, 81932492, 13806437, 78131730, 59999618, 70839329, 3515802};
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

    integerStatistics.updateStats(76074100, 2783647123649L);
    integerStatistics.setStartTime(2783647123649L);
    integerStatistics.setEndTime(2783647123649L);
    assertFalse(integerStatistics.isEmpty());
    assertEquals(76074100, (long) integerStatistics.getMaxInfo().val);
    assertEquals(2783647123649L, (long) integerStatistics.getMaxInfo().timestamp);
    assertEquals(76074100, (long) integerStatistics.getMinInfo().val);
    assertEquals(2783647123649L, (long) integerStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, integerStatistics.getStartTime());
    //    assertEquals(2783647123649L, integerStatistics.getEndTime());
    assertEquals(76074100, (long) integerStatistics.getFirstValue());
    assertEquals(76074100, (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(40275440, 2783647123650L);
    integerStatistics.setEndTime(2783647123650L);
    assertFalse(integerStatistics.isEmpty());
    assertEquals(76074100, (long) integerStatistics.getMaxInfo().val);
    assertEquals(2783647123649L, (long) integerStatistics.getMaxInfo().timestamp);
    assertEquals(40275440, (long) integerStatistics.getMinInfo().val);
    assertEquals(2783647123650L, (long) integerStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, integerStatistics.getStartTime());
    //    assertEquals(2783647123650L, integerStatistics.getEndTime());
    assertEquals(76074100, (long) integerStatistics.getFirstValue());
    assertEquals(40275440, (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(81932492, 2783647123651L);
    integerStatistics.updateStats(13806437, 2783647123652L);
    integerStatistics.updateStats(78131730, 2783647123653L);
    integerStatistics.updateStats(59999618, 2783647123654L);
    integerStatistics.updateStats(70839329, 2783647123655L);
    integerStatistics.updateStats(3515802, 2783647123656L);
    integerStatistics.setEndTime(2783647123656L);

    assertEquals(81932492, (long) integerStatistics.getMaxInfo().val);
    assertEquals(2783647123651L, (long) integerStatistics.getMaxInfo().timestamp);
    assertEquals(3515802, (long) integerStatistics.getMinInfo().val);
    assertEquals(2783647123656L, (long) integerStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, integerStatistics.getStartTime());
    //    assertEquals(2783647123656L, integerStatistics.getEndTime());
    assertEquals(76074100, (long) integerStatistics.getFirstValue());
    assertEquals(3515802, (long) integerStatistics.getLastValue());

    long sum = 0;
    for (int i : vals) {
      sum += i;
    }
    assertEquals(sum, integerStatistics.getSumLongValue());
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 76074100 2783647123650 76074100 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 3515802 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testSameValueUpdate() {
    Statistics<Integer> integerStatistics = new IntegerStatistics();
    assertTrue(integerStatistics.isEmpty());

    int[] vals =
        new int[] {76074100, 76074100, 76074100, 13806437, 78131730, 59999618, 3515802, 3515802};
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

    integerStatistics.updateStats(vals[0], times[0]);
    integerStatistics.setStartTime(times[0]);
    integerStatistics.setEndTime(times[0]);
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[0], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[0], (long) integerStatistics.getMaxInfo().timestamp);
    assertEquals(vals[0], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[0], (long) integerStatistics.getMinInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[0], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[0], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[1], times[1]);
    integerStatistics.setEndTime(times[1]);
    assertFalse(integerStatistics.isEmpty());
    assertEquals(76074100, (long) integerStatistics.getMaxInfo().val);
    long expectedTimestamp = times[0];
    assertEquals(expectedTimestamp, (long) integerStatistics.getMaxInfo().timestamp);
    assertEquals(76074100, (long) integerStatistics.getMinInfo().val);
    assertEquals(expectedTimestamp, (long) integerStatistics.getMinInfo().timestamp);
    //    assertEquals(2783647123649L, integerStatistics.getStartTime());
    //    assertEquals(2783647123650L, integerStatistics.getEndTime());
    assertEquals(76074100, (long) integerStatistics.getFirstValue());
    assertEquals(76074100, (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[2], times[2]);
    integerStatistics.updateStats(vals[3], times[3]);
    integerStatistics.setEndTime(times[3]);
    assertEquals(76074100, (long) integerStatistics.getMaxInfo().val);
    assertEquals(expectedTimestamp, (long) integerStatistics.getMaxInfo().timestamp);
    assertEquals(13806437, (long) integerStatistics.getMinInfo().val);
    assertEquals(times[3], (long) integerStatistics.getMinInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[3], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[3], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[4], times[4]);
    integerStatistics.updateStats(vals[5], times[5]);
    integerStatistics.updateStats(vals[6], times[6]);
    integerStatistics.updateStats(vals[7], times[7]);
    integerStatistics.setEndTime(times[7]);

    assertEquals(78131730, (long) integerStatistics.getMaxInfo().val);
    assertEquals(2783647123653L, (long) integerStatistics.getMaxInfo().timestamp);

    expectedTimestamp = times[6];
    assertEquals(3515802, (long) integerStatistics.getMinInfo().val);
    assertEquals(expectedTimestamp, (long) integerStatistics.getMinInfo().timestamp);

    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[7], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[7], (long) integerStatistics.getLastValue());

    long sum = 0;
    for (int i : vals) {
      sum += i;
    }
    assertEquals(sum, integerStatistics.getSumLongValue());
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123653 81932492 2783647123656 13806437
   *     2783647123652 78131730 2783647123650 59999618 2783647123651 70839329 2783647123655 3515802
   *     2783647123654
   */
  @Test
  public void testOutOfOrderUpdate() {
    Statistics<Integer> integerStatistics = new IntegerStatistics();
    assertTrue(integerStatistics.isEmpty());

    int[] vals =
        new int[] {76074100, 40275440, 81932492, 13806437, 78131730, 59999618, 70839329, 3515802};
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

    integerStatistics.updateStats(vals[0], times[0]);
    integerStatistics.setStartTime(times[0]);
    integerStatistics.setEndTime(times[0]);
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[0], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[0], (long) integerStatistics.getMaxInfo().timestamp);
    assertEquals(vals[0], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[0], (long) integerStatistics.getMinInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[0], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[0], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[1], times[1]);
    integerStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 2));
    integerStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 2));
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[1], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[1], (long) integerStatistics.getMinInfo().timestamp);
    assertEquals(vals[0], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[0], (long) integerStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[1], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[1], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[2], times[2]);
    integerStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 3));
    integerStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 3));
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[1], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[1], (long) integerStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[2], (long) integerStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[2], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[2], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[3], times[3]);
    integerStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 4));
    integerStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 4));
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[3], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[3], (long) integerStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[2], (long) integerStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[3], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[3], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[4], times[4]);
    integerStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 5));
    integerStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 5));
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[3], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[3], (long) integerStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[2], (long) integerStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[4], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[4], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[5], times[5]);
    integerStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 6));
    integerStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 6));
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[3], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[3], (long) integerStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[2], (long) integerStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[5], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[5], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[6], times[6]);
    integerStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 7));
    integerStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 7));
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[3], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[3], (long) integerStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[2], (long) integerStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[6], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[6], (long) integerStatistics.getLastValue());

    integerStatistics.updateStats(vals[7], times[7]);
    integerStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 8));
    integerStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 8));
    assertFalse(integerStatistics.isEmpty());
    assertEquals(vals[7], (long) integerStatistics.getMinInfo().val);
    assertEquals(times[7], (long) integerStatistics.getMinInfo().timestamp);
    assertEquals(vals[2], (long) integerStatistics.getMaxInfo().val);
    assertEquals(times[2], (long) integerStatistics.getMaxInfo().timestamp);
    //    assertEquals(times[0], integerStatistics.getStartTime());
    //    assertEquals(times[7], integerStatistics.getEndTime());
    assertEquals(vals[0], (long) integerStatistics.getFirstValue());
    assertEquals(vals[7], (long) integerStatistics.getLastValue());

    long sum = 0;
    for (int i : vals) {
      sum += i;
    }
    assertEquals(sum, integerStatistics.getSumLongValue());
  }

  /** @author Yuyuan Kang */
  @Test
  public void testMergeNoOverlap() {
    Statistics<Integer> longStatistics1 = new IntegerStatistics();
    longStatistics1.setStartTime(1000L);
    longStatistics1.setEndTime(5000L);
    longStatistics1.updateStats(100, 1000L);
    longStatistics1.updateStats(10000, 5000L);

    Statistics<Integer> longStatistics2 = new IntegerStatistics();
    longStatistics2.setStartTime(6000L);
    longStatistics2.setEndTime(7000L);
    longStatistics2.updateStats(600, 6000L);
    longStatistics2.updateStats(8000, 7000L);

    longStatistics1.mergeStatistics(longStatistics2);
    assertFalse(longStatistics1.isEmpty());
    assertEquals(100, (long) longStatistics1.getMinInfo().val);
    assertEquals(1000L, (long) longStatistics1.getMinInfo().timestamp);
    assertEquals(10000, (long) longStatistics1.getMaxInfo().val);
    assertEquals(5000L, (long) longStatistics1.getMaxInfo().timestamp);
    //    assertEquals(1000L, longStatistics1.getStartTime());
    //    assertEquals(7000L, longStatistics1.getEndTime());
    assertEquals(100L, (long) longStatistics1.getFirstValue());
    assertEquals(8000L, (long) longStatistics1.getLastValue());

    longStatistics1 = new IntegerStatistics();
    longStatistics1.updateStats(100, 1000L);
    longStatistics1.updateStats(10000, 5000L);
    longStatistics1.setStartTime(1000L);
    longStatistics1.setStartTime(5000L);
    longStatistics2 = new IntegerStatistics();
    longStatistics2.updateStats(600, 6000L);
    longStatistics2.updateStats(80000, 7000L);
    longStatistics2.setStartTime(6000L);
    longStatistics2.setStartTime(7000L);
    longStatistics1.mergeStatistics(longStatistics2);
    assertFalse(longStatistics1.isEmpty());
    assertEquals(100, (long) longStatistics1.getMinInfo().val);
    assertEquals(1000L, (long) longStatistics1.getMinInfo().timestamp);
    assertEquals(80000, (long) longStatistics1.getMaxInfo().val);
    assertEquals(7000L, (long) longStatistics1.getMaxInfo().timestamp);
    //    assertEquals(1000L, longStatistics1.getStartTime());
    //    assertEquals(7000L, longStatistics1.getEndTime());
    assertEquals(100, (long) longStatistics1.getFirstValue());
    assertEquals(80000, (long) longStatistics1.getLastValue());

    longStatistics1 = new IntegerStatistics();
    longStatistics1.updateStats(100, 1000L);
    longStatistics1.updateStats(10000, 5000L);
    longStatistics1.setStartTime(1000L);
    longStatistics1.setEndTime(5000L);
    longStatistics2 = new IntegerStatistics();
    longStatistics2.updateStats(10, 6000L);
    longStatistics2.updateStats(1000, 7000L);
    longStatistics2.setStartTime(6000L);
    longStatistics2.setEndTime(7000L);
    longStatistics1.mergeStatistics(longStatistics2);
    assertFalse(longStatistics1.isEmpty());
    assertEquals(10, (long) longStatistics1.getMinInfo().val);
    assertEquals(6000L, (long) longStatistics1.getMinInfo().timestamp);
    assertEquals(10000, (long) longStatistics1.getMaxInfo().val);
    assertEquals(5000L, (long) longStatistics1.getMaxInfo().timestamp);
    //    assertEquals(1000L, longStatistics1.getStartTime());
    //    assertEquals(7000L, longStatistics1.getEndTime());
    assertEquals(100, (long) longStatistics1.getFirstValue());
    assertEquals(1000, (long) longStatistics1.getLastValue());
  }

  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap1() {
  //    Statistics<Integer> longStatistics1 = new IntegerStatistics();
  //    longStatistics1.updateStats(100, 1000L);
  //    longStatistics1.updateStats(10000, 5000L);
  //    longStatistics1.setStartTime(1000L);
  //    longStatistics1.setEndTime(5000L);
  //    Statistics<Integer> longStatistics2 = new IntegerStatistics();
  //    longStatistics2.updateStats(600, 3000L);
  //    longStatistics2.updateStats(8000, 7000L);
  //    longStatistics2.setStartTime(3000L);
  //    longStatistics2.setEndTime(7000L);
  //    longStatistics1.mergeStatistics(longStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap2() {
  //    Statistics<Integer> longStatistics1 = new IntegerStatistics();
  //    longStatistics1.updateStats(100, 1000L);
  //    longStatistics1.updateStats(10000, 5000L);
  //    longStatistics1.setStartTime(1000L);
  //    longStatistics1.setEndTime(5000L);
  //    Statistics<Integer> longStatistics2 = new IntegerStatistics();
  //    longStatistics2.updateStats(600, 10L);
  //    longStatistics2.updateStats(8000, 7000L);
  //    longStatistics2.setStartTime(10L);
  //    longStatistics2.setEndTime(7000L);
  //    longStatistics1.mergeStatistics(longStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap3() {
  //    Statistics<Integer> longStatistics1 = new IntegerStatistics();
  //    longStatistics1.updateStats(100, 1000L);
  //    longStatistics1.updateStats(10000, 5000L);
  //    longStatistics1.setStartTime(1000L);
  //    longStatistics1.setEndTime(5000L);
  //    Statistics<Integer> longStatistics2 = new IntegerStatistics();
  //    longStatistics2.updateStats(600, 10L);
  //    longStatistics2.updateStats(8000, 2000L);
  //    longStatistics2.setStartTime(10L);
  //    longStatistics2.setEndTime(2000L);
  //    longStatistics1.mergeStatistics(longStatistics2);
  //  }
}
