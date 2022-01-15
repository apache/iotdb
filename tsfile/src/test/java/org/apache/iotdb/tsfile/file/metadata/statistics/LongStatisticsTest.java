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

public class LongStatisticsTest {

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123650 81932492 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 70839329 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testInOrderUpdate() {
    Statistics<Long> longStatistics = new LongStatistics();
    assertTrue(longStatistics.isEmpty());

    long[] vals =
        new long[] {
          76074100L, 40275440L, 81932492L, 13806437L, 78131730L, 59999618L, 70839329L, 3515802L
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

    longStatistics.updateStats(76074100L, 2783647123649L);
    longStatistics.setStartTime(2783647123649L);
    longStatistics.setEndTime(2783647123649L);
    assertFalse(longStatistics.isEmpty());
    assertEquals(76074100L, (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(2783647123649L), longStatistics.getMaxInfo().timestamps);
    assertEquals(76074100L, (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(2783647123649L), longStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, longStatistics.getStartTime());
    //    assertEquals(2783647123649L, longStatistics.getEndTime());
    assertEquals(76074100L, (long) longStatistics.getFirstValue());
    assertEquals(76074100L, (long) longStatistics.getLastValue());

    longStatistics.updateStats(40275440L, 2783647123650L);
    longStatistics.setEndTime(2783647123650L);
    assertFalse(longStatistics.isEmpty());
    assertEquals(76074100L, (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(2783647123649L), longStatistics.getMaxInfo().timestamps);
    assertEquals(40275440L, (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(2783647123650L), longStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, longStatistics.getStartTime());
    //    assertEquals(2783647123650L, longStatistics.getEndTime());
    assertEquals(76074100L, (long) longStatistics.getFirstValue());
    assertEquals(40275440L, (long) longStatistics.getLastValue());

    longStatistics.updateStats(81932492L, 2783647123651L);
    longStatistics.updateStats(13806437L, 2783647123652L);
    longStatistics.updateStats(78131730L, 2783647123653L);
    longStatistics.updateStats(59999618L, 2783647123654L);
    longStatistics.updateStats(70839329L, 2783647123655L);
    longStatistics.updateStats(3515802L, 2783647123656L);
    longStatistics.setEndTime(2783647123656L);

    assertEquals(81932492L, (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(2783647123651L), longStatistics.getMaxInfo().timestamps);
    assertEquals(3515802L, (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(2783647123656L), longStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, longStatistics.getStartTime());
    //    assertEquals(2783647123656L, longStatistics.getEndTime());
    assertEquals(76074100L, (long) longStatistics.getFirstValue());
    assertEquals(3515802L, (long) longStatistics.getLastValue());

    long sum = 0;
    for (long i : vals) {
      sum += i;
    }
    assertEquals(sum, (long) longStatistics.getSumDoubleValue());
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 76074100 2783647123650 76074100 2783647123651 13806437
   *     2783647123652 78131730 2783647123653 59999618 2783647123654 3515802 2783647123655 3515802
   *     2783647123656
   */
  @Test
  public void testSameValueUpdate() {
    Statistics<Long> longStatistics = new LongStatistics();
    assertTrue(longStatistics.isEmpty());

    long[] vals =
        new long[] {
          76074100L, 76074100L, 76074100L, 13806437L, 78131730L, 59999618L, 3515802L, 3515802L
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

    longStatistics.updateStats(vals[0], times[0]);
    longStatistics.setStartTime(times[0]);
    longStatistics.setEndTime(times[0]);
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[0], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[0]), longStatistics.getMaxInfo().timestamps);
    assertEquals(vals[0], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[0]), longStatistics.getMinInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[0], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[0], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[1], times[1]);
    longStatistics.setEndTime(times[1]);
    assertFalse(longStatistics.isEmpty());
    assertEquals(76074100L, (long) longStatistics.getMaxInfo().val);
    Set<Long> expectedTimestamps = new HashSet<>();
    expectedTimestamps.add(times[0]);
    expectedTimestamps.add(times[1]);
    assertEquals(expectedTimestamps, longStatistics.getMaxInfo().timestamps);
    assertEquals(76074100L, (long) longStatistics.getMinInfo().val);
    assertEquals(expectedTimestamps, longStatistics.getMinInfo().timestamps);
    //    assertEquals(2783647123649L, longStatistics.getStartTime());
    //    assertEquals(2783647123650L, longStatistics.getEndTime());
    assertEquals(76074100L, (long) longStatistics.getFirstValue());
    assertEquals(76074100L, (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[2], times[2]);
    longStatistics.updateStats(vals[3], times[3]);
    longStatistics.setEndTime(times[3]);
    assertEquals(76074100L, (long) longStatistics.getMaxInfo().val);
    expectedTimestamps.add(times[2]);
    assertEquals(expectedTimestamps, longStatistics.getMaxInfo().timestamps);
    assertEquals(13806437L, (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[3]), longStatistics.getMinInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[3], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[3], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[4], times[4]);
    longStatistics.updateStats(vals[5], times[5]);
    longStatistics.updateStats(vals[6], times[6]);
    longStatistics.updateStats(vals[7], times[7]);
    longStatistics.setEndTime(times[7]);

    assertEquals(78131730L, (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(2783647123653L), longStatistics.getMaxInfo().timestamps);

    expectedTimestamps = new HashSet<>();
    expectedTimestamps.add(times[6]);
    expectedTimestamps.add(times[7]);
    assertEquals(3515802L, (long) longStatistics.getMinInfo().val);
    assertEquals(expectedTimestamps, longStatistics.getMinInfo().timestamps);

    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[7], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[7], (long) longStatistics.getLastValue());

    long sum = 0;
    for (long i : vals) {
      sum += i;
    }
    assertEquals(sum, (long) longStatistics.getSumDoubleValue());
  }

  /**
   * @author Yuyuan Kang
   *     <p>value time 76074100 2783647123649 40275440 2783647123653 81932492 2783647123656 13806437
   *     2783647123652 78131730 2783647123650 59999618 2783647123651 70839329 2783647123655 3515802
   *     2783647123654
   */
  @Test
  public void testOutOfOrderUpdate() {
    Statistics<Long> longStatistics = new LongStatistics();
    assertTrue(longStatistics.isEmpty());

    long[] vals =
        new long[] {
          76074100L, 40275440L, 81932492L, 13806437L, 78131730L, 59999618L, 70839329L, 3515802L
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

    longStatistics.updateStats(vals[0], times[0]);
    longStatistics.setStartTime(times[0]);
    longStatistics.setEndTime(times[0]);
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[0], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[0]), longStatistics.getMaxInfo().timestamps);
    assertEquals(vals[0], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[0]), longStatistics.getMinInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[0], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[0], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[1], times[1]);
    longStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 2));
    longStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 2));
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[1], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[1]), longStatistics.getMinInfo().timestamps);
    assertEquals(vals[0], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[0]), longStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[1], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[1], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[2], times[2]);
    longStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 3));
    longStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 3));
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[1], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[1]), longStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[2]), longStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[2], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[2], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[3], times[3]);
    longStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 4));
    longStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 4));
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[3], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[3]), longStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[2]), longStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[3], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[3], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[4], times[4]);
    longStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 5));
    longStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 5));
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[3], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[3]), longStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[2]), longStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[4], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[4], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[5], times[5]);
    longStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 6));
    longStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 6));
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[3], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[3]), longStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[2]), longStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[5], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[5], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[6], times[6]);
    longStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 7));
    longStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 7));
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[3], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[3]), longStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[2]), longStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[6], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[6], (long) longStatistics.getLastValue());

    longStatistics.updateStats(vals[7], times[7]);
    longStatistics.setStartTime(MaxMinUtils.minLong(times, 0, 8));
    longStatistics.setEndTime(MaxMinUtils.maxLong(times, 0, 8));
    assertFalse(longStatistics.isEmpty());
    assertEquals(vals[7], (long) longStatistics.getMinInfo().val);
    assertEquals(Collections.singleton(times[7]), longStatistics.getMinInfo().timestamps);
    assertEquals(vals[2], (long) longStatistics.getMaxInfo().val);
    assertEquals(Collections.singleton(times[2]), longStatistics.getMaxInfo().timestamps);
    //    assertEquals(times[0], longStatistics.getStartTime());
    //    assertEquals(times[7], longStatistics.getEndTime());
    assertEquals(vals[0], (long) longStatistics.getFirstValue());
    assertEquals(vals[7], (long) longStatistics.getLastValue());

    long sum = 0;
    for (long i : vals) {
      sum += i;
    }
    assertEquals(sum, (long) longStatistics.getSumDoubleValue());
  }

  /** @author Yuyuan Kang */
  @Test
  public void testMergeNoOverlap() {
    Statistics<Long> longStatistics1 = new LongStatistics();
    longStatistics1.setStartTime(1000L);
    longStatistics1.setEndTime(5000L);
    longStatistics1.updateStats(100L, 1000L);
    longStatistics1.updateStats(10000L, 5000L);

    Statistics<Long> longStatistics2 = new LongStatistics();
    longStatistics2.setStartTime(6000L);
    longStatistics2.setEndTime(7000L);
    longStatistics2.updateStats(600L, 6000L);
    longStatistics2.updateStats(8000L, 7000L);

    longStatistics1.mergeStatistics(longStatistics2);
    assertFalse(longStatistics1.isEmpty());
    assertEquals(100L, (long) longStatistics1.getMinInfo().val);
    assertEquals(Collections.singleton(1000L), longStatistics1.getMinInfo().timestamps);
    assertEquals(10000L, (long) longStatistics1.getMaxInfo().val);
    assertEquals(Collections.singleton(5000L), longStatistics1.getMaxInfo().timestamps);
    //    assertEquals(1000L, longStatistics1.getStartTime());
    //    assertEquals(7000L, longStatistics1.getEndTime());
    assertEquals(100L, (long) longStatistics1.getFirstValue());
    assertEquals(8000L, (long) longStatistics1.getLastValue());

    longStatistics1 = new LongStatistics();
    longStatistics1.updateStats(100L, 1000L);
    longStatistics1.updateStats(10000L, 5000L);
    longStatistics1.setStartTime(1000L);
    longStatistics1.setStartTime(5000L);
    longStatistics2 = new LongStatistics();
    longStatistics2.updateStats(600L, 6000L);
    longStatistics2.updateStats(80000L, 7000L);
    longStatistics2.setStartTime(6000L);
    longStatistics2.setStartTime(7000L);
    longStatistics1.mergeStatistics(longStatistics2);
    assertFalse(longStatistics1.isEmpty());
    assertEquals(100L, (long) longStatistics1.getMinInfo().val);
    assertEquals(Collections.singleton(1000L), longStatistics1.getMinInfo().timestamps);
    assertEquals(80000L, (long) longStatistics1.getMaxInfo().val);
    assertEquals(Collections.singleton(7000L), longStatistics1.getMaxInfo().timestamps);
    //    assertEquals(1000L, longStatistics1.getStartTime());
    //    assertEquals(7000L, longStatistics1.getEndTime());
    assertEquals(100L, (long) longStatistics1.getFirstValue());
    assertEquals(80000L, (long) longStatistics1.getLastValue());

    longStatistics1 = new LongStatistics();
    longStatistics1.updateStats(100L, 1000L);
    longStatistics1.updateStats(10000L, 5000L);
    longStatistics1.setStartTime(1000L);
    longStatistics1.setEndTime(5000L);
    longStatistics2 = new LongStatistics();
    longStatistics2.updateStats(10L, 6000L);
    longStatistics2.updateStats(1000L, 7000L);
    longStatistics2.setStartTime(6000L);
    longStatistics2.setEndTime(7000L);
    longStatistics1.mergeStatistics(longStatistics2);
    assertFalse(longStatistics1.isEmpty());
    assertEquals(10L, (long) longStatistics1.getMinInfo().val);
    assertEquals(Collections.singleton(6000L), longStatistics1.getMinInfo().timestamps);
    assertEquals(10000L, (long) longStatistics1.getMaxInfo().val);
    assertEquals(Collections.singleton(5000L), longStatistics1.getMaxInfo().timestamps);
    //    assertEquals(1000L, longStatistics1.getStartTime());
    //    assertEquals(7000L, longStatistics1.getEndTime());
    assertEquals(100L, (long) longStatistics1.getFirstValue());
    assertEquals(1000L, (long) longStatistics1.getLastValue());
  }

  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap1() {
  //    Statistics<Long> longStatistics1 = new LongStatistics();
  //    longStatistics1.updateStats(100L, 1000L);
  //    longStatistics1.updateStats(10000L, 5000L);
  //    longStatistics1.setStartTime(1000L);
  //    longStatistics1.setEndTime(5000L);
  //    Statistics<Long> longStatistics2 = new LongStatistics();
  //    longStatistics2.updateStats(600L, 3000L);
  //    longStatistics2.updateStats(8000L, 7000L);
  //    longStatistics2.setStartTime(3000L);
  //    longStatistics2.setEndTime(7000L);
  //    longStatistics1.mergeStatistics(longStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap2() {
  //    Statistics<Long> longStatistics1 = new LongStatistics();
  //    longStatistics1.updateStats(100L, 1000L);
  //    longStatistics1.updateStats(10000L, 5000L);
  //    longStatistics1.setStartTime(1000L);
  //    longStatistics1.setEndTime(5000L);
  //    Statistics<Long> longStatistics2 = new LongStatistics();
  //    longStatistics2.updateStats(600L, 10L);
  //    longStatistics2.updateStats(8000L, 7000L);
  //    longStatistics2.setStartTime(10L);
  //    longStatistics2.setEndTime(7000L);
  //    longStatistics1.mergeStatistics(longStatistics2);
  //  }
  //
  //  /** @author Yuyuan Kang */
  //  @Test(expected = StatisticsClassException.class)
  //  public void testMergeWithOverlap3() {
  //    Statistics<Long> longStatistics1 = new LongStatistics();
  //    longStatistics1.updateStats(100L, 1000L);
  //    longStatistics1.updateStats(10000L, 5000L);
  //    longStatistics1.setStartTime(1000L);
  //    longStatistics1.setEndTime(5000L);
  //    Statistics<Long> longStatistics2 = new LongStatistics();
  //    longStatistics2.updateStats(600L, 10L);
  //    longStatistics2.updateStats(8000L, 2000L);
  //    longStatistics2.setStartTime(10L);
  //    longStatistics2.setEndTime(2000L);
  //    longStatistics1.mergeStatistics(longStatistics2);
  //  }
}
