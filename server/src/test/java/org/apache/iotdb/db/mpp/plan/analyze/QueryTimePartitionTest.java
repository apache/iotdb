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
package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;
import org.apache.iotdb.tsfile.read.filter.operator.OrFilter;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryTimePartitionTest {

  @Test
  public void testAndTimeFilter() {
    TimeFilter.TimeGt left = TimeFilter.gt(10);
    TimeFilter.TimeLt right = TimeFilter.lt(20);

    // time > 10 and time < 20
    AndFilter andFilter = new AndFilter(left, right);
    List<TimeRange> timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(19, timeRangeList.get(0).getMax());

    // time > 10 and time <= 20
    andFilter = new AndFilter(left, TimeFilter.ltEq(20));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time >= 10 and time <= 20
    andFilter = new AndFilter(TimeFilter.gtEq(10), TimeFilter.ltEq(20));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time <= 20 and time >= 10
    andFilter = new AndFilter(TimeFilter.ltEq(20), TimeFilter.gtEq(10));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time >= 20 and time <= 10
    andFilter = new AndFilter(TimeFilter.gtEq(20), TimeFilter.ltEq(10));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(0, timeRangeList.size());

    // time >= 20 and time < 20
    andFilter = new AndFilter(TimeFilter.gtEq(20), TimeFilter.lt(20));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(0, timeRangeList.size());
  }

  @Test
  public void testOrTimeFilter() {

    // time < 10 or time > 20
    OrFilter filter = new OrFilter(TimeFilter.lt(10), TimeFilter.gt(20));
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time < 10 or time >= 20
    filter = new OrFilter(TimeFilter.lt(10), TimeFilter.gtEq(20));
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(20, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time <= 10 or time >= 20
    filter = new OrFilter(TimeFilter.ltEq(10), TimeFilter.gtEq(20));
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(20, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time >= 20 or time <= 10
    filter = new OrFilter(TimeFilter.gtEq(20), TimeFilter.ltEq(10));
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(20, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time >= 20 or time >= 10
    filter = new OrFilter(TimeFilter.gtEq(20), TimeFilter.gtEq(10));
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // time < 20 or time <= 10
    filter = new OrFilter(TimeFilter.lt(20), TimeFilter.ltEq(10));
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(19, timeRangeList.get(0).getMax());
  }

  @Test
  public void testAndOrTimeFilter() {

    // (time >= 10 and time <= 20) or (time > 30)
    OrFilter filter =
        new OrFilter(new AndFilter(TimeFilter.gtEq(10), TimeFilter.ltEq(20)), TimeFilter.gt(30));
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());
    assertEquals(31, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // (time <= 10 or time > 20) and (time >= 30)
    AndFilter filter1 =
        new AndFilter(new OrFilter(TimeFilter.ltEq(10), TimeFilter.gt(20)), TimeFilter.gtEq(30));
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(30, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // (time <= 10 or time > 20) and (time <= 30)
    filter1 =
        new AndFilter(new OrFilter(TimeFilter.ltEq(10), TimeFilter.gt(20)), TimeFilter.ltEq(30));
    timeRangeList = filter1.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(30, timeRangeList.get(1).getMax());

    // (time >= 10 and time <= 20) or (time < 99 and time > 30)
    filter =
        new OrFilter(
            new AndFilter(TimeFilter.gtEq(10), TimeFilter.ltEq(20)),
            new AndFilter(TimeFilter.lt(100), TimeFilter.gt(30)));
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());
    assertEquals(31, timeRangeList.get(1).getMin());
    assertEquals(99, timeRangeList.get(1).getMax());
  }

  @Test
  public void testBetweenTimeFilter() {

    // time between 10 and 20
    TimeFilter.TimeBetween filter = TimeFilter.between(10, 20, false);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time not between 10 and 20
    filter = TimeFilter.between(10, 20, true);
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time not between 10 and Long.MAX_VALUE
    filter = TimeFilter.between(10, Long.MAX_VALUE, true);
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());

    // time not between Long.MIN_VALUE and 20
    filter = TimeFilter.between(Long.MIN_VALUE, 20, true);
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(21, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());
  }

  @Test
  public void testNotTimeFilter() {

    // !(time > 10 and time <= 20)
    NotFilter filter = new NotFilter(new AndFilter(TimeFilter.gt(10), TimeFilter.ltEq(20)));
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // !(time > 20 or time <= 10)
    filter = new NotFilter(new OrFilter(TimeFilter.gt(20), TimeFilter.ltEq(10)));
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeEqFilter() {

    // time = 10
    TimeFilter.TimeEq filter = TimeFilter.eq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());

    // !(time = 10)
    NotFilter filter1 = new NotFilter(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(11, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());
  }

  @Test
  public void testTimeNotEqFilter() {

    // time != 10
    TimeFilter.TimeNotEq filter = TimeFilter.notEq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(11, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // !(time != 10)
    NotFilter filter1 = new NotFilter(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeGtFilter() {

    // time > 10
    TimeFilter.TimeGt filter = TimeFilter.gt(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // !(time > 10)
    NotFilter filter1 = new NotFilter(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeGtEqFilter() {

    // time >= 10
    TimeFilter.TimeGtEq filter = TimeFilter.gtEq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // !(time >= 10)
    NotFilter filter1 = new NotFilter(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeLtFilter() {

    // time < 10
    TimeFilter.TimeLt filter = TimeFilter.lt(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());

    // !(time < 10)
    NotFilter filter1 = new NotFilter(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeLtEqFilter() {

    // time <= 10
    TimeFilter.TimeLtEq filter = TimeFilter.ltEq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());

    // !(time <= 10)
    NotFilter filter1 = new NotFilter(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());
  }

  @Test
  public void testGetTimePartitionSlotList() {

    // time >= 10 and time <= 9
    Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
        getTimePartitionSlotList(new AndFilter(TimeFilter.gtEq(10), TimeFilter.ltEq(9)));
    assertTrue(res.left.isEmpty());
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // time >= 10
    res = getTimePartitionSlotList(TimeFilter.gtEq(10));
    assertEquals(1, res.left.size());
    List<TTimePartitionSlot> expected = Collections.singletonList(new TTimePartitionSlot(0));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertTrue(res.right.right);

    // time < 20
    res = getTimePartitionSlotList(TimeFilter.lt(20));
    assertEquals(1, res.left.size());
    expected = Collections.singletonList(new TTimePartitionSlot(0));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertTrue(res.right.left);
    assertFalse(res.right.right);

    // time > 10 and time <= 20
    res = getTimePartitionSlotList(new AndFilter(TimeFilter.gt(10), TimeFilter.ltEq(20)));
    expected = Collections.singletonList(new TTimePartitionSlot(0));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // time > 0 and time <= IoTDBDescriptor.getInstance()
    //                                     .getConfig().getTimePartitionInterval() * 3 + 1
    res =
        getTimePartitionSlotList(
            new AndFilter(
                TimeFilter.gt(0),
                TimeFilter.ltEq(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3 + 1)));
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // time >= IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() - 1 and time <
    // IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1
    res =
        getTimePartitionSlotList(
            new AndFilter(
                TimeFilter.gtEq(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() - 1),
                TimeFilter.lt(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1)));
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // time between IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() - 1 and
    // time < IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()
    res =
        getTimePartitionSlotList(
            TimeFilter.between(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() - 1,
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval(),
                false));
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // time >= IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() and time <=
    // IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1
    res =
        getTimePartitionSlotList(
            new AndFilter(
                TimeFilter.gtEq(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()),
                TimeFilter.ltEq(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1)));
    expected =
        Collections.singletonList(
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // time between IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() and time <=
    // IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1
    res =
        getTimePartitionSlotList(
            TimeFilter.between(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval(),
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1,
                false));
    expected =
        Collections.singletonList(
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // (time >= 10 and time < IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval())
    // or (time > IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() and time <
    // IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 100)
    // or (time > IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 50 and
    // time <= IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 40)
    // or (time > IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 20 and
    // time <= IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3 + 10)
    // or (time > IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5 + 1 and
    // time < IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5 + 10)

    OrFilter orFilter1 =
        new OrFilter(
            new AndFilter(
                TimeFilter.gtEq(10),
                TimeFilter.lt(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval())),
            new AndFilter(
                TimeFilter.gt(IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()),
                TimeFilter.lt(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2
                        - 100)));
    OrFilter orFilter2 =
        new OrFilter(
            orFilter1,
            new AndFilter(
                TimeFilter.gt(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 50),
                TimeFilter.ltEq(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2
                        - 40)));
    OrFilter orFilter3 =
        new OrFilter(
            orFilter2,
            new AndFilter(
                TimeFilter.gt(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 20),
                TimeFilter.ltEq(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3
                        + 10)));
    OrFilter orFilter4 =
        new OrFilter(
            orFilter3,
            new AndFilter(
                TimeFilter.gt(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5 + 1),
                TimeFilter.lt(
                    IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5
                        + 10)));

    res = getTimePartitionSlotList(orFilter4);
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval()),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3),
            new TTimePartitionSlot(
                IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);
  }
}
