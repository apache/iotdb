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
package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryTimePartitionTest {

  @Test
  public void testAndTimeFilter() {
    Filter left = TimeFilterApi.gt(10);
    Filter right = TimeFilterApi.lt(20);

    // time > 10 and time < 20
    Filter andFilter = FilterFactory.and(left, right);
    List<TimeRange> timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(19, timeRangeList.get(0).getMax());

    // time > 10 and time <= 20
    andFilter = FilterFactory.and(left, TimeFilterApi.ltEq(20));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time >= 10 and time <= 20
    andFilter = FilterFactory.and(TimeFilterApi.gtEq(10), TimeFilterApi.ltEq(20));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time <= 20 and time >= 10
    andFilter = FilterFactory.and(TimeFilterApi.ltEq(20), TimeFilterApi.gtEq(10));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time >= 20 and time <= 10
    andFilter = FilterFactory.and(TimeFilterApi.gtEq(20), TimeFilterApi.ltEq(10));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(0, timeRangeList.size());

    // time >= 20 and time < 20
    andFilter = FilterFactory.and(TimeFilterApi.gtEq(20), TimeFilterApi.lt(20));
    timeRangeList = andFilter.getTimeRanges();
    assertEquals(0, timeRangeList.size());
  }

  @Test
  public void testOrTimeFilter() {

    // time < 10 or time > 20
    Filter filter = FilterFactory.or(TimeFilterApi.lt(10), TimeFilterApi.gt(20));
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time < 10 or time >= 20
    filter = FilterFactory.or(TimeFilterApi.lt(10), TimeFilterApi.gtEq(20));
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(20, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time <= 10 or time >= 20
    filter = FilterFactory.or(TimeFilterApi.ltEq(10), TimeFilterApi.gtEq(20));
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(20, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time >= 20 or time <= 10
    filter = FilterFactory.or(TimeFilterApi.gtEq(20), TimeFilterApi.ltEq(10));
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(20, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time >= 20 or time >= 10
    filter = FilterFactory.or(TimeFilterApi.gtEq(20), TimeFilterApi.gtEq(10));
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // time < 20 or time <= 10
    filter = FilterFactory.or(TimeFilterApi.lt(20), TimeFilterApi.ltEq(10));
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(19, timeRangeList.get(0).getMax());
  }

  @Test
  public void testAndOrTimeFilter() {

    // (time >= 10 and time <= 20) or (time > 30)
    Filter filter =
        FilterFactory.or(
            FilterFactory.and(TimeFilterApi.gtEq(10), TimeFilterApi.ltEq(20)),
            TimeFilterApi.gt(30));
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());
    assertEquals(31, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // (time <= 10 or time > 20) and (time >= 30)
    Filter filter1 =
        FilterFactory.and(
            FilterFactory.or(TimeFilterApi.ltEq(10), TimeFilterApi.gt(20)), TimeFilterApi.gtEq(30));
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(30, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // (time <= 10 or time > 20) and (time <= 30)
    filter1 =
        FilterFactory.and(
            FilterFactory.or(TimeFilterApi.ltEq(10), TimeFilterApi.gt(20)), TimeFilterApi.ltEq(30));
    timeRangeList = filter1.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(30, timeRangeList.get(1).getMax());

    // (time >= 10 and time <= 20) or (time < 99 and time > 30)
    filter =
        FilterFactory.or(
            FilterFactory.and(TimeFilterApi.gtEq(10), TimeFilterApi.ltEq(20)),
            FilterFactory.and(TimeFilterApi.lt(100), TimeFilterApi.gt(30)));
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
    Filter filter = TimeFilterApi.between(10, 20);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());

    // time not between 10 and 20
    filter = TimeFilterApi.notBetween(10, 20);
    timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // time not between 10 and Long.MAX_VALUE
    filter = TimeFilterApi.notBetween(10, Long.MAX_VALUE);
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());

    // time not between Long.MIN_VALUE and 20
    filter = TimeFilterApi.notBetween(Long.MIN_VALUE, 20);
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(21, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());
  }

  @Test
  public void testNotTimeFilter() {

    // !(time > 10 and time <= 20)
    Filter filter =
        FilterFactory.not(FilterFactory.and(TimeFilterApi.gt(10), TimeFilterApi.ltEq(20)));
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
    assertEquals(21, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // !(time > 20 or time <= 10)
    filter = FilterFactory.not(FilterFactory.or(TimeFilterApi.gt(20), TimeFilterApi.ltEq(10)));
    timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(20, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeEqFilter() {

    // time = 10
    Filter filter = TimeFilterApi.eq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());

    // !(time = 10)
    Filter filter1 = FilterFactory.not(filter);
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
    Filter filter = TimeFilterApi.notEq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(2, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
    assertEquals(11, timeRangeList.get(1).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(1).getMax());

    // !(time != 10)
    Filter filter1 = FilterFactory.not(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeGtFilter() {

    // time > 10
    Filter filter = TimeFilterApi.gt(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // !(time > 10)
    Filter filter1 = FilterFactory.not(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeGtEqFilter() {

    // time >= 10
    Filter filter = TimeFilterApi.gtEq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());

    // !(time >= 10)
    Filter filter1 = FilterFactory.not(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeLtFilter() {

    // time < 10
    Filter filter = TimeFilterApi.lt(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(9, timeRangeList.get(0).getMax());

    // !(time < 10)
    Filter filter1 = FilterFactory.not(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(10, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());
  }

  @Test
  public void testTimeLtEqFilter() {

    // time <= 10
    Filter filter = TimeFilterApi.ltEq(10);
    List<TimeRange> timeRangeList = filter.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(Long.MIN_VALUE, timeRangeList.get(0).getMin());
    assertEquals(10, timeRangeList.get(0).getMax());

    // !(time <= 10)
    Filter filter1 = FilterFactory.not(filter);
    timeRangeList = filter1.getTimeRanges();
    assertEquals(1, timeRangeList.size());
    assertEquals(11, timeRangeList.get(0).getMin());
    assertEquals(Long.MAX_VALUE, timeRangeList.get(0).getMax());
  }

  @Test
  public void testGetTimePartitionSlotList() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("time_partition_test"));
    // time >= 10 and time <= 9
    Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
        getTimePartitionSlotList(
            FilterFactory.and(TimeFilterApi.gtEq(10), TimeFilterApi.ltEq(9)), context);
    assertTrue(res.left.isEmpty());
    assertFalse(res.right.left);
    assertFalse(res.right.right);

    // time >= 10
    res = getTimePartitionSlotList(TimeFilterApi.gtEq(10), context);
    assertEquals(1, res.left.size());
    List<TTimePartitionSlot> expected = Collections.singletonList(new TTimePartitionSlot(0));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertTrue(res.right.right);

    // time < 20
    res = getTimePartitionSlotList(TimeFilterApi.lt(20), context);
    assertEquals(1, res.left.size());
    expected = Collections.singletonList(new TTimePartitionSlot(0));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertTrue(res.right.left);
    assertFalse(res.right.right);

    // time > 10 and time <= 20
    res =
        getTimePartitionSlotList(
            FilterFactory.and(TimeFilterApi.gt(10), TimeFilterApi.ltEq(20)), context);
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
            FilterFactory.and(
                TimeFilterApi.gt(0),
                TimeFilterApi.ltEq(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3 + 1)),
            context);
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3));
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
            FilterFactory.and(
                TimeFilterApi.gtEq(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() - 1),
                TimeFilterApi.lt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1)),
            context);
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()));
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
            TimeFilterApi.between(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() - 1,
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()),
            context);
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()));
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
            FilterFactory.and(
                TimeFilterApi.gtEq(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()),
                TimeFilterApi.ltEq(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1)),
            context);
    expected =
        Collections.singletonList(
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()));
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
            TimeFilterApi.between(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval(),
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() + 1),
            context);
    expected =
        Collections.singletonList(
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()));
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

    Filter orFilter1 =
        FilterFactory.or(
            FilterFactory.and(
                TimeFilterApi.gtEq(10),
                TimeFilterApi.lt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval())),
            FilterFactory.and(
                TimeFilterApi.gt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()),
                TimeFilterApi.lt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2
                        - 100)));
    Filter orFilter2 =
        FilterFactory.or(
            orFilter1,
            FilterFactory.and(
                TimeFilterApi.gt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 50),
                TimeFilterApi.ltEq(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2
                        - 40)));
    Filter orFilter3 =
        FilterFactory.or(
            orFilter2,
            FilterFactory.and(
                TimeFilterApi.gt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2 - 20),
                TimeFilterApi.ltEq(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3
                        + 10)));
    Filter orFilter4 =
        FilterFactory.or(
            orFilter3,
            FilterFactory.and(
                TimeFilterApi.gt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5 + 1),
                TimeFilterApi.lt(
                    CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5
                        + 10)));

    res = getTimePartitionSlotList(orFilter4, context);
    expected =
        Arrays.asList(
            new TTimePartitionSlot(0),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval()),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 2),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 3),
            new TTimePartitionSlot(
                CommonDescriptor.getInstance().getConfig().getTimePartitionInterval() * 5));
    assertEquals(expected.size(), res.left.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), res.left.get(i));
    }
    assertFalse(res.right.left);
    assertFalse(res.right.right);
  }
}
