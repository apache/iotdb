/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TimeRangeIteratorFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.TimeDuration;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;

public class TimeRangeIteratorTest {
  private static final long MS_TO_DAY = 86400_000L;

  @Test
  public void testNotSplitTimeRange() {
    String[] res = {
      "[ 0 : 3 ]",
      "[ 3 : 6 ]",
      "[ 6 : 9 ]",
      "[ 9 : 12 ]",
      "[ 12 : 15 ]",
      "[ 15 : 18 ]",
      "[ 18 : 21 ]",
      "[ 21 : 24 ]",
      "[ 24 : 27 ]",
      "[ 27 : 30 ]",
      "[ 30 : 31 ]"
    };

    long startTime = 0, endTime = 32;
    TimeDuration interval = new TimeDuration(0, 4), slidingStep = new TimeDuration(0, 3);

    ITimeRangeIterator timeRangeIterator =
        TimeRangeIteratorFactory.getTimeRangeIterator(
            startTime, endTime, interval, slidingStep, true, true, false);

    checkRes(timeRangeIterator, res);

    ITimeRangeIterator descTimeRangeIterator =
        TimeRangeIteratorFactory.getTimeRangeIterator(
            startTime, endTime, interval, slidingStep, false, true, false);

    checkRes(descTimeRangeIterator, res);
  }

  @Test
  public void testSplitTimeRange() {
    String[] res4_1 = {
      "[ 0 : 0 ]",
      "[ 1 : 1 ]",
      "[ 2 : 2 ]",
      "[ 3 : 3 ]",
      "[ 4 : 4 ]",
      "[ 5 : 5 ]",
      "[ 6 : 6 ]",
      "[ 7 : 7 ]",
      "[ 8 : 8 ]",
      "[ 9 : 9 ]",
      "[ 10 : 10 ]",
      "[ 11 : 11 ]",
      "[ 12 : 12 ]",
      "[ 13 : 13 ]",
      "[ 14 : 14 ]",
      "[ 15 : 15 ]",
      "[ 16 : 16 ]",
      "[ 17 : 17 ]",
      "[ 18 : 18 ]",
      "[ 19 : 19 ]",
      "[ 20 : 20 ]",
      "[ 21 : 21 ]",
      "[ 22 : 22 ]",
      "[ 23 : 23 ]",
      "[ 24 : 24 ]",
      "[ 25 : 25 ]",
      "[ 26 : 26 ]",
      "[ 27 : 27 ]",
      "[ 28 : 28 ]",
      "[ 29 : 29 ]",
      "[ 30 : 30 ]",
      "[ 31 : 31 ]"
    };
    String[] res4_2 = {
      "[ 0 : 1 ]",
      "[ 2 : 3 ]",
      "[ 4 : 5 ]",
      "[ 6 : 7 ]",
      "[ 8 : 9 ]",
      "[ 10 : 11 ]",
      "[ 12 : 13 ]",
      "[ 14 : 15 ]",
      "[ 16 : 17 ]",
      "[ 18 : 19 ]",
      "[ 20 : 21 ]",
      "[ 22 : 23 ]",
      "[ 24 : 25 ]",
      "[ 26 : 27 ]",
      "[ 28 : 29 ]",
      "[ 30 : 31 ]"
    };
    String[] res4_3 = {
      "[ 0 : 0 ]",
      "[ 1 : 2 ]",
      "[ 3 : 3 ]",
      "[ 4 : 5 ]",
      "[ 6 : 6 ]",
      "[ 7 : 8 ]",
      "[ 9 : 9 ]",
      "[ 10 : 11 ]",
      "[ 12 : 12 ]",
      "[ 13 : 14 ]",
      "[ 15 : 15 ]",
      "[ 16 : 17 ]",
      "[ 18 : 18 ]",
      "[ 19 : 20 ]",
      "[ 21 : 21 ]",
      "[ 22 : 23 ]",
      "[ 24 : 24 ]",
      "[ 25 : 26 ]",
      "[ 27 : 27 ]",
      "[ 28 : 29 ]",
      "[ 30 : 30 ]",
      "[ 31 : 31 ]"
    };
    String[] res4_4 = {
      "[ 0 : 3 ]",
      "[ 4 : 7 ]",
      "[ 8 : 11 ]",
      "[ 12 : 15 ]",
      "[ 16 : 19 ]",
      "[ 20 : 23 ]",
      "[ 24 : 27 ]",
      "[ 28 : 31 ]"
    };
    String[] res4_5 = {
      "[ 0 : 3 ]",
      "[ 5 : 8 ]",
      "[ 10 : 13 ]",
      "[ 15 : 18 ]",
      "[ 20 : 23 ]",
      "[ 25 : 28 ]",
      "[ 30 : 31 ]"
    };
    String[] res4_6 = {
      "[ 0 : 3 ]", "[ 6 : 9 ]", "[ 12 : 15 ]", "[ 18 : 21 ]", "[ 24 : 27 ]", "[ 30 : 31 ]"
    };

    TimeDuration interval = new TimeDuration(0, 4);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 1), true, true, true),
        res4_1);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 2), true, true, true),
        res4_2);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 3), true, true, true),
        res4_3);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 4), true, true, true),
        res4_4);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 5), true, true, true),
        res4_5);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 6), true, true, true),
        res4_6);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 1), false, true, true),
        res4_1);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 2), false, true, true),
        res4_2);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 3), false, true, true),
        res4_3);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 4), false, true, true),
        res4_4);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 5), false, true, true),
        res4_5);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            0, 32, interval, new TimeDuration(0, 6), false, true, true),
        res4_6);
  }

  @Test
  public void testNaturalMonthTimeRange() {
    String[] res1 = {
      "[ 1604102400000 : 1606694399999 ]",
      "[ 1606694400000 : 1609372799999 ]",
      "[ 1609372800000 : 1612051199999 ]",
      "[ 1612051200000 : 1614470399999 ]",
      "[ 1614470400000 : 1617148799999 ]"
    };
    String[] res2 = {
      "[ 1604102400000 : 1604966399999 ]",
      "[ 1606694400000 : 1607558399999 ]",
      "[ 1609372800000 : 1610236799999 ]",
      "[ 1612051200000 : 1612915199999 ]",
      "[ 1614470400000 : 1615334399999 ]"
    };
    String[] res3 = {
      "[ 1604102400000 : 1606694399999 ]",
      "[ 1604966400000 : 1607558399999 ]",
      "[ 1605830400000 : 1608422399999 ]",
      "[ 1606694400000 : 1609372799999 ]",
      "[ 1607558400000 : 1610236799999 ]",
      "[ 1608422400000 : 1611100799999 ]",
      "[ 1609286400000 : 1611964799999 ]",
      "[ 1610150400000 : 1612828799999 ]",
      "[ 1611014400000 : 1613692799999 ]",
      "[ 1611878400000 : 1614470399999 ]",
      "[ 1612742400000 : 1615161599999 ]",
      "[ 1613606400000 : 1616025599999 ]",
      "[ 1614470400000 : 1617148799999 ]",
      "[ 1615334400000 : 1617148799999 ]",
      "[ 1616198400000 : 1617148799999 ]",
      "[ 1617062400000 : 1617148799999 ]"
    };
    String[] res4 = {
      "[ 1604102400000 : 1604966399999 ]",
      "[ 1604966400000 : 1605830399999 ]",
      "[ 1605830400000 : 1606694399999 ]",
      "[ 1606694400000 : 1607558399999 ]",
      "[ 1607558400000 : 1608422399999 ]",
      "[ 1608422400000 : 1609286399999 ]",
      "[ 1609286400000 : 1609372799999 ]",
      "[ 1609372800000 : 1610150399999 ]",
      "[ 1610150400000 : 1610236799999 ]",
      "[ 1610236800000 : 1611014399999 ]",
      "[ 1611014400000 : 1611100799999 ]",
      "[ 1611100800000 : 1611878399999 ]",
      "[ 1611878400000 : 1611964799999 ]",
      "[ 1611964800000 : 1612742399999 ]",
      "[ 1612742400000 : 1612828799999 ]",
      "[ 1612828800000 : 1613606399999 ]",
      "[ 1613606400000 : 1613692799999 ]",
      "[ 1613692800000 : 1614470399999 ]",
      "[ 1614470400000 : 1615161599999 ]",
      "[ 1615161600000 : 1615334399999 ]",
      "[ 1615334400000 : 1616025599999 ]",
      "[ 1616025600000 : 1616198399999 ]",
      "[ 1616198400000 : 1617062399999 ]",
      "[ 1617062400000 : 1617148799999 ]"
    };
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            1604102400000L,
            1617148800000L,
            new TimeDuration(1, 0),
            new TimeDuration(1, 0),
            true,
            true,
            false),
        res1);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            1604102400000L,
            1617148800000L,
            new TimeDuration(1, 0),
            new TimeDuration(1, 0),
            true,
            true,
            true),
        res1);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            1604102400000L,
            1617148800000L,
            new TimeDuration(0, 10 * MS_TO_DAY),
            new TimeDuration(1, 0),
            true,
            true,
            false),
        res2);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            1604102400000L,
            1617148800000L,
            new TimeDuration(0, 10 * MS_TO_DAY),
            new TimeDuration(1, 0),
            true,
            true,
            true),
        res2);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            1604102400000L,
            1617148800000L,
            new TimeDuration(1, 0),
            new TimeDuration(0, 10 * MS_TO_DAY),
            true,
            true,
            false),
        res3);
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            1604102400000L,
            1617148800000L,
            new TimeDuration(1, 0),
            new TimeDuration(0, 10 * MS_TO_DAY),
            true,
            true,
            true),
        res4);
  }

  @Test()
  @Ignore("This test is failing and needs fixing (Possibly related to time-zones ... " +
          "It's failing in CET and the difference between expected and actual is exactly one hour)")
  public void testMixedUnit() {
    String[] res =
        new String[] {
          "[ "
              + Timestamp.valueOf("2023-01-28 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-03-01 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-03-01 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-04-02 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-04-02 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-05-03 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-05-03 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-05-29 00:00:00").getTime() - 1L)
              + " ]"
        };
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            Timestamp.valueOf("2023-01-28 00:00:00").getTime(),
            Timestamp.valueOf("2023-05-29 00:00:00").getTime(),
            new TimeDuration(1, MS_TO_DAY),
            new TimeDuration(1, MS_TO_DAY),
            true,
            true,
            true),
        res);

    res =
        new String[] {
          "[ "
              + Timestamp.valueOf("2023-01-28 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-03-01 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-03-01 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-03-02 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-03-02 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-04-02 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-04-02 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-04-03 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-04-03 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-05-03 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-05-03 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-05-04 00:00:00").getTime() - 1L)
              + " ]",
          "[ "
              + Timestamp.valueOf("2023-05-04 00:00:00").getTime()
              + " : "
              + (Timestamp.valueOf("2023-05-29 00:00:00").getTime() - 1L)
              + " ]",
        };
    // slidingStep has overlap with interval
    checkRes(
        TimeRangeIteratorFactory.getTimeRangeIterator(
            Timestamp.valueOf("2023-01-28 00:00:00").getTime(),
            Timestamp.valueOf("2023-05-29 00:00:00").getTime(),
            new TimeDuration(1, 2 * MS_TO_DAY),
            new TimeDuration(1, MS_TO_DAY),
            true,
            true,
            true),
        res);
  }

  private void checkRes(ITimeRangeIterator timeRangeIterator, String[] res) {
    Assert.assertEquals(res.length, timeRangeIterator.getTotalIntervalNum());

    boolean isAscending = timeRangeIterator.isAscending();
    int cnt = isAscending ? 0 : res.length - 1;

    // test next time ranges
    while (timeRangeIterator.hasNextTimeRange()) {
      TimeRange curTimeRange = timeRangeIterator.nextTimeRange();
      Assert.assertEquals(res[cnt], curTimeRange.toString());
      cnt += isAscending ? 1 : -1;
    }
  }
}
