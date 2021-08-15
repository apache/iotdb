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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.junit.Test;

import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class GroupByMonthFilterTest {

  // The number of milliseconds in 30 days
  private final long MS_TO_DAY = 86400_000L;
  private final long MS_TO_MONTH = 30 * MS_TO_DAY;
  // 1970-12-31 23:59:59
  private final long END_TIME = 31507199000L;

  /** Test filter with slidingStep = 2 month, and timeInterval = 1 month */
  @Test
  public void TestSatisfy1() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(
            MS_TO_MONTH, 2 * MS_TO_MONTH, 0, END_TIME, true, true, TimeZone.getTimeZone("+08:00"));

    // 1970-01-01 08:00:00, timezone = GMT+08:00
    assertTrue(filter.satisfy(0, null));

    // 1970-02-01 07:59:59
    assertTrue(filter.satisfy(2678399000L, null));

    // 1970-02-01 08:00:00
    assertFalse(filter.satisfy(2678400000L, null));

    // 1970-03-01 07:59:59
    assertFalse(filter.satisfy(5097599000L, null));

    // 1970-03-01 08:00:00
    assertTrue(filter.satisfy(5097600000L, null));

    // 1970-04-05 00:00:00
    assertFalse(filter.satisfy(8092800000L, null));

    // 1970-07-01 07:59:59
    assertFalse(filter.satisfy(15638399000L, null));

    // 1970-11-30 23:59:59
    assertTrue(filter.satisfy(28828799000L, null));

    // 1970-12-31 23:59:59
    assertFalse(filter.satisfy(31507199000L, null));
  }

  /** Test filter with slidingStep = 1 month, and timeInterval = 1 month */
  @Test
  public void TestSatisfy2() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(
            MS_TO_MONTH, MS_TO_MONTH, 0, END_TIME, true, true, TimeZone.getTimeZone("+08:00"));

    // 1970-01-01 08:00:00, timezone = GMT+08:00
    assertTrue(filter.satisfy(0, null));

    // 1970-02-01 07:59:59
    assertTrue(filter.satisfy(2678399000L, null));

    // 1970-02-01 08:00:00
    assertTrue(filter.satisfy(2678400000L, null));

    // 1970-03-01 07:59:59
    assertTrue(filter.satisfy(5097599000L, null));

    // 1970-03-01 08:00:00
    assertTrue(filter.satisfy(5097600000L, null));

    // 1970-12-30 08:00:00
    assertTrue(filter.satisfy(31363200000L, null));

    // 1970-12-31 23:59:58
    assertTrue(filter.satisfy(31507198000L, null));

    // 1970-12-31 23:59:59
    assertFalse(filter.satisfy(31507199000L, null));
  }

  /** Test filter with slidingStep = 1 month, and timeInterval = 1 day */
  @Test
  public void TestSatisfy3() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(
            MS_TO_DAY, MS_TO_MONTH, 0, END_TIME, true, false, TimeZone.getTimeZone("+08:00"));

    // 1970-01-01 08:00:00, timezone = GMT+08:00
    assertTrue(filter.satisfy(0, null));

    // 1970-01-02 07:59:59
    assertTrue(filter.satisfy(86399000L, null));

    // 1970-01-02 08:00:00
    assertFalse(filter.satisfy(86400000L, null));

    // 1970-02-01 07:59:59
    assertFalse(filter.satisfy(2678399000L, null));

    // 1970-02-01 08:00:00
    assertTrue(filter.satisfy(2678400000L, null));

    // 1970-03-01 08:00:00
    assertTrue(filter.satisfy(5097600000L, null));

    // 1970-12-01 08:00:00
    assertTrue(filter.satisfy(28857600000L, null));

    // 1970-12-31 23:59:59
    assertFalse(filter.satisfy(31507199000L, null));
  }

  /** Test filter with slidingStep = 100 days, and timeInterval = 1 mo */
  @Test
  public void TestSatisfy4() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(
            MS_TO_MONTH, MS_TO_DAY * 100, 0, END_TIME, false, true, TimeZone.getTimeZone("+08:00"));

    // 1970-01-01 08:00:00, timezone = GMT+08:00
    assertTrue(filter.satisfy(0, null));

    // 1970-02-01 07:59:59
    assertTrue(filter.satisfy(2678399000L, null));

    // 1970-03-01 08:00:00
    assertFalse(filter.satisfy(5097600000L, null));

    // 1970-05-01 08:00:00
    assertTrue(filter.satisfy(10368000000L, null));
  }

  /** Test filter with slidingStep = 1 month, and timeInterval = 1 day */
  @Test
  public void TestSatisfyStartEndTime() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(
            MS_TO_DAY, MS_TO_MONTH, 0, END_TIME, true, false, TimeZone.getTimeZone("+08:00"));

    // 1970-01-01 08:00:00 - 1970-01-02 08:00:00, timezone = GMT+08:00
    Statistics statistics = new LongStatistics();
    statistics.setStartTime(0);
    statistics.setEndTime(MS_TO_DAY);
    assertTrue(filter.satisfy(statistics));

    // 1970-01-01 20:00:00 - 1970-01-02 08:00:00
    statistics.setStartTime(MS_TO_DAY / 2);
    statistics.setEndTime(MS_TO_DAY);
    assertTrue(filter.satisfy(statistics));

    // 1970-01-01 20:00:00 - 1970-01-03 08:00:00
    statistics.setStartTime(MS_TO_DAY / 2);
    statistics.setEndTime(MS_TO_DAY * 2);
    assertTrue(filter.satisfy(statistics));

    // 1970-01-02 08:00:00 - 1970-01-03 08:00:00
    statistics.setStartTime(MS_TO_DAY);
    statistics.setEndTime(MS_TO_DAY * 2);
    assertFalse(filter.satisfy(statistics));

    // 1970-02-28 08:00:00 - 1970-03-01 07:59:59
    statistics.setStartTime(5011200000L);
    statistics.setEndTime(5097599000L);
    assertFalse(filter.satisfy(statistics));

    // 1970-03-01 09:00:00 - 1970-03-01 10:00:00
    statistics.setStartTime(5101200000L);
    statistics.setEndTime(5104800000L);
    assertTrue(filter.satisfy(statistics));

    // 1970-05-01 07:00:00 - 1970-05-01 08:00:00
    statistics.setStartTime(10364400000L);
    statistics.setEndTime(10368000000L);
    assertTrue(filter.satisfy(statistics));

    // 1970-01-02 07:59:59
    assertTrue(filter.satisfy(86399000L, null));
  }

  /** Test filter with slidingStep = 1 month, and timeInterval = 1 day */
  @Test
  public void TestContainStartEndTime() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(
            MS_TO_DAY, MS_TO_MONTH, 0, END_TIME, true, false, TimeZone.getTimeZone("+08:00"));

    // 1970-01-01 08:00:00 - 1970-01-02 08:00:00, timezone = GMT+08:00
    assertFalse(filter.containStartEndTime(0, MS_TO_DAY));

    // 1970-01-01 08:00:00 - 1970-01-02 07:59:59
    assertTrue(filter.containStartEndTime(0, MS_TO_DAY - 1000));

    // 1970-02-01 07:59:59 - 1970-02-02 07:59:59
    assertFalse(filter.containStartEndTime(2678399000L, 2764799000L));

    // 1970-02-01 08:00:00 - 1970-02-02 07:59:59
    assertTrue(filter.containStartEndTime(2678400000L, 2764799000L));

    // 1970-02-01 08:00:00 - 1970-02-02 08:00:00
    assertFalse(filter.containStartEndTime(2678400000L, 2764800000L));

    // 1970-02-10 08:00:00 - 1970-02-11 08:00:00
    assertFalse(filter.containStartEndTime(3456000000L, 3542400000L));

    // 1970-10-01 10:00:00 - 1970-10-01 12:00:00
    assertTrue(filter.containStartEndTime(23594400000L, 23601600000L));

    // 1970-05-01 08:00:00 - 1970-05-01 10:00:00
    assertTrue(filter.containStartEndTime(10368000000L, 10375200000L));

    // 1970-03-01 08:00:00 - 1970-05-01 10:00:00
    assertFalse(filter.containStartEndTime(5097600000L, 10375200000L));

    // 1970-01-02 07:59:59
    assertTrue(filter.satisfy(86399000L, null));
  }

  @Test
  public void TestEquals() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(
            MS_TO_DAY, MS_TO_MONTH, 0, END_TIME, true, false, TimeZone.getTimeZone("+08:00"));
    Filter filter2 = filter.copy();
    assertEquals(filter, filter2);
    GroupByMonthFilter filter3 =
        new GroupByMonthFilter(
            MS_TO_MONTH, MS_TO_MONTH, 0, END_TIME, true, true, TimeZone.getTimeZone("+08:00"));
    assertNotEquals(filter, filter3);
  }
}
