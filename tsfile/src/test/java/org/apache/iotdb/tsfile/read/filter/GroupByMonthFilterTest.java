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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
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
        new GroupByMonthFilter(MS_TO_MONTH, 2 * MS_TO_MONTH, 0, END_TIME, true, true);

    // 1970-01-01 08:00:00
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

    // 1970-12-31 23:59:58
    assertFalse(filter.satisfy(31507198000L, null));

    // 1970-12-31 23:59:59
    assertFalse(filter.satisfy(31507199000L, null));
  }

  /** Test filter with slidingStep = 1 month, and timeInterval = 1 month */
  @Test
  public void TestSatisfy2() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(MS_TO_MONTH, MS_TO_MONTH, 0, END_TIME, true, true);

    // 1970-01-01 08:00:00
    assertTrue(filter.satisfy(0, null));

    // 1970-02-01 07:59:59
    assertTrue(filter.satisfy(2678399000L, null));

    // 1970-02-01 08:00:00
    assertTrue(filter.satisfy(2678400000L, null));

    // 1970-03-01 07:59:59
    assertTrue(filter.satisfy(5097599000L, null));

    // 1970-03-01 08:00:00
    assertTrue(filter.satisfy(5097600000L, null));

    // 1970-12-31 23:59:58
    assertTrue(filter.satisfy(31507198000L, null));

    // 1970-12-31 23:59:59
    assertFalse(filter.satisfy(31507199000L, null));
  }

  /** Test filter with slidingStep = 1 month, and timeInterval = 1 day */
  @Test
  public void TestSatisfy3() {
    GroupByMonthFilter filter =
        new GroupByMonthFilter(MS_TO_DAY, MS_TO_MONTH, 0, END_TIME, true, false);

    // 1970-01-01 08:00:00
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
}
