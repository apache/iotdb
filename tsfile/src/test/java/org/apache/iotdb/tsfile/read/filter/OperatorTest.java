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

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

public class OperatorTest {

  private static final long EFFICIENCY_TEST_COUNT = 10000000;
  private static final long TESTED_TIMESTAMP = 1513585371L;

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(100L);
    Assert.assertTrue(timeEq.satisfy(100, 100));
    Assert.assertFalse(timeEq.satisfy(101, 100));

    Filter filter2 = FilterFactory.and(TimeFilter.eq(100L), ValueFilter.eq(50));
    Assert.assertTrue(filter2.satisfy(100, 50));
    Assert.assertFalse(filter2.satisfy(100, 51));

    Filter filter3 = ValueFilter.eq(true);
    Assert.assertTrue(filter3.satisfy(100, true));
    Assert.assertFalse(filter3.satisfy(100, false));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilter.gt(TESTED_TIMESTAMP);
    Assert.assertTrue(timeGt.satisfy(TESTED_TIMESTAMP + 1, 100));
    Assert.assertFalse(timeGt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeGt.satisfy(TESTED_TIMESTAMP - 1, 100));

    Filter valueGt = ValueFilter.gt(0.01f);
    Assert.assertTrue(valueGt.satisfy(TESTED_TIMESTAMP, 0.02f));
    Assert.assertFalse(valueGt.satisfy(TESTED_TIMESTAMP, 0.01f));
    Assert.assertFalse(valueGt.satisfy(TESTED_TIMESTAMP, -0.01f));

    Filter binaryFilter = ValueFilter.gt(new Binary("test1"));
    Assert.assertTrue(binaryFilter.satisfy(TESTED_TIMESTAMP, new Binary("test2")));
    Assert.assertFalse(binaryFilter.satisfy(TESTED_TIMESTAMP, new Binary("test0")));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilter.gtEq(TESTED_TIMESTAMP);
    Assert.assertTrue(timeGtEq.satisfy(TESTED_TIMESTAMP + 1, 100));
    Assert.assertTrue(timeGtEq.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeGtEq.satisfy(TESTED_TIMESTAMP - 1, 100));

    Filter valueGtEq = ValueFilter.gtEq(0.01);
    Assert.assertTrue(valueGtEq.satisfy(TESTED_TIMESTAMP, 0.02));
    Assert.assertTrue(valueGtEq.satisfy(TESTED_TIMESTAMP, 0.01));
    Assert.assertFalse(valueGtEq.satisfy(TESTED_TIMESTAMP, -0.01));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilter.lt(TESTED_TIMESTAMP);
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLt = ValueFilter.lt(100L);
    Assert.assertTrue(valueLt.satisfy(TESTED_TIMESTAMP, 99L));
    Assert.assertFalse(valueLt.satisfy(TESTED_TIMESTAMP, 100L));
    Assert.assertFalse(valueLt.satisfy(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilter.ltEq(TESTED_TIMESTAMP);
    Assert.assertTrue(timeLtEq.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertTrue(timeLtEq.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeLtEq.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLtEq = ValueFilter.ltEq(100L);
    Assert.assertTrue(valueLtEq.satisfy(TESTED_TIMESTAMP, 99L));
    Assert.assertTrue(valueLtEq.satisfy(TESTED_TIMESTAMP, 100L));
    Assert.assertFalse(valueLtEq.satisfy(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testNot() {
    Filter timeLt = TimeFilter.not(TimeFilter.lt(TESTED_TIMESTAMP));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLt = ValueFilter.not(ValueFilter.lt(100L));
    Assert.assertFalse(valueLt.satisfy(TESTED_TIMESTAMP, 99L));
    Assert.assertTrue(valueLt.satisfy(TESTED_TIMESTAMP, 100L));
    Assert.assertTrue(valueLt.satisfy(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testNotEq() {
    Filter timeNotEq = TimeFilter.notEq(100L);
    Assert.assertFalse(timeNotEq.satisfy(100, 100));
    Assert.assertTrue(timeNotEq.satisfy(101, 100));

    Filter valueNotEq = ValueFilter.notEq(50);
    Assert.assertFalse(valueNotEq.satisfy(100, 50));
    Assert.assertTrue(valueNotEq.satisfy(100, 51));
  }

  @Test
  public void testAndOr() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
    Assert.assertTrue(andFilter.satisfy(101L, 50d));
    Assert.assertFalse(andFilter.satisfy(101L, 60d));
    Assert.assertFalse(andFilter.satisfy(99L, 50d));

    Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));
    Assert.assertTrue(orFilter.satisfy(101L, 50d));
    Assert.assertFalse(orFilter.satisfy(101L, 60d));
    Assert.assertTrue(orFilter.satisfy(1000L, 50d));

    Filter andFilter2 = FilterFactory.and(orFilter, ValueFilter.notEq(50.0));
    Assert.assertFalse(andFilter2.satisfy(101L, 50d));
    Assert.assertFalse(andFilter2.satisfy(101L, 60d));
    Assert.assertTrue(andFilter2.satisfy(1000L, 51d));
  }

  @Test
  public void testWrongUsage() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(true));
    try {
      andFilter.satisfy(101L, 50);
      Assert.fail();
    } catch (ClassCastException e) {

    }
  }

  @Test
  public void efficiencyTest() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
    Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));

    long startTime = System.currentTimeMillis();
    for (long i = 0; i < EFFICIENCY_TEST_COUNT; i++) {
      orFilter.satisfy(i, i + 0.1);
    }
    long endTime = System.currentTimeMillis();
    System.out.println(
        "EfficiencyTest for Filter: \n\tFilter Expression = "
            + orFilter
            + "\n\tCOUNT = "
            + EFFICIENCY_TEST_COUNT
            + "\n\tTotal Time = "
            + (endTime - startTime)
            + "ms.");
  }
}
