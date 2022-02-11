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
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import org.junit.Assert;
import org.junit.Test;

public class MinTimeMaxTimeFilterTest {

  long minTime = 100;
  long maxTime = 200;

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.eq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, minTime));

    timeEq = TimeFilter.eq(150L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.eq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.eq(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    UnaryFilter<Long> uTimeEq = TimeFilter.eq(400L);
    uTimeEq.setValue(401L);
    Assert.assertEquals(401L, uTimeEq.getValue().longValue());

    Filter valueEq = ValueFilter.eq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testGt() {
    Filter timeEq = TimeFilter.gt(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gt(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gt(200L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gt(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.gt(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testGtEq() {
    Filter timeEq = TimeFilter.gtEq(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gtEq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gtEq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gtEq(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.gtEq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));

    valueEq = ValueFilter.gtEq(150);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testLt() {
    Filter timeEq = TimeFilter.lt(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(100L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(300L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.lt(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testLtEq() {
    Filter timeEq = TimeFilter.ltEq(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(300L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.ltEq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testAnd() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(10L), TimeFilter.lt(50));
    Assert.assertFalse(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));

    andFilter = FilterFactory.and(TimeFilter.gt(100L), TimeFilter.lt(200));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));

    andFilter = FilterFactory.and(TimeFilter.gt(99L), TimeFilter.lt(201));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(andFilter.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testOr() {
    Filter orFilter = FilterFactory.or(TimeFilter.gt(10L), TimeFilter.lt(50));
    Assert.assertTrue(orFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(orFilter.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testNotEq() {
    Filter timeEq = TimeFilter.notEq(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));

    long startTime = 10, endTime = 10;
    Assert.assertFalse(timeEq.satisfyStartEndTime(startTime, endTime));
    Assert.assertFalse(timeEq.containStartEndTime(5, 50));
  }

  @Test
  public void testNot() {
    Filter not = FilterFactory.not(TimeFilter.ltEq(10L));
    Assert.assertTrue(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(100L));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(200L));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(300L));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(not.containStartEndTime(minTime, maxTime));

    not = FilterFactory.not(ValueFilter.ltEq(100));
    Assert.assertFalse(not.satisfyStartEndTime(minTime, maxTime));
  }
}
