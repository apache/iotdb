/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.junit.Assert;
import org.junit.Test;

public class MinTimeMaxTimeFilterTest {

  long minTime = 100;
  long maxTime = 200;

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(10L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.eq(100L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.eq(200L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.eq(300L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.eq(100);
    Assert.assertEquals(true, valueEq.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testGt() {
    Filter timeEq = TimeFilter.gt(10L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gt(100L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gt(200L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gt(300L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.gt(100);
    Assert.assertEquals(true, valueEq.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testGtEq() {
    Filter timeEq = TimeFilter.gtEq(10L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gtEq(100L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gtEq(200L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.gtEq(300L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.gtEq(100);
    Assert.assertEquals(true, valueEq.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testLt() {
    Filter timeEq = TimeFilter.lt(10L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(100L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(200L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.lt(300L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.lt(100);
    Assert.assertEquals(true, valueEq.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testLtEq() {
    Filter timeEq = TimeFilter.ltEq(10L);
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(100L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(200L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    timeEq = TimeFilter.ltEq(300L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilter.ltEq(100);
    Assert.assertEquals(true, valueEq.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testAnd() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(10L), TimeFilter.lt(50));
    Assert.assertEquals(false, andFilter.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testOr() {
    Filter orFilter = FilterFactory.or(TimeFilter.gt(10L), TimeFilter.lt(50));
    Assert.assertEquals(true, orFilter.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testNotEq() {
    Filter timeEq = TimeFilter.notEq(10L);
    Assert.assertEquals(true, timeEq.satisfyStartEndTime(minTime, maxTime));

    long startTime = 10, endTime = 10;
    Assert.assertEquals(false, timeEq.satisfyStartEndTime(startTime, endTime));
  }

  @Test
  public void testNot() {
    Filter not = FilterFactory.not(TimeFilter.ltEq(10L));
    Assert.assertEquals(true, not.satisfyStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(100L));
    Assert.assertEquals(false, not.satisfyStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(200L));
    Assert.assertEquals(false, not.satisfyStartEndTime(minTime, maxTime));

    not = FilterFactory.not(TimeFilter.ltEq(300L));
    Assert.assertEquals(false, not.satisfyStartEndTime(minTime, maxTime));

    not = FilterFactory.not(ValueFilter.ltEq(100));
    Assert.assertEquals(false, not.satisfyStartEndTime(minTime, maxTime));
  }
}
