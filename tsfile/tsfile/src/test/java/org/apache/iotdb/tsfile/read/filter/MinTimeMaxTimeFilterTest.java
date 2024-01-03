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
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilterApi;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.iotdb.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;
import static org.junit.Assert.fail;

public class MinTimeMaxTimeFilterTest {

  long minTime = 100;
  long maxTime = 200;

  @Test
  public void testEq() {
    Filter timeEq = TimeFilterApi.eq(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.eq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, minTime));

    timeEq = TimeFilterApi.eq(150L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.eq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.eq(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilterApi.eq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
  }

  @Test
  public void testGt() {
    Filter timeEq = TimeFilterApi.gt(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.gt(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.gt(200L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.gt(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilterApi.gt(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testGtEq() {
    Filter timeEq = TimeFilterApi.gtEq(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.gtEq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.gtEq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.gtEq(300L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilterApi.gtEq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(valueEq.containStartEndTime(minTime, maxTime));

    valueEq = ValueFilterApi.gtEq(150);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testLt() {
    Filter timeEq = TimeFilterApi.lt(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.lt(100L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.lt(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.lt(300L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilterApi.lt(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testLtEq() {
    Filter timeEq = TimeFilterApi.ltEq(10L);
    Assert.assertFalse(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.ltEq(100L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.ltEq(200L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    timeEq = TimeFilterApi.ltEq(300L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.containStartEndTime(minTime, maxTime));

    Filter valueEq = ValueFilterApi.ltEq(100);
    Assert.assertTrue(valueEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(valueEq.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testAnd() {
    Filter andFilter = FilterFactory.and(TimeFilterApi.gt(10L), TimeFilterApi.lt(50));
    Assert.assertFalse(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));

    andFilter = FilterFactory.and(TimeFilterApi.gt(100L), TimeFilterApi.lt(200));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertFalse(andFilter.containStartEndTime(minTime, maxTime));

    andFilter = FilterFactory.and(TimeFilterApi.gt(99L), TimeFilterApi.lt(201));
    Assert.assertTrue(andFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(andFilter.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testOr() {
    Filter orFilter = FilterFactory.or(TimeFilterApi.gt(10L), TimeFilterApi.lt(50));
    Assert.assertTrue(orFilter.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(orFilter.containStartEndTime(minTime, maxTime));
  }

  @Test
  public void testNotEq() {
    Filter timeEq = TimeFilterApi.notEq(10L);
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));
    Assert.assertTrue(timeEq.satisfyStartEndTime(minTime, maxTime));

    long startTime = 10, endTime = 10;
    Assert.assertFalse(timeEq.satisfyStartEndTime(startTime, endTime));
    Assert.assertFalse(timeEq.containStartEndTime(5, 50));
  }

  @Test
  public void testNot() {
    Filter not = FilterFactory.not(TimeFilterApi.ltEq(10L));
    try {
      not.satisfyStartEndTime(minTime, maxTime);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
    try {
      not.containStartEndTime(minTime, maxTime);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
  }
}
