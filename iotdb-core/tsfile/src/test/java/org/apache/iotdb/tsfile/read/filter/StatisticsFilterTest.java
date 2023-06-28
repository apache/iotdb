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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.apache.iotdb.tsfile.read.filter.operator.NotFilter.CONTAIN_NOT_ERR_MSG;
import static org.junit.Assert.fail;

public class StatisticsFilterTest {

  private final Statistics<? extends Serializable> statistics1 =
      Statistics.getStatsByType(TSDataType.INT64);
  private final Statistics<? extends Serializable> statistics2 =
      Statistics.getStatsByType(TSDataType.INT64);
  private final Statistics<? extends Serializable> statistics3 =
      Statistics.getStatsByType(TSDataType.INT64);

  @Before
  public void before() {
    statistics1.update(1L, 1L);
    statistics1.update(100L, 100L);
    statistics2.update(101L, 101L);
    statistics2.update(200L, 200L);
    statistics3.update(10L, 10L);
  }

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(10L);
    Assert.assertTrue(timeEq.satisfy(statistics1));
    Assert.assertFalse(timeEq.satisfy(statistics2));
    Assert.assertFalse(timeEq.allSatisfy(statistics1));
    Assert.assertFalse(timeEq.allSatisfy(statistics2));
    Assert.assertTrue(timeEq.allSatisfy(statistics3));

    Filter valueEq = ValueFilter.eq(101L);
    Assert.assertFalse(valueEq.satisfy(statistics1));
    Assert.assertTrue(valueEq.satisfy(statistics2));
    Assert.assertFalse(valueEq.allSatisfy(statistics1));
    Assert.assertFalse(valueEq.allSatisfy(statistics2));
    Assert.assertFalse(valueEq.allSatisfy(statistics3));
  }

  @Test
  public void testNotEq() {
    Filter timeNotEq = TimeFilter.notEq(10L);
    Assert.assertTrue(timeNotEq.satisfy(statistics1));
    Assert.assertTrue(timeNotEq.satisfy(statistics2));
    Assert.assertFalse(timeNotEq.allSatisfy(statistics1));
    Assert.assertTrue(timeNotEq.allSatisfy(statistics2));
    Assert.assertFalse(timeNotEq.allSatisfy(statistics3));

    Filter valueNotEq = ValueFilter.notEq(101L);
    Assert.assertTrue(valueNotEq.satisfy(statistics1));
    Assert.assertTrue(valueNotEq.satisfy(statistics2));
    Assert.assertTrue(valueNotEq.allSatisfy(statistics1));
    Assert.assertFalse(valueNotEq.allSatisfy(statistics2));
    Assert.assertTrue(valueNotEq.allSatisfy(statistics3));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilter.gt(10L);
    Assert.assertTrue(timeGt.satisfy(statistics1));
    Assert.assertTrue(timeGt.satisfy(statistics2));
    Assert.assertFalse(timeGt.allSatisfy(statistics1));
    Assert.assertTrue(timeGt.allSatisfy(statistics2));

    Filter valueGt = ValueFilter.gt(100L);
    Assert.assertFalse(valueGt.satisfy(statistics1));
    Assert.assertTrue(valueGt.satisfy(statistics2));
    Assert.assertFalse(valueGt.allSatisfy(statistics1));
    Assert.assertTrue(valueGt.allSatisfy(statistics2));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilter.gtEq(10L);
    Assert.assertTrue(timeGtEq.satisfy(statistics1));
    Assert.assertTrue(timeGtEq.satisfy(statistics2));
    Assert.assertFalse(timeGtEq.allSatisfy(statistics1));
    Assert.assertTrue(timeGtEq.allSatisfy(statistics2));

    Filter valueGtEq = ValueFilter.gtEq(100L);
    Assert.assertTrue(valueGtEq.satisfy(statistics1));
    Assert.assertTrue(valueGtEq.satisfy(statistics2));
    Assert.assertFalse(valueGtEq.allSatisfy(statistics1));
    Assert.assertTrue(valueGtEq.allSatisfy(statistics2));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilter.lt(101L);
    Assert.assertTrue(timeLt.satisfy(statistics1));
    Assert.assertFalse(timeLt.satisfy(statistics2));
    Assert.assertTrue(timeLt.allSatisfy(statistics1));
    Assert.assertFalse(timeLt.allSatisfy(statistics2));

    Filter valueLt = ValueFilter.lt(11L);
    Assert.assertTrue(valueLt.satisfy(statistics1));
    Assert.assertFalse(valueLt.satisfy(statistics2));
    Assert.assertFalse(valueLt.allSatisfy(statistics1));
    Assert.assertFalse(valueLt.allSatisfy(statistics2));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilter.ltEq(101L);
    Assert.assertTrue(timeLtEq.satisfy(statistics1));
    Assert.assertTrue(timeLtEq.satisfy(statistics2));

    Filter valueLtEq = ValueFilter.ltEq(11L);
    Assert.assertTrue(valueLtEq.satisfy(statistics1));
    Assert.assertFalse(valueLtEq.satisfy(statistics2));
    Assert.assertFalse(valueLtEq.allSatisfy(statistics1));
    Assert.assertFalse(valueLtEq.allSatisfy(statistics2));
  }

  @Test
  public void testAndOr() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(10L), ValueFilter.lt(50L));
    Assert.assertTrue(andFilter.satisfy(statistics1));
    Assert.assertFalse(andFilter.satisfy(statistics2));

    Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(200L));
    Assert.assertTrue(orFilter.satisfy(statistics1));
    Assert.assertTrue(orFilter.satisfy(statistics2));
  }

  @Test
  public void testNot() {
    Filter timeNotEq = FilterFactory.not(TimeFilter.eq(10L));
    try {
      timeNotEq.satisfy(statistics1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
    try {
      timeNotEq.allSatisfy(statistics1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }

    Filter valueNotEq = FilterFactory.not(ValueFilter.eq(101L));
    try {
      valueNotEq.satisfy(statistics1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
    try {
      valueNotEq.allSatisfy(statistics1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
  }

  @Test
  public void testBetweenAnd() {
    Filter timeBetweenAnd = TimeFilter.between(0, 20);
    Assert.assertTrue(timeBetweenAnd.satisfy(statistics1));
    Assert.assertFalse(timeBetweenAnd.satisfy(statistics2));
    Assert.assertTrue(timeBetweenAnd.satisfy(statistics3));
    Assert.assertFalse(timeBetweenAnd.allSatisfy(statistics1));
    Assert.assertFalse(timeBetweenAnd.allSatisfy(statistics2));
    Assert.assertTrue(timeBetweenAnd.allSatisfy(statistics3));

    Filter timeNotBetweenAnd = TimeFilter.notBetween(0, 20);
    Assert.assertTrue(timeNotBetweenAnd.satisfy(statistics1));
    Assert.assertTrue(timeNotBetweenAnd.satisfy(statistics2));
    Assert.assertFalse(timeNotBetweenAnd.satisfy(statistics3));
    Assert.assertFalse(timeNotBetweenAnd.allSatisfy(statistics1));
    Assert.assertTrue(timeNotBetweenAnd.allSatisfy(statistics2));
    Assert.assertFalse(timeNotBetweenAnd.allSatisfy(statistics3));
  }
}
