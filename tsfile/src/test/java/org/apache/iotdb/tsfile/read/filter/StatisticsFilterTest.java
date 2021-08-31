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

public class StatisticsFilterTest {

  private Statistics statistics1 = Statistics.getStatsByType(TSDataType.INT64);
  private Statistics statistics2 = Statistics.getStatsByType(TSDataType.INT64);

  @Before
  public void before() {
    statistics1.update(1L, 1L);
    statistics1.update(100L, 100L);
    statistics2.update(101L, 101L);
    statistics2.update(200L, 200L);
  }

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(10L);
    Assert.assertTrue(timeEq.satisfy(statistics1));
    Assert.assertFalse(timeEq.satisfy(statistics2));

    Filter valueEq = ValueFilter.eq(101L);
    Assert.assertFalse(valueEq.satisfy(statistics1));
    Assert.assertTrue(valueEq.satisfy(statistics2));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilter.gt(100L);
    Assert.assertFalse(timeGt.satisfy(statistics1));
    Assert.assertTrue(timeGt.satisfy(statistics2));

    Filter valueGt = ValueFilter.gt(100L);
    Assert.assertFalse(valueGt.satisfy(statistics1));
    Assert.assertTrue(valueGt.satisfy(statistics2));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilter.gtEq(100L);
    Assert.assertTrue(timeGtEq.satisfy(statistics1));
    Assert.assertTrue(timeGtEq.satisfy(statistics2));

    Filter valueGtEq = ValueFilter.gtEq(100L);
    Assert.assertTrue(valueGtEq.satisfy(statistics1));
    Assert.assertTrue(valueGtEq.satisfy(statistics2));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilter.lt(101L);
    Assert.assertTrue(timeLt.satisfy(statistics1));
    Assert.assertFalse(timeLt.satisfy(statistics2));

    Filter valueLt = ValueFilter.lt(101L);
    Assert.assertTrue(valueLt.satisfy(statistics1));
    Assert.assertFalse(valueLt.satisfy(statistics2));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilter.ltEq(101L);
    Assert.assertTrue(timeLtEq.satisfy(statistics1));
    Assert.assertTrue(timeLtEq.satisfy(statistics2));

    Filter valueLtEq = ValueFilter.ltEq(101L);
    Assert.assertTrue(valueLtEq.satisfy(statistics1));
    Assert.assertTrue(valueLtEq.satisfy(statistics2));
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
}
