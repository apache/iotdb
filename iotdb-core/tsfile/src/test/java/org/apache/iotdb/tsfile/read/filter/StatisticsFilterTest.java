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

import org.apache.iotdb.tsfile.file.metadata.IMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilterApi;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iotdb.tsfile.read.filter.FilterTestUtil.newAlignedMetadata;
import static org.apache.iotdb.tsfile.read.filter.FilterTestUtil.newMetadata;
import static org.apache.iotdb.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;
import static org.apache.iotdb.tsfile.read.filter.operator.ValueFilterOperators.CANNOT_PUSH_DOWN_MSG;
import static org.junit.Assert.fail;

public class StatisticsFilterTest {

  private IMetadata metadata1;
  private IMetadata metadata2;
  private IMetadata metadata3;

  private IMetadata alignedMetadata1;
  private IMetadata alignedMetadata2;
  private IMetadata alignedMetadata3;

  @Before
  public void before() {
    LongStatistics statistic1 = new LongStatistics();
    statistic1.update(1L, 1L);
    statistic1.update(100L, 100L);

    TimeStatistics timeStatistics1 = new TimeStatistics();
    timeStatistics1.update(1L);
    timeStatistics1.update(100L);

    LongStatistics statistic2 = new LongStatistics();
    statistic2.update(101L, 101L);
    statistic2.update(200L, 200L);

    TimeStatistics timeStatistics2 = new TimeStatistics();
    timeStatistics2.update(101L);
    timeStatistics2.update(200L);
    timeStatistics2.update(201L);

    LongStatistics statistic3 = new LongStatistics();
    statistic3.update(10L, 10L);

    metadata1 = newMetadata(statistic1);
    metadata2 = newMetadata(statistic2);
    metadata3 = newMetadata(statistic3);

    alignedMetadata1 = newAlignedMetadata(timeStatistics1, statistic1);
    alignedMetadata2 = newAlignedMetadata(timeStatistics2, statistic2);
    alignedMetadata3 = newAlignedMetadata(timeStatistics1, null);
  }

  @Test
  public void testEq() {
    Filter timeEq = TimeFilterApi.eq(10L);
    Assert.assertFalse(timeEq.canSkip(metadata1));
    Assert.assertTrue(timeEq.canSkip(metadata2));
    Assert.assertFalse(timeEq.allSatisfy(metadata1));
    Assert.assertFalse(timeEq.allSatisfy(metadata2));
    Assert.assertTrue(timeEq.allSatisfy(metadata3));

    Filter valueEq = ValueFilterApi.eq(101L);
    Assert.assertTrue(valueEq.canSkip(metadata1));
    Assert.assertFalse(valueEq.canSkip(metadata2));
    Assert.assertFalse(valueEq.allSatisfy(metadata1));
    Assert.assertFalse(valueEq.allSatisfy(metadata2));
    Assert.assertFalse(valueEq.allSatisfy(metadata3));
  }

  @Test
  public void testNotEq() {
    Filter timeNotEq = TimeFilterApi.notEq(10L);
    Assert.assertFalse(timeNotEq.canSkip(metadata1));
    Assert.assertFalse(timeNotEq.canSkip(metadata2));
    Assert.assertFalse(timeNotEq.allSatisfy(metadata1));
    Assert.assertTrue(timeNotEq.allSatisfy(metadata2));
    Assert.assertFalse(timeNotEq.allSatisfy(metadata3));

    Filter valueNotEq = ValueFilterApi.notEq(101L);
    Assert.assertFalse(valueNotEq.canSkip(metadata1));
    Assert.assertFalse(valueNotEq.canSkip(metadata2));
    Assert.assertTrue(valueNotEq.allSatisfy(metadata1));
    Assert.assertFalse(valueNotEq.allSatisfy(metadata2));
    Assert.assertTrue(valueNotEq.allSatisfy(metadata3));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilterApi.gt(10L);
    Assert.assertFalse(timeGt.canSkip(metadata1));
    Assert.assertFalse(timeGt.canSkip(metadata2));
    Assert.assertFalse(timeGt.allSatisfy(metadata1));
    Assert.assertTrue(timeGt.allSatisfy(metadata2));

    Filter valueGt = ValueFilterApi.gt(100L);
    Assert.assertTrue(valueGt.canSkip(metadata1));
    Assert.assertFalse(valueGt.canSkip(metadata2));
    Assert.assertFalse(valueGt.allSatisfy(metadata1));
    Assert.assertTrue(valueGt.allSatisfy(metadata2));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilterApi.gtEq(10L);
    Assert.assertFalse(timeGtEq.canSkip(metadata1));
    Assert.assertFalse(timeGtEq.canSkip(metadata2));
    Assert.assertFalse(timeGtEq.allSatisfy(metadata1));
    Assert.assertTrue(timeGtEq.allSatisfy(metadata2));

    Filter valueGtEq = ValueFilterApi.gtEq(100L);
    Assert.assertFalse(valueGtEq.canSkip(metadata1));
    Assert.assertFalse(valueGtEq.canSkip(metadata2));
    Assert.assertFalse(valueGtEq.allSatisfy(metadata1));
    Assert.assertTrue(valueGtEq.allSatisfy(metadata2));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilterApi.lt(101L);
    Assert.assertFalse(timeLt.canSkip(metadata1));
    Assert.assertTrue(timeLt.canSkip(metadata2));
    Assert.assertTrue(timeLt.allSatisfy(metadata1));
    Assert.assertFalse(timeLt.allSatisfy(metadata2));

    Filter valueLt = ValueFilterApi.lt(11L);
    Assert.assertFalse(valueLt.canSkip(metadata1));
    Assert.assertTrue(valueLt.canSkip(metadata2));
    Assert.assertFalse(valueLt.allSatisfy(metadata1));
    Assert.assertFalse(valueLt.allSatisfy(metadata2));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilterApi.ltEq(101L);
    Assert.assertFalse(timeLtEq.canSkip(metadata1));
    Assert.assertFalse(timeLtEq.canSkip(metadata2));

    Filter valueLtEq = ValueFilterApi.ltEq(11L);
    Assert.assertFalse(valueLtEq.canSkip(metadata1));
    Assert.assertTrue(valueLtEq.canSkip(metadata2));
    Assert.assertFalse(valueLtEq.allSatisfy(metadata1));
    Assert.assertFalse(valueLtEq.allSatisfy(metadata2));
  }

  @Test
  public void testAndOr() {
    Filter andFilter = FilterFactory.and(TimeFilterApi.gt(10L), ValueFilterApi.lt(50L));
    Assert.assertFalse(andFilter.canSkip(metadata1));
    Assert.assertTrue(andFilter.canSkip(metadata2));

    Filter orFilter = FilterFactory.or(andFilter, TimeFilterApi.eq(200L));
    Assert.assertFalse(orFilter.canSkip(metadata1));
    Assert.assertFalse(orFilter.canSkip(metadata2));
  }

  @Test
  public void testNot() {
    Filter timeNotEq = FilterFactory.not(TimeFilterApi.eq(10L));
    try {
      timeNotEq.canSkip(metadata1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
    try {
      timeNotEq.allSatisfy(metadata1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }

    Filter valueNotEq = FilterFactory.not(ValueFilterApi.eq(101L));
    try {
      valueNotEq.canSkip(metadata1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
    try {
      valueNotEq.allSatisfy(metadata1);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(CONTAIN_NOT_ERR_MSG));
    }
  }

  @Test
  public void testBetweenAnd() {
    Filter timeBetweenAnd = TimeFilterApi.between(0L, 20L);
    Assert.assertFalse(timeBetweenAnd.canSkip(metadata1));
    Assert.assertTrue(timeBetweenAnd.canSkip(metadata2));
    Assert.assertFalse(timeBetweenAnd.canSkip(metadata3));
    Assert.assertFalse(timeBetweenAnd.allSatisfy(metadata1));
    Assert.assertFalse(timeBetweenAnd.allSatisfy(metadata2));
    Assert.assertTrue(timeBetweenAnd.allSatisfy(metadata3));

    Filter valueBetweenAnd = ValueFilterApi.between(0L, 20L);
    Assert.assertFalse(valueBetweenAnd.canSkip(metadata1));
    Assert.assertTrue(valueBetweenAnd.canSkip(metadata2));
    Assert.assertFalse(valueBetweenAnd.canSkip(metadata3));
    Assert.assertFalse(valueBetweenAnd.allSatisfy(metadata1));
    Assert.assertFalse(valueBetweenAnd.allSatisfy(metadata2));
    Assert.assertTrue(valueBetweenAnd.allSatisfy(metadata3));
  }

  @Test
  public void testNotBetweenAnd() {
    Filter timeNotBetweenAnd = TimeFilterApi.notBetween(0L, 20L);
    Assert.assertFalse(timeNotBetweenAnd.canSkip(metadata1));
    Assert.assertFalse(timeNotBetweenAnd.canSkip(metadata2));
    Assert.assertTrue(timeNotBetweenAnd.canSkip(metadata3));
    Assert.assertFalse(timeNotBetweenAnd.allSatisfy(metadata1));
    Assert.assertTrue(timeNotBetweenAnd.allSatisfy(metadata2));
    Assert.assertFalse(timeNotBetweenAnd.allSatisfy(metadata3));

    Filter valueNotBetweenAnd = ValueFilterApi.notBetween(0L, 20L);
    Assert.assertFalse(valueNotBetweenAnd.canSkip(metadata1));
    Assert.assertFalse(valueNotBetweenAnd.canSkip(metadata2));
    Assert.assertTrue(valueNotBetweenAnd.canSkip(metadata3));
    Assert.assertFalse(valueNotBetweenAnd.allSatisfy(metadata1));
    Assert.assertTrue(valueNotBetweenAnd.allSatisfy(metadata2));
    Assert.assertFalse(valueNotBetweenAnd.allSatisfy(metadata3));
  }

  @Test
  public void testIsNull() {
    Filter valueIsNull = ValueFilterApi.isNull(0);

    try {
      valueIsNull.canSkip(alignedMetadata1);
      fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(CANNOT_PUSH_DOWN_MSG));
    }

    try {
      valueIsNull.allSatisfy(alignedMetadata1);
      fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(CANNOT_PUSH_DOWN_MSG));
    }
  }

  @Test
  public void testIsNotNull() {
    Filter valueIsNotNull = ValueFilterApi.isNotNull(0);

    Assert.assertFalse(valueIsNotNull.canSkip(alignedMetadata1));
    Assert.assertFalse(valueIsNotNull.canSkip(alignedMetadata2));
    Assert.assertTrue(valueIsNotNull.canSkip(alignedMetadata3));

    Assert.assertTrue(valueIsNotNull.allSatisfy(alignedMetadata1));
    Assert.assertFalse(valueIsNotNull.allSatisfy(alignedMetadata2));
    Assert.assertFalse(valueIsNotNull.allSatisfy(alignedMetadata3));
  }
}
