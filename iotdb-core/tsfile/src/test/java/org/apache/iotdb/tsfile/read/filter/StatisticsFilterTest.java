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

import org.apache.iotdb.tsfile.file.metadata.IAlignedMetadataProvider;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;
import static org.junit.Assert.fail;

public class StatisticsFilterTest {

  private final Statistics<? extends Serializable> statistics1 =
      Statistics.getStatsByType(TSDataType.INT64);
  private final Statistics<? extends Serializable> statistics2 =
      Statistics.getStatsByType(TSDataType.INT64);
  private final Statistics<? extends Serializable> statistics3 =
      Statistics.getStatsByType(TSDataType.INT64);

  private final TimeStatistics timeStatistics1 = new TimeStatistics();
  private final TimeStatistics timeStatistics2 = new TimeStatistics();
  private final TimeStatistics timeStatistics3 = new TimeStatistics();

  @Before
  public void before() {
    statistics1.update(1L, 1L);
    statistics1.update(100L, 100L);
    timeStatistics1.update(1L);
    timeStatistics1.update(100L);

    statistics2.update(101L, 101L);
    statistics2.update(200L, 200L);
    timeStatistics2.update(101L);
    timeStatistics2.update(200L);
    timeStatistics2.update(201L);

    statistics3.update(10L, 10L);
  }

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(10L);
    Assert.assertFalse(timeEq.canSkip(statistics1));
    Assert.assertTrue(timeEq.canSkip(statistics2));
    Assert.assertFalse(timeEq.allSatisfy(statistics1));
    Assert.assertFalse(timeEq.allSatisfy(statistics2));
    Assert.assertTrue(timeEq.allSatisfy(statistics3));

    Filter valueEq = ValueFilter.eq(101L);
    Assert.assertTrue(valueEq.canSkip(statistics1));
    Assert.assertFalse(valueEq.canSkip(statistics2));
    Assert.assertFalse(valueEq.allSatisfy(statistics1));
    Assert.assertFalse(valueEq.allSatisfy(statistics2));
    Assert.assertFalse(valueEq.allSatisfy(statistics3));
  }

  @Test
  public void testNotEq() {
    Filter timeNotEq = TimeFilter.notEq(10L);
    Assert.assertFalse(timeNotEq.canSkip(statistics1));
    Assert.assertFalse(timeNotEq.canSkip(statistics2));
    Assert.assertFalse(timeNotEq.allSatisfy(statistics1));
    Assert.assertTrue(timeNotEq.allSatisfy(statistics2));
    Assert.assertFalse(timeNotEq.allSatisfy(statistics3));

    Filter valueNotEq = ValueFilter.notEq(101L);
    Assert.assertFalse(valueNotEq.canSkip(statistics1));
    Assert.assertFalse(valueNotEq.canSkip(statistics2));
    Assert.assertTrue(valueNotEq.allSatisfy(statistics1));
    Assert.assertFalse(valueNotEq.allSatisfy(statistics2));
    Assert.assertTrue(valueNotEq.allSatisfy(statistics3));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilter.gt(10L);
    Assert.assertFalse(timeGt.canSkip(statistics1));
    Assert.assertFalse(timeGt.canSkip(statistics2));
    Assert.assertFalse(timeGt.allSatisfy(statistics1));
    Assert.assertTrue(timeGt.allSatisfy(statistics2));

    Filter valueGt = ValueFilter.gt(100L);
    Assert.assertTrue(valueGt.canSkip(statistics1));
    Assert.assertFalse(valueGt.canSkip(statistics2));
    Assert.assertFalse(valueGt.allSatisfy(statistics1));
    Assert.assertTrue(valueGt.allSatisfy(statistics2));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilter.gtEq(10L);
    Assert.assertFalse(timeGtEq.canSkip(statistics1));
    Assert.assertFalse(timeGtEq.canSkip(statistics2));
    Assert.assertFalse(timeGtEq.allSatisfy(statistics1));
    Assert.assertTrue(timeGtEq.allSatisfy(statistics2));

    Filter valueGtEq = ValueFilter.gtEq(100L);
    Assert.assertFalse(valueGtEq.canSkip(statistics1));
    Assert.assertFalse(valueGtEq.canSkip(statistics2));
    Assert.assertFalse(valueGtEq.allSatisfy(statistics1));
    Assert.assertTrue(valueGtEq.allSatisfy(statistics2));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilter.lt(101L);
    Assert.assertFalse(timeLt.canSkip(statistics1));
    Assert.assertTrue(timeLt.canSkip(statistics2));
    Assert.assertTrue(timeLt.allSatisfy(statistics1));
    Assert.assertFalse(timeLt.allSatisfy(statistics2));

    Filter valueLt = ValueFilter.lt(11L);
    Assert.assertFalse(valueLt.canSkip(statistics1));
    Assert.assertTrue(valueLt.canSkip(statistics2));
    Assert.assertFalse(valueLt.allSatisfy(statistics1));
    Assert.assertFalse(valueLt.allSatisfy(statistics2));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilter.ltEq(101L);
    Assert.assertFalse(timeLtEq.canSkip(statistics1));
    Assert.assertFalse(timeLtEq.canSkip(statistics2));

    Filter valueLtEq = ValueFilter.ltEq(11L);
    Assert.assertFalse(valueLtEq.canSkip(statistics1));
    Assert.assertTrue(valueLtEq.canSkip(statistics2));
    Assert.assertFalse(valueLtEq.allSatisfy(statistics1));
    Assert.assertFalse(valueLtEq.allSatisfy(statistics2));
  }

  @Test
  public void testAndOr() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(10L), ValueFilter.lt(50L));
    Assert.assertFalse(andFilter.canSkip(statistics1));
    Assert.assertTrue(andFilter.canSkip(statistics2));

    Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(200L));
    Assert.assertFalse(orFilter.canSkip(statistics1));
    Assert.assertFalse(orFilter.canSkip(statistics2));
  }

  @Test
  public void testNot() {
    Filter timeNotEq = FilterFactory.not(TimeFilter.eq(10L));
    try {
      timeNotEq.canSkip(statistics1);
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
      valueNotEq.canSkip(statistics1);
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
    Assert.assertFalse(timeBetweenAnd.canSkip(statistics1));
    Assert.assertTrue(timeBetweenAnd.canSkip(statistics2));
    Assert.assertFalse(timeBetweenAnd.canSkip(statistics3));
    Assert.assertFalse(timeBetweenAnd.allSatisfy(statistics1));
    Assert.assertFalse(timeBetweenAnd.allSatisfy(statistics2));
    Assert.assertTrue(timeBetweenAnd.allSatisfy(statistics3));

    Filter timeNotBetweenAnd = TimeFilter.notBetween(0, 20);
    Assert.assertFalse(timeNotBetweenAnd.canSkip(statistics1));
    Assert.assertFalse(timeNotBetweenAnd.canSkip(statistics2));
    Assert.assertTrue(timeNotBetweenAnd.canSkip(statistics3));
    Assert.assertFalse(timeNotBetweenAnd.allSatisfy(statistics1));
    Assert.assertTrue(timeNotBetweenAnd.allSatisfy(statistics2));
    Assert.assertFalse(timeNotBetweenAnd.allSatisfy(statistics3));
  }

  @Test
  public void testIsNull() {
    Filter valueIsNull = ValueFilter.isNull(0);

    Assert.assertTrue(
        valueIsNull.canSkip(getAlignedMetadataProvider(timeStatistics1, statistics1)));
    Assert.assertFalse(
        valueIsNull.canSkip(getAlignedMetadataProvider(timeStatistics2, statistics2)));
    Assert.assertFalse(valueIsNull.canSkip(getAlignedMetadataProvider(timeStatistics1, null)));

    Assert.assertFalse(
        valueIsNull.allSatisfy(getAlignedMetadataProvider(timeStatistics1, statistics1)));
    Assert.assertFalse(
        valueIsNull.allSatisfy(getAlignedMetadataProvider(timeStatistics2, statistics2)));
    Assert.assertTrue(valueIsNull.allSatisfy(getAlignedMetadataProvider(timeStatistics1, null)));
  }

  @Test
  public void testIsNotNull() {
    Filter valueIsNotNull = ValueFilter.isNotNull(0);

    Assert.assertFalse(
        valueIsNotNull.canSkip(getAlignedMetadataProvider(timeStatistics1, statistics1)));
    Assert.assertFalse(
        valueIsNotNull.canSkip(getAlignedMetadataProvider(timeStatistics2, statistics2)));
    Assert.assertTrue(valueIsNotNull.canSkip(getAlignedMetadataProvider(timeStatistics1, null)));

    Assert.assertTrue(
        valueIsNotNull.allSatisfy(getAlignedMetadataProvider(timeStatistics1, statistics1)));
    Assert.assertFalse(
        valueIsNotNull.allSatisfy(getAlignedMetadataProvider(timeStatistics2, statistics2)));
    Assert.assertFalse(
        valueIsNotNull.allSatisfy(getAlignedMetadataProvider(timeStatistics1, null)));
  }

  private AlignedMetadataProvider getAlignedMetadataProvider(
      TimeStatistics timeStatistics, Statistics<? extends Serializable> valueStatistics) {
    return new AlignedMetadataProvider(timeStatistics, Collections.singletonList(valueStatistics));
  }

  private static class AlignedMetadataProvider implements IAlignedMetadataProvider {

    private final TimeStatistics timeStatistics;
    private final List<Statistics<? extends Serializable>> statisticsList;

    public AlignedMetadataProvider(
        TimeStatistics timeStatistics, List<Statistics<? extends Serializable>> statisticsList) {
      this.timeStatistics = timeStatistics;
      this.statisticsList = statisticsList;
    }

    @Override
    public Statistics<? extends Serializable> getTimeStatistics() {
      return timeStatistics;
    }

    @Override
    public Statistics<? extends Serializable> getMeasurementStatistics(int measurementIndex) {
      return statisticsList.get(measurementIndex);
    }
  }
}
