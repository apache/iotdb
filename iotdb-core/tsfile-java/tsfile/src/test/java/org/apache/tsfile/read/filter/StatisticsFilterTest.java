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
package org.apache.tsfile.read.filter;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.tsfile.file.metadata.statistics.StringStatistics;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.tsfile.common.conf.TSFileConfig.STRING_CHARSET;
import static org.apache.tsfile.read.filter.FilterTestUtil.newAlignedMetadata;
import static org.apache.tsfile.read.filter.FilterTestUtil.newMetadata;
import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;
import static org.apache.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;
import static org.junit.Assert.fail;

public class StatisticsFilterTest {

  private IMetadata metadata1;
  private IMetadata metadata2;
  private IMetadata metadata3;
  private IMetadata metadata4;
  private IMetadata metadata5;
  private IMetadata metadata6;

  private IMetadata alignedMetadata1;
  private IMetadata alignedMetadata2;
  private IMetadata alignedMetadata3;
  private IMetadata alignedMetadata4;
  private IMetadata alignedMetadata5;

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

    StringStatistics statistic4 = new StringStatistics();
    statistic4.update(100L, new Binary("a", STRING_CHARSET));
    statistic4.update(200L, new Binary("a", STRING_CHARSET));

    StringStatistics statistic5 = new StringStatistics();
    statistic5.update(100L, new Binary("b", STRING_CHARSET));
    statistic5.update(200L, new Binary("b", STRING_CHARSET));

    StringStatistics statistic6 = new StringStatistics();
    statistic6.update(100L, new Binary("a", STRING_CHARSET));
    statistic6.update(200L, new Binary("b", STRING_CHARSET));

    metadata1 = newMetadata(statistic1);
    metadata2 = newMetadata(statistic2);
    metadata3 = newMetadata(statistic3);
    metadata4 = newMetadata(statistic4);
    metadata5 = newMetadata(statistic5);
    metadata6 = newMetadata(statistic6);

    alignedMetadata1 = newAlignedMetadata(timeStatistics1, statistic1);
    alignedMetadata2 = newAlignedMetadata(timeStatistics2, statistic2);
    alignedMetadata3 = newAlignedMetadata(timeStatistics1, null);

    alignedMetadata4 = newAlignedMetadata(timeStatistics1, statistic4);
    alignedMetadata5 = newAlignedMetadata(timeStatistics1, statistic5);
  }

  @Test
  public void testEq() {
    Filter timeEq = TimeFilterApi.eq(10L);
    Assert.assertFalse(timeEq.canSkip(metadata1));
    Assert.assertTrue(timeEq.canSkip(metadata2));
    Assert.assertFalse(timeEq.allSatisfy(metadata1));
    Assert.assertFalse(timeEq.allSatisfy(metadata2));
    Assert.assertTrue(timeEq.allSatisfy(metadata3));

    Filter valueEq = ValueFilterApi.eq(0, 101L, TSDataType.INT64);
    Assert.assertTrue(valueEq.canSkip(metadata1));
    Assert.assertFalse(valueEq.canSkip(metadata2));
    Assert.assertFalse(valueEq.allSatisfy(metadata1));
    Assert.assertFalse(valueEq.allSatisfy(metadata2));
    Assert.assertFalse(valueEq.allSatisfy(metadata3));

    Filter stringEq =
        ValueFilterApi.eq(
            DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TSDataType.STRING);
    Assert.assertFalse(stringEq.canSkip(metadata4));
    Assert.assertTrue(stringEq.canSkip(metadata5));
    Assert.assertFalse(stringEq.canSkip(metadata6));

    Assert.assertTrue(stringEq.allSatisfy(metadata4));
    Assert.assertFalse(stringEq.allSatisfy(metadata5));
    Assert.assertFalse(stringEq.allSatisfy(metadata6));
  }

  @Test
  public void testNotEq() {
    Filter timeNotEq = TimeFilterApi.notEq(10L);
    Assert.assertFalse(timeNotEq.canSkip(metadata1));
    Assert.assertFalse(timeNotEq.canSkip(metadata2));
    Assert.assertFalse(timeNotEq.allSatisfy(metadata1));
    Assert.assertTrue(timeNotEq.allSatisfy(metadata2));
    Assert.assertFalse(timeNotEq.allSatisfy(metadata3));

    Filter valueNotEq = ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 101L, TSDataType.INT64);
    Assert.assertFalse(valueNotEq.canSkip(metadata1));
    Assert.assertFalse(valueNotEq.canSkip(metadata2));
    Assert.assertTrue(valueNotEq.allSatisfy(metadata1));
    Assert.assertFalse(valueNotEq.allSatisfy(metadata2));
    Assert.assertTrue(valueNotEq.allSatisfy(metadata3));

    Filter stringNotEq =
        ValueFilterApi.notEq(
            DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TSDataType.STRING);
    Assert.assertTrue(stringNotEq.canSkip(metadata4));
    Assert.assertFalse(stringNotEq.canSkip(metadata5));
    Assert.assertFalse(stringNotEq.canSkip(metadata6));

    Assert.assertFalse(stringNotEq.allSatisfy(metadata4));
    Assert.assertTrue(stringNotEq.allSatisfy(metadata5));
    Assert.assertFalse(stringNotEq.allSatisfy(metadata6));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilterApi.gt(10L);
    Assert.assertFalse(timeGt.canSkip(metadata1));
    Assert.assertFalse(timeGt.canSkip(metadata2));
    Assert.assertFalse(timeGt.allSatisfy(metadata1));
    Assert.assertTrue(timeGt.allSatisfy(metadata2));

    Filter valueGt = ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64);
    Assert.assertTrue(valueGt.canSkip(metadata1));
    Assert.assertFalse(valueGt.canSkip(metadata2));
    Assert.assertFalse(valueGt.allSatisfy(metadata1));
    Assert.assertTrue(valueGt.allSatisfy(metadata2));

    Filter stringGt =
        ValueFilterApi.gt(
            DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TSDataType.STRING);
    Assert.assertTrue(stringGt.canSkip(metadata4));
    Assert.assertFalse(stringGt.canSkip(metadata5));
    Assert.assertFalse(stringGt.canSkip(metadata6));

    Assert.assertFalse(stringGt.allSatisfy(metadata4));
    Assert.assertTrue(stringGt.allSatisfy(metadata5));
    Assert.assertFalse(stringGt.allSatisfy(metadata6));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilterApi.gtEq(10L);
    Assert.assertFalse(timeGtEq.canSkip(metadata1));
    Assert.assertFalse(timeGtEq.canSkip(metadata2));
    Assert.assertFalse(timeGtEq.allSatisfy(metadata1));
    Assert.assertTrue(timeGtEq.allSatisfy(metadata2));

    Filter valueGtEq = ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64);
    Assert.assertFalse(valueGtEq.canSkip(metadata1));
    Assert.assertFalse(valueGtEq.canSkip(metadata2));
    Assert.assertFalse(valueGtEq.allSatisfy(metadata1));
    Assert.assertTrue(valueGtEq.allSatisfy(metadata2));

    Filter stringGtEq =
        ValueFilterApi.gtEq(
            DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TSDataType.STRING);
    Assert.assertFalse(stringGtEq.canSkip(metadata4));
    Assert.assertFalse(stringGtEq.canSkip(metadata5));
    Assert.assertFalse(stringGtEq.canSkip(metadata6));

    Assert.assertTrue(stringGtEq.allSatisfy(metadata4));
    Assert.assertTrue(stringGtEq.allSatisfy(metadata5));
    Assert.assertTrue(stringGtEq.allSatisfy(metadata6));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilterApi.lt(101L);
    Assert.assertFalse(timeLt.canSkip(metadata1));
    Assert.assertTrue(timeLt.canSkip(metadata2));
    Assert.assertTrue(timeLt.allSatisfy(metadata1));
    Assert.assertFalse(timeLt.allSatisfy(metadata2));

    Filter valueLt = ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 11L, TSDataType.INT64);
    Assert.assertFalse(valueLt.canSkip(metadata1));
    Assert.assertTrue(valueLt.canSkip(metadata2));
    Assert.assertFalse(valueLt.allSatisfy(metadata1));
    Assert.assertFalse(valueLt.allSatisfy(metadata2));

    Filter stringLt =
        ValueFilterApi.lt(
            DEFAULT_MEASUREMENT_INDEX, new Binary("b", STRING_CHARSET), TSDataType.STRING);
    Assert.assertFalse(stringLt.canSkip(metadata4));
    Assert.assertTrue(stringLt.canSkip(metadata5));
    Assert.assertFalse(stringLt.canSkip(metadata6));

    Assert.assertTrue(stringLt.allSatisfy(metadata4));
    Assert.assertFalse(stringLt.allSatisfy(metadata5));
    Assert.assertFalse(stringLt.allSatisfy(metadata6));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilterApi.ltEq(101L);
    Assert.assertFalse(timeLtEq.canSkip(metadata1));
    Assert.assertFalse(timeLtEq.canSkip(metadata2));

    Filter valueLtEq = ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 11L, TSDataType.INT64);
    Assert.assertFalse(valueLtEq.canSkip(metadata1));
    Assert.assertTrue(valueLtEq.canSkip(metadata2));
    Assert.assertFalse(valueLtEq.allSatisfy(metadata1));
    Assert.assertFalse(valueLtEq.allSatisfy(metadata2));

    Filter stringLtEq =
        ValueFilterApi.ltEq(
            DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TSDataType.STRING);
    Assert.assertFalse(stringLtEq.canSkip(metadata4));
    Assert.assertTrue(stringLtEq.canSkip(metadata5));
    Assert.assertFalse(stringLtEq.canSkip(metadata6));

    Assert.assertTrue(stringLtEq.allSatisfy(metadata4));
    Assert.assertFalse(stringLtEq.allSatisfy(metadata5));
    Assert.assertFalse(stringLtEq.allSatisfy(metadata6));
  }

  @Test
  public void testAndOr() {
    Filter andFilter =
        FilterFactory.and(
            TimeFilterApi.gt(10L),
            ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 50L, TSDataType.INT64));
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

    Filter valueNotEq =
        FilterFactory.not(ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 101L, TSDataType.INT64));
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

    Filter valueBetweenAnd =
        ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, 0L, 20L, TSDataType.INT64);
    Assert.assertFalse(valueBetweenAnd.canSkip(metadata1));
    Assert.assertTrue(valueBetweenAnd.canSkip(metadata2));
    Assert.assertFalse(valueBetweenAnd.canSkip(metadata3));
    Assert.assertFalse(valueBetweenAnd.allSatisfy(metadata1));
    Assert.assertFalse(valueBetweenAnd.allSatisfy(metadata2));
    Assert.assertTrue(valueBetweenAnd.allSatisfy(metadata3));

    Filter stringBetweenAnd =
        ValueFilterApi.between(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("a", STRING_CHARSET),
            new Binary("b", STRING_CHARSET),
            TSDataType.STRING);
    Assert.assertFalse(stringBetweenAnd.canSkip(metadata4));
    Assert.assertFalse(stringBetweenAnd.canSkip(metadata5));
    Assert.assertFalse(stringBetweenAnd.canSkip(metadata6));

    Assert.assertTrue(stringBetweenAnd.allSatisfy(metadata4));
    Assert.assertTrue(stringBetweenAnd.allSatisfy(metadata5));
    Assert.assertTrue(stringBetweenAnd.allSatisfy(metadata6));
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

    Filter valueNotBetweenAnd =
        ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, 0L, 20L, TSDataType.INT64);
    Assert.assertFalse(valueNotBetweenAnd.canSkip(metadata1));
    Assert.assertFalse(valueNotBetweenAnd.canSkip(metadata2));
    Assert.assertTrue(valueNotBetweenAnd.canSkip(metadata3));
    Assert.assertFalse(valueNotBetweenAnd.allSatisfy(metadata1));
    Assert.assertTrue(valueNotBetweenAnd.allSatisfy(metadata2));
    Assert.assertFalse(valueNotBetweenAnd.allSatisfy(metadata3));

    Filter stringNotBetweenAnd =
        ValueFilterApi.notBetween(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("b", STRING_CHARSET),
            new Binary("c", STRING_CHARSET),
            TSDataType.STRING);
    Assert.assertFalse(stringNotBetweenAnd.canSkip(metadata4));
    Assert.assertTrue(stringNotBetweenAnd.canSkip(metadata5));
    Assert.assertFalse(stringNotBetweenAnd.canSkip(metadata6));

    Assert.assertTrue(stringNotBetweenAnd.allSatisfy(metadata4));
    Assert.assertFalse(stringNotBetweenAnd.allSatisfy(metadata5));
    Assert.assertFalse(stringNotBetweenAnd.allSatisfy(metadata6));
  }

  @Test
  public void testIsNull() {
    Filter valueIsNull = ValueFilterApi.isNull(DEFAULT_MEASUREMENT_INDEX);

    Assert.assertTrue(valueIsNull.canSkip(alignedMetadata1));
    Assert.assertFalse(valueIsNull.canSkip(alignedMetadata2));
    Assert.assertFalse(valueIsNull.canSkip(alignedMetadata3));
    Assert.assertTrue(valueIsNull.canSkip(alignedMetadata4));
    Assert.assertTrue(valueIsNull.canSkip(alignedMetadata5));

    Assert.assertFalse(valueIsNull.allSatisfy(alignedMetadata1));
    Assert.assertFalse(valueIsNull.allSatisfy(alignedMetadata2));
    Assert.assertTrue(valueIsNull.allSatisfy(alignedMetadata3));
    Assert.assertFalse(valueIsNull.allSatisfy(alignedMetadata4));
    Assert.assertFalse(valueIsNull.allSatisfy(alignedMetadata5));
  }

  @Test
  public void testIsNotNull() {
    Filter valueIsNotNull = ValueFilterApi.isNotNull(DEFAULT_MEASUREMENT_INDEX);

    Assert.assertFalse(valueIsNotNull.canSkip(alignedMetadata1));
    Assert.assertFalse(valueIsNotNull.canSkip(alignedMetadata2));
    Assert.assertTrue(valueIsNotNull.canSkip(alignedMetadata3));
    Assert.assertFalse(valueIsNotNull.canSkip(alignedMetadata4));
    Assert.assertFalse(valueIsNotNull.canSkip(alignedMetadata5));

    Assert.assertTrue(valueIsNotNull.allSatisfy(alignedMetadata1));
    Assert.assertFalse(valueIsNotNull.allSatisfy(alignedMetadata2));
    Assert.assertFalse(valueIsNotNull.allSatisfy(alignedMetadata3));
    Assert.assertTrue(valueIsNotNull.allSatisfy(alignedMetadata4));
    Assert.assertTrue(valueIsNotNull.allSatisfy(alignedMetadata5));
  }

  @Test
  public void testIn() {
    // Non-null candidates
    Set<Long> candidates = new HashSet<>();
    candidates.add(1L);
    candidates.add(10L);

    // Time
    Filter timeIn = TimeFilterApi.in(candidates);
    Assert.assertFalse(timeIn.canSkip(metadata1));
    Assert.assertTrue(timeIn.canSkip(metadata2));
    Assert.assertFalse(timeIn.canSkip(metadata3));
    Assert.assertFalse(timeIn.allSatisfy(metadata1));
    Assert.assertFalse(timeIn.allSatisfy(metadata2));
    Assert.assertFalse(timeIn.allSatisfy(metadata3));
    // Value
    Filter valueIn = ValueFilterApi.in(DEFAULT_MEASUREMENT_INDEX, candidates, TSDataType.INT64);
    Assert.assertFalse(valueIn.canSkip(metadata1));
    Assert.assertTrue(valueIn.canSkip(metadata2));
    Assert.assertFalse(valueIn.canSkip(metadata3));
    Assert.assertTrue(valueIn.canSkip(alignedMetadata3));
    Assert.assertFalse(valueIn.allSatisfy(metadata1));
    Assert.assertFalse(valueIn.allSatisfy(metadata2));
    Assert.assertTrue(valueIn.allSatisfy(metadata3));
    Assert.assertFalse(valueIn.allSatisfy(alignedMetadata3));

    // Null candidate
    Set<Long> nullCandidate = new HashSet<>();
    candidates.add(null);

    // Time cannot be null
    Filter valueIn2 = ValueFilterApi.in(DEFAULT_MEASUREMENT_INDEX, nullCandidate, TSDataType.INT64);
    Assert.assertTrue(valueIn2.canSkip(metadata1));
    Assert.assertTrue(valueIn2.canSkip(metadata2));
    Assert.assertTrue(valueIn2.canSkip(metadata3));
    Assert.assertFalse(valueIn2.canSkip(alignedMetadata3));
    Assert.assertFalse(valueIn2.allSatisfy(metadata1));
    Assert.assertFalse(valueIn2.allSatisfy(metadata2));
    Assert.assertFalse(valueIn2.allSatisfy(metadata3));
    Assert.assertTrue(valueIn2.allSatisfy(alignedMetadata3));

    Filter stringIn =
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX,
            new HashSet<>(
                Arrays.asList(new Binary("a", STRING_CHARSET), new Binary("b", STRING_CHARSET))),
            TSDataType.STRING);
    Assert.assertFalse(stringIn.canSkip(metadata4));
    Assert.assertFalse(stringIn.canSkip(metadata5));
    Assert.assertFalse(stringIn.canSkip(metadata6));

    Assert.assertTrue(stringIn.allSatisfy(metadata4));
    Assert.assertTrue(stringIn.allSatisfy(metadata5));
    Assert.assertFalse(stringIn.allSatisfy(metadata6));
  }

  @Test
  public void testNotIn() {
    Set<Long> candidates = new HashSet<>();
    candidates.add(1L);
    candidates.add(10L);

    Filter timeNotIn = TimeFilterApi.notIn(candidates);
    Assert.assertFalse(timeNotIn.canSkip(metadata1));
    Assert.assertFalse(timeNotIn.canSkip(metadata2));
    Assert.assertFalse(timeNotIn.canSkip(metadata3));
    Assert.assertFalse(timeNotIn.allSatisfy(metadata1));
    Assert.assertFalse(timeNotIn.allSatisfy(metadata2));
    Assert.assertFalse(timeNotIn.allSatisfy(metadata3));

    Filter valueNotIn =
        ValueFilterApi.notIn(DEFAULT_MEASUREMENT_INDEX, candidates, TSDataType.INT64);
    Assert.assertFalse(valueNotIn.canSkip(metadata1));
    Assert.assertFalse(valueNotIn.canSkip(metadata2));
    Assert.assertFalse(valueNotIn.canSkip(metadata3));
    Assert.assertFalse(valueNotIn.allSatisfy(metadata1));
    Assert.assertFalse(valueNotIn.allSatisfy(metadata2));
    Assert.assertFalse(valueNotIn.allSatisfy(metadata3));
  }
}
