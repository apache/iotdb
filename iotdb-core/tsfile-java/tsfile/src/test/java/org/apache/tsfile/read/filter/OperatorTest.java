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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class OperatorTest {

  private static final long TESTED_TIMESTAMP = 1513585371L;

  @Test
  public void testEq() {
    Filter timeEq = TimeFilterApi.eq(100L);
    Assert.assertTrue(timeEq.satisfy(100, 100));
    Assert.assertFalse(timeEq.satisfy(101, 100));

    Filter filter2 =
        FilterFactory.and(
            TimeFilterApi.eq(100L),
            ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    Assert.assertTrue(filter2.satisfyInteger(100, 50));
    Assert.assertFalse(filter2.satisfyInteger(100, 51));

    Filter filter3 = ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, true, TSDataType.BOOLEAN);
    Assert.assertTrue(filter3.satisfyBoolean(100, true));
    Assert.assertFalse(filter3.satisfyBoolean(100, false));
  }

  @Test
  public void testIsNull() {
    Filter isNullFilter = ValueFilterApi.isNull(DEFAULT_MEASUREMENT_INDEX);
    isNullFilter.satisfyRow(100, new Object[] {null});
    Assert.assertTrue(isNullFilter.satisfyRow(100, new Object[] {null}));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilterApi.gt(TESTED_TIMESTAMP);
    Assert.assertTrue(timeGt.satisfy(TESTED_TIMESTAMP + 1, 100));
    Assert.assertFalse(timeGt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeGt.satisfy(TESTED_TIMESTAMP - 1, 100));

    Filter valueGt = ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 0.01f, TSDataType.FLOAT);
    Assert.assertTrue(valueGt.satisfyFloat(TESTED_TIMESTAMP, 0.02f));
    Assert.assertFalse(valueGt.satisfyFloat(TESTED_TIMESTAMP, 0.01f));
    Assert.assertFalse(valueGt.satisfyFloat(TESTED_TIMESTAMP, -0.01f));

    Filter binaryFilter =
        ValueFilterApi.gt(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("test1", TSFileConfig.STRING_CHARSET),
            TSDataType.TEXT);
    Assert.assertTrue(
        binaryFilter.satisfyBinary(
            TESTED_TIMESTAMP, new Binary("test2", TSFileConfig.STRING_CHARSET)));
    Assert.assertFalse(
        binaryFilter.satisfyBinary(
            TESTED_TIMESTAMP, new Binary("test0", TSFileConfig.STRING_CHARSET)));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilterApi.gtEq(TESTED_TIMESTAMP);
    Assert.assertTrue(timeGtEq.satisfy(TESTED_TIMESTAMP + 1, 100));
    Assert.assertTrue(timeGtEq.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeGtEq.satisfy(TESTED_TIMESTAMP - 1, 100));

    Filter valueGtEq = ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0.01, TSDataType.DOUBLE);
    Assert.assertTrue(valueGtEq.satisfyDouble(TESTED_TIMESTAMP, 0.02));
    Assert.assertTrue(valueGtEq.satisfyDouble(TESTED_TIMESTAMP, 0.01));
    Assert.assertFalse(valueGtEq.satisfyDouble(TESTED_TIMESTAMP, -0.01));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilterApi.lt(TESTED_TIMESTAMP);
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLt = ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64);
    Assert.assertTrue(valueLt.satisfyLong(TESTED_TIMESTAMP, 99L));
    Assert.assertFalse(valueLt.satisfyLong(TESTED_TIMESTAMP, 100L));
    Assert.assertFalse(valueLt.satisfyLong(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilterApi.ltEq(TESTED_TIMESTAMP);
    Assert.assertTrue(timeLtEq.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertTrue(timeLtEq.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertFalse(timeLtEq.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLtEq = ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64);
    Assert.assertTrue(valueLtEq.satisfyLong(TESTED_TIMESTAMP, 99L));
    Assert.assertTrue(valueLtEq.satisfyLong(TESTED_TIMESTAMP, 100L));
    Assert.assertFalse(valueLtEq.satisfyLong(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testNot() {
    Filter timeLt = FilterFactory.not(TimeFilterApi.lt(TESTED_TIMESTAMP));
    Assert.assertFalse(timeLt.satisfy(TESTED_TIMESTAMP - 1, 100));
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP, 100));
    Assert.assertTrue(timeLt.satisfy(TESTED_TIMESTAMP + 1, 100));

    Filter valueLt =
        FilterFactory.not(ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64));
    Assert.assertFalse(valueLt.satisfyLong(TESTED_TIMESTAMP, 99L));
    Assert.assertTrue(valueLt.satisfyLong(TESTED_TIMESTAMP, 100L));
    Assert.assertTrue(valueLt.satisfyLong(TESTED_TIMESTAMP, 101L));
  }

  @Test
  public void testNotEq() {
    Filter timeNotEq = TimeFilterApi.notEq(100L);
    Assert.assertFalse(timeNotEq.satisfy(100, 100));
    Assert.assertTrue(timeNotEq.satisfy(101, 100));

    Filter valueNotEq = ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32);
    Assert.assertFalse(valueNotEq.satisfyInteger(100, 50));
    Assert.assertTrue(valueNotEq.satisfyInteger(100, 51));
  }

  @Test
  public void testIsNotNull() {
    Filter isNotNullFilter = ValueFilterApi.isNotNull(DEFAULT_MEASUREMENT_INDEX);
    Assert.assertFalse(isNotNullFilter.satisfyRow(100, new Object[] {null}));
    Assert.assertTrue(isNotNullFilter.satisfyRow(100, new Object[] {1}));
  }

  @Test
  public void testAndOr() {
    Filter andFilter =
        FilterFactory.and(
            TimeFilterApi.gt(100L),
            ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 50.9, TSDataType.DOUBLE));
    Assert.assertTrue(andFilter.satisfyDouble(101L, 50d));
    Assert.assertFalse(andFilter.satisfyDouble(101L, 60d));
    Assert.assertFalse(andFilter.satisfyDouble(99L, 50d));

    Filter orFilter = FilterFactory.or(andFilter, TimeFilterApi.eq(1000L));
    Assert.assertTrue(orFilter.satisfyDouble(101L, 50d));
    Assert.assertFalse(orFilter.satisfyDouble(101L, 60d));
    Assert.assertTrue(orFilter.satisfyDouble(1000L, 50d));

    Filter andFilter2 =
        FilterFactory.and(
            orFilter, ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 50.0, TSDataType.DOUBLE));
    Assert.assertFalse(andFilter2.satisfyDouble(101L, 50d));
    Assert.assertFalse(andFilter2.satisfyDouble(101L, 60d));
    Assert.assertTrue(andFilter2.satisfyDouble(1000L, 51d));
  }

  @Test(expected = ClassCastException.class)
  public void testWrongUsage() {
    Filter andFilter =
        FilterFactory.and(
            TimeFilterApi.gt(100L),
            ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, true, TSDataType.INT32));
    andFilter.satisfyInteger(101L, 50);
    Assert.fail();
  }
}
