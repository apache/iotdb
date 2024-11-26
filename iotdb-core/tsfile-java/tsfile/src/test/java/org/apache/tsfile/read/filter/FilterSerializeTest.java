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

import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TimeDuration;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.tsfile.common.conf.TSFileConfig.STRING_CHARSET;
import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;
import static org.junit.Assert.assertEquals;

public class FilterSerializeTest {

  @Test
  public void testTimeFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          TimeFilterApi.eq(1),
          TimeFilterApi.notEq(7),
          TimeFilterApi.gt(2),
          TimeFilterApi.gtEq(3),
          TimeFilterApi.lt(4),
          TimeFilterApi.ltEq(5),
          FilterFactory.not(TimeFilterApi.eq(6)),
          TimeFilterApi.in(new HashSet<>(Arrays.asList(1L, 2L))),
          TimeFilterApi.notIn(new HashSet<>(Arrays.asList(3L, 4L))),
          TimeFilterApi.between(1, 100),
          TimeFilterApi.notBetween(1, 100)
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testBinaryLogicalFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          FilterFactory.and(
              TimeFilterApi.eq(1),
              ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 1, TSDataType.INT32)),
          FilterFactory.or(
              ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 2L, TSDataType.INT64),
              FilterFactory.not(ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 6, TSDataType.INT32)))
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testGroupByFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          TimeFilterApi.groupBy(1, 2, 3, 4), TimeFilterApi.groupBy(4, 3, 2, 1),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testGroupByMonthFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          TimeFilterApi.groupByMonth(
              3,
              4,
              new TimeDuration(0, 1),
              new TimeDuration(0, 2),
              TimeZone.getTimeZone("Asia/Shanghai"),
              TimeUnit.MILLISECONDS),
          TimeFilterApi.groupByMonth(
              2,
              1,
              new TimeDuration(0, 4),
              new TimeDuration(0, 3),
              TimeZone.getTimeZone("Atlantic/Faeroe"),
              TimeUnit.MILLISECONDS),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testNullFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.isNull(DEFAULT_MEASUREMENT_INDEX),
          ValueFilterApi.isNotNull(DEFAULT_MEASUREMENT_INDEX)
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testBooleanFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, false, TSDataType.BOOLEAN),
          ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, false, TSDataType.BOOLEAN),
          ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, true, TSDataType.BOOLEAN),
          ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, true, TSDataType.BOOLEAN),
          ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, true, TSDataType.BOOLEAN),
          ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, true, TSDataType.BOOLEAN),
          ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, false, true, TSDataType.BOOLEAN),
          ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, false, true, TSDataType.BOOLEAN),
          ValueFilterApi.like(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("true", Optional.empty()),
              TSDataType.BOOLEAN),
          ValueFilterApi.notLike(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("true", Optional.empty()),
              TSDataType.BOOLEAN),
          ValueFilterApi.regexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("true"), TSDataType.BOOLEAN),
          ValueFilterApi.notRegexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("true"), TSDataType.BOOLEAN),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(true, false)),
              TSDataType.BOOLEAN),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(true, false)),
              TSDataType.BOOLEAN),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testIntegerFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 100, TSDataType.INT32),
          ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100, TSDataType.INT32),
          ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 100, TSDataType.INT32),
          ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 100, TSDataType.INT32),
          ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 100, TSDataType.INT32),
          ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 100, TSDataType.INT32),
          ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, 100, 200, TSDataType.INT32),
          ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, 100, 200, TSDataType.INT32),
          ValueFilterApi.like(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.INT32),
          ValueFilterApi.notLike(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.INT32),
          ValueFilterApi.regexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.INT32),
          ValueFilterApi.notRegexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.INT32),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(100, 200)), TSDataType.INT32),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(100, 200)), TSDataType.INT32),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testLongFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64),
          ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64),
          ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64),
          ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64),
          ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64),
          ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 100L, TSDataType.INT64),
          ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, 100L, 200L, TSDataType.INT64),
          ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, 100L, 200L, TSDataType.INT64),
          ValueFilterApi.like(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.INT64),
          ValueFilterApi.notLike(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.INT64),
          ValueFilterApi.regexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.INT64),
          ValueFilterApi.notRegexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.INT64),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(100L, 200L)),
              TSDataType.INT64),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(100L, 200L)),
              TSDataType.INT64),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testFloatFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 100.5f, TSDataType.FLOAT),
          ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100.5f, TSDataType.FLOAT),
          ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 100.5f, TSDataType.FLOAT),
          ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 100.5f, TSDataType.FLOAT),
          ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 100.5f, TSDataType.FLOAT),
          ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 100.5f, TSDataType.FLOAT),
          ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, 100.5f, 200.5f, TSDataType.FLOAT),
          ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, 100.5f, 200.5f, TSDataType.FLOAT),
          ValueFilterApi.like(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.FLOAT),
          ValueFilterApi.notLike(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.FLOAT),
          ValueFilterApi.regexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.FLOAT),
          ValueFilterApi.notRegexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.FLOAT),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(100.5f, 200.5f)),
              TSDataType.FLOAT),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(100.5f, 200.5f)),
              TSDataType.FLOAT),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testDoubleFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 100.5d, TSDataType.DOUBLE),
          ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 100.5d, TSDataType.DOUBLE),
          ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 100.5d, TSDataType.DOUBLE),
          ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 100.5d, TSDataType.DOUBLE),
          ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 100.5d, TSDataType.DOUBLE),
          ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 100.5d, TSDataType.DOUBLE),
          ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, 100.5d, 200.5d, TSDataType.DOUBLE),
          ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, 100.5d, 200.5d, TSDataType.DOUBLE),
          ValueFilterApi.like(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.DOUBLE),
          ValueFilterApi.notLike(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("1%", Optional.empty()),
              TSDataType.DOUBLE),
          ValueFilterApi.regexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.DOUBLE),
          ValueFilterApi.notRegexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), TSDataType.DOUBLE),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(100.5d, 200.5d)),
              TSDataType.DOUBLE),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(100.5d, 200.5d)),
              TSDataType.DOUBLE),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testBinaryFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.gt(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.TEXT),
          ValueFilterApi.gtEq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.TEXT),
          ValueFilterApi.lt(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.TEXT),
          ValueFilterApi.ltEq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.TEXT),
          ValueFilterApi.eq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.TEXT),
          ValueFilterApi.notEq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.TEXT),
          ValueFilterApi.between(
              DEFAULT_MEASUREMENT_INDEX,
              new Binary("test", STRING_CHARSET),
              new Binary("string", STRING_CHARSET),
              TSDataType.TEXT),
          ValueFilterApi.notBetween(
              DEFAULT_MEASUREMENT_INDEX,
              new Binary("test", STRING_CHARSET),
              new Binary("string", STRING_CHARSET),
              TSDataType.TEXT),
          ValueFilterApi.like(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("t%", Optional.empty()),
              TSDataType.TEXT),
          ValueFilterApi.notLike(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("t%", Optional.empty()),
              TSDataType.TEXT),
          ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("t.*"), TSDataType.TEXT),
          ValueFilterApi.notRegexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("t.*"), TSDataType.TEXT),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(
                  Arrays.asList(
                      new Binary("test", STRING_CHARSET), new Binary("string", STRING_CHARSET))),
              TSDataType.TEXT),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(
                  Arrays.asList(
                      new Binary("test", STRING_CHARSET), new Binary("string", STRING_CHARSET))),
              TSDataType.TEXT),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testStringFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.gt(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.STRING),
          ValueFilterApi.gtEq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.STRING),
          ValueFilterApi.lt(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.STRING),
          ValueFilterApi.ltEq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.STRING),
          ValueFilterApi.eq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.STRING),
          ValueFilterApi.notEq(
              DEFAULT_MEASUREMENT_INDEX, new Binary("test", STRING_CHARSET), TSDataType.STRING),
          ValueFilterApi.between(
              DEFAULT_MEASUREMENT_INDEX,
              new Binary("test", STRING_CHARSET),
              new Binary("string", STRING_CHARSET),
              TSDataType.STRING),
          ValueFilterApi.notBetween(
              DEFAULT_MEASUREMENT_INDEX,
              new Binary("test", STRING_CHARSET),
              new Binary("string", STRING_CHARSET),
              TSDataType.STRING),
          ValueFilterApi.like(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("t%", Optional.empty()),
              TSDataType.STRING),
          ValueFilterApi.notLike(
              DEFAULT_MEASUREMENT_INDEX,
              LikePattern.compile("t%", Optional.empty()),
              TSDataType.STRING),
          ValueFilterApi.regexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("t.*"), TSDataType.STRING),
          ValueFilterApi.notRegexp(
              DEFAULT_MEASUREMENT_INDEX, Pattern.compile("t.*"), TSDataType.STRING),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(
                  Arrays.asList(
                      new Binary("test", STRING_CHARSET), new Binary("string", STRING_CHARSET))),
              TSDataType.STRING),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(
                  Arrays.asList(
                      new Binary("test", STRING_CHARSET), new Binary("string", STRING_CHARSET))),
              TSDataType.STRING),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  private void validateSerialization(Filter filter) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    filter.serialize(dataOutputStream);

    ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());
    Filter serialized = Filter.deserialize(buffer);
    assertEquals(buffer.limit(), buffer.position());
    assertEquals(filter, serialized);
  }
}
