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

package org.apache.iotdb.db.queryengine.expression.predicate;

import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.TimeZone;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.and;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.between;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.eq;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.groupByTime;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gte;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.in;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.longValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.lt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.lte;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.neq;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.not;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.notBetween;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.notIn;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.or;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.time;
import static org.apache.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;

public class ConvertPredicateToTimeFilterTest {

  @Test
  public void testNormal() {
    // test comparison
    testConvertToTimeFilter(eq(time(), longValue(1)), TimeFilterApi.eq(1));
    testConvertToTimeFilter(neq(time(), longValue(1)), TimeFilterApi.notEq(1));
    testConvertToTimeFilter(gt(time(), longValue(1)), TimeFilterApi.gt(1));
    testConvertToTimeFilter(gte(time(), longValue(1)), TimeFilterApi.gtEq(1));
    testConvertToTimeFilter(lt(time(), longValue(1)), TimeFilterApi.lt(1));
    testConvertToTimeFilter(lte(time(), longValue(1)), TimeFilterApi.ltEq(1));

    // test between
    testConvertToTimeFilter(
        between(time(), longValue(1), longValue(10)), TimeFilterApi.between(1, 10));
    testConvertToTimeFilter(
        notBetween(time(), longValue(1), longValue(10)), TimeFilterApi.notBetween(1, 10));
    testConvertToTimeFilter(between(longValue(1), time(), longValue(10)), TimeFilterApi.ltEq(1));
    testConvertToTimeFilter(notBetween(longValue(1), time(), longValue(10)), TimeFilterApi.gt(1));
    testConvertToTimeFilter(between(longValue(10), longValue(1), time()), TimeFilterApi.gtEq(10));
    testConvertToTimeFilter(notBetween(longValue(10), longValue(1), time()), TimeFilterApi.lt(10));

    // test in
    LinkedHashSet<String> stringValueSet = new LinkedHashSet<>(Arrays.asList("1", "2", "3"));
    LinkedHashSet<Long> valueSet = new LinkedHashSet<>(Arrays.asList(1L, 2L, 3L));
    testConvertToTimeFilter(in(time(), stringValueSet), TimeFilterApi.in(valueSet));
    testConvertToTimeFilter(notIn(time(), stringValueSet), TimeFilterApi.notIn(valueSet));

    // test and/or
    testConvertToTimeFilter(
        and(eq(time(), longValue(1L)), eq(time(), longValue(2L))),
        FilterFactory.and(TimeFilterApi.eq(1), TimeFilterApi.eq(2)));
    testConvertToTimeFilter(
        or(eq(time(), longValue(1L)), eq(time(), longValue(2L))),
        FilterFactory.or(TimeFilterApi.eq(1), TimeFilterApi.eq(2)));

    // test group by time
    testConvertToTimeFilter(groupByTime(1, 100, 10, 20), TimeFilterApi.groupBy(1, 100, 10, 20));

    GroupByTimeParameter parameter =
        new GroupByTimeParameter(
            1, 1000000000, new TimeDuration(1, 0), new TimeDuration(2, 0), true);
    testConvertToTimeFilter(
        groupByTime(parameter),
        TimeFilterApi.groupByMonth(
            parameter.getStartTime(),
            parameter.getEndTime(),
            parameter.getInterval(),
            parameter.getSlidingStep(),
            TimeZone.getTimeZone("+00:00"),
            TimestampPrecisionUtils.currPrecision));
  }

  @Test
  public void testRewrite() {
    LinkedHashSet<String> stringValueSet = new LinkedHashSet<>(Collections.singletonList("1"));
    testConvertToTimeFilter(in(time(), stringValueSet), TimeFilterApi.eq(1));
    testConvertToTimeFilter(notIn(time(), stringValueSet), TimeFilterApi.notEq(1));

    testConvertToTimeFilter(between(time(), longValue(1), longValue(1)), TimeFilterApi.eq(1));
    testConvertToTimeFilter(notBetween(time(), longValue(1), longValue(1)), TimeFilterApi.notEq(1));

    // TODO: consider the following cases future
    //    testConvertToTimeFilter(groupByTime(1, 100, 10, 10), TimeFilter.between(1, 100));
    //    testConvertToTimeFilter(groupByTime(1, 100, 10, 5), TimeFilter.between(1, 100));
    //    GroupByTimeParameter parameter1 =
    //        new GroupByTimeParameter(
    //            1, 1000000000, new TimeDuration(1, 0), new TimeDuration(1, 0), true);
    //    testConvertToTimeFilter(
    //        groupByTime(parameter1),
    //        TimeFilter.between(parameter1.getStartTime(), parameter1.getEndTime()));
    //    GroupByTimeParameter parameter2 =
    //        new GroupByTimeParameter(
    //            1, 1000000000, new TimeDuration(2, 0), new TimeDuration(1, 0), true);
    //    testConvertToTimeFilter(
    //        groupByTime(parameter2),
    //        TimeFilter.between(parameter2.getStartTime(), parameter2.getEndTime()));
  }

  @Test
  public void testNot() {
    try {
      PredicateUtils.convertPredicateToTimeFilter(not(eq(time(), longValue(1L))));
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(CONTAIN_NOT_ERR_MSG, e.getMessage());
    }
  }

  private void testConvertToTimeFilter(Expression predicate, Filter expectedFilter) {
    Assert.assertEquals(expectedFilter, PredicateUtils.convertPredicateToTimeFilter(predicate));
  }
}
