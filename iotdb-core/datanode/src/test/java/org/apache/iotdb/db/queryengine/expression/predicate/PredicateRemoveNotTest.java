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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.function.Function;

import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.predicateRemoveNot;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.reversePredicate;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.and;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.between;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.eq;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.function;
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
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;

public class PredicateRemoveNotTest {

  private static Function<Expression, TSDataType> typeProvider(TSDataType seriesType) {
    return expression ->
        expression instanceof TimeSeriesOperand
            ? seriesType
            : ((ConstantOperand) expression).getDataType();
  }

  private static final Function<Expression, TSDataType> DOUBLE_TYPE_PROVIDER =
      typeProvider(TSDataType.DOUBLE);

  @Test
  public void testReverse() {
    // test comparison
    Assert.assertEquals(gt(time(), longValue(1)), reversePredicate(lte(time(), longValue(1))));
    Assert.assertEquals(gte(time(), longValue(1)), reversePredicate(lt(time(), longValue(1))));
    Assert.assertEquals(lt(time(), longValue(1)), reversePredicate(gte(time(), longValue(1))));
    Assert.assertEquals(lte(time(), longValue(1)), reversePredicate(gt(time(), longValue(1))));
    Assert.assertEquals(eq(time(), longValue(1)), reversePredicate(neq(time(), longValue(1))));
    Assert.assertEquals(neq(time(), longValue(1)), reversePredicate(eq(time(), longValue(1))));

    // test range
    Assert.assertEquals(
        between(time(), longValue(1), longValue(100)),
        reversePredicate(notBetween(time(), longValue(1), longValue(100))));
    Assert.assertEquals(
        notBetween(time(), longValue(1), longValue(100)),
        reversePredicate(between(time(), longValue(1), longValue(100))));

    // test set
    LinkedHashSet<String> values = new LinkedHashSet<>(Arrays.asList("a", "b"));
    Assert.assertEquals(in(time(), values), reversePredicate(notIn(time(), values)));
    Assert.assertEquals(notIn(time(), values), reversePredicate(in(time(), values)));

    // test logical
    Assert.assertEquals(gt(time(), longValue(1)), reversePredicate(not(gt(time(), longValue(1)))));
    Assert.assertEquals(
        and(gt(time(), longValue(1)), lte(time(), longValue(1))),
        reversePredicate(or(lte(time(), longValue(1)), gt(time(), longValue(1)))));
    Assert.assertEquals(
        or(gt(time(), longValue(1)), lte(time(), longValue(1))),
        reversePredicate(and(lte(time(), longValue(1)), gt(time(), longValue(1)))));
  }

  @Test
  public void testRemoveNot() {
    Assert.assertEquals(
        lte(time(), longValue(1)), predicateRemoveNot(not(gt(time(), longValue(1)))));
    Assert.assertEquals(
        between(time(), longValue(1), longValue(100)),
        predicateRemoveNot(not(notBetween(time(), longValue(1), longValue(100)))));
    Assert.assertEquals(
        or(gt(time(), longValue(1)), lte(time(), longValue(1))),
        predicateRemoveNot(or(not(lte(time(), longValue(1))), not(gt(time(), longValue(1))))));
    Assert.assertEquals(
        and(gt(time(), longValue(1)), lte(time(), longValue(1))),
        predicateRemoveNot(and(not(lte(time(), longValue(1))), not(gt(time(), longValue(1))))));
  }

  @Test
  public void testRemoveNotKeepsUnsupportedNot() throws IllegalPathException {
    Expression booleanSeries = timeSeries("root.sg.d.s1");
    Expression predicate = not(booleanSeries);

    // The kept not() must be the original instance so callers can detect that nothing changed.
    Assert.assertSame(predicate, predicateRemoveNot(predicate));
  }

  @Test
  public void testRemoveNotKeepsFunctionPredicate() throws IllegalPathException {
    Expression predicate = not(function("udf", timeSeries("root.sg.d.s1")));

    Assert.assertSame(predicate, predicateRemoveNot(predicate));
  }

  @Test
  public void testRemoveNotKeepsBetweenOnDouble() throws IllegalPathException {
    Expression series = timeSeries("root.sg.d.s1");
    Expression predicate =
        not(
            between(
                series,
                new ConstantOperand(TSDataType.DOUBLE, "1.0"),
                new ConstantOperand(TSDataType.DOUBLE, "5.0")));

    Assert.assertSame(predicate, predicateRemoveNot(predicate, DOUBLE_TYPE_PROVIDER));
  }

  @Test
  public void testRemoveNotKeepsComparisonWithUnknownType() throws IllegalPathException {
    Expression predicate = not(gt(timeSeries("root.sg.d.s1"), longValue(1)));

    // An operand of unknown type cannot be inverted safely either.
    Assert.assertSame(predicate, predicateRemoveNot(predicate, expression -> null));
  }

  @Test
  public void testRemoveNotHandlesNestedNot() {
    Expression predicate = not(not(not(and(gt(time(), longValue(1)), lt(time(), longValue(10))))));
    Expression expected = or(lte(time(), longValue(1)), gte(time(), longValue(10)));

    Assert.assertEquals(expected, predicateRemoveNot(predicate));
  }

  @Test
  public void testRemoveNotUsesTypeToPreserveNaNSemantics() throws IllegalPathException {
    Expression series = timeSeries("root.sg.d.s1");
    Expression predicate = not(gt(series, new ConstantOperand(TSDataType.DOUBLE, "5.0")));

    Expression result = predicateRemoveNot(predicate, DOUBLE_TYPE_PROVIDER);

    Assert.assertSame(predicate, result);
  }
}
