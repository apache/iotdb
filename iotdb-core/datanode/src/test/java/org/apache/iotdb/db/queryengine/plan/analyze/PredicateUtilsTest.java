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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.GlobalTimePredicateExtractionResult;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;

import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.and;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.function;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.longValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.lt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.not;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.or;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.time;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PredicateUtilsTest {

  @Test
  public void testExtractTimePredicateFromAndWithoutMutatingInput() throws IllegalPathException {
    Expression timePredicate = gt(time(), longValue(1));
    Expression valuePredicate = gt(timeSeries("root.sg.d.s1"), intValue("1"));
    BinaryExpression predicate = and(timePredicate, valuePredicate);
    String originalExpressionString = predicate.getExpressionString();

    GlobalTimePredicateExtractionResult result =
        PredicateUtils.extractGlobalTimePredicate(predicate);

    assertSame(timePredicate, result.getGlobalTimePredicate());
    assertSame(valuePredicate, result.getResidualPredicate());
    assertTrue(result.hasValueFilter());
    assertSame(timePredicate, predicate.getLeftExpression());
    assertSame(valuePredicate, predicate.getRightExpression());
    assertEquals(originalExpressionString, predicate.getExpressionString());
  }

  @Test
  public void testExtractPureTimeOrWithoutMutatingInput() {
    Expression leftTimePredicate = gt(time(), longValue(1));
    Expression rightTimePredicate = lt(time(), longValue(10));
    BinaryExpression predicate = or(leftTimePredicate, rightTimePredicate);

    GlobalTimePredicateExtractionResult result =
        PredicateUtils.extractGlobalTimePredicate(predicate);

    assertEquals(predicate, result.getGlobalTimePredicate());
    assertSame(ConstantOperand.TRUE, result.getResidualPredicate());
    assertFalse(result.hasValueFilter());
    assertSame(leftTimePredicate, predicate.getLeftExpression());
    assertSame(rightTimePredicate, predicate.getRightExpression());
  }

  @Test
  public void testMixedOrKeepsOriginalPredicateAsResidual() throws IllegalPathException {
    Expression leftTimePredicate = gt(time(), longValue(1));
    Expression rightTimePredicate = gt(time(), longValue(10));
    Expression leftValuePredicate = gt(timeSeries("root.sg.d.s1"), intValue("1"));
    Expression rightValuePredicate = gt(timeSeries("root.sg.d.s2"), intValue("2"));
    BinaryExpression left = and(leftTimePredicate, leftValuePredicate);
    BinaryExpression right = and(rightTimePredicate, rightValuePredicate);
    BinaryExpression predicate = or(left, right);

    GlobalTimePredicateExtractionResult result =
        PredicateUtils.extractGlobalTimePredicate(predicate);

    assertEquals(or(leftTimePredicate, rightTimePredicate), result.getGlobalTimePredicate());
    assertSame(predicate, result.getResidualPredicate());
    assertTrue(result.hasValueFilter());
    assertSame(left, predicate.getLeftExpression());
    assertSame(right, predicate.getRightExpression());
  }

  @Test
  public void testMixedNotDoesNotExtractTimePredicate() throws IllegalPathException {
    Expression timePredicate = gt(time(), longValue(1));
    Expression valuePredicate = gt(timeSeries("root.sg.d.s1"), intValue("1"));
    BinaryExpression child = and(timePredicate, valuePredicate);
    LogicNotExpression predicate = not(child);

    GlobalTimePredicateExtractionResult result =
        PredicateUtils.extractGlobalTimePredicate(predicate);

    assertNull(result.getGlobalTimePredicate());
    assertSame(predicate, result.getResidualPredicate());
    assertTrue(result.hasValueFilter());
    assertSame(child, predicate.getExpression());
  }

  @Test
  public void testPureTimeNotCanBeExtracted() {
    Expression timePredicate = gt(time(), longValue(1));
    LogicNotExpression predicate = not(timePredicate);

    GlobalTimePredicateExtractionResult result =
        PredicateUtils.extractGlobalTimePredicate(predicate);

    assertEquals(predicate, result.getGlobalTimePredicate());
    assertSame(ConstantOperand.TRUE, result.getResidualPredicate());
    assertFalse(result.hasValueFilter());
    assertSame(timePredicate, predicate.getExpression());
  }

  @Test
  public void testSimplifierUsesCopyOnWriteForUnaryExpression() throws IllegalPathException {
    Expression valuePredicate = gt(timeSeries("root.sg.d.s1"), intValue("1"));
    BinaryExpression reducibleChild = and(ConstantOperand.TRUE, valuePredicate);
    LogicNotExpression predicate = not(reducibleChild);

    Expression simplified = PredicateUtils.simplifyPredicate(predicate);

    assertNotSame(predicate, simplified);
    assertTrue(simplified instanceof LogicNotExpression);
    assertSame(valuePredicate, ((LogicNotExpression) simplified).getExpression());
    assertSame(reducibleChild, predicate.getExpression());
    assertSame(ConstantOperand.TRUE, reducibleChild.getLeftExpression());
    assertSame(valuePredicate, reducibleChild.getRightExpression());
  }

  @Test
  public void testSimplifierUsesCopyOnWriteForFunctionExpression() throws IllegalPathException {
    Expression valuePredicate = gt(timeSeries("root.sg.d.s1"), intValue("1"));
    BinaryExpression reducibleChild = and(ConstantOperand.TRUE, valuePredicate);
    FunctionExpression functionExpression = function("test", reducibleChild);

    Expression simplified = PredicateUtils.simplifyPredicate(functionExpression);

    assertNotSame(functionExpression, simplified);
    assertTrue(simplified instanceof FunctionExpression);
    assertSame(valuePredicate, ((FunctionExpression) simplified).getExpressions().get(0));
    assertSame(reducibleChild, functionExpression.getExpressions().get(0));
    assertSame(ConstantOperand.TRUE, reducibleChild.getLeftExpression());
  }

  @Test
  public void testSimplifierReusesUnchangedExpression() throws IllegalPathException {
    Expression predicate = gt(timeSeries("root.sg.d.s1"), intValue("1"));

    assertSame(predicate, PredicateUtils.simplifyPredicate(predicate));
  }
}
