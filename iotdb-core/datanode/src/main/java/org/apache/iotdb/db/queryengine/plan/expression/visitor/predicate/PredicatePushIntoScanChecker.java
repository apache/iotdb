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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;

import static org.apache.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;

public class PredicatePushIntoScanChecker extends PredicateVisitor<Boolean, Void> {

  @Override
  public Boolean visitExpression(Expression expression, Void context) {
    return false;
  }

  @Override
  public Boolean visitInExpression(InExpression inExpression, Void context) {
    Expression inputExpression = inExpression.getExpression();
    return checkOperand(inputExpression);
  }

  @Override
  public Boolean visitIsNullExpression(IsNullExpression isNullExpression, Void context) {
    if (!isNullExpression.isNot()) {
      throw new IllegalArgumentException("IS NULL can be pushed down");
    }
    Expression inputExpression = isNullExpression.getExpression();
    if (inputExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new IllegalArgumentException("TIMESTAMP does not support IS NULL/IS NOT NULL");
    }
    return inputExpression.getExpressionType().equals(ExpressionType.TIMESERIES);
  }

  @Override
  public Boolean visitLikeExpression(LikeExpression likeExpression, Void context) {
    Expression inputExpression = likeExpression.getExpression();
    if (inputExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new IllegalArgumentException("TIMESTAMP does not support LIKE/NOT LIKE");
    }
    return inputExpression.getExpressionType().equals(ExpressionType.TIMESERIES);
  }

  @Override
  public Boolean visitRegularExpression(RegularExpression regularExpression, Void context) {
    Expression inputExpression = regularExpression.getExpression();
    if (inputExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new IllegalArgumentException("TIMESTAMP does not support REGEXP/NOT REGEXP");
    }
    return inputExpression.getExpressionType().equals(ExpressionType.TIMESERIES);
  }

  @Override
  public Boolean visitLogicNotExpression(LogicNotExpression logicNotExpression, Void context) {
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG);
  }

  @Override
  public Boolean visitLogicAndExpression(LogicAndExpression logicAndExpression, Void context) {
    return process(logicAndExpression.getLeftExpression(), context)
        && process(logicAndExpression.getRightExpression(), context);
  }

  @Override
  public Boolean visitLogicOrExpression(LogicOrExpression logicOrExpression, Void context) {
    return process(logicOrExpression.getLeftExpression(), context)
        && process(logicOrExpression.getRightExpression(), context);
  }

  @Override
  public Boolean visitEqualToExpression(EqualToExpression equalToExpression, Void context) {
    return processCompareBinaryExpression(equalToExpression);
  }

  @Override
  public Boolean visitNonEqualExpression(NonEqualExpression nonEqualExpression, Void context) {
    return processCompareBinaryExpression(nonEqualExpression);
  }

  @Override
  public Boolean visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Void context) {
    return processCompareBinaryExpression(greaterThanExpression);
  }

  @Override
  public Boolean visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Void context) {
    return processCompareBinaryExpression(greaterEqualExpression);
  }

  @Override
  public Boolean visitLessThanExpression(LessThanExpression lessThanExpression, Void context) {
    return processCompareBinaryExpression(lessThanExpression);
  }

  @Override
  public Boolean visitLessEqualExpression(LessEqualExpression lessEqualExpression, Void context) {
    return processCompareBinaryExpression(lessEqualExpression);
  }

  private Boolean processCompareBinaryExpression(CompareBinaryExpression compareBinaryExpression) {
    Expression leftExpression = compareBinaryExpression.getLeftExpression();
    Expression rightExpression = compareBinaryExpression.getRightExpression();
    return (checkOperand(leftExpression)
            && rightExpression.getExpressionType().equals(ExpressionType.CONSTANT))
        || (checkOperand(rightExpression)
            && leftExpression.getExpressionType().equals(ExpressionType.CONSTANT));
  }

  @Override
  public Boolean visitBetweenExpression(BetweenExpression betweenExpression, Void context) {
    Expression firstExpression = betweenExpression.getFirstExpression();
    Expression secondExpression = betweenExpression.getSecondExpression();
    Expression thirdExpression = betweenExpression.getThirdExpression();

    return (checkOperand(firstExpression)
            && secondExpression.getExpressionType().equals(ExpressionType.CONSTANT)
            && thirdExpression.getExpressionType().equals(ExpressionType.CONSTANT))
        || (checkOperand(secondExpression)
            && firstExpression.getExpressionType().equals(ExpressionType.CONSTANT)
            && thirdExpression.getExpressionType().equals(ExpressionType.CONSTANT))
        || (checkOperand(thirdExpression)
            && firstExpression.getExpressionType().equals(ExpressionType.CONSTANT)
            && secondExpression.getExpressionType().equals(ExpressionType.CONSTANT));
  }

  private Boolean checkOperand(Expression expression) {
    return expression.getExpressionType().equals(ExpressionType.TIMESTAMP)
        || expression.getExpressionType().equals(ExpressionType.TIMESERIES);
  }

  @Override
  public Boolean visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Void context) {
    throw new IllegalArgumentException("GroupByTime filter cannot exist in value filter.");
  }
}
