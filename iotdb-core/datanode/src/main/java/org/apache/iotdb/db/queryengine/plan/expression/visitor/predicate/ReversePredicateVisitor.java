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

import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;

import org.apache.tsfile.enums.TSDataType;

import java.util.function.Function;

public class ReversePredicateVisitor extends PredicateVisitor<Expression, Void> {

  private final Function<Expression, TSDataType> typeProvider;

  public ReversePredicateVisitor() {
    this(null);
  }

  public ReversePredicateVisitor(Function<Expression, TSDataType> typeProvider) {
    this.typeProvider = typeProvider;
  }

  @Override
  public Expression visitExpression(Expression expression, Void context) {
    return new LogicNotExpression(expression);
  }

  @Override
  public Expression visitInExpression(InExpression inExpression, Void context) {
    return new InExpression(
        inExpression.getExpression(), !inExpression.isNotIn(), inExpression.getValues());
  }

  @Override
  public Expression visitIsNullExpression(IsNullExpression isNullExpression, Void context) {
    return new IsNullExpression(isNullExpression.getExpression(), !isNullExpression.isNot());
  }

  @Override
  public Expression visitLikeExpression(LikeExpression likeExpression, Void context) {
    return new LikeExpression(
        likeExpression.getExpression(),
        likeExpression.getPattern(),
        likeExpression.getEscape(),
        !likeExpression.isNot());
  }

  @Override
  public Expression visitRegularExpression(RegularExpression regularExpression, Void context) {
    return new RegularExpression(
        regularExpression.getExpression(),
        regularExpression.getPatternString(),
        regularExpression.getPattern(),
        !regularExpression.isNot());
  }

  @Override
  public Expression visitLogicNotExpression(LogicNotExpression logicNotExpression, Void context) {
    return PredicateUtils.predicateRemoveNot(logicNotExpression.getExpression(), typeProvider);
  }

  @Override
  public Expression visitLogicAndExpression(LogicAndExpression logicAndExpression, Void context) {
    return ExpressionFactory.or(
        logicAndExpression.getLeftExpression().accept(this, context),
        logicAndExpression.getRightExpression().accept(this, context));
  }

  @Override
  public Expression visitLogicOrExpression(LogicOrExpression logicOrExpression, Void context) {
    return ExpressionFactory.and(
        logicOrExpression.getLeftExpression().accept(this, context),
        logicOrExpression.getRightExpression().accept(this, context));
  }

  @Override
  public Expression visitEqualToExpression(EqualToExpression equalToExpression, Void context) {
    return new NonEqualExpression(
        equalToExpression.getLeftExpression(), equalToExpression.getRightExpression());
  }

  @Override
  public Expression visitNonEqualExpression(NonEqualExpression nonEqualExpression, Void context) {
    return new EqualToExpression(
        nonEqualExpression.getLeftExpression(), nonEqualExpression.getRightExpression());
  }

  @Override
  public Expression visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Void context) {
    if (!canSafelyInvertOrderedComparison(greaterThanExpression)) {
      return new LogicNotExpression(greaterThanExpression);
    }
    return new LessEqualExpression(
        greaterThanExpression.getLeftExpression(), greaterThanExpression.getRightExpression());
  }

  @Override
  public Expression visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Void context) {
    if (!canSafelyInvertOrderedComparison(greaterEqualExpression)) {
      return new LogicNotExpression(greaterEqualExpression);
    }
    return new LessThanExpression(
        greaterEqualExpression.getLeftExpression(), greaterEqualExpression.getRightExpression());
  }

  @Override
  public Expression visitLessThanExpression(LessThanExpression lessThanExpression, Void context) {
    if (!canSafelyInvertOrderedComparison(lessThanExpression)) {
      return new LogicNotExpression(lessThanExpression);
    }
    return new GreaterEqualExpression(
        lessThanExpression.getLeftExpression(), lessThanExpression.getRightExpression());
  }

  @Override
  public Expression visitLessEqualExpression(
      LessEqualExpression lessEqualExpression, Void context) {
    if (!canSafelyInvertOrderedComparison(lessEqualExpression)) {
      return new LogicNotExpression(lessEqualExpression);
    }
    return new GreaterThanExpression(
        lessEqualExpression.getLeftExpression(), lessEqualExpression.getRightExpression());
  }

  @Override
  public Expression visitBetweenExpression(BetweenExpression betweenExpression, Void context) {
    if (!canSafelyInvertBetween(betweenExpression)) {
      return new LogicNotExpression(betweenExpression);
    }
    return new BetweenExpression(
        betweenExpression.getFirstExpression(),
        betweenExpression.getSecondExpression(),
        betweenExpression.getThirdExpression(),
        !betweenExpression.isNotBetween());
  }

  @Override
  public Expression visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Void context) {
    return new LogicNotExpression(groupByTimeExpression);
  }

  private boolean canSafelyInvertOrderedComparison(Expression comparisonExpression) {
    BinaryExpression binaryExpression = (BinaryExpression) comparisonExpression;
    TSDataType leftType = getType(binaryExpression.getLeftExpression());
    TSDataType rightType = getType(binaryExpression.getRightExpression());
    return leftType != null
        && rightType != null
        && !isFloatingPoint(leftType)
        && !isFloatingPoint(rightType);
  }

  private boolean canSafelyInvertBetween(BetweenExpression betweenExpression) {
    TSDataType firstType = getType(betweenExpression.getFirstExpression());
    TSDataType secondType = getType(betweenExpression.getSecondExpression());
    TSDataType thirdType = getType(betweenExpression.getThirdExpression());
    return firstType != null
        && secondType != null
        && thirdType != null
        && !isFloatingPoint(firstType)
        && !isFloatingPoint(secondType)
        && !isFloatingPoint(thirdType);
  }

  private boolean isFloatingPoint(TSDataType type) {
    return type == TSDataType.FLOAT || type == TSDataType.DOUBLE;
  }

  private TSDataType getType(Expression expression) {
    if (typeProvider != null) {
      TSDataType type = typeProvider.apply(expression);
      if (type != null) {
        return type;
      }
    }
    if (expression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return TSDataType.INT64;
    }
    if (expression.getExpressionType().equals(ExpressionType.CONSTANT)) {
      return ((ConstantOperand) expression).getDataType();
    }
    return null;
  }
}
