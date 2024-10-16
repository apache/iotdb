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

package org.apache.iotdb.db.queryengine.plan.expression.visitor;

import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.AdditionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.DivisionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.ModuloViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.MultiplicationViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.SubtractionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.EqualToViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.GreaterEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.GreaterThanViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.LessEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.LessThanViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.NonEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.logic.LogicAndViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.logic.LogicOrViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.NullViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimestampViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.multi.FunctionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ternary.BetweenViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.InViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.IsNullViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.LikeViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.LogicNotViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.NegationViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.RegularViewExpression;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TransformToViewExpressionVisitor extends ExpressionVisitor<ViewExpression, Void> {

  @Override
  public ViewExpression process(Expression expression, Void context) {
    return expression.accept(this, context);
  }

  @Override
  public ViewExpression visitExpression(Expression expression, Void context) {
    throw new UnsupportedOperationException(
        "Unsupported expression type in TransformToViewExpressionVisitor: "
            + expression.getExpressionType());
  }

  // region leaf operand

  @Override
  public ViewExpression visitConstantOperand(ConstantOperand constantOperand, Void context) {
    return new ConstantViewOperand(constantOperand.getDataType(), constantOperand.getValueString());
  }

  @Override
  public ViewExpression visitNullOperand(NullOperand nullOperand, Void context) {
    return new NullViewOperand();
  }

  @Override
  public ViewExpression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
    return new TimeSeriesViewOperand(timeSeriesOperand.getPath().toString());
  }

  @Override
  public ViewExpression visitTimeStampOperand(TimestampOperand timestampOperand, Void context) {
    return new TimestampViewOperand();
  }

  // endregion

  // region Unary Expressions

  @Override
  public ViewExpression visitInExpression(InExpression inExpression, Void context) {
    ViewExpression child = this.process(inExpression.getExpression(), context);
    List<String> valueList = new ArrayList<>(inExpression.getValues());
    return new InViewExpression(child, inExpression.isNotIn(), valueList);
  }

  @Override
  public ViewExpression visitIsNullExpression(IsNullExpression isNullExpression, Void context) {
    ViewExpression child = this.process(isNullExpression.getExpression(), context);
    return new IsNullViewExpression(child, isNullExpression.isNot());
  }

  @Override
  public ViewExpression visitLikeExpression(LikeExpression likeExpression, Void context) {
    ViewExpression child = this.process(likeExpression.getExpression(), context);
    return new LikeViewExpression(
        child, likeExpression.getPattern(), likeExpression.getEscape(), likeExpression.isNot());
  }

  @Override
  public ViewExpression visitLogicNotExpression(
      LogicNotExpression logicNotExpression, Void context) {
    ViewExpression child = this.process(logicNotExpression.getExpression(), context);
    return new LogicNotViewExpression(child);
  }

  @Override
  public ViewExpression visitNegationExpression(
      NegationExpression negationExpression, Void context) {
    ViewExpression child = this.process(negationExpression.getExpression(), context);
    return new NegationViewExpression(child);
  }

  @Override
  public ViewExpression visitRegularExpression(RegularExpression regularExpression, Void context) {
    ViewExpression child = this.process(regularExpression.getExpression(), context);
    return new RegularViewExpression(
        child,
        regularExpression.getPatternString(),
        regularExpression.getPattern(),
        regularExpression.isNot());
  }

  // endregion

  // region Binary Expressions

  private Pair<ViewExpression, ViewExpression> getExpressionsForBinaryExpression(
      BinaryExpression binaryExpression) {
    ViewExpression left = this.process(binaryExpression.getLeftExpression(), null);
    ViewExpression right = this.process(binaryExpression.getRightExpression(), null);
    return new Pair<>(left, right);
  }

  // region Binary : Arithmetic Binary Expression
  @Override
  public ViewExpression visitAdditionExpression(
      AdditionExpression additionExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(additionExpression);
    return new AdditionViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitDivisionExpression(
      DivisionExpression divisionExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(divisionExpression);
    return new DivisionViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitModuloExpression(ModuloExpression moduloExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(moduloExpression);
    return new ModuloViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitMultiplicationExpression(
      MultiplicationExpression multiplicationExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(multiplicationExpression);
    return new MultiplicationViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitSubtractionExpression(
      SubtractionExpression subtractionExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(subtractionExpression);
    return new SubtractionViewExpression(pair.left, pair.right);
  }

  // endregion

  // region Binary: Compare Binary Expression

  @Override
  public ViewExpression visitEqualToExpression(EqualToExpression equalToExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(equalToExpression);
    return new EqualToViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(greaterEqualExpression);
    return new GreaterEqualViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(greaterThanExpression);
    return new GreaterThanViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitLessEqualExpression(
      LessEqualExpression lessEqualExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(lessEqualExpression);
    return new LessEqualViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitLessThanExpression(
      LessThanExpression lessThanExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(lessThanExpression);
    return new LessThanViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitNonEqualExpression(
      NonEqualExpression nonEqualExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(nonEqualExpression);
    return new NonEqualViewExpression(pair.left, pair.right);
  }

  // endregion

  // region Binary : Logic Binary Expression

  @Override
  public ViewExpression visitLogicAndExpression(
      LogicAndExpression logicAndExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(logicAndExpression);
    return new LogicAndViewExpression(pair.left, pair.right);
  }

  @Override
  public ViewExpression visitLogicOrExpression(LogicOrExpression logicOrExpression, Void context) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(logicOrExpression);
    return new LogicOrViewExpression(pair.left, pair.right);
  }

  // endregion

  // endregion

  // region Ternary Expressions

  @Override
  public ViewExpression visitBetweenExpression(BetweenExpression betweenExpression, Void context) {
    ViewExpression first = this.process(betweenExpression.getFirstExpression(), null);
    ViewExpression second = this.process(betweenExpression.getSecondExpression(), null);
    ViewExpression third = this.process(betweenExpression.getSecondExpression(), null);
    return new BetweenViewExpression(first, second, third, betweenExpression.isNotBetween());
  }

  // endregion

  // region FunctionExpression
  @Override
  public ViewExpression visitFunctionExpression(
      FunctionExpression functionExpression, Void context) {
    List<Expression> expressionList = functionExpression.getExpressions();
    List<ViewExpression> viewExpressionList = new ArrayList<>();
    for (Expression expression : expressionList) {
      viewExpressionList.add(this.process(expression, null));
    }
    LinkedHashMap<String, String> map = functionExpression.getFunctionAttributes();
    List<String> keyList = new ArrayList<>();
    List<String> valueList = new ArrayList<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      keyList.add(entry.getKey());
      valueList.add(entry.getValue());
    }
    return new FunctionViewExpression(
        functionExpression.getFunctionName(), keyList, valueList, viewExpressionList);
  }
  // endregion

}
