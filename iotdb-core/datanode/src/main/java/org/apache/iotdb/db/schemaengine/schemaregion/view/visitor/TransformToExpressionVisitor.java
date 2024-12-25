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

package org.apache.iotdb.db.schemaengine.schemaregion.view.visitor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.BinaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.AdditionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.ArithmeticBinaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.DivisionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.ModuloViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.MultiplicationViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.SubtractionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.CompareBinaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.EqualToViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.GreaterEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.GreaterThanViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.LessEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.LessThanViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.NonEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.logic.LogicAndViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.logic.LogicBinaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.logic.LogicOrViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.LeafViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.NullViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimestampViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.multi.FunctionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ternary.BetweenViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ternary.TernaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.InViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.IsNullViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.LikeViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.LogicNotViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.NegationViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.RegularViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.UnaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.AdditionExpression;
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

import org.apache.tsfile.utils.Pair;

import javax.ws.rs.NotSupportedException;

import java.util.ArrayList;
import java.util.List;

public class TransformToExpressionVisitor extends ViewExpressionVisitor<Expression, Void> {

  @Override
  public Expression process(ViewExpression viewExpression, Void context) {
    return viewExpression.accept(this, context);
  }

  @Override
  public Expression visitExpression(ViewExpression expression, Void context) {
    throw new RuntimeException(
        new NotSupportedException(
            "visitExpression in TransformToExpressionVisitor is not supported."));
  }

  // region leaf operand
  @Override
  public Expression visitLeafOperand(LeafViewOperand leafViewOperand, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

  @Override
  public Expression visitConstantOperand(ConstantViewOperand constantOperand, Void context) {
    return new ConstantOperand(constantOperand.getDataType(), constantOperand.getValueString());
  }

  @Override
  public Expression visitNullOperand(NullViewOperand nullOperand, Void context) {
    return new NullOperand();
  }

  @Override
  public Expression visitTimeSeriesOperand(TimeSeriesViewOperand timeSeriesOperand, Void context) {
    try {
      PartialPath path = new PartialPath(timeSeriesOperand.getPathString());
      return new TimeSeriesOperand(path);
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Expression visitTimeStampOperand(TimestampViewOperand timestampOperand, Void context) {
    return new TimestampOperand();
  }

  // endregion

  // region Unary Expressions
  @Override
  public Expression visitUnaryExpression(UnaryViewExpression unaryViewExpression, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

  @Override
  public Expression visitInExpression(InViewExpression inExpression, Void context) {
    Expression child = this.process(inExpression.getExpression(), context);
    return new org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression(
        child, inExpression.isNotIn(), inExpression.getValuesInLinkedHashSet());
  }

  @Override
  public Expression visitIsNullExpression(IsNullViewExpression isNullExpression, Void context) {
    Expression child = this.process(isNullExpression.getExpression(), context);
    return new org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression(
        child, isNullExpression.isNot());
  }

  @Override
  public Expression visitLikeExpression(LikeViewExpression likeExpression, Void context) {
    Expression child = this.process(likeExpression.getExpression(), context);
    return new org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression(
        child, likeExpression.getPattern(), likeExpression.getEscape(), likeExpression.isNot());
  }

  @Override
  public Expression visitLogicNotExpression(
      LogicNotViewExpression logicNotExpression, Void context) {
    Expression child = this.process(logicNotExpression.getExpression(), context);
    return new org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression(child);
  }

  @Override
  public Expression visitNegationExpression(
      NegationViewExpression negationExpression, Void context) {
    Expression child = this.process(negationExpression.getExpression(), context);
    return new org.apache.iotdb.db.queryengine.plan.expression.unary.NegationExpression(child);
  }

  @Override
  public Expression visitRegularExpression(RegularViewExpression regularExpression, Void context) {
    Expression child = this.process(regularExpression.getExpression(), context);
    return new org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression(
        child,
        regularExpression.getPatternString(),
        regularExpression.getPattern(),
        regularExpression.isNot());
  }

  // endregion

  @Override
  // region Binary Expressions
  public Expression visitBinaryExpression(BinaryViewExpression binaryViewExpression, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

  private Pair<Expression, Expression> getExpressionsForBinaryExpression(
      BinaryViewExpression binaryViewExpression) {
    Expression left = this.process(binaryViewExpression.getLeftExpression(), null);
    Expression right = this.process(binaryViewExpression.getRightExpression(), null);
    return new Pair<>(left, right);
  }

  // region Binary : Arithmetic Binary Expression
  public Expression visitArithmeticBinaryExpression(
      ArithmeticBinaryViewExpression arithmeticBinaryExpression, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

  public Expression visitAdditionExpression(
      AdditionViewExpression additionExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(additionExpression);
    return new AdditionExpression(pair.left, pair.right);
  }

  public Expression visitDivisionExpression(
      DivisionViewExpression divisionExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(divisionExpression);
    return new DivisionExpression(pair.left, pair.right);
  }

  public Expression visitModuloExpression(ModuloViewExpression moduloExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(moduloExpression);
    return new ModuloExpression(pair.left, pair.right);
  }

  public Expression visitMultiplicationExpression(
      MultiplicationViewExpression multiplicationExpression, Void context) {
    Pair<Expression, Expression> pair =
        this.getExpressionsForBinaryExpression(multiplicationExpression);
    return new MultiplicationExpression(pair.left, pair.right);
  }

  public Expression visitSubtractionExpression(
      SubtractionViewExpression subtractionExpression, Void context) {
    Pair<Expression, Expression> pair =
        this.getExpressionsForBinaryExpression(subtractionExpression);
    return new SubtractionExpression(pair.left, pair.right);
  }

  // endregion

  // region Binary: Compare Binary Expression
  public Expression visitCompareBinaryExpression(
      CompareBinaryViewExpression compareBinaryExpression, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

  public Expression visitEqualToExpression(EqualToViewExpression equalToExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(equalToExpression);
    return new EqualToExpression(pair.left, pair.right);
  }

  public Expression visitGreaterEqualExpression(
      GreaterEqualViewExpression greaterEqualExpression, Void context) {
    Pair<Expression, Expression> pair =
        this.getExpressionsForBinaryExpression(greaterEqualExpression);
    return new GreaterEqualExpression(pair.left, pair.right);
  }

  public Expression visitGreaterThanExpression(
      GreaterThanViewExpression greaterThanExpression, Void context) {
    Pair<Expression, Expression> pair =
        this.getExpressionsForBinaryExpression(greaterThanExpression);
    return new GreaterThanExpression(pair.left, pair.right);
  }

  public Expression visitLessEqualExpression(
      LessEqualViewExpression lessEqualExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(lessEqualExpression);
    return new LessEqualExpression(pair.left, pair.right);
  }

  public Expression visitLessThanExpression(
      LessThanViewExpression lessThanExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(lessThanExpression);
    return new LessThanExpression(pair.left, pair.right);
  }

  public Expression visitNonEqualExpression(
      NonEqualViewExpression nonEqualExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(nonEqualExpression);
    return new NonEqualExpression(pair.left, pair.right);
  }

  // endregion

  // region Binary : Logic Binary Expression
  public Expression visitLogicBinaryExpression(
      LogicBinaryViewExpression logicBinaryExpression, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

  public Expression visitLogicAndExpression(
      LogicAndViewExpression logicAndExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(logicAndExpression);
    return new LogicAndExpression(pair.left, pair.right);
  }

  public Expression visitLogicOrExpression(LogicOrViewExpression logicOrExpression, Void context) {
    Pair<Expression, Expression> pair = this.getExpressionsForBinaryExpression(logicOrExpression);
    return new LogicOrExpression(pair.left, pair.right);
  }

  // endregion

  // endregion

  // region Ternary Expressions
  public Expression visitTernaryExpression(
      TernaryViewExpression ternaryViewExpression, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

  public Expression visitBetweenExpression(
      BetweenViewExpression betweenViewExpression, Void context) {
    Expression first = this.process(betweenViewExpression.getFirstExpression(), null);
    Expression second = this.process(betweenViewExpression.getSecondExpression(), null);
    Expression third = this.process(betweenViewExpression.getThirdExpression(), null);
    return new org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression(
        first, second, third, betweenViewExpression.isNotBetween());
  }

  // endregion

  // region FunctionExpression
  public Expression visitFunctionExpression(
      FunctionViewExpression functionViewExpression, Void context) {
    List<ViewExpression> viewExpressionList = functionViewExpression.getExpressions();
    List<Expression> expressionList = new ArrayList<>();
    for (ViewExpression viewExpression : viewExpressionList) {
      expressionList.add(this.process(viewExpression, null));
    }
    return new org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression(
        functionViewExpression.getFunctionName(),
        functionViewExpression.getFunctionAttributes(),
        expressionList);
  }
  // endregion

}
