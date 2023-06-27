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

package org.apache.iotdb.commons.schema.view.viewExpression.visitor;

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

/**
 * This class provides a visitor of {@link ViewExpression}, which can be extended to create a
 * visitor which only needs to handle a subset of the available methods.
 *
 * @param <R> The return type of the visit operation.
 * @param <C> The context information during visiting.
 */
public abstract class ViewExpressionVisitor<R, C> {

  public R process(ViewExpression expression, C context) {
    return expression.accept(this, context);
  }

  public abstract R visitExpression(ViewExpression expression, C context);

  // region leaf operand
  public R visitLeafOperand(LeafViewOperand leafViewOperand, C context) {
    return visitExpression(leafViewOperand, context);
  }

  public R visitConstantOperand(ConstantViewOperand constantOperand, C context) {
    return visitLeafOperand(constantOperand, context);
  }

  public R visitNullOperand(NullViewOperand nullOperand, C context) {
    return visitLeafOperand(nullOperand, context);
  }

  public R visitTimeSeriesOperand(TimeSeriesViewOperand timeSeriesOperand, C context) {
    return visitLeafOperand(timeSeriesOperand, context);
  }

  public R visitTimeStampOperand(TimestampViewOperand timestampOperand, C context) {
    return visitLeafOperand(timestampOperand, context);
  }
  // endregion

  // region Unary Expressions
  public R visitUnaryExpression(UnaryViewExpression unaryViewExpression, C context) {
    return visitExpression(unaryViewExpression, context);
  }

  public R visitInExpression(InViewExpression inExpression, C context) {
    return visitUnaryExpression(inExpression, context);
  }

  public R visitIsNullExpression(IsNullViewExpression isNullExpression, C context) {
    return visitUnaryExpression(isNullExpression, context);
  }

  public R visitLikeExpression(LikeViewExpression likeExpression, C context) {
    return visitUnaryExpression(likeExpression, context);
  }

  public R visitLogicNotExpression(LogicNotViewExpression logicNotExpression, C context) {
    return visitUnaryExpression(logicNotExpression, context);
  }

  public R visitNegationExpression(NegationViewExpression negationExpression, C context) {
    return visitUnaryExpression(negationExpression, context);
  }

  public R visitRegularExpression(RegularViewExpression regularExpression, C context) {
    return visitUnaryExpression(regularExpression, context);
  }
  // endregion

  // region Binary Expressions
  public R visitBinaryExpression(BinaryViewExpression binaryViewExpression, C context) {
    return visitExpression(binaryViewExpression, context);
  }

  // region Binary : Arithmetic Binary Expression
  public R visitArithmeticBinaryExpression(
      ArithmeticBinaryViewExpression arithmeticBinaryExpression, C context) {
    return visitBinaryExpression(arithmeticBinaryExpression, context);
  }

  public R visitAdditionExpression(AdditionViewExpression additionExpression, C context) {
    return visitArithmeticBinaryExpression(additionExpression, context);
  }

  public R visitDivisionExpression(DivisionViewExpression divisionExpression, C context) {
    return visitArithmeticBinaryExpression(divisionExpression, context);
  }

  public R visitModuloExpression(ModuloViewExpression moduloExpression, C context) {
    return visitArithmeticBinaryExpression(moduloExpression, context);
  }

  public R visitMultiplicationExpression(
      MultiplicationViewExpression multiplicationExpression, C context) {
    return visitArithmeticBinaryExpression(multiplicationExpression, context);
  }

  public R visitSubtractionExpression(SubtractionViewExpression subtractionExpression, C context) {
    return visitArithmeticBinaryExpression(subtractionExpression, context);
  }
  // endregion

  // region Binary: Compare Binary Expression
  public R visitCompareBinaryExpression(
      CompareBinaryViewExpression compareBinaryExpression, C context) {
    return visitBinaryExpression(compareBinaryExpression, context);
  }

  public R visitEqualToExpression(EqualToViewExpression equalToExpression, C context) {
    return visitCompareBinaryExpression(equalToExpression, context);
  }

  public R visitGreaterEqualExpression(
      GreaterEqualViewExpression greaterEqualExpression, C context) {
    return visitCompareBinaryExpression(greaterEqualExpression, context);
  }

  public R visitGreaterThanExpression(GreaterThanViewExpression greaterThanExpression, C context) {
    return visitCompareBinaryExpression(greaterThanExpression, context);
  }

  public R visitLessEqualExpression(LessEqualViewExpression lessEqualExpression, C context) {
    return visitCompareBinaryExpression(lessEqualExpression, context);
  }

  public R visitLessThanExpression(LessThanViewExpression lessThanExpression, C context) {
    return visitCompareBinaryExpression(lessThanExpression, context);
  }

  public R visitNonEqualExpression(NonEqualViewExpression nonEqualExpression, C context) {
    return visitCompareBinaryExpression(nonEqualExpression, context);
  }
  // endregion

  // region Binary : Logic Binary Expression
  public R visitLogicBinaryExpression(LogicBinaryViewExpression logicBinaryExpression, C context) {
    return visitBinaryExpression(logicBinaryExpression, context);
  }

  public R visitLogicAndExpression(LogicAndViewExpression logicAndExpression, C context) {
    return visitLogicBinaryExpression(logicAndExpression, context);
  }

  public R visitLogicOrExpression(LogicOrViewExpression logicOrExpression, C context) {
    return visitLogicBinaryExpression(logicOrExpression, context);
  }
  // endregion

  // endregion

  // region Ternary Expressions
  public R visitTernaryExpression(TernaryViewExpression ternaryViewExpression, C context) {
    return visitExpression(ternaryViewExpression, context);
  }

  public R visitBetweenExpression(BetweenViewExpression betweenViewExpression, C context) {
    return visitTernaryExpression(betweenViewExpression, context);
  }
  // endregion

  // region FunctionExpression
  public R visitFunctionExpression(FunctionViewExpression functionViewExpression, C context) {
    return visitExpression(functionViewExpression, context);
  }
  // endregion
}
