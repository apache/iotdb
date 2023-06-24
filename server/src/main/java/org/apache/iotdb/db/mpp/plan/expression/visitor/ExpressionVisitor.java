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

package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.ArithmeticBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

/**
 * This class provides a visitor of {@link Expression}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 *
 * @param <R> The return type of the visit operation.
 * @param <C> The context information during visiting.
 */
public abstract class ExpressionVisitor<R, C> {

  public R process(Expression expression, C context) {
    return expression.accept(this, context);
  }

  public abstract R visitExpression(Expression expression, C context);

  public R visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    return visitExpression(unaryExpression, context);
  }

  public R visitInExpression(InExpression inExpression, C context) {
    return visitUnaryExpression(inExpression, context);
  }

  public R visitIsNullExpression(IsNullExpression isNullExpression, C context) {
    return visitUnaryExpression(isNullExpression, context);
  }

  public R visitLikeExpression(LikeExpression likeExpression, C context) {
    return visitUnaryExpression(likeExpression, context);
  }

  public R visitRegularExpression(RegularExpression regularExpression, C context) {
    return visitUnaryExpression(regularExpression, context);
  }

  public R visitLogicNotExpression(LogicNotExpression logicNotExpression, C context) {
    return visitUnaryExpression(logicNotExpression, context);
  }

  public R visitNegationExpression(NegationExpression negationExpression, C context) {
    return visitUnaryExpression(negationExpression, context);
  }

  public R visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    return visitExpression(binaryExpression, context);
  }

  public R visitArithmeticBinaryExpression(
      ArithmeticBinaryExpression arithmeticBinaryExpression, C context) {
    return visitBinaryExpression(arithmeticBinaryExpression, context);
  }

  public R visitAdditionExpression(AdditionExpression additionExpression, C context) {
    return visitArithmeticBinaryExpression(additionExpression, context);
  }

  public R visitSubtractionExpression(SubtractionExpression subtractionExpression, C context) {
    return visitArithmeticBinaryExpression(subtractionExpression, context);
  }

  public R visitMultiplicationExpression(
      MultiplicationExpression multiplicationExpression, C context) {
    return visitArithmeticBinaryExpression(multiplicationExpression, context);
  }

  public R visitDivisionExpression(DivisionExpression divisionExpression, C context) {
    return visitArithmeticBinaryExpression(divisionExpression, context);
  }

  public R visitModuloExpression(ModuloExpression moduloExpression, C context) {
    return visitArithmeticBinaryExpression(moduloExpression, context);
  }

  public R visitLogicBinaryExpression(LogicBinaryExpression logicBinaryExpression, C context) {
    return visitBinaryExpression(logicBinaryExpression, context);
  }

  public R visitLogicAndExpression(LogicAndExpression logicAndExpression, C context) {
    return visitLogicBinaryExpression(logicAndExpression, context);
  }

  public R visitLogicOrExpression(LogicOrExpression logicOrExpression, C context) {
    return visitLogicBinaryExpression(logicOrExpression, context);
  }

  public R visitCompareBinaryExpression(
      CompareBinaryExpression compareBinaryExpression, C context) {
    return visitBinaryExpression(compareBinaryExpression, context);
  }

  public R visitEqualToExpression(EqualToExpression equalToExpression, C context) {
    return visitCompareBinaryExpression(equalToExpression, context);
  }

  public R visitNonEqualExpression(NonEqualExpression nonEqualExpression, C context) {
    return visitCompareBinaryExpression(nonEqualExpression, context);
  }

  public R visitGreaterThanExpression(GreaterThanExpression greaterThanExpression, C context) {
    return visitCompareBinaryExpression(greaterThanExpression, context);
  }

  public R visitGreaterEqualExpression(GreaterEqualExpression greaterEqualExpression, C context) {
    return visitCompareBinaryExpression(greaterEqualExpression, context);
  }

  public R visitLessThanExpression(LessThanExpression lessThanExpression, C context) {
    return visitCompareBinaryExpression(lessThanExpression, context);
  }

  public R visitLessEqualExpression(LessEqualExpression lessEqualExpression, C context) {
    return visitCompareBinaryExpression(lessEqualExpression, context);
  }

  public R visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    return visitExpression(ternaryExpression, context);
  }

  public R visitBetweenExpression(BetweenExpression betweenExpression, C context) {
    return visitTernaryExpression(betweenExpression, context);
  }

  public R visitFunctionExpression(FunctionExpression functionExpression, C context) {
    return visitExpression(functionExpression, context);
  }

  public R visitLeafOperand(LeafOperand leafOperand, C context) {
    return visitExpression(leafOperand, context);
  }

  public R visitTimeStampOperand(TimestampOperand timestampOperand, C context) {
    return visitLeafOperand(timestampOperand, context);
  }

  public R visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, C context) {
    return visitLeafOperand(timeSeriesOperand, context);
  }

  public R visitConstantOperand(ConstantOperand constantOperand, C context) {
    return visitLeafOperand(constantOperand, context);
  }

  public R visitNullOperand(NullOperand nullOperand, C context) {
    return visitLeafOperand(nullOperand, context);
  }

  public R visitCaseWhenThenExpression(CaseWhenThenExpression caseWhenThenExpression, C context) {
    return visitExpression(caseWhenThenExpression, context);
  }

  public R visitWhenThenExpression(WhenThenExpression whenThenExpression, C context) {
    return visitBinaryExpression(whenThenExpression, context);
  }
}
