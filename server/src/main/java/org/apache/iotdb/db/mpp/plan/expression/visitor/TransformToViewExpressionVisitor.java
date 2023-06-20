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
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.tsfile.utils.Pair;

import javax.ws.rs.NotSupportedException;

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
    throw new RuntimeException(
        new NotSupportedException(
            "visitExpression in TransformToViewExpressionVisitor is not supported."));
  }

  // region leaf operand
  @Override
  public ViewExpression visitLeafOperand(LeafOperand leafOperand, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

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
  public ViewExpression visitUnaryExpression(UnaryExpression unaryExpression, Void context) {
    throw new RuntimeException(new NotSupportedException("Can not construct abstract class."));
  }

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
        child, likeExpression.getPatternString(), likeExpression.getPattern());
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
        child, regularExpression.getPatternString(), regularExpression.getPattern());
  }
  // endregion

  // region Binary Expressions
  @Override
  public ViewExpression visitBinaryExpression(BinaryExpression binaryExpression, Void context) {
    if (binaryExpression instanceof ArithmeticBinaryExpression) {
      return this.visitArithmeticBinaryExpression(
          (ArithmeticBinaryExpression) binaryExpression, context);
    } else if (binaryExpression instanceof CompareBinaryExpression) {
      return this.visitCompareBinaryExpression((CompareBinaryExpression) binaryExpression, context);
    } else if (binaryExpression instanceof LogicBinaryExpression) {
      return this.visitLogicBinaryExpression((LogicBinaryExpression) binaryExpression, context);
    }
    throw new RuntimeException(
        new NotSupportedException(
            "unsupported expression type:" + binaryExpression.getExpressionType()));
  }

  private Pair<ViewExpression, ViewExpression> getExpressionsForBinaryExpression(
      BinaryExpression binaryExpression) {
    ViewExpression left = this.process(binaryExpression.getLeftExpression(), null);
    ViewExpression right = this.process(binaryExpression.getRightExpression(), null);
    return new Pair<>(left, right);
  }

  // region Binary : Arithmetic Binary Expression
  @Override
  public ViewExpression visitArithmeticBinaryExpression(
      ArithmeticBinaryExpression arithmeticBinaryExpression, Void context) {
    if (arithmeticBinaryExpression instanceof AdditionExpression) {
      return this.visitAdditionExpression((AdditionExpression) arithmeticBinaryExpression);
    } else if (arithmeticBinaryExpression instanceof DivisionExpression) {
      return this.visitDivisionExpression((DivisionExpression) arithmeticBinaryExpression);
    } else if (arithmeticBinaryExpression instanceof ModuloExpression) {
      return this.visitModuloExpression((ModuloExpression) arithmeticBinaryExpression);
    } else if (arithmeticBinaryExpression instanceof MultiplicationExpression) {
      return this.visitMultiplicationExpression(
          (MultiplicationExpression) arithmeticBinaryExpression);
    } else if (arithmeticBinaryExpression instanceof SubtractionExpression) {
      return this.visitSubtractionExpression((SubtractionExpression) arithmeticBinaryExpression);
    }
    throw new UnsupportedOperationException(
        "unsupported expression type:" + arithmeticBinaryExpression.getExpressionType());
  }

  public ViewExpression visitAdditionExpression(AdditionExpression additionExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(additionExpression);
    return new AdditionViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitDivisionExpression(DivisionExpression divisionExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(divisionExpression);
    return new DivisionViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitModuloExpression(ModuloExpression moduloExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(moduloExpression);
    return new ModuloViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitMultiplicationExpression(
      MultiplicationExpression multiplicationExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(multiplicationExpression);
    return new MultiplicationViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitSubtractionExpression(SubtractionExpression subtractionExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(subtractionExpression);
    return new SubtractionViewExpression(pair.left, pair.right);
  }
  // endregion

  // region Binary: Compare Binary Expression
  @Override
  public ViewExpression visitCompareBinaryExpression(
      CompareBinaryExpression compareBinaryExpression, Void context) {
    if (compareBinaryExpression instanceof EqualToExpression) {
      return this.visitEqualToExpression((EqualToExpression) compareBinaryExpression);
    } else if (compareBinaryExpression instanceof GreaterEqualExpression) {
      return this.visitGreaterEqualExpression((GreaterEqualExpression) compareBinaryExpression);
    } else if (compareBinaryExpression instanceof GreaterThanExpression) {
      return this.visitGreaterThanExpression((GreaterThanExpression) compareBinaryExpression);
    } else if (compareBinaryExpression instanceof LessEqualExpression) {
      return this.visitLessEqualExpression((LessEqualExpression) compareBinaryExpression);
    } else if (compareBinaryExpression instanceof LessThanExpression) {
      return this.visitLessThanExpression((LessThanExpression) compareBinaryExpression);
    } else if (compareBinaryExpression instanceof NonEqualExpression) {
      return this.visitNonEqualExpression((NonEqualExpression) compareBinaryExpression);
    }
    throw new UnsupportedOperationException(
        "unsupported expression type:" + compareBinaryExpression.getExpressionType());
  }

  public ViewExpression visitEqualToExpression(EqualToExpression equalToExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(equalToExpression);
    return new EqualToViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitGreaterEqualExpression(GreaterEqualExpression greaterEqualExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(greaterEqualExpression);
    return new GreaterEqualViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitGreaterThanExpression(GreaterThanExpression greaterThanExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(greaterThanExpression);
    return new GreaterThanViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitLessEqualExpression(LessEqualExpression lessEqualExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(lessEqualExpression);
    return new LessEqualViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitLessThanExpression(LessThanExpression lessThanExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(lessThanExpression);
    return new LessThanViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitNonEqualExpression(NonEqualExpression nonEqualExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(nonEqualExpression);
    return new NonEqualViewExpression(pair.left, pair.right);
  }
  // endregion

  // region Binary : Logic Binary Expression
  @Override
  public ViewExpression visitLogicBinaryExpression(
      LogicBinaryExpression logicBinaryExpression, Void context) {
    if (logicBinaryExpression instanceof LogicAndExpression) {
      return this.visitLogicAndExpression((LogicAndExpression) logicBinaryExpression);
    } else if (logicBinaryExpression instanceof LogicOrExpression) {
      return this.visitLogicOrExpression((LogicOrExpression) logicBinaryExpression);
    }
    throw new UnsupportedOperationException(
        "unsupported expression type:" + logicBinaryExpression.getExpressionType());
  }

  public ViewExpression visitLogicAndExpression(LogicAndExpression logicAndExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(logicAndExpression);
    return new LogicAndViewExpression(pair.left, pair.right);
  }

  public ViewExpression visitLogicOrExpression(LogicOrExpression logicOrExpression) {
    Pair<ViewExpression, ViewExpression> pair =
        this.getExpressionsForBinaryExpression(logicOrExpression);
    return new LogicOrViewExpression(pair.left, pair.right);
  }
  // endregion

  // endregion

  // region Ternary Expressions
  @Override
  public ViewExpression visitTernaryExpression(TernaryExpression ternaryExpression, Void context) {
    throw new NotSupportedException("Can not construct abstract class.");
  }

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
