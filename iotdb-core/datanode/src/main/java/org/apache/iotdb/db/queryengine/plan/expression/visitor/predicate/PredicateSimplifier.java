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
import org.apache.iotdb.db.queryengine.plan.expression.binary.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionAnalyzeVisitor;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.ConvertPredicateToFilterVisitor.getValue;

public class PredicateSimplifier extends ExpressionAnalyzeVisitor<Expression, Void> {

  @Override
  public Expression visitIsNullExpression(IsNullExpression isNullExpression, Void context) {
    Expression simplifiedInputExpression = process(isNullExpression.getExpression(), context);
    if (simplifiedInputExpression.getExpressionType().equals(ExpressionType.NULL)) {
      if (isNullExpression.isNot()) {
        return ConstantOperand.FALSE;
      } else {
        return ConstantOperand.TRUE;
      }
    }
    isNullExpression.setExpression(simplifiedInputExpression);
    return isNullExpression;
  }

  @Override
  public Expression visitInExpression(InExpression inExpression, Void context) {
    return processFilterUnaryExpression(inExpression, context);
  }

  @Override
  public Expression visitLikeExpression(LikeExpression likeExpression, Void context) {
    return processFilterUnaryExpression(likeExpression, context);
  }

  @Override
  public Expression visitRegularExpression(RegularExpression regularExpression, Void context) {
    return processFilterUnaryExpression(regularExpression, context);
  }

  private Expression processFilterUnaryExpression(UnaryExpression unaryExpression, Void context) {
    Expression simplifiedInputExpression = process(unaryExpression.getExpression(), context);
    if (simplifiedInputExpression.getExpressionType().equals(ExpressionType.NULL)) {
      return ConstantOperand.FALSE;
    }
    unaryExpression.setExpression(simplifiedInputExpression);
    return unaryExpression;
  }

  @Override
  public Expression visitLogicNotExpression(LogicNotExpression logicNotExpression, Void context) {
    Expression simplifiedInputExpression = process(logicNotExpression.getExpression(), context);
    if (simplifiedInputExpression.equals(ConstantOperand.TRUE)) {
      return ConstantOperand.FALSE;
    }
    if (simplifiedInputExpression.equals(ConstantOperand.FALSE)) {
      return ConstantOperand.TRUE;
    }
    logicNotExpression.setExpression(simplifiedInputExpression);
    return logicNotExpression;
  }

  @Override
  public Expression visitNegationExpression(NegationExpression negationExpression, Void context) {
    Expression simplifiedInputExpression = process(negationExpression.getExpression(), context);
    if (simplifiedInputExpression.getExpressionType().equals(ExpressionType.NULL)) {
      return new NullOperand();
    }
    negationExpression.setExpression(simplifiedInputExpression);
    return negationExpression;
  }

  @Override
  public Expression visitArithmeticBinaryExpression(
      ArithmeticBinaryExpression arithmeticBinaryExpression, Void context) {
    Expression simplifiedLeft = process(arithmeticBinaryExpression.getLeftExpression(), context);
    Expression simplifiedRight = process(arithmeticBinaryExpression.getRightExpression(), context);
    boolean leftIsNull = simplifiedLeft.getExpressionType().equals(ExpressionType.NULL);
    boolean rightIsNull = simplifiedRight.getExpressionType().equals(ExpressionType.NULL);
    if (leftIsNull || rightIsNull) {
      // null calculation will return null
      return new NullOperand();
    }
    arithmeticBinaryExpression.setLeftExpression(simplifiedLeft);
    arithmeticBinaryExpression.setRightExpression(simplifiedRight);
    return arithmeticBinaryExpression;
  }

  @Override
  public Expression visitLogicAndExpression(LogicAndExpression logicAndExpression, Void context) {
    Expression simplifiedLeft = process(logicAndExpression.getLeftExpression(), context);
    Expression simplifiedRight = process(logicAndExpression.getRightExpression(), context);
    boolean isLeftTrue = simplifiedLeft.equals(ConstantOperand.TRUE);
    boolean isRightTrue = simplifiedRight.equals(ConstantOperand.TRUE);
    if (isLeftTrue && isRightTrue) {
      return ConstantOperand.TRUE;
    } else if (isLeftTrue) {
      return simplifiedRight;
    } else if (isRightTrue) {
      return simplifiedLeft;
    }
    boolean isLeftFalse = simplifiedLeft.equals(ConstantOperand.FALSE);
    boolean isRightFalse = simplifiedRight.equals(ConstantOperand.FALSE);
    if (isLeftFalse || isRightFalse) {
      return ConstantOperand.FALSE;
    }
    logicAndExpression.setLeftExpression(simplifiedLeft);
    logicAndExpression.setRightExpression(simplifiedRight);
    return logicAndExpression;
  }

  @Override
  public Expression visitLogicOrExpression(LogicOrExpression logicOrExpression, Void context) {
    Expression simplifiedLeft = process(logicOrExpression.getLeftExpression(), context);
    Expression simplifiedRight = process(logicOrExpression.getRightExpression(), context);
    boolean isLeftTrue = simplifiedLeft.equals(ConstantOperand.TRUE);
    boolean isRightTrue = simplifiedRight.equals(ConstantOperand.TRUE);
    if (isRightTrue || isLeftTrue) {
      return ConstantOperand.TRUE;
    }
    boolean isLeftFalse = simplifiedLeft.equals(ConstantOperand.FALSE);
    boolean isRightFalse = simplifiedRight.equals(ConstantOperand.FALSE);
    if (isLeftFalse && isRightFalse) {
      return ConstantOperand.FALSE;
    } else if (isLeftFalse) {
      return simplifiedRight;
    } else if (isRightFalse) {
      return simplifiedLeft;
    }
    logicOrExpression.setLeftExpression(simplifiedLeft);
    logicOrExpression.setRightExpression(simplifiedRight);
    return logicOrExpression;
  }

  @Override
  public Expression visitCompareBinaryExpression(
      CompareBinaryExpression compareBinaryExpression, Void context) {
    Expression simplifiedLeft = process(compareBinaryExpression.getLeftExpression(), context);
    Expression simplifiedRight = process(compareBinaryExpression.getRightExpression(), context);
    boolean leftIsNull = simplifiedLeft.getExpressionType().equals(ExpressionType.NULL);
    boolean rightIsNull = simplifiedRight.getExpressionType().equals(ExpressionType.NULL);
    if (leftIsNull || rightIsNull) {
      // null comparison will return false
      return ConstantOperand.FALSE;
    }
    compareBinaryExpression.setLeftExpression(simplifiedLeft);
    compareBinaryExpression.setRightExpression(simplifiedRight);
    return compareBinaryExpression;
  }

  @Override
  public Expression visitBetweenExpression(BetweenExpression betweenExpression, Void context) {
    Expression simplifiedFirstExpression = process(betweenExpression.getFirstExpression(), context);
    Expression simplifiedSecondExpression =
        process(betweenExpression.getSecondExpression(), context);
    Expression simplifiedThirdExpression = process(betweenExpression.getThirdExpression(), context);
    boolean firstIsNull = simplifiedFirstExpression.getExpressionType().equals(ExpressionType.NULL);
    boolean secondIsNull =
        simplifiedSecondExpression.getExpressionType().equals(ExpressionType.NULL);
    boolean thirdIsNull = simplifiedThirdExpression.getExpressionType().equals(ExpressionType.NULL);
    if (firstIsNull || secondIsNull || thirdIsNull) {
      // null comparison will return false
      return ConstantOperand.FALSE;
    }

    boolean firstIsConstant = simplifiedFirstExpression.isConstantOperand();
    boolean secondIsConstant = simplifiedSecondExpression.isConstantOperand();
    boolean thirdIsConstant = simplifiedThirdExpression.isConstantOperand();
    boolean isNotBetween = betweenExpression.isNotBetween();

    if ((firstIsConstant
            && secondIsConstant
            && lessThan(
                (ConstantOperand) simplifiedFirstExpression,
                (ConstantOperand) simplifiedSecondExpression))
        || (firstIsConstant
            && thirdIsConstant
            && lessThan(
                (ConstantOperand) simplifiedThirdExpression,
                (ConstantOperand) simplifiedFirstExpression))) {
      if (isNotBetween) {
        // 1 NOT BETWEEN time AND 0 => TRUE
        // 1 NOT BETWEEN 2 AND time => TRUE
        return ConstantOperand.TRUE;
      } else {
        // 1 BETWEEN time AND 0 => FALSE
        // 1 BETWEEN 2 AND time => FALSE
        return ConstantOperand.FALSE;
      }
    }
    betweenExpression.setFirstExpression(simplifiedFirstExpression);
    betweenExpression.setSecondExpression(simplifiedSecondExpression);
    betweenExpression.setThirdExpression(simplifiedThirdExpression);
    return betweenExpression;
  }

  private <T extends Comparable<T>> boolean lessThan(
      ConstantOperand operand1, ConstantOperand operand2) {
    T value1 = getValue(operand1.getValueString(), operand1.getDataType());
    T value2 = getValue(operand2.getValueString(), operand2.getDataType());
    return value1.compareTo(value2) < 0;
  }

  @Override
  public Expression visitFunctionExpression(FunctionExpression functionExpression, Void context) {
    List<Expression> simplifiedInputExpressions = new ArrayList<>();
    for (Expression inputExpression : functionExpression.getExpressions()) {
      Expression simplifiedInputExpression = process(inputExpression, context);
      if (simplifiedInputExpression.getExpressionType().equals(ExpressionType.NULL)) {
        return new NullOperand();
      }
      simplifiedInputExpressions.add(simplifiedInputExpression);
    }
    functionExpression.setExpressions(simplifiedInputExpressions);
    return functionExpression;
  }

  @Override
  public Expression visitLeafOperand(LeafOperand leafOperand, Void context) {
    return leafOperand;
  }

  @Override
  public Expression visitCaseWhenThenExpression(
      CaseWhenThenExpression caseWhenThenExpression, Void context) {
    return caseWhenThenExpression;
  }

  @Override
  public Expression visitWhenThenExpression(WhenThenExpression whenThenExpression, Void context) {
    return whenThenExpression;
  }

  @Override
  public Expression visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Void context) {
    return groupByTimeExpression;
  }
}
