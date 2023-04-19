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

package org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;

import java.util.List;
import java.util.stream.Collectors;

public class IdentifyOutputColumnTypeVisitor
    extends ExpressionAnalyzeVisitor<ResultColumn.ColumnType, Boolean> {
  @Override
  public ResultColumn.ColumnType visitTernaryExpression(
      TernaryExpression ternaryExpression, Boolean isRoot) {
    ResultColumn.ColumnType firstType = this.process(ternaryExpression.getFirstExpression(), false);
    ResultColumn.ColumnType secondType =
        this.process(ternaryExpression.getSecondExpression(), false);
    ResultColumn.ColumnType thirdType = this.process(ternaryExpression.getThirdExpression(), false);
    boolean rawFlag = false, aggregationFlag = false;
    if (firstType == ResultColumn.ColumnType.RAW
        || secondType == ResultColumn.ColumnType.RAW
        || thirdType == ResultColumn.ColumnType.RAW) {
      rawFlag = true;
    }
    if (firstType == ResultColumn.ColumnType.AGGREGATION
        || secondType == ResultColumn.ColumnType.AGGREGATION
        || thirdType == ResultColumn.ColumnType.AGGREGATION) {
      aggregationFlag = true;
    }
    if (rawFlag && aggregationFlag) {
      throw new SemanticException(
          "Raw data and aggregation result hybrid calculation is not supported.");
    }
    if (firstType == ResultColumn.ColumnType.CONSTANT
        && secondType == ResultColumn.ColumnType.CONSTANT
        && thirdType == ResultColumn.ColumnType.CONSTANT) {
      throw new SemanticException("Constant column is not supported.");
    }
    if (firstType != ResultColumn.ColumnType.CONSTANT) {
      return firstType;
    }
    if (secondType != ResultColumn.ColumnType.CONSTANT) {
      return secondType;
    }
    return thirdType;
  }

  @Override
  public ResultColumn.ColumnType visitBinaryExpression(
      BinaryExpression binaryExpression, Boolean isRoot) {
    ResultColumn.ColumnType leftType = this.process(binaryExpression.getLeftExpression(), false);
    ResultColumn.ColumnType rightType = this.process(binaryExpression.getRightExpression(), false);
    if ((leftType == ResultColumn.ColumnType.RAW
            && rightType == ResultColumn.ColumnType.AGGREGATION)
        || (leftType == ResultColumn.ColumnType.AGGREGATION
            && rightType == ResultColumn.ColumnType.RAW)) {
      throw new SemanticException(
          "Raw data and aggregation result hybrid calculation is not supported.");
    }
    if (isRoot
        && leftType == ResultColumn.ColumnType.CONSTANT
        && rightType == ResultColumn.ColumnType.CONSTANT) {
      throw new SemanticException("Constant column is not supported.");
    }
    if (leftType != ResultColumn.ColumnType.CONSTANT) {
      return leftType;
    }
    return rightType;
  }

  @Override
  public ResultColumn.ColumnType visitUnaryExpression(
      UnaryExpression unaryExpression, Boolean context) {
    return this.process(unaryExpression.getExpression(), false);
  }

  @Override
  public ResultColumn.ColumnType visitFunctionExpression(
      FunctionExpression functionExpression, Boolean context) {
    List<Expression> inputExpressions = functionExpression.getExpressions();
    if (functionExpression.isBuiltInAggregationFunctionExpression()) {
      for (Expression inputExpression : inputExpressions) {
        if (this.process(inputExpression, false) == ResultColumn.ColumnType.AGGREGATION) {
          throw new SemanticException(
              "Aggregation results cannot be as input of the aggregation function.");
        }
      }
      return ResultColumn.ColumnType.AGGREGATION;
    } else {
      ResultColumn.ColumnType checkedType = null;
      int lastCheckedIndex = 0;
      for (int i = 0; i < inputExpressions.size(); i++) {
        ResultColumn.ColumnType columnType = this.process(inputExpressions.get(i), false);
        if (columnType != ResultColumn.ColumnType.CONSTANT) {
          checkedType = columnType;
          lastCheckedIndex = i;
          break;
        }
      }
      if (checkedType == null) {
        throw new SemanticException(
            String.format("Input of '%s' is illegal.", functionExpression.getFunctionName()));
      }
      for (int i = lastCheckedIndex; i < inputExpressions.size(); i++) {
        ResultColumn.ColumnType columnType = this.process(inputExpressions.get(i), false);
        if (columnType != ResultColumn.ColumnType.CONSTANT && columnType != checkedType) {
          throw new SemanticException(
              String.format(
                  "Raw data and aggregation result hybrid input of '%s' is not supported.",
                  functionExpression.getFunctionName()));
        }
      }
      return checkedType;
    }
  }

  @Override
  public ResultColumn.ColumnType visitCaseWhenThenExpression(
      CaseWhenThenExpression caseExpression, Boolean context) {
    // first, get all subexpression's type
    List<ResultColumn.ColumnType> typeList =
        caseExpression.getExpressions().stream()
            .map(e -> this.process(e, false))
            .collect(Collectors.toList());
    // if at least one subexpression is RAW, I'm RAW too
    boolean rawFlag =
        typeList.stream().anyMatch(columnType -> columnType == ResultColumn.ColumnType.RAW);
    // if at least one subexpression is AGGREGATION, I'm AGGREGATION too
    boolean aggregationFlag =
        typeList.stream().anyMatch(columnType -> columnType == ResultColumn.ColumnType.AGGREGATION);
    // not allow RAW && AGGREGATION
    if (rawFlag && aggregationFlag) {
      throw new SemanticException(
          "Raw data and aggregation result hybrid calculation is not supported.");
    }
    // not allow all const
    boolean allConst =
        typeList.stream().allMatch(columnType -> columnType == ResultColumn.ColumnType.CONSTANT);
    if (allConst) {
      throw new SemanticException("Constant column is not supported.");
    }
    for (ResultColumn.ColumnType type : typeList) {
      if (type != ResultColumn.ColumnType.CONSTANT) {
        return type;
      }
    }
    throw new IllegalArgumentException("shouldn't attach here");
  }

  @Override
  public ResultColumn.ColumnType visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, Boolean context) {
    return ResultColumn.ColumnType.RAW;
  }

  @Override
  public ResultColumn.ColumnType visitTimeStampOperand(
      TimestampOperand timestampOperand, Boolean context) {
    return ResultColumn.ColumnType.RAW;
  }

  @Override
  public ResultColumn.ColumnType visitConstantOperand(
      ConstantOperand constantOperand, Boolean context) {
    return ResultColumn.ColumnType.CONSTANT;
  }

  @Override
  public ResultColumn.ColumnType visitNullOperand(NullOperand nullOperand, Boolean context) {
    return ResultColumn.ColumnType.CONSTANT;
  }
}
