/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.db.mpp.execution.operator.window.IWindow;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

import java.util.List;
import java.util.Map;

public class CountIfAccumulator implements Accumulator {

  // number of the point segment that satisfies the KEEP expression
  private long countValue = 0;

  // number of the continues data points satisfy IF expression
  private long keep;

  private final Evaluator keepEvaluator;

  private final boolean ignoreNull;

  @FunctionalInterface
  private interface Evaluator {
    boolean evaluate();
  }

  public CountIfAccumulator(
      List<Expression> inputExpressions, Map<String, String> inputAttributes) {
    this.keepEvaluator = initKeepEvaluator(inputExpressions.get(1));
    this.ignoreNull = Boolean.parseBoolean(inputAttributes.getOrDefault("ignoreNull", "true"));
  }

  private Evaluator initKeepEvaluator(Expression keepExpression) {
    // We have check semantic in FE,
    // keep expression must be ConstantOperand or CompareBinaryExpression here
    if (keepExpression instanceof ConstantOperand) {
      return () -> keep >= Long.parseLong(keepExpression.toString());
    } else {
      long constant =
          Long.parseLong(
              ((CompareBinaryExpression) keepExpression)
                  .getRightExpression()
                  .getExpressionString());
      switch (keepExpression.getExpressionType()) {
        case LESS_THAN:
          return () -> keep < constant;
        case LESS_EQUAL:
          return () -> keep <= constant;
        case GREATER_THAN:
          return () -> keep > constant;
        case GREATER_EQUAL:
          return () -> keep >= constant;
        case EQUAL_TO:
          return () -> keep == constant;
        case NON_EQUAL:
          return () -> keep != constant;
        default:
          throw new IllegalArgumentException(
              "unsupported expression type: " + keepExpression.getExpressionType());
      }
    }
  }

  // Column should be like: | ControlColumn | Time | Value |
  @Override
  public int addInput(Column[] column, IWindow curWindow) {
    int curPositionCount = column[0].getPositionCount();
    for (int i = 0; i < curPositionCount; i++) {
      // skip null value in control column
      if (column[0].isNull(i)) {
        continue;
      }
      if (!curWindow.satisfy(column[0], i)) {
        return i;
      }
      curWindow.mergeOnePoint(column, i);

      if (column[2].isNull(i)) {
        if (!this.ignoreNull) {
          keep = 0;
        }
      } else {
        if (column[2].getBoolean(i)) {
          keep++;
          if (keepEvaluator.evaluate()) {
            countValue++;
            keep = 0;
          }
        }
      }
    }

    return curPositionCount;
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    countValue = finalResult.getLong(0);
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(countValue);
  }

  @Override
  public void reset() {
    this.countValue = 0;
    this.keep = 0;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.INT64;
  }
}
