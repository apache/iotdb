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

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import javax.annotation.Nullable;

import java.time.ZoneId;

import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.AND;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.OR;
import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.dateBin;

public class GapFillStartAndEndTimeExtractVisitor
    extends AstVisitor<Boolean, GapFillStartAndEndTimeExtractVisitor.Context> {

  private static final String UPDATE_START_TIME_ERROR_MSG =
      "Operator of updateStatTime should only be GREATER_THAN and GREATER_THAN_OR_EQUAL, now is %s";

  private static final String UPDATE_END_TIME_ERROR_MSG =
      "Operator of updateEndTime should only be LESS_THAN and LESS_THAN_OR_EQUAL, now is %s";

  private final Symbol timeColumn;

  public GapFillStartAndEndTimeExtractVisitor(Symbol timeColumn) {
    this.timeColumn = timeColumn;
  }

  public static final String CAN_NOT_INFER_TIME_RANGE =
      "could not infer startTime or endTime from WHERE clause";

  @Override
  public Boolean visitNode(Node node, GapFillStartAndEndTimeExtractVisitor.Context context) {
    for (Node child : node.getChildren()) {
      if (Boolean.TRUE.equals(super.process(child, context))) {
        throw new SemanticException(CAN_NOT_INFER_TIME_RANGE);
      }
    }
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitSymbolReference(
      SymbolReference node, GapFillStartAndEndTimeExtractVisitor.Context context) {
    return isTimeIdentifier(node);
  }

  @Override
  protected Boolean visitLogicalExpression(
      LogicalExpression node, GapFillStartAndEndTimeExtractVisitor.Context context) {
    if (node.getOperator() == AND) {
      boolean hasMeetGapFillTimeFilter = false;
      for (Expression term : node.getTerms()) {
        hasMeetGapFillTimeFilter = term.accept(this, context) || hasMeetGapFillTimeFilter;
      }
      return hasMeetGapFillTimeFilter;
    } else if (node.getOperator() == OR) {
      for (Expression term : node.getTerms()) {
        if (Boolean.TRUE.equals(term.accept(this, context))) {
          throw new SemanticException(CAN_NOT_INFER_TIME_RANGE);
        }
      }
      return false;
    } else {
      throw new IllegalStateException("Illegal state in visitLogicalExpression");
    }
  }

  @Override
  protected Boolean visitComparisonExpression(
      ComparisonExpression node, GapFillStartAndEndTimeExtractVisitor.Context context) {
    Expression leftExpression = node.getLeft();
    Expression rightExpression = node.getRight();
    if (checkIsValidTimeFilter(leftExpression, rightExpression, node.getOperator(), context)
        || checkIsValidTimeFilter(
            rightExpression, leftExpression, node.getOperator().flip(), context)) {
      return Boolean.TRUE;
    } else {
      if (Boolean.TRUE.equals(leftExpression.accept(this, context))
          || Boolean.TRUE.equals(rightExpression.accept(this, context))) {
        throw new SemanticException(CAN_NOT_INFER_TIME_RANGE);
      } else {
        return Boolean.FALSE;
      }
    }
  }

  @Override
  protected Boolean visitBetweenPredicate(
      BetweenPredicate node, GapFillStartAndEndTimeExtractVisitor.Context context) {
    Expression firstExpression = node.getValue();
    Expression secondExpression = node.getMin();
    Expression thirdExpression = node.getMax();

    boolean result1 =
        checkIsValidTimeFilter(firstExpression, secondExpression, GREATER_THAN_OR_EQUAL, context);
    boolean result2 =
        checkIsValidTimeFilter(firstExpression, thirdExpression, LESS_THAN_OR_EQUAL, context);

    if (result1 || result2) {
      return Boolean.TRUE;
    } else {
      if (Boolean.TRUE.equals(firstExpression.accept(this, context))
          || Boolean.TRUE.equals(secondExpression.accept(this, context))
          || Boolean.TRUE.equals(thirdExpression.accept(this, context))) {
        throw new SemanticException(CAN_NOT_INFER_TIME_RANGE);
      } else {
        return Boolean.FALSE;
      }
    }
  }

  private boolean isTimeIdentifier(Expression e) {
    return e instanceof SymbolReference
        && timeColumn.getName().equalsIgnoreCase(((SymbolReference) e).getName());
  }

  private boolean checkIsValidTimeFilter(
      Expression timeExpression,
      Expression valueExpression,
      ComparisonExpression.Operator operator,
      GapFillStartAndEndTimeExtractVisitor.Context context) {
    if (isTimeIdentifier(timeExpression) && valueExpression instanceof LongLiteral) {
      long value = ((LongLiteral) valueExpression).getParsedValue();
      switch (operator) {
        case EQUAL:
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return false;
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          context.updateEndTime(value, operator);
          return true;
        case GREATER_THAN_OR_EQUAL:
        case GREATER_THAN:
          context.updateStartTime(value, operator);
          return true;
      }
    }
    return false;
  }

  public static class Context {
    @Nullable public ComparisonExpression.Operator leftOperator;
    long startTime;
    @Nullable public ComparisonExpression.Operator rightOperator;
    long endTime;

    void updateStartTime(long startTime, ComparisonExpression.Operator operator) {
      if (leftOperator == null) {
        leftOperator = operator;
        this.startTime = startTime;
      } else if (leftOperator == GREATER_THAN) {
        if (operator == GREATER_THAN) {
          this.startTime = Math.max(this.startTime, startTime);
        } else if (operator == GREATER_THAN_OR_EQUAL) {
          if (startTime > this.startTime) {
            leftOperator = operator;
            this.startTime = startTime;
          }
        } else {
          throw new IllegalArgumentException(String.format(UPDATE_START_TIME_ERROR_MSG, operator));
        }
      } else if (leftOperator == GREATER_THAN_OR_EQUAL) {
        if (operator == GREATER_THAN) {
          if (startTime >= this.startTime) {
            leftOperator = operator;
            this.startTime = startTime;
          }
        } else if (operator == GREATER_THAN_OR_EQUAL) {
          this.startTime = Math.max(this.startTime, startTime);
        } else {
          throw new IllegalArgumentException(String.format(UPDATE_START_TIME_ERROR_MSG, operator));
        }
      } else {
        throw new IllegalArgumentException(String.format(UPDATE_START_TIME_ERROR_MSG, operator));
      }
    }

    void updateEndTime(long endTime, ComparisonExpression.Operator operator) {
      if (rightOperator == null) {
        rightOperator = operator;
        this.endTime = endTime;
      } else if (rightOperator == LESS_THAN) {
        if (operator == LESS_THAN) {
          this.endTime = Math.min(this.endTime, endTime);
        } else if (operator == LESS_THAN_OR_EQUAL) {
          if (endTime < this.endTime) {
            rightOperator = operator;
            this.endTime = endTime;
          }
        } else {
          throw new IllegalArgumentException(String.format(UPDATE_END_TIME_ERROR_MSG, operator));
        }
      } else if (rightOperator == LESS_THAN_OR_EQUAL) {
        if (operator == LESS_THAN) {
          if (endTime <= this.endTime) {
            rightOperator = operator;
            this.endTime = endTime;
          }
        } else if (operator == LESS_THAN_OR_EQUAL) {
          this.endTime = Math.max(this.endTime, endTime);
        } else {
          throw new IllegalArgumentException(String.format(UPDATE_START_TIME_ERROR_MSG, operator));
        }
      } else {
        throw new IllegalArgumentException(String.format(UPDATE_START_TIME_ERROR_MSG, operator));
      }
    }

    public long[] getTimeRange(
        long origin, int monthDuration, long nonMonthDuration, ZoneId zoneId) {
      if (leftOperator == null || rightOperator == null) {
        throw new SemanticException(CAN_NOT_INFER_TIME_RANGE);
      }
      long[] result = new long[2];
      if (leftOperator == GREATER_THAN) {
        startTime++;
      }
      result[0] = dateBin(startTime, origin, monthDuration, nonMonthDuration, zoneId);
      if (rightOperator == LESS_THAN) {
        endTime--;
      }
      result[1] = dateBin(endTime, origin, monthDuration, nonMonthDuration, zoneId);

      return result;
    }
  }
}
