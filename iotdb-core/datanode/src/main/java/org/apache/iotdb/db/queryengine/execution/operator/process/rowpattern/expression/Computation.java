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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers.Assignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// Computation Logic: Represents how data corresponding to multiple accessors should be calculated

// For example, if there are two parameters, the defined operation could be #0 < #1; If there are
// three parameters, the defined operation could be #0 + #1 + #2

// The computation logic is stored within the `Computation`. Invoking the `evaluate` method and
// passing in the values of the parameter variables will execute the computation. The number of
// parameters is determined by the size of the valueAccessors.

// A leaf node must be a ReferenceComputation
public abstract class Computation {
  /**
   * Performs computation based on the given parameters, with the order of parameters corresponding
   * to PatternExpressionComputation.valueAccessors.
   *
   * @param values The list of parameters
   * @return The result of the computation
   */
  public abstract Object evaluate(List<Object> values);

  /**
   * The `ComputationParser` is utilized to recursively transform an Expression tree into a
   * Computation tree. During the traversal process, when encountering a `SymbolReference`, it
   * converts it into a `ReferenceComputation` and sequentially assigns parameter indices (starting
   * from 0).
   */
  public static class ComputationParser {

    public static Computation parse(ExpressionAndValuePointers expressionAndValuePointers) {
      Expression expression = expressionAndValuePointers.getExpression();
      List<Assignment> assignments = expressionAndValuePointers.getAssignments();
      AtomicInteger counter = new AtomicInteger(0);
      Map<String, Integer> symbolToIndex = new HashMap<>();
      for (int i = 0; i < assignments.size(); i++) {
        symbolToIndex.put(assignments.get(i).getSymbol().getName(), i);
      }
      return parse(expression, counter, symbolToIndex);
    }

    /**
     * A helper method for recursively parsing an Expression.
     *
     * @param expression The expression currently being parsed
     * @param counter Used to sequentially assign parameter indices
     * @param symbolToIndex Records the index corresponding to each symbol, ensuring that the same
     *     index is used when the same symbol reappears
     * @return The constructed Computation expression tree
     */
    private static Computation parse(
        Expression expression, AtomicInteger counter, Map<String, Integer> symbolToIndex) {
      if (expression instanceof ArithmeticBinaryExpression) {
        ArithmeticBinaryExpression arithmeticExpr = (ArithmeticBinaryExpression) expression;
        Computation left = parse(arithmeticExpr.getLeft(), counter, symbolToIndex);
        Computation right = parse(arithmeticExpr.getRight(), counter, symbolToIndex);
        ArithmeticOperator op = mapArithmeticOperator(arithmeticExpr.getOperator());
        return new BinaryComputation(left, right, op);
      } else if (expression instanceof ComparisonExpression) {
        ComparisonExpression comparisonExpr = (ComparisonExpression) expression;
        Computation left = parse(comparisonExpr.getLeft(), counter, symbolToIndex);
        Computation right = parse(comparisonExpr.getRight(), counter, symbolToIndex);
        ComparisonOperator op = mapComparisonOperator(comparisonExpr.getOperator());
        return new BinaryComputation(left, right, op);
      } else if (expression instanceof LogicalExpression) {
        LogicalExpression logicalExpr = (LogicalExpression) expression;
        List<Computation> computations = new ArrayList<>();
        for (Expression term : logicalExpr.getTerms()) {
          computations.add(parse(term, counter, symbolToIndex));
        }
        NaryOperator op = mapLogicalOperator(logicalExpr.getOperator());
        return new NaryComputation(computations, op);
      } else if (expression instanceof SymbolReference) {
        // upon encountering a SymbolReference type, it is converted into a ReferenceComputation.
        // B.value < LAST(B.value) -> b_0 < b_1
        // LAST(B.value, 1) < LAST(B.value, 2) -> b_0 < b_1 + 1
        SymbolReference symRef = (SymbolReference) expression;
        String name = symRef.getName();
        int index = symbolToIndex.get(name);
        return new ReferenceComputation(index);
      } else if (expression instanceof LongLiteral) {
        LongLiteral constExpr = (LongLiteral) expression;
        return new ConstantComputation(constExpr.getParsedValue());
      } else if (expression instanceof DoubleLiteral) {
        DoubleLiteral constExpr = (DoubleLiteral) expression;
        return new ConstantComputation(constExpr.getValue());
      } else if (expression instanceof StringLiteral) {
        StringLiteral constExpr = (StringLiteral) expression;
        return new ConstantComputation(constExpr.getValue());
      } else if (expression instanceof BooleanLiteral) {
        // undefined pattern variable is 'true'
        BooleanLiteral constExpr = (BooleanLiteral) expression;
        return new ConstantComputation(constExpr.getValue());
      } else if (expression instanceof BinaryLiteral) {
        BinaryLiteral constExpr = (BinaryLiteral) expression;
        return new ConstantComputation(constExpr.getValue());
      } else if (expression instanceof GenericLiteral) { // handle the CAST function
        GenericLiteral constExpr = (GenericLiteral) expression;
        String type = constExpr.getType();

        if ("DATE".equalsIgnoreCase(type)) { // CAST(... AS DATE)
          String dateStr = constExpr.getValue();
          int dateInt = Integer.parseInt(dateStr);
          return new ConstantComputation(dateInt);
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) { // CAST(... AS TIMESTAMP)
          String timestampStr = constExpr.getValue();
          long timestampLong = Long.parseLong(timestampStr);
          return new ConstantComputation(timestampLong);
        } else {
          return new ConstantComputation(constExpr.getValue());
        }
      } else if (expression instanceof Cast) {
        // non-constant CAST scenario, such AS CAST(A.quantity AS INT64)
        Cast castExpr = (Cast) expression;
        Computation inner = parse(castExpr.getExpression(), counter, symbolToIndex);
        DataType targetType = castExpr.getType();

        return new CastComputation(inner, targetType);
      } else {
        throw new SemanticException(
            "Unsupported expression type: " + expression.getClass().getName());
      }
    }

    private static ArithmeticOperator mapArithmeticOperator(
        ArithmeticBinaryExpression.Operator operator) {
      switch (operator) {
        case ADD:
          return ArithmeticOperator.ADD;
        case SUBTRACT:
          return ArithmeticOperator.SUBTRACT;
        case MULTIPLY:
          return ArithmeticOperator.MULTIPLY;
        case DIVIDE:
          return ArithmeticOperator.DIVIDE;
        case MODULUS:
          return ArithmeticOperator.MODULUS;
        default:
          throw new SemanticException("Unsupported arithmetic operator: " + operator);
      }
    }

    private static ComparisonOperator mapComparisonOperator(
        ComparisonExpression.Operator operator) {
      switch (operator) {
        case LESS_THAN:
          return ComparisonOperator.LESS_THAN;
        case GREATER_THAN:
          return ComparisonOperator.GREATER_THAN;
        case EQUAL:
          return ComparisonOperator.EQUAL;
        case NOT_EQUAL:
          return ComparisonOperator.NOT_EQUAL;
        case LESS_THAN_OR_EQUAL:
          return ComparisonOperator.LESS_THAN_OR_EQUAL;
        case GREATER_THAN_OR_EQUAL:
          return ComparisonOperator.GREATER_THAN_OR_EQUAL;
        case IS_DISTINCT_FROM:
          return ComparisonOperator.IS_DISTINCT_FROM;
        default:
          throw new SemanticException("Unsupported comparison operator: " + operator);
      }
    }

    private static LogicalOperator mapLogicalOperator(LogicalExpression.Operator operator) {
      switch (operator) {
        case AND:
          return LogicalOperator.AND;
        case OR:
          return LogicalOperator.OR;
        default:
          throw new SemanticException("Unsupported logical operator: " + operator);
      }
    }
  }
}
