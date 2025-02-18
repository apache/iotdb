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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// 计算逻辑：用于表示多个访问器对应的数据之间应该如何计算。其实就是一个传统的表达式了。
// 如一共有两个参数，定义的运算可以为 #0 < #1
// 如一共有三个参数，定义的运算可以为 #0 + #1 + #2
// Computation 中存储计算逻辑。调用 evaluate 方法可以进行计算。
// 参数的个数由 valueAccessors 的大小决定。

// 叶子节点一定是 ReferenceComputation
public abstract class Computation {
  /**
   * 根据给定参数进行计算，参数顺序与 PatternExpressionComputation.valueAccessors 对应。
   *
   * @param values 参数列表
   * @return 计算结果
   */
  public abstract Object evaluate(List<Object> values);

  /**
   * ComputationParser 用于将 Expression 递归转换为 Computation 表达式树， 并且在左深遍历过程中，遇到 SymbolReference 类型时，将其变为
   * ReferenceComputation，并依次分配参数索引（从0开始）。
   */
  public static class ComputationParser {

    /** 入口方法，传入 Expression 返回对应的 Computation 表达式树。 */
    public static Computation parse(Expression expression) {
      // 使用 AtomicInteger 作为计数器，Map 用于保存已分配索引的 symbol
      AtomicInteger counter = new AtomicInteger(0);
      Map<String, Integer> symbolToIndex = new HashMap<>();
      return parse(expression, counter, symbolToIndex);
    }

    /**
     * 递归解析 Expression 的辅助方法。
     *
     * @param expression 当前解析的表达式
     * @param counter 用于依次分配参数索引
     * @param symbolToIndex 记录每个 symbol 对应的索引，保证同一 symbol 重复出现时使用同一个索引
     * @return 构造好的 Computation 表达式树
     */
    private static Computation parse(
        Expression expression, AtomicInteger counter, Map<String, Integer> symbolToIndex) {
      if (expression instanceof ArithmeticBinaryExpression) {
        ArithmeticBinaryExpression arithmeticExpr = (ArithmeticBinaryExpression) expression;
        Computation left = parse(arithmeticExpr.getLeft(), counter, symbolToIndex);
        Computation right = parse(arithmeticExpr.getRight(), counter, symbolToIndex);
        // 将表达式中的运算符映射到对应的 ArithmeticOperator 枚举
        ArithmeticOperator op = mapArithmeticOperator(arithmeticExpr.getOperator());
        return new BinaryComputation(left, right, op);
      } else if (expression instanceof ComparisonExpression) {
        ComparisonExpression comparisonExpr = (ComparisonExpression) expression;
        Computation left = parse(comparisonExpr.getLeft(), counter, symbolToIndex);
        Computation right = parse(comparisonExpr.getRight(), counter, symbolToIndex);
        // 映射比较运算符
        ComparisonOperator op = mapComparisonOperator(comparisonExpr.getOperator());
        return new BinaryComputation(left, right, op);
      } else if (expression instanceof SymbolReference) {
        // 遇到 SymbolReference 类型，将其转换为 ReferenceComputation
        // B.value < LAST(B.value) -> b < b_4
        // B.value < LAST(B.value) -> b < b_4 + 1
        SymbolReference symRef = (SymbolReference) expression;
        String name = symRef.getName();
        // 如果该 symbol 之前没有分配过索引，则分配一个新的索引
        if (!symbolToIndex.containsKey(name)) {
          symbolToIndex.put(name, counter.getAndIncrement());
        }
        int index = symbolToIndex.get(name);
        return new ReferenceComputation(index);
      } else if (expression instanceof LongLiteral) {
        LongLiteral constExpr = (LongLiteral) expression;
        return new ConstantComputation(constExpr.getValue());
      } else if (expression instanceof BooleanLiteral) {
        // 未定义的 patternVariable 是恒真表达式，传入为 true 的 BooleanLiteral
        BooleanLiteral constExpr = (BooleanLiteral) expression;
        return new ConstantComputation(constExpr.getValue());
      } else {
        throw new IllegalArgumentException(
            "Unsupported expression type: " + expression.getClass().getName());
      }
    }

    /** 将算术表达式中的运算符映射为对应的 ArithmeticOperator 枚举 */
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
          throw new IllegalArgumentException("Unsupported arithmetic operator: " + operator);
      }
    }

    /** 将比较表达式中的运算符映射为对应的 ComparisonOperator 枚举 */
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
          throw new IllegalArgumentException("Unsupported comparison operator: " + operator);
      }
    }
  }
}
