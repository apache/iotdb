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

import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.LogicalExpression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator.isDeterministic;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combinePredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractPredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.tree.LogicalExpression.Operator.OR;

public final class ExtractCommonPredicatesExpressionRewriter {

  public static Expression extractCommonPredicates(Expression expression) {
    return new Visitor().process(expression, NodeContext.ROOT_NODE);
  }

  private ExtractCommonPredicatesExpressionRewriter() {}

  private static class Visitor extends RewritingVisitor<NodeContext> {

    @Override
    public Expression visitLogicalExpression(LogicalExpression node, NodeContext context) {
      Expression expression =
          combinePredicates(
              node.getOperator(),
              extractPredicates(node.getOperator(), node).stream()
                  .map(subExpression -> process(subExpression, NodeContext.NOT_ROOT_NODE))
                  .collect(toImmutableList()));

      if (!(expression instanceof LogicalExpression)) {
        return expression;
      }

      Expression simplified = extractCommonPredicates((LogicalExpression) expression);

      // Prefer AND LogicalBinaryExpression at the root if possible
      if (context.isRootNode()
          && simplified instanceof LogicalExpression
          && ((LogicalExpression) simplified).getOperator() == OR) {
        return distributeIfPossible((LogicalExpression) simplified);
      }

      return simplified;
    }

    private Expression extractCommonPredicates(LogicalExpression node) {
      List<List<Expression>> subPredicates = getSubPredicates(node);

      Set<Expression> commonPredicates =
          ImmutableSet.copyOf(
              subPredicates.stream()
                  .map(this::filterDeterministicPredicates)
                  .reduce(Sets::intersection)
                  .orElse(emptySet()));

      List<List<Expression>> uncorrelatedSubPredicates =
          subPredicates.stream()
              .map(predicateList -> removeAll(predicateList, commonPredicates))
              .collect(toImmutableList());

      LogicalExpression.Operator flippedOperator = node.getOperator().flip();

      List<Expression> uncorrelatedPredicates =
          uncorrelatedSubPredicates.stream()
              .map(predicate -> combinePredicates(flippedOperator, predicate))
              .collect(toImmutableList());
      Expression combinedUncorrelatedPredicates =
          combinePredicates(node.getOperator(), uncorrelatedPredicates);

      return combinePredicates(
          flippedOperator,
          ImmutableList.<Expression>builder()
              .addAll(commonPredicates)
              .add(combinedUncorrelatedPredicates)
              .build());
    }

    private static List<List<Expression>> getSubPredicates(LogicalExpression expression) {
      return extractPredicates(expression.getOperator(), expression).stream()
          .map(
              predicate ->
                  predicate instanceof LogicalExpression
                      ? extractPredicates((LogicalExpression) predicate)
                      : ImmutableList.of(predicate))
          .collect(toImmutableList());
    }

    /**
     * Applies the boolean distributive property.
     *
     * <p>For example: ( A & B ) | ( C & D ) => ( A | C ) & ( A | D ) & ( B | C ) & ( B | D)
     *
     * <p>Returns the original expression if the expression is non-deterministic or if the
     * distribution will expand the expression by too much.
     */
    private Expression distributeIfPossible(LogicalExpression expression) {
      if (!isDeterministic(expression)) {
        // Do not distribute boolean expressions if there are any non-deterministic elements
        // TODO: This can be optimized further if non-deterministic elements are not repeated
        return expression;
      }
      List<Set<Expression>> subPredicates =
          getSubPredicates(expression).stream().map(ImmutableSet::copyOf).collect(toList());

      int originalBaseExpressions = subPredicates.stream().mapToInt(Set::size).sum();

      int newBaseExpressions;
      try {
        newBaseExpressions =
            Math.multiplyExact(
                subPredicates.stream().mapToInt(Set::size).reduce(Math::multiplyExact).getAsInt(),
                subPredicates.size());
      } catch (ArithmeticException e) {
        // Integer overflow from multiplication means there are too many expressions
        return expression;
      }

      if (newBaseExpressions > originalBaseExpressions * 2) {
        // Do not distribute boolean expressions if it would create 2x more base expressions
        // (e.g. A, B, C, D from the above example). This is just an arbitrary heuristic to
        // avoid cross product expression explosion.
        return expression;
      }

      Set<List<Expression>> crossProduct = Sets.cartesianProduct(subPredicates);

      return combinePredicates(
          expression.getOperator().flip(),
          crossProduct.stream()
              .map(expressions -> combinePredicates(expression.getOperator(), expressions))
              .collect(toImmutableList()));
    }

    private Set<Expression> filterDeterministicPredicates(List<Expression> predicates) {
      return predicates.stream().filter(expression -> isDeterministic(expression)).collect(toSet());
    }

    private static <T> List<T> removeAll(Collection<T> collection, Collection<T> elementsToRemove) {
      return collection.stream()
          .filter(element -> !elementsToRemove.contains(element))
          .collect(toImmutableList());
    }
  }

  private enum NodeContext {
    ROOT_NODE,
    NOT_ROOT_NODE;

    boolean isRootNode() {
      return this == ROOT_NODE;
    }
  }
}
