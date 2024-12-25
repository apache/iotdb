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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combinePredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractPredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;

public final class PushDownNegationsExpressionRewriter {
  public static Expression pushDownNegations(
      Metadata metadata, Expression expression, Map<NodeRef<Expression>, Type> expressionTypes) {
    return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata, expressionTypes), expression);
  }

  private PushDownNegationsExpressionRewriter() {}

  private static class Visitor extends ExpressionRewriter<Void> {
    private final Metadata metadata;
    private final Map<NodeRef<Expression>, Type> expressionTypes;

    public Visitor(Metadata metadata, Map<NodeRef<Expression>, Type> expressionTypes) {
      this.metadata = requireNonNull(metadata, "metadata is null");
      this.expressionTypes =
          ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
    }

    @Override
    public Expression rewriteNotExpression(
        NotExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      if (node.getValue() instanceof LogicalExpression) {
        LogicalExpression child = (LogicalExpression) node.getValue();
        List<Expression> predicates = extractPredicates(child);
        List<Expression> negatedPredicates =
            predicates.stream()
                .map(
                    predicate ->
                        treeRewriter.rewrite((Expression) new NotExpression(predicate), context))
                .collect(toImmutableList());
        return combinePredicates(child.getOperator().flip(), negatedPredicates);
      }
      if (node.getValue() instanceof ComparisonExpression
          && ((ComparisonExpression) node.getValue()).getOperator() != IS_DISTINCT_FROM) {
        ComparisonExpression child = (ComparisonExpression) node.getValue();
        ComparisonExpression.Operator operator = child.getOperator();
        Expression left = child.getLeft();
        Expression right = child.getRight();
        Type leftType = expressionTypes.get(NodeRef.of(left));
        Type rightType = expressionTypes.get(NodeRef.of(right));
        checkState(leftType != null && rightType != null, "missing type for expression");
        if ((typeHasNaN(leftType) || typeHasNaN(rightType))
            && (operator == GREATER_THAN_OR_EQUAL
                || operator == GREATER_THAN
                || operator == LESS_THAN_OR_EQUAL
                || operator == LESS_THAN)) {
          return new NotExpression(
              new ComparisonExpression(
                  operator,
                  treeRewriter.rewrite(left, context),
                  treeRewriter.rewrite(right, context)));
        }
        return new ComparisonExpression(
            operator.negate(),
            treeRewriter.rewrite(left, context),
            treeRewriter.rewrite(right, context));
      }
      if (node.getValue() instanceof NotExpression) {
        NotExpression child = (NotExpression) node.getValue();
        return treeRewriter.rewrite(child.getValue(), context);
      }

      return new NotExpression(treeRewriter.rewrite(node.getValue(), context));
    }

    private boolean typeHasNaN(Type type) {
      return type instanceof DoubleType || type instanceof FloatType;
    }
  }
}
