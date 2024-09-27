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

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.isEffectivelyLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.ADD;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.MULTIPLY;

public class CanonicalizeExpressionRewriter {
  public static Expression canonicalizeExpression(
      Expression expression,
      IrTypeAnalyzer typeAnalyzer,
      TypeProvider types,
      PlannerContext plannerContext,
      SessionInfo session) {
    return ExpressionTreeRewriter.rewriteWith(
        new Visitor(session, plannerContext, typeAnalyzer, types), expression);
  }

  private CanonicalizeExpressionRewriter() {}

  public static Expression rewrite(
      Expression expression,
      SessionInfo session,
      PlannerContext plannerContext,
      IrTypeAnalyzer typeAnalyzer,
      TypeProvider types) {
    requireNonNull(plannerContext, "plannerContext is null");
    requireNonNull(typeAnalyzer, "typeAnalyzer is null");

    if (expression instanceof SymbolReference) {
      return expression;
    }

    return ExpressionTreeRewriter.rewriteWith(
        new Visitor(session, plannerContext, typeAnalyzer, types), expression);
  }

  private static class Visitor extends ExpressionRewriter<Void> {
    private final SessionInfo session;
    private final PlannerContext plannerContext;
    private final IrTypeAnalyzer typeAnalyzer;
    private final TypeProvider types;

    public Visitor(
        SessionInfo session,
        PlannerContext plannerContext,
        IrTypeAnalyzer typeAnalyzer,
        TypeProvider types) {
      this.session = session;
      this.plannerContext = plannerContext;
      this.typeAnalyzer = typeAnalyzer;
      this.types = types;
    }

    @SuppressWarnings("ArgumentSelectionDefectChecker")
    @Override
    public Expression rewriteComparisonExpression(
        ComparisonExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      // if we have a comparison of the form <constant> <op> <expr>, normalize it to
      // <expr> <op-flipped> <constant>
      if (isConstant(node.getLeft()) && !isConstant(node.getRight())) {
        node = new ComparisonExpression(node.getOperator().flip(), node.getRight(), node.getLeft());
      }

      return treeRewriter.defaultRewrite(node, context);
    }

    @SuppressWarnings("ArgumentSelectionDefectChecker")
    @Override
    public Expression rewriteArithmeticBinary(
        ArithmeticBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      if (node.getOperator() == MULTIPLY || node.getOperator() == ADD) {
        // if we have a operation of the form <constant> [+|*] <expr>, normalize it to
        // <expr> [+|*] <constant>
        if (isConstant(node.getLeft()) && !isConstant(node.getRight())) {
          node =
              new ArithmeticBinaryExpression(node.getOperator(), node.getRight(), node.getLeft());
        }
      }

      return treeRewriter.defaultRewrite(node, context);
    }

    @Override
    public Expression rewriteIsNotNullPredicate(
        IsNotNullPredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      Expression value = treeRewriter.rewrite(node.getValue(), context);
      return new NotExpression(new IsNullPredicate(value));
    }

    @Override
    public Expression rewriteIfExpression(
        IfExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      Expression condition = treeRewriter.rewrite(node.getCondition(), context);
      Expression trueValue = treeRewriter.rewrite(node.getTrueValue(), context);

      Optional<Expression> falseValue =
          node.getFalseValue().map(value -> treeRewriter.rewrite(value, context));

      return falseValue
          .map(
              expression ->
                  new SearchedCaseExpression(
                      ImmutableList.of(new WhenClause(condition, trueValue)), expression))
          .orElseGet(
              () ->
                  new SearchedCaseExpression(
                      ImmutableList.of(new WhenClause(condition, trueValue))));
    }

    @Override
    public Expression rewriteFunctionCall(
        FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      return treeRewriter.defaultRewrite(node, context);
    }

    private boolean isConstant(Expression expression) {
      // Current IR has no way to represent typed constants. It encodes simple ones as Cast(Literal)
      // This is the simplest possible check that
      //   1) doesn't require ExpressionInterpreter.optimize(), which is not cheap
      //   2) doesn't try to duplicate all the logic in LiteralEncoder
      //   3) covers a sufficient portion of the use cases that occur in practice
      // TODO: this should eventually be removed when IR includes types
      return isEffectivelyLiteral(expression, plannerContext, session);
    }
  }
}
