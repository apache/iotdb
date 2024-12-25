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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentTime;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.isAggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ScopeReferenceExtractor.isFieldFromScope;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware.scopeAwareKey;

/** Checks whether an expression is constant with respect to the group */
class AggregationAnalyzer {

  // fields and expressions in the group by clause
  private final Set<FieldId> groupingFields;
  private final Set<ScopeAware<Expression>> expressions;
  private final Map<NodeRef<Expression>, ResolvedField> columnReferences;

  private final Analysis analysis;
  private final Scope sourceScope;
  private final Optional<Scope> orderByScope;

  public static void verifySourceAggregations(
      List<Expression> groupByExpressions,
      Scope sourceScope,
      List<Expression> expressions,
      Analysis analysis) {
    AggregationAnalyzer analyzer =
        new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.empty(), analysis);
    for (Expression expression : expressions) {
      analyzer.analyze(expression);
    }
  }

  public static void verifyOrderByAggregations(
      List<Expression> groupByExpressions,
      Scope sourceScope,
      Scope orderByScope,
      List<Expression> expressions,
      Analysis analysis) {
    AggregationAnalyzer analyzer =
        new AggregationAnalyzer(
            groupByExpressions, sourceScope, Optional.of(orderByScope), analysis);
    for (Expression expression : expressions) {
      analyzer.analyze(expression);
    }
  }

  private AggregationAnalyzer(
      List<Expression> groupByExpressions,
      Scope sourceScope,
      Optional<Scope> orderByScope,
      Analysis analysis) {
    requireNonNull(groupByExpressions, "groupByExpressions is null");
    requireNonNull(sourceScope, "sourceScope is null");
    requireNonNull(orderByScope, "orderByScope is null");
    requireNonNull(analysis, "analysis is null");

    this.sourceScope = sourceScope;
    this.orderByScope = orderByScope;
    this.analysis = analysis;
    this.expressions =
        groupByExpressions.stream()
            .map(expression -> scopeAwareKey(expression, analysis, sourceScope))
            .collect(toImmutableSet());

    // No defensive copy here for performance reasons.
    // Copying this map may lead to quadratic time complexity
    this.columnReferences = analysis.getColumnReferenceFields();

    this.groupingFields =
        groupByExpressions.stream()
            .map(NodeRef::of)
            .filter(columnReferences::containsKey)
            .map(columnReferences::get)
            .map(ResolvedField::getFieldId)
            .collect(toImmutableSet());

    this.groupingFields.forEach(
        fieldId -> {
          checkState(
              isFieldFromScope(fieldId, sourceScope),
              "Grouping field %s should originate from %s",
              fieldId,
              sourceScope.getRelationType());
        });
  }

  private void analyze(Expression expression) {
    Visitor visitor = new Visitor();
    if (!visitor.process(expression, null)) {
      throw new SemanticException(
          String.format(
              "'%s' must be an aggregate expression or appear in GROUP BY clause", expression));
    }
  }

  /** visitor returns true if all expressions are constant with respect to the group. */
  private class Visitor extends AstVisitor<Boolean, Void> {
    @Override
    protected Boolean visitExpression(Expression node, Void context) {
      throw new UnsupportedOperationException(
          "aggregation analysis not yet implemented for: " + node.getClass().getName());
    }

    @Override
    protected Boolean visitSubqueryExpression(SubqueryExpression node, Void context) {
      /*
       * Column reference can resolve to (a) some subquery's scope, (b) a projection (ORDER BY scope),
       * (c) source scope or (d) outer query scope (effectively a constant).
       * From AggregationAnalyzer's perspective, only case (c) needs verification.
       */
      getReferencesToScope(node, analysis, sourceScope)
          .filter(expression -> !isGroupingKey(expression))
          .findFirst()
          .ifPresent(
              expression -> {
                throw new SemanticException(
                    String.format(
                        "Subquery uses '%s' which must appear in GROUP BY clause", expression));
              });

      return true;
    }

    @Override
    protected Boolean visitExists(ExistsPredicate node, Void context) {
      checkState(node.getSubquery() instanceof SubqueryExpression);
      return process(node.getSubquery(), context);
    }

    @Override
    protected Boolean visitCast(Cast node, Void context) {
      return process(node.getExpression(), context);
    }

    @Override
    protected Boolean visitCoalesceExpression(CoalesceExpression node, Void context) {
      return node.getOperands().stream().allMatch(expression -> process(expression, context));
    }

    @Override
    protected Boolean visitNullIfExpression(NullIfExpression node, Void context) {
      return process(node.getFirst(), context) && process(node.getSecond(), context);
    }

    @Override
    protected Boolean visitBetweenPredicate(BetweenPredicate node, Void context) {
      return process(node.getMin(), context)
          && process(node.getValue(), context)
          && process(node.getMax(), context);
    }

    @Override
    protected Boolean visitCurrentTime(CurrentTime node, Void context) {
      return true;
    }

    @Override
    protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
      return process(node.getLeft(), context) && process(node.getRight(), context);
    }

    @Override
    protected Boolean visitComparisonExpression(ComparisonExpression node, Void context) {
      return process(node.getLeft(), context) && process(node.getRight(), context);
    }

    @Override
    protected Boolean visitLiteral(Literal node, Void context) {
      return true;
    }

    @Override
    protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
      return process(node.getValue(), context);
    }

    @Override
    protected Boolean visitIsNullPredicate(IsNullPredicate node, Void context) {
      return process(node.getValue(), context);
    }

    @Override
    protected Boolean visitLikePredicate(LikePredicate node, Void context) {
      return process(node.getValue(), context) && process(node.getPattern(), context);
    }

    @Override
    protected Boolean visitInListExpression(InListExpression node, Void context) {
      return node.getValues().stream().allMatch(expression -> process(expression, context));
    }

    @Override
    protected Boolean visitInPredicate(InPredicate node, Void context) {
      return process(node.getValue(), context) && process(node.getValueList(), context);
    }

    @Override
    protected Boolean visitQuantifiedComparisonExpression(
        QuantifiedComparisonExpression node, Void context) {
      return process(node.getValue(), context) && process(node.getSubquery(), context);
    }

    @Override
    protected Boolean visitTrim(Trim node, Void context) {
      return process(node.getTrimSource(), context)
          && (!node.getTrimCharacter().isPresent()
              || process(node.getTrimCharacter().get(), context));
    }

    @Override
    protected Boolean visitFunctionCall(FunctionCall node, Void context) {
      if (isAggregationFunction(node.getName().toString())) {
        List<FunctionCall> aggregateFunctions = extractAggregateFunctions(node.getArguments());

        if (!aggregateFunctions.isEmpty()) {
          throw new SemanticException(
              String.format(
                  "Cannot nest aggregations inside aggregation '%s': %s",
                  node.getName(), aggregateFunctions));
        }

        return true;
      }

      return node.getArguments().stream().allMatch(expression -> process(expression, context));
    }

    @Override
    protected Boolean visitIdentifier(Identifier node, Void context) {
      //      if (analysis.getLambdaArgumentReferences().containsKey(NodeRef.of(node))) {
      //        return true;
      //      }

      if (!hasReferencesToScope(node, analysis, sourceScope)) {
        // reference to outer scope is group-invariant
        return true;
      }

      return isGroupingKey(node);
    }

    @Override
    protected Boolean visitDereferenceExpression(DereferenceExpression node, Void context) {

      if (!hasReferencesToScope(node, analysis, sourceScope)) {
        // reference to outer scope is group-invariant
        return true;
      }

      if (columnReferences.containsKey(NodeRef.<Expression>of(node))) {
        return isGroupingKey(node);
      }

      // Allow SELECT col1.f1 FROM table1 GROUP BY col1
      return process(node.getBase(), context);
    }

    private boolean isGroupingKey(Expression node) {
      FieldId fieldId =
          requireNonNull(columnReferences.get(NodeRef.of(node)), () -> "No field for " + node)
              .getFieldId();

      if (orderByScope.isPresent() && isFieldFromScope(fieldId, orderByScope.get())) {
        return true;
      }

      return groupingFields.contains(fieldId);
    }

    @Override
    protected Boolean visitFieldReference(FieldReference node, Void context) {
      if (orderByScope.isPresent()) {
        return true;
      }

      FieldId fieldId =
          requireNonNull(columnReferences.get(NodeRef.of(node)), () -> "No field for " + node)
              .getFieldId();
      boolean inGroup = groupingFields.contains(fieldId);
      if (!inGroup) {
        Field field = sourceScope.getRelationType().getFieldByIndex(node.getFieldIndex());

        String column;
        if (!field.getName().isPresent()) {
          column = Integer.toString(node.getFieldIndex() + 1);
        } else if (field.getRelationAlias().isPresent()) {
          column = format("'%s.%s'", field.getRelationAlias().get(), field.getName().get());
        } else {
          column = "'" + field.getName().get() + "'";
        }

        throw new SemanticException(String.format("Column %s not in GROUP BY clause", column));
      }
      return inGroup;
    }

    @Override
    protected Boolean visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
      return process(node.getValue(), context);
    }

    @Override
    protected Boolean visitNotExpression(NotExpression node, Void context) {
      return process(node.getValue(), context);
    }

    @Override
    protected Boolean visitLogicalExpression(LogicalExpression node, Void context) {
      return node.getTerms().stream().allMatch(item -> process(item, context));
    }

    @Override
    protected Boolean visitIfExpression(IfExpression node, Void context) {
      ImmutableList.Builder<Expression> expressions =
          ImmutableList.<Expression>builder().add(node.getCondition()).add(node.getTrueValue());

      if (node.getFalseValue().isPresent()) {
        expressions.add(node.getFalseValue().get());
      }

      return expressions.build().stream().allMatch(expression -> process(expression, context));
    }

    @Override
    protected Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
      if (!process(node.getOperand(), context)) {
        return false;
      }

      for (WhenClause whenClause : node.getWhenClauses()) {
        if (!process(whenClause.getOperand(), context)
            || !process(whenClause.getResult(), context)) {
          return false;
        }
      }

      return !node.getDefaultValue().isPresent() || process(node.getDefaultValue().get(), context);
    }

    @Override
    protected Boolean visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
      for (WhenClause whenClause : node.getWhenClauses()) {
        if (!process(whenClause.getOperand(), context)
            || !process(whenClause.getResult(), context)) {
          return false;
        }
      }

      return !node.getDefaultValue().isPresent() || process(node.getDefaultValue().get(), context);
    }

    @Override
    protected Boolean visitRow(Row node, Void context) {
      return node.getItems().stream().allMatch(item -> process(item, context));
    }

    @Override
    protected Boolean visitParameter(Parameter node, Void context) {
      //      if (analysis.isDescribe()) {
      //        return true;
      //      }
      Map<NodeRef<Parameter>, Expression> parameters = analysis.getParameters();
      checkArgument(
          node.getId() < parameters.size(),
          "Invalid parameter number %s, max values is %s",
          node.getId(),
          parameters.size() - 1);
      return process(parameters.get(NodeRef.of(node)), context);
    }

    @Override
    public Boolean process(Node node, @Nullable Void context) {
      if (node instanceof Expression
          && expressions.contains(scopeAwareKey(node, analysis, sourceScope))
          && (!orderByScope.isPresent() || !hasOrderByReferencesToOutputColumns(node))) {
        return true;
      }

      return super.process(node, context);
    }

    private boolean hasOrderByReferencesToOutputColumns(Node node) {
      return hasReferencesToScope(node, analysis, orderByScope.get());
    }

    private void verifyNoOrderByReferencesToOutputColumns(Node node, String errorString) {
      getReferencesToScope(node, analysis, orderByScope.get())
          .findFirst()
          .ifPresent(
              expression -> {
                throw new SemanticException(errorString);
              });
    }
  }
}
