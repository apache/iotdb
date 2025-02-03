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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.FunctionNullability;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SimplePlanRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.Type;

import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SimplePlanRewriter.rewriteWith;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.globalAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode.Quantifier.ALL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.FALSE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.NOT_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;

public class TransformQuantifiedComparisonApplyToCorrelatedJoin implements PlanOptimizer {
  private final Metadata metadata;

  public TransformQuantifiedComparisonApplyToCorrelatedJoin(Metadata metadata) {
    this.metadata = requireNonNull(metadata, "metadata is null");
  }

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    return rewriteWith(
        new Rewriter(context.idAllocator(), context.getSymbolAllocator(), metadata), plan, null);
  }

  private static class Rewriter extends SimplePlanRewriter<PlanNode> {
    private final QueryId idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final Metadata metadata;

    public Rewriter(QueryId idAllocator, SymbolAllocator symbolAllocator, Metadata metadata) {
      this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
      this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
      this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context) {
      if (node.getSubqueryAssignments().size() != 1) {
        return context.defaultRewrite(node);
      }

      ApplyNode.SetExpression expression = getOnlyElement(node.getSubqueryAssignments().values());
      if (expression instanceof ApplyNode.QuantifiedComparison) {
        return rewriteQuantifiedApplyNode(
            node, (ApplyNode.QuantifiedComparison) expression, context);
      }

      return context.defaultRewrite(node);
    }

    private PlanNode rewriteQuantifiedApplyNode(
        ApplyNode node,
        ApplyNode.QuantifiedComparison quantifiedComparison,
        RewriteContext<PlanNode> context) {
      PlanNode subqueryPlan = context.rewrite(node.getSubquery());

      Symbol outputColumn = getOnlyElement(subqueryPlan.getOutputSymbols());
      Type outputColumnType = symbolAllocator.getTypes().getTableModelType(outputColumn);
      checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");

      Symbol minValue = symbolAllocator.newSymbol("min", outputColumnType);
      Symbol maxValue = symbolAllocator.newSymbol("max", outputColumnType);
      Symbol countAllValue = symbolAllocator.newSymbol("count_all", LongType.getInstance());
      Symbol countNonNullValue =
          symbolAllocator.newSymbol("count_non_null", LongType.getInstance());

      List<Expression> outputColumnReferences = ImmutableList.of(outputColumn.toSymbolReference());

      subqueryPlan =
          singleAggregation(
              idAllocator.genPlanNodeId(),
              subqueryPlan,
              ImmutableMap.of(
                  minValue,
                      new AggregationNode.Aggregation(
                          getResolvedBuiltInAggregateFunction(
                              "min", ImmutableList.of(outputColumnType)),
                          outputColumnReferences,
                          false,
                          Optional.empty(),
                          Optional.empty(),
                          Optional.empty()),
                  maxValue,
                      new AggregationNode.Aggregation(
                          getResolvedBuiltInAggregateFunction(
                              "max", ImmutableList.of(outputColumnType)),
                          outputColumnReferences,
                          false,
                          Optional.empty(),
                          Optional.empty(),
                          Optional.empty()),
                  countAllValue,
                      new AggregationNode.Aggregation(
                          getResolvedBuiltInAggregateFunction(
                              "count_all", ImmutableList.of(outputColumnType)),
                          outputColumnReferences,
                          false,
                          Optional.empty(),
                          Optional.empty(),
                          Optional.empty()),
                  countNonNullValue,
                      new AggregationNode.Aggregation(
                          getResolvedBuiltInAggregateFunction(
                              "count", ImmutableList.of(outputColumnType)),
                          outputColumnReferences,
                          false,
                          Optional.empty(),
                          Optional.empty(),
                          Optional.empty())),
              globalAggregation());

      PlanNode join =
          new CorrelatedJoinNode(
              node.getPlanNodeId(),
              context.rewrite(node.getInput()),
              subqueryPlan,
              node.getCorrelation(),
              JoinNode.JoinType.INNER,
              TRUE_LITERAL,
              node.getOriginSubquery());

      Expression valueComparedToSubquery =
          rewriteUsingBounds(
              quantifiedComparison, minValue, maxValue, countAllValue, countNonNullValue);

      Symbol quantifiedComparisonSymbol = getOnlyElement(node.getSubqueryAssignments().keySet());

      return projectExpressions(
          join, Assignments.of(quantifiedComparisonSymbol, valueComparedToSubquery));
    }

    private ResolvedFunction getResolvedBuiltInAggregateFunction(
        String functionName, List<Type> argumentTypes) {
      // The same as the code in ExpressionAnalyzer
      Type type = metadata.getFunctionReturnType(functionName, argumentTypes);
      return new ResolvedFunction(
          new BoundSignature(functionName.toLowerCase(Locale.ENGLISH), type, argumentTypes),
          new FunctionId("noop"),
          FunctionKind.AGGREGATE,
          true,
          FunctionNullability.getAggregationFunctionNullability(argumentTypes.size()));
    }

    public Expression rewriteUsingBounds(
        ApplyNode.QuantifiedComparison quantifiedComparison,
        Symbol minValue,
        Symbol maxValue,
        Symbol countAllValue,
        Symbol countNonNullValue) {
      BooleanLiteral emptySetResult;
      Function<List<Expression>, Expression> quantifier;
      if (quantifiedComparison.getQuantifier() == ALL) {
        emptySetResult = TRUE_LITERAL;
        quantifier = IrUtils::combineConjuncts;
      } else {
        emptySetResult = FALSE_LITERAL;
        quantifier = IrUtils::combineDisjuncts;
      }
      Expression comparisonWithExtremeValue =
          getBoundComparisons(quantifiedComparison, minValue, maxValue);

      return new SimpleCaseExpression(
          countAllValue.toSymbolReference(),
          ImmutableList.of(new WhenClause(new GenericLiteral("INT64", "0"), emptySetResult)),
          quantifier.apply(
              ImmutableList.of(
                  comparisonWithExtremeValue,
                  new SearchedCaseExpression(
                      ImmutableList.of(
                          new WhenClause(
                              new ComparisonExpression(
                                  NOT_EQUAL,
                                  countAllValue.toSymbolReference(),
                                  countNonNullValue.toSymbolReference()),
                              new Cast(new NullLiteral(), toSqlType(BOOLEAN)))),
                      emptySetResult))));
    }

    private Expression getBoundComparisons(
        ApplyNode.QuantifiedComparison quantifiedComparison, Symbol minValue, Symbol maxValue) {
      if (mapOperator(quantifiedComparison) == EQUAL
          && quantifiedComparison.getQuantifier() == ALL) {
        // A = ALL B <=> min B = max B && A = min B
        return combineConjuncts(
            new ComparisonExpression(
                EQUAL, minValue.toSymbolReference(), maxValue.toSymbolReference()),
            new ComparisonExpression(
                EQUAL,
                quantifiedComparison.getValue().toSymbolReference(),
                maxValue.toSymbolReference()));
      }

      if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL)
          .contains(mapOperator(quantifiedComparison))) {
        // A < ALL B <=> A < min B
        // A > ALL B <=> A > max B
        // A < ANY B <=> A < max B
        // A > ANY B <=> A > min B
        Symbol boundValue =
            shouldCompareValueWithLowerBound(quantifiedComparison) ? minValue : maxValue;
        return new ComparisonExpression(
            mapOperator(quantifiedComparison),
            quantifiedComparison.getValue().toSymbolReference(),
            boundValue.toSymbolReference());
      }
      throw new IllegalArgumentException(
          "Unsupported quantified comparison: " + quantifiedComparison);
    }

    private static ComparisonExpression.Operator mapOperator(
        ApplyNode.QuantifiedComparison quantifiedComparison) {
      switch (quantifiedComparison.getOperator()) {
        case EQUAL:
          return EQUAL;
        case NOT_EQUAL:
          return NOT_EQUAL;
        case LESS_THAN:
          return LESS_THAN;
        case LESS_THAN_OR_EQUAL:
          return LESS_THAN_OR_EQUAL;
        case GREATER_THAN:
          return GREATER_THAN;
        case GREATER_THAN_OR_EQUAL:
          return GREATER_THAN_OR_EQUAL;
        default:
          throw new IllegalArgumentException(
              "Unexpected quantifiedComparison: " + quantifiedComparison.getOperator());
      }
    }

    private static boolean shouldCompareValueWithLowerBound(
        ApplyNode.QuantifiedComparison quantifiedComparison) {
      ComparisonExpression.Operator operator = mapOperator(quantifiedComparison);
      switch (quantifiedComparison.getQuantifier()) {
        case ALL:
          switch (operator) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
              return true;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
              return false;
            default:
              throw new IllegalArgumentException("Unexpected value: " + operator);
          }
        case ANY:
        case SOME:
          switch (operator) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
              return false;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
              return true;
            default:
              throw new IllegalArgumentException("Unexpected value: " + operator);
          }
        default:
          throw new IllegalArgumentException(
              "Unexpected Quantifier: " + quantifiedComparison.getQuantifier());
      }
    }

    private ProjectNode projectExpressions(PlanNode input, Assignments subqueryAssignments) {
      Assignments assignments =
          Assignments.builder()
              .putIdentities(input.getOutputSymbols())
              .putAll(subqueryAssignments)
              .build();
      return new ProjectNode(idAllocator.genPlanNodeId(), input, assignments);
    }
  }
}
