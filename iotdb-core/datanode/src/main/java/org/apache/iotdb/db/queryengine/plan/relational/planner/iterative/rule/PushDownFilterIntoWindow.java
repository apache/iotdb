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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.toTopNRankingType;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

public class PushDownFilterIntoWindow implements Rule<FilterNode> {
  private static final Capture<WindowNode> childCapture = newCapture();

  private final Pattern<FilterNode> pattern;
  private final PlannerContext plannerContext;

  public PushDownFilterIntoWindow(PlannerContext plannerContext) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    this.pattern =
        filter()
            .with(
                source()
                    .matching(
                        window()
                            .matching(
                                window -> window.getSpecification().getOrderingScheme().isPresent())
                            .matching(window -> toTopNRankingType(window).isPresent())
                            .capturedAs(childCapture)));
  }

  @Override
  public Pattern<FilterNode> getPattern() {
    return pattern;
  }

  @Override
  public Result apply(FilterNode node, Captures captures, Context context) {
    WindowNode windowNode = captures.get(childCapture);
    Optional<TopKRankingNode.RankingType> rankingType = toTopNRankingType(windowNode);
    Symbol rankingSymbol = getOnlyElement(windowNode.getWindowFunctions().keySet());

    OptionalInt upperBound = extractUpperBoundFromComparison(node.getPredicate(), rankingSymbol);

    if (!upperBound.isPresent()) {
      return Result.empty();
    }

    if (upperBound.getAsInt() <= 0) {
      return Result.ofPlanNode(
          new ValuesNode(node.getPlanNodeId(), node.getOutputSymbols(), ImmutableList.of()));
    }

    TopKRankingNode newSource =
        new TopKRankingNode(
            windowNode.getPlanNodeId(),
            windowNode.getChild(),
            windowNode.getSpecification(),
            rankingType.get(),
            rankingSymbol,
            upperBound.getAsInt(),
            false);

    if (needToKeepFilter(node.getPredicate(), rankingSymbol, upperBound.getAsInt())) {
      return Result.ofPlanNode(
          new FilterNode(node.getPlanNodeId(), newSource, node.getPredicate()));
    }

    return Result.ofPlanNode(newSource);
  }

  private OptionalInt extractUpperBoundFromComparison(Expression predicate, Symbol rankingSymbol) {
    if (!(predicate instanceof ComparisonExpression)) {
      return OptionalInt.empty();
    }

    ComparisonExpression comparison = (ComparisonExpression) predicate;
    Expression left = comparison.getLeft();
    Expression right = comparison.getRight();

    if (!(left instanceof SymbolReference) || !(right instanceof Literal)) {
      return OptionalInt.empty();
    }

    SymbolReference symbolRef = (SymbolReference) left;
    if (!symbolRef.getName().equals(rankingSymbol.getName())) {
      return OptionalInt.empty();
    }

    Literal literal = (Literal) right;
    Object value = literal.getTsValue();
    if (!(value instanceof Number)) {
      return OptionalInt.empty();
    }

    long constantValue = ((Number) value).longValue();

    switch (comparison.getOperator()) {
      case LESS_THAN:
        return OptionalInt.of(toIntExact(constantValue - 1));
      case LESS_THAN_OR_EQUAL:
      case EQUAL:
        return OptionalInt.of(toIntExact(constantValue));
      default:
        return OptionalInt.empty();
    }
  }

  private boolean needToKeepFilter(Expression predicate, Symbol rankingSymbol, int upperBound) {
    if (!(predicate instanceof ComparisonExpression)) {
      return true;
    }

    ComparisonExpression comparison = (ComparisonExpression) predicate;

    if (comparison.getOperator() == ComparisonExpression.Operator.EQUAL) {
      return true;
    }

    if (comparison.getOperator() == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL
        || comparison.getOperator() == ComparisonExpression.Operator.LESS_THAN) {
      return false;
    }

    return true;
  }
}
