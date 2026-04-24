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

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValuesNode;
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

import static java.lang.Math.toIntExact;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.rowNumber;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * Pushes a row-number upper-bound filter through an identity projection into {@link RowNumberNode}
 * by setting {@code maxRowCountPerPartition}.
 *
 * <p>Before:
 *
 * <pre>
 *   FilterNode(rn &lt;= N)
 *     └── ProjectNode (identity)
 *           └── RowNumberNode(rowNumberSymbol=rn, maxRowCountPerPartition=empty)
 * </pre>
 *
 * After (for LESS_THAN / LESS_THAN_OR_EQUAL — filter fully absorbed):
 *
 * <pre>
 *   ProjectNode (identity)
 *     └── RowNumberNode(rowNumberSymbol=rn, maxRowCountPerPartition=N)
 * </pre>
 *
 * After (for EQUAL — filter kept):
 *
 * <pre>
 *   FilterNode(rn = N)
 *     └── ProjectNode (identity)
 *           └── RowNumberNode(rowNumberSymbol=rn, maxRowCountPerPartition=N)
 * </pre>
 */
public class PushPredicateThroughProjectIntoRowNumber implements Rule<FilterNode> {
  private static final Capture<ProjectNode> PROJECT = newCapture();
  private static final Capture<RowNumberNode> ROW_NUMBER = newCapture();

  private static final Pattern<FilterNode> PATTERN =
      filter()
          .with(
              source()
                  .matching(
                      project()
                          .matching(ProjectNode::isIdentity)
                          .capturedAs(PROJECT)
                          .with(
                              source()
                                  .matching(
                                      rowNumber()
                                          .matching(
                                              rn -> !rn.getMaxRowCountPerPartition().isPresent())
                                          .capturedAs(ROW_NUMBER)))));

  @Override
  public Pattern<FilterNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(FilterNode filter, Captures captures, Context context) {
    ProjectNode project = captures.get(PROJECT);
    RowNumberNode rowNumberNode = captures.get(ROW_NUMBER);

    Symbol rowNumberSymbol = rowNumberNode.getRowNumberSymbol();
    if (!project.getAssignments().getSymbols().contains(rowNumberSymbol)) {
      return Result.empty();
    }

    OptionalInt upperBound = extractUpperBound(filter.getPredicate(), rowNumberSymbol);
    if (!upperBound.isPresent()) {
      return Result.empty();
    }
    if (upperBound.getAsInt() <= 0) {
      return Result.ofPlanNode(
          new ValuesNode(filter.getPlanNodeId(), filter.getOutputSymbols(), ImmutableList.of()));
    }

    project =
        (ProjectNode)
            project.replaceChildren(
                ImmutableList.of(
                    new RowNumberNode(
                        rowNumberNode.getPlanNodeId(),
                        rowNumberNode.getChild(),
                        rowNumberNode.getPartitionBy(),
                        rowNumberNode.isOrderSensitive(),
                        rowNumberSymbol,
                        Optional.of(upperBound.getAsInt()))));

    if (needToKeepFilter(filter.getPredicate())) {
      return Result.ofPlanNode(
          new FilterNode(filter.getPlanNodeId(), project, filter.getPredicate()));
    }
    return Result.ofPlanNode(project);
  }

  private OptionalInt extractUpperBound(Expression predicate, Symbol rowNumberSymbol) {
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
    if (!symbolRef.getName().equals(rowNumberSymbol.getName())) {
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

  /**
   * For {@code LESS_THAN} and {@code LESS_THAN_OR_EQUAL}, the RowNumberNode with
   * maxRowCountPerPartition produces exactly the rows that satisfy the predicate (row numbers
   * 1..N), so the filter can be removed. For {@code EQUAL} (e.g. {@code rn = 5}), the RowNumberNode
   * produces rows 1..5 but only rows where {@code rn = 5} are wanted, so the filter must be kept.
   */
  private static boolean needToKeepFilter(Expression predicate) {
    if (!(predicate instanceof ComparisonExpression)) {
      return true;
    }

    ComparisonExpression comparison = (ComparisonExpression) predicate;
    switch (comparison.getOperator()) {
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return false;
      default:
        return true;
    }
  }
}
