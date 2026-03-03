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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import java.util.Optional;
import java.util.OptionalInt;

import static java.lang.Math.toIntExact;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.rowNumber;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * Pushes a row-number upper-bound filter (e.g. {@code rn <= N}) into {@link RowNumberNode} by
 * setting {@code maxRowCountPerPartition}. The filter is eliminated because the row-number node
 * guarantees that no partition will emit more than {@code N} rows.
 *
 * <p>Before:
 *
 * <pre>
 *   FilterNode(rn &lt;= N)
 *     └── RowNumberNode(rowNumberSymbol=rn, maxRowCountPerPartition=empty)
 * </pre>
 *
 * After:
 *
 * <pre>
 *   RowNumberNode(rowNumberSymbol=rn, maxRowCountPerPartition=N)
 * </pre>
 */
public class PushFilterIntoRowNumber implements Rule<FilterNode> {
  private static final Capture<RowNumberNode> CHILD = newCapture();

  private static final Pattern<FilterNode> PATTERN =
      filter()
          .with(
              source()
                  .matching(
                      rowNumber()
                          .matching(
                              rowNumber -> !rowNumber.getMaxRowCountPerPartition().isPresent())
                          .capturedAs(CHILD)));

  @Override
  public Pattern<FilterNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(FilterNode node, Captures captures, Context context) {
    RowNumberNode rowNumberNode = captures.get(CHILD);
    Symbol rowNumberSymbol = rowNumberNode.getRowNumberSymbol();

    OptionalInt upperBound = extractUpperBound(node.getPredicate(), rowNumberSymbol);
    if (!upperBound.isPresent()) {
      return Result.empty();
    }

    if (upperBound.getAsInt() <= 0) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        new RowNumberNode(
            rowNumberNode.getPlanNodeId(),
            rowNumberNode.getChild(),
            rowNumberNode.getPartitionBy(),
            rowNumberNode.isOrderSensitive(),
            rowNumberSymbol,
            Optional.of(upperBound.getAsInt())));
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
        return OptionalInt.of(toIntExact(constantValue));
      default:
        return OptionalInt.empty();
    }
  }
}
