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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ChangePointNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * Replaces a Filter(Window(LEAD)) change-point detection pattern with a single ChangePointNode.
 *
 * <p>Matches the SQL pattern: SELECT * FROM (SELECT *, LEAD(col) OVER (PARTITION BY ... ORDER BY
 * ...) AS next FROM t) WHERE col != next OR next IS NULL
 */
public class ReplaceFilterWindowLeadWithChangePoint implements Rule<FilterNode> {

  private static final Capture<WindowNode> WINDOW_CAPTURE = newCapture();

  private final Pattern<FilterNode> pattern;

  public ReplaceFilterWindowLeadWithChangePoint() {
    this.pattern =
        filter()
            .with(
                source()
                    .matching(
                        window()
                            .matching(ReplaceFilterWindowLeadWithChangePoint::isLeadWindow)
                            .capturedAs(WINDOW_CAPTURE)));
  }

  @Override
  public Pattern<FilterNode> getPattern() {
    return pattern;
  }

  @Override
  public Result apply(FilterNode filterNode, Captures captures, Context context) {
    WindowNode windowNode = captures.get(WINDOW_CAPTURE);

    Map.Entry<Symbol, WindowNode.Function> entry =
        getOnlyElement(windowNode.getWindowFunctions().entrySet());
    Symbol nextSymbol = entry.getKey();
    WindowNode.Function function = entry.getValue();

    List<Expression> arguments = function.getArguments();
    if (arguments.isEmpty() || !(arguments.get(0) instanceof SymbolReference)) {
      return Result.empty();
    }

    String measurementName = ((SymbolReference) arguments.get(0)).getName();
    Symbol measurementSymbol = new Symbol(measurementName);

    if (!isChangePointPredicate(filterNode.getPredicate(), measurementName, nextSymbol.getName())) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        new ChangePointNode(
            filterNode.getPlanNodeId(), windowNode.getChild(), measurementSymbol, nextSymbol));
  }

  private static boolean isLeadWindow(WindowNode window) {
    if (window.getWindowFunctions().size() != 1) {
      return false;
    }

    WindowNode.Function function = getOnlyElement(window.getWindowFunctions().values());
    String functionName = function.getResolvedFunction().getSignature().getName();
    if (!"lead".equals(functionName)) {
      return false;
    }

    List<Expression> arguments = function.getArguments();
    if (arguments.isEmpty()) {
      return false;
    }

    // LEAD with default offset (1 argument) or explicit offset=1 (2 arguments)
    if (arguments.size() == 1) {
      return arguments.get(0) instanceof SymbolReference;
    }
    if (arguments.size() == 2) {
      if (!(arguments.get(0) instanceof SymbolReference)) {
        return false;
      }
      Expression offsetExpr = arguments.get(1);
      if (offsetExpr instanceof Literal) {
        Object val = ((Literal) offsetExpr).getTsValue();
        return val instanceof Number && ((Number) val).longValue() == 1;
      }
      return false;
    }
    return false;
  }

  /**
   * Checks if the predicate matches: col != next OR next IS NULL, in either order of the OR terms.
   */
  private static boolean isChangePointPredicate(
      Expression predicate, String measurementName, String nextName) {
    if (!(predicate instanceof LogicalExpression)) {
      return false;
    }

    LogicalExpression logical = (LogicalExpression) predicate;
    if (logical.getOperator() != LogicalExpression.Operator.OR) {
      return false;
    }

    List<Expression> terms = logical.getTerms();
    if (terms.size() != 2) {
      return false;
    }

    Expression first = terms.get(0);
    Expression second = terms.get(1);

    return (isNotEqualComparison(first, measurementName, nextName) && isNullCheck(second, nextName))
        || (isNullCheck(first, nextName)
            && isNotEqualComparison(second, measurementName, nextName));
  }

  private static boolean isNotEqualComparison(
      Expression expr, String measurementName, String nextName) {
    if (!(expr instanceof ComparisonExpression)) {
      return false;
    }
    ComparisonExpression comparison = (ComparisonExpression) expr;
    if (comparison.getOperator() != ComparisonExpression.Operator.NOT_EQUAL) {
      return false;
    }

    Expression left = comparison.getLeft();
    Expression right = comparison.getRight();
    return (isSymbolRef(left, measurementName) && isSymbolRef(right, nextName))
        || (isSymbolRef(left, nextName) && isSymbolRef(right, measurementName));
  }

  private static boolean isNullCheck(Expression expr, String symbolName) {
    if (!(expr instanceof IsNullPredicate)) {
      return false;
    }
    return isSymbolRef(((IsNullPredicate) expr).getValue(), symbolName);
  }

  private static boolean isSymbolRef(Expression expr, String name) {
    return expr instanceof SymbolReference && ((SymbolReference) expr).getName().equals(name);
  }
}
