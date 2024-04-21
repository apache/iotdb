/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FieldReference;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PlanBuilder {

  private final PlanNode root;

  // current mappings of underlying field -> symbol for translating direct field references
  private final Symbol[] fieldSymbols;

  public PlanBuilder(PlanNode root, Symbol[] fieldSymbols) {
    requireNonNull(root, "root is null");

    this.root = root;
    this.fieldSymbols = fieldSymbols;
  }

  public static PlanBuilder newPlanBuilder(
      RelationPlan plan, Analysis analysis, SessionInfo session) {
    return newPlanBuilder(plan, analysis, ImmutableMap.of(), session);
  }

  public static PlanBuilder newPlanBuilder(
      RelationPlan plan,
      Analysis analysis,
      Map<ScopeAware<Expression>, Symbol> mappings,
      SessionInfo session) {
    return new PlanBuilder(plan.getRoot(), plan.getFieldMappings().toArray(new Symbol[0]));
  }

  public PlanBuilder withNewRoot(PlanNode root) {
    return new PlanBuilder(root, fieldSymbols);
  }

  public PlanBuilder withScope(Scope scope, List<Symbol> fields) {
    return new PlanBuilder(root, fields.toArray(new Symbol[0]));
  }

  public PlanNode getRoot() {
    return root;
  }

  public Symbol[] getFieldSymbols() {
    return this.fieldSymbols;
  }

  public Symbol translate(Analysis analysis, Expression expression) {
    verify(
        analysis.isAnalyzed(expression),
        "Expression is not analyzed (%s): %s",
        expression.getClass().getName(),
        expression);
    Expression ret = translate(expression, true);
    return Symbol.from(ret);
  }

  private Expression translate(Expression expression, boolean isRoot) {
    if (expression instanceof FieldReference) {}

    return expression;
  }

  public <T extends Expression> PlanBuilder appendProjections(
      Iterable<T> expressions,
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      QueryId idAllocator) {
    Assignments.Builder projections = Assignments.builder();

    // add an identity projection for underlying plan
    projections.putIdentities(root.getOutputSymbols());

    Set<String> set = new HashSet<>();
    for (Symbol symbol : root.getOutputSymbols()) {
      set.add(symbol.toString());
    }

    Map<Expression, Symbol> mappings = new HashMap<>();
    for (T expression : expressions) {
      // Skip any expressions that have already been translated and recorded in the
      // translation map, or that are duplicated in the list of exp
      if (!mappings.containsKey(expression)
          && !set.contains(expression.toString().toLowerCase())
          && !(expression instanceof FieldReference)) {
        set.add(expression.toString());
        Symbol symbol = symbolAllocator.newSymbol("expr", analysis.getType(expression));
        projections.put(symbol, expression);
        mappings.put(expression, symbol);
      }
    }

    return new PlanBuilder(
        new ProjectNode(idAllocator.genPlanNodeId(), root, projections.build()), fieldSymbols);
  }
}
