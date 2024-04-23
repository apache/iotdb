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
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.ResolvedField;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FieldReference;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PlanBuilder {

  private final PlanNode root;

  private final Analysis analysis;

  // current mappings of underlying field -> symbol for translating direct field references
  private final Symbol[] fieldSymbols;

  public PlanBuilder(PlanNode root, Analysis analysis, Symbol[] fieldSymbols) {
    requireNonNull(root, "root is null");

    this.root = root;
    this.analysis = analysis;
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
    return new PlanBuilder(
        plan.getRoot(), analysis, plan.getFieldMappings().toArray(new Symbol[0]));
  }

  public PlanBuilder withNewRoot(PlanNode root) {
    return new PlanBuilder(root, this.analysis, this.fieldSymbols);
  }

  public PlanBuilder withScope(Scope scope, List<Symbol> fields) {
    return new PlanBuilder(root, this.analysis, fields.toArray(new Symbol[0]));
  }

  public PlanNode getRoot() {
    return root;
  }

  public Symbol[] getFieldSymbols() {
    return this.fieldSymbols;
  }

  public Expression rewrite(Expression root, boolean isRoot) {
    verify(
        analysis.isAnalyzed(root),
        "Expression is not analyzed (%s): %s",
        root.getClass().getName(),
        root);
    return translate(root, isRoot);
  }

  private Expression translate(Expression expression, boolean isRoot) {
    // TODO add more translate expressions impl
    if (expression instanceof FieldReference) {
      return new SymbolReference(getSymbolForColumn(expression).get().getName());
    } else if (expression instanceof ComparisonExpression) {
      ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
      Expression left = translate(comparisonExpression.getLeft(), false);
      Expression right = translate(comparisonExpression.getRight(), false);
      return new ComparisonExpression(comparisonExpression.getOperator(), left, right);
    } else if (expression instanceof Identifier) {
      return getSymbolForColumn(expression).map(Symbol::toSymbolReference).get();
    } else if (expression instanceof LogicalExpression) {
      LogicalExpression logicalExpression = (LogicalExpression) expression;
      return new LogicalExpression(
          logicalExpression.getOperator(),
          logicalExpression.getTerms().stream()
              .map(e -> translate(e, false))
              .collect(toImmutableList()));
    }

    return expression;
  }

  private Optional<Symbol> getSymbolForColumn(Expression expression) {
    if (!analysis.isColumnReference(expression)) {
      // Expression can be a reference to lambda argument (or DereferenceExpression based on lambda
      // argument reference).
      // In such case, the expression might still be resolvable with plan.getScope() but we should
      // not resolve it.
      return Optional.empty();
    }

    ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(expression));

    if (field != null) {
      return Optional.of(fieldSymbols[field.getHierarchyFieldIndex()]);
    }

    return Optional.empty();
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
        new ProjectNode(idAllocator.genPlanNodeId(), this.root, projections.build()),
        this.analysis,
        this.fieldSymbols);
  }
}
