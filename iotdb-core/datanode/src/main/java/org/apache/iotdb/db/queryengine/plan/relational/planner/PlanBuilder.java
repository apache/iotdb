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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.ResolvedField;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTranslateVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FieldReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
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

  public static PlanBuilder newPlanBuilder(RelationPlan plan, Analysis analysis) {
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

  public Expression rewrite(Expression root) {
    verify(
        analysis.isAnalyzed(root),
        "Expression is not analyzed (%s): %s",
        root.getClass().getName(),
        root);
    return translate(root);
  }

  private Expression translate(Expression expression) {
    return ExpressionTranslateVisitor.translateToSymbolReference(expression, this);
  }

  public Optional<Symbol> getSymbolForColumn(Expression expression) {
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
      MPPQueryContext queryContext) {
    Assignments.Builder projections = Assignments.builder();

    // add an identity projection for underlying plan
    projections.putIdentities(root.getOutputSymbols());

    Set<String> symbolSet =
        root.getOutputSymbols().stream().map(Symbol::getName).collect(Collectors.toSet());

    Map<Expression, Symbol> mappings = new HashMap<>();
    for (T expression : expressions) {
      // Skip any expressions that have already been translated and recorded in the
      // translation map, or that are duplicated in the list of exp
      if (!mappings.containsKey(expression)
          && !symbolSet.contains(expression.toString().toLowerCase())
          && !(expression instanceof FieldReference)) {
        symbolSet.add(expression.toString());
        // Symbol symbol = symbolAllocator.newSymbol("expr", analysis.getType(expression));
        Symbol symbol =
            symbolAllocator.newSymbol(expression.toString(), analysis.getType(expression));
        queryContext.getTypeProvider().putTableModelType(symbol, analysis.getType(expression));
        projections.put(symbol, translate(expression));
        mappings.put(expression, symbol);
      }
    }

    return new PlanBuilder(
        new ProjectNode(queryContext.getQueryId().genPlanNodeId(), this.root, projections.build()),
        this.analysis,
        this.fieldSymbols);
  }
}
