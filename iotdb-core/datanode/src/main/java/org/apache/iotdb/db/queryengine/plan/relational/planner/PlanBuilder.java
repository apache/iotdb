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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.session.Session;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

class PlanBuilder {
  private final PlanNode root;

  public PlanBuilder(PlanNode root) {
    requireNonNull(root, "root is null");

    this.root = root;
  }

  public static PlanBuilder newPlanBuilder(RelationPlan plan, Analysis analysis, Session session) {
    return newPlanBuilder(plan, analysis, ImmutableMap.of(), session);
  }

  public static PlanBuilder newPlanBuilder(
      RelationPlan plan,
      Analysis analysis,
      Map<ScopeAware<Expression>, Symbol> mappings,
      Session session) {
    return new PlanBuilder(plan.getRoot());
  }

  public PlanBuilder withNewRoot(PlanNode root) {
    return new PlanBuilder(root);
  }

  public PlanBuilder withScope(Scope scope, List<Symbol> fields) {
    return new PlanBuilder(root);
  }

  public PlanNode getRoot() {
    return root;
  }

  public <T extends Expression> PlanBuilder appendProjections(
      Iterable<T> expressions, SymbolAllocator symbolAllocator, QueryId idAllocator) {
    Assignments.Builder projections = Assignments.builder();

    // add an identity projection for underlying plan
    // TODO needed?
    // projections.putIdentities(root.getOutputSymbols());

    Map<ScopeAware<Expression>, Symbol> mappings = new HashMap<>();
    //        for (T expression : expressions) {
    //            // Skip any expressions that have already been translated and recorded in the
    // translation map, or that are duplicated in the list of exp
    //            if (!mappings.containsKey(scopeAwareKey(expression, translations.getAnalysis(),
    //                    translations.getScope())) && !alreadyHasTranslation.test(translations,
    // expression)) {
    //                Symbol symbol = symbolAllocator.newSymbol("expr",
    // translations.getAnalysis().getType(expression));
    //                projections.put(symbol, rewriter.apply(translations, expression));
    //                mappings.put(scopeAwareKey(expression, translations.getAnalysis(),
    // translations.getScope()), symbol);
    //            }
    //        }

    return new PlanBuilder(new ProjectNode(idAllocator.genPlanNodeId(), root, projections.build()));
  }
}
