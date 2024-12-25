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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware.scopeAwareKey;

public class PlanBuilder {

  private final TranslationMap translations;
  private final PlanNode root;

  public PlanBuilder(TranslationMap translations, PlanNode root) {
    requireNonNull(translations, "translations is null");
    requireNonNull(root, "root is null");

    this.translations = translations;
    this.root = root;
  }

  public static PlanBuilder newPlanBuilder(RelationPlan plan, Analysis analysis) {
    return newPlanBuilder(plan, analysis, ImmutableMap.of());
  }

  public static PlanBuilder newPlanBuilder(
      RelationPlan plan, Analysis analysis, Map<ScopeAware<Expression>, Symbol> mappings) {
    return new PlanBuilder(
        new TranslationMap(
            plan.getOuterContext(),
            plan.getScope(),
            analysis,
            plan.getFieldMappings(),
            mappings,
            new PlannerContext(
                LocalExecutionPlanner.getInstance().metadata, new InternalTypeManager())),
        plan.getRoot());
  }

  public PlanBuilder withNewRoot(PlanNode root) {
    return new PlanBuilder(translations, root);
  }

  public PlanBuilder withScope(Scope scope, List<Symbol> fields) {
    return new PlanBuilder(translations.withScope(scope, fields), root);
  }

  public boolean canTranslate(Expression expression) {
    return translations.canTranslate(expression);
  }

  public TranslationMap getTranslations() {
    return translations;
  }

  public Scope getScope() {
    return translations.getScope();
  }

  public PlanNode getRoot() {
    return root;
  }

  public Symbol[] getFieldSymbols() {
    return translations.getFieldSymbols();
  }

  public Symbol translate(Expression expression) {
    return Symbol.from(translations.rewrite(expression));
  }

  public Expression rewrite(Expression root) {
    verify(
        translations.getAnalysis().isAnalyzed(root),
        "Expression is not analyzed (%s): %s",
        root.getClass().getName(),
        root);
    return translations.rewrite(root);
  }

  public <T extends Expression> PlanBuilder appendProjections(
      Iterable<T> expressions, SymbolAllocator symbolAllocator, MPPQueryContext queryContext) {
    return appendProjections(
        expressions,
        symbolAllocator,
        queryContext,
        TranslationMap::rewrite,
        TranslationMap::canTranslate);
  }

  public <T extends Expression> PlanBuilder appendProjections(
      Iterable<T> expressions,
      SymbolAllocator symbolAllocator,
      MPPQueryContext queryContext,
      BiFunction<TranslationMap, T, Expression> rewriter,
      BiPredicate<TranslationMap, T> alreadyHasTranslation) {
    Assignments.Builder projections = Assignments.builder();

    // add an identity projection for underlying plan
    projections.putIdentities(root.getOutputSymbols());
    Analysis analysis = translations.getAnalysis();
    Map<ScopeAware<Expression>, Symbol> mappings = new HashMap<>();
    for (T expression : expressions) {
      // Skip any expressions that have already been translated and recorded in the translation map,
      // or that are duplicated in the list of exp
      if (!mappings.containsKey(scopeAwareKey(expression, analysis, translations.getScope()))
          && !alreadyHasTranslation.test(translations, expression)) {
        Symbol symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));
        projections.put(symbol, rewriter.apply(translations, expression));
        mappings.put(scopeAwareKey(expression, analysis, translations.getScope()), symbol);
      }
    }

    /*if (mappings.isEmpty()) {
      return this;
    }*/

    return new PlanBuilder(
        getTranslations().withAdditionalMappings(mappings),
        new ProjectNode(queryContext.getQueryId().genPlanNodeId(), root, projections.build()));
  }
}
