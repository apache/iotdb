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

import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.ResolvedField;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTranslateVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.*;
import org.apache.iotdb.db.queryengine.plan.relational.utils.AstUtils;

import com.google.common.collect.ImmutableMap;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware.scopeAwareKey;

/**
 * Keeps mappings of fields and AST expressions to symbols in the current plan within query
 * boundary.
 *
 * <p>AST and IR expressions use the same class hierarchy ({@link
 * org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression}, but differ in the following
 * ways:
 * <li>AST expressions contain Identifiers, while IR expressions contain SymbolReferences
 * <li>FunctionCalls in AST expressions are SQL function names. In IR expressions, they contain an
 *     encoded name representing a resolved function
 */
public class TranslationMap {
  // all expressions are rewritten in terms of fields declared by this relation plan
  private final Scope scope;
  private final Analysis analysis;
  private final Optional<TranslationMap> outerContext;
  // TODO unused now
  private final PlannerContext plannerContext;

  // current mappings of underlying field -> symbol for translating direct field references
  private final Symbol[] fieldSymbols;

  // current mappings of sub-expressions -> symbol
  private final Map<ScopeAware<Expression>, Symbol> astToSymbols;
  // TODO unused now
  private final Map<NodeRef<Expression>, Symbol> substitutions;

  public TranslationMap(
      Optional<TranslationMap> outerContext,
      Scope scope,
      Analysis analysis,
      List<Symbol> fieldSymbols,
      PlannerContext plannerContext) {
    this(
        outerContext,
        scope,
        analysis,
        fieldSymbols.toArray(new Symbol[0]).clone(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        plannerContext);
  }

  public TranslationMap(
      Optional<TranslationMap> outerContext,
      Scope scope,
      Analysis analysis,
      List<Symbol> fieldSymbols,
      Map<ScopeAware<Expression>, Symbol> astToSymbols,
      PlannerContext plannerContext) {
    this(
        outerContext,
        scope,
        analysis,
        fieldSymbols.toArray(new Symbol[0]),
        astToSymbols,
        ImmutableMap.of(),
        plannerContext);
  }

  public TranslationMap(
      Optional<TranslationMap> outerContext,
      Scope scope,
      Analysis analysis,
      Symbol[] fieldSymbols,
      Map<ScopeAware<Expression>, Symbol> astToSymbols,
      Map<NodeRef<Expression>, Symbol> substitutions,
      PlannerContext plannerContext) {
    this.outerContext = requireNonNull(outerContext, "outerContext is null");
    this.scope = requireNonNull(scope, "scope is null");
    this.analysis = requireNonNull(analysis, "analysis is null");
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    this.substitutions = ImmutableMap.copyOf(substitutions);

    requireNonNull(fieldSymbols, "fieldSymbols is null");
    this.fieldSymbols = fieldSymbols.clone();

    requireNonNull(astToSymbols, "astToSymbols is null");
    this.astToSymbols = ImmutableMap.copyOf(astToSymbols);

    checkArgument(
        scope.getLocalScopeFieldCount() == fieldSymbols.length,
        "scope: %s, fields mappings: %s",
        scope.getRelationType().getAllFieldCount(),
        fieldSymbols.length);

    astToSymbols.keySet().stream()
        .map(ScopeAware::getNode)
        .forEach(TranslationMap::verifyAstExpression);
  }

  public TranslationMap withScope(Scope scope, List<Symbol> fields) {
    return new TranslationMap(
        outerContext,
        scope,
        analysis,
        fields.toArray(new Symbol[0]),
        astToSymbols,
        substitutions,
        plannerContext);
  }

  public TranslationMap withNewMappings(
      Map<ScopeAware<Expression>, Symbol> mappings, List<Symbol> fields) {
    return new TranslationMap(outerContext, scope, analysis, fields, mappings, plannerContext);
  }

  public TranslationMap withAdditionalMappings(Map<ScopeAware<Expression>, Symbol> mappings) {
    Map<ScopeAware<Expression>, Symbol> newMappings = new HashMap<>();
    newMappings.putAll(this.astToSymbols);
    newMappings.putAll(mappings);

    return new TranslationMap(
        outerContext, scope, analysis, fieldSymbols, newMappings, substitutions, plannerContext);
  }

  public TranslationMap withAdditionalIdentityMappings(Map<NodeRef<Expression>, Symbol> mappings) {
    Map<NodeRef<Expression>, Symbol> newMappings = new HashMap<>();
    newMappings.putAll(this.substitutions);
    newMappings.putAll(mappings);

    return new TranslationMap(
        outerContext, scope, analysis, fieldSymbols, astToSymbols, newMappings, plannerContext);
  }

  public List<Symbol> getFieldSymbolsList() {
    return Collections.unmodifiableList(Arrays.asList(fieldSymbols));
  }

  public Symbol[] getFieldSymbols() {
    return fieldSymbols;
  }

  public Map<ScopeAware<Expression>, Symbol> getMappings() {
    return astToSymbols;
  }

  public Analysis getAnalysis() {
    return analysis;
  }

  public boolean canTranslate(Expression expression) {
    verifyAstExpression(expression);

    if (astToSymbols.containsKey(scopeAwareKey(expression, analysis, scope))
        || substitutions.containsKey(NodeRef.of(expression))
        || expression instanceof FieldReference) {
      return true;
    }

    if (analysis.isColumnReference(expression)) {
      ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(expression));
      return scope.isLocalScope(field.getScope());
    }

    return false;
  }

  public Expression rewrite(Expression expression) {
    verifyAstExpression(expression);
    verify(
        analysis.isAnalyzed(expression),
        "Expression is not analyzed (%s): %s",
        expression.getClass().getName(),
        expression);

    return ExpressionTranslateVisitor.translateToSymbolReference(expression, this);
  }

  public Optional<SymbolReference> tryGetMapping(Expression expression) {
    Symbol symbol = substitutions.get(NodeRef.of(expression));
    if (symbol == null) {
      symbol = astToSymbols.get(scopeAwareKey(expression, analysis, scope));
    }

    return Optional.ofNullable(symbol).map(Symbol::toSymbolReference);
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

    if (scope.isLocalScope(field.getScope())) {
      return Optional.of(fieldSymbols[field.getHierarchyFieldIndex()]);
    }

    if (outerContext.isPresent()) {
      return Optional.of(Symbol.from(outerContext.get().rewrite(expression)));
    }

    return Optional.empty();
  }

  private static void verifyAstExpression(Expression astExpression) {
    verify(
        AstUtils.preOrder(astExpression)
            .noneMatch(expression -> expression instanceof SymbolReference),
        "symbol references are not allowed");
  }

  public Scope getScope() {
    return scope;
  }
}
