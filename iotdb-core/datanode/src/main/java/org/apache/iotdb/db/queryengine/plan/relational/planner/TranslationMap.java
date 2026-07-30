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

import org.apache.iotdb.commons.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.ResolvedField;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware.scopeAwareKey;

/**
 * Keeps mappings of fields and AST expressions to symbols in the current plan within query
 * boundary.
 *
 * <p>AST and IR expressions use the same class hierarchy ({@link
 * org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression}, but differ in the
 * following ways:
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
    this.outerContext =
        requireNonNull(outerContext, DataNodeQueryMessages.EXCEPTION_OUTERCONTEXT_IS_NULL_031CD366);
    this.scope = requireNonNull(scope, DataNodeQueryMessages.EXCEPTION_SCOPE_IS_NULL_4F364BA2);
    this.analysis =
        requireNonNull(analysis, DataNodeQueryMessages.EXCEPTION_ANALYSIS_IS_NULL_66666A58);
    this.plannerContext =
        requireNonNull(
            plannerContext, DataNodeQueryMessages.EXCEPTION_PLANNERCONTEXT_IS_NULL_B7C7DE50);
    this.substitutions = ImmutableMap.copyOf(substitutions);

    requireNonNull(fieldSymbols, DataNodeQueryMessages.EXCEPTION_FIELDSYMBOLS_IS_NULL_5130E49C);
    this.fieldSymbols = fieldSymbols.clone();

    requireNonNull(astToSymbols, DataNodeQueryMessages.EXCEPTION_ASTTOSYMBOLS_IS_NULL_80B3970F);
    this.astToSymbols = ImmutableMap.copyOf(astToSymbols);

    //    checkArgument(
    //        scope.getLocalScopeFieldCount() == fieldSymbols.length,
    //        "scope: %s, fields mappings: %s",
    //        scope.getRelationType().getAllFieldCount(),
    //        fieldSymbols.length);

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
        DataNodeQueryMessages
            .EXCEPTION_EXPRESSION_IS_NOT_ANALYZED_LEFT_PAREN_ARG_RIGHT_PAREN_COLON_ARG_DAE760B6,
        expression.getClass().getName(),
        expression);

    return ExpressionTreeRewriter.rewriteWith(
        new ExpressionRewriter<Void>() {
          @Override
          protected Expression rewriteExpression(
              Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Optional<SymbolReference> mapped = tryGetMapping(node);
            if (mapped.isPresent()) {
              return mapped.get();
            }

            return treeRewriter.defaultRewrite(node, context);
          }

          @Override
          public Expression rewriteFieldReference(
              FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Optional<SymbolReference> mapped = tryGetMapping(node);
            return mapped.orElseGet(
                () ->
                    getSymbolForColumn(node)
                        .map(Symbol::toSymbolReference)
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    format(
                                        DataNodeQueryMessages.NO_SYMBOL_MAPPING_FOR_NODE_FMT,
                                        node,
                                        node.getFieldIndex()))));
          }

          @Override
          public Expression rewriteIdentifier(
              Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Optional<SymbolReference> mapped = tryGetMapping(node);
            if (mapped.isPresent()) {
              return mapped.get();
            }

            return getSymbolForColumn(node)
                .map(symbol -> (Expression) symbol.toSymbolReference())
                .orElse(node);
          }

          @Override
          public Expression rewriteFunctionCall(
              FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            // for function in RPR, do this change: FunctionCall -> SymbolReference
            if (analysis.isPatternNavigationFunction(node)) {
              return coerceIfNecessary(
                  node, treeRewriter.rewrite(node.getArguments().get(0), context));
            }

            Optional<SymbolReference> mapped = tryGetMapping(node);
            if (mapped.isPresent()) {
              return mapped.get();
            }

            // ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
            // checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

            List<Expression> newArguments =
                node.getArguments().stream()
                    .map(TranslationMap.this::rewrite)
                    .collect(Collectors.toList());
            return new FunctionCall(node.getName(), node.isDistinct(), newArguments);
          }

          @Override
          public Expression rewriteTrim(
              Trim node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Optional<SymbolReference> mapped = tryGetMapping(node);
            if (mapped.isPresent()) {
              return mapped.get();
            }

            List<Expression> newArguments = new ArrayList<>();
            newArguments.add(rewrite(node.getTrimSource()));
            node.getTrimCharacter().ifPresent(argument -> newArguments.add(rewrite(argument)));

            return new FunctionCall(
                QualifiedName.of(node.getSpecification().getFunctionName()), newArguments);
          }

          @Override
          public Expression rewriteDereferenceExpression(
              DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Optional<SymbolReference> mapped = tryGetMapping(node);
            if (mapped.isPresent()) {
              return coerceIfNecessary(node, mapped.get());
            }

            if (analysis.isColumnReference(node)) {
              return coerceIfNecessary(
                  node,
                  getSymbolForColumn(node)
                      .map(Symbol::toSymbolReference)
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  format(DataNodeQueryMessages.NO_MAPPING_FOR_S, node))));
            } else {
              throw new IllegalStateException(
                  DataNodeQueryMessages.SUBSCRIPT_IS_NOT_SUPPORTED_IN_CURRENT_VERSION);
            }
          }

          private Expression coerceIfNecessary(Expression original, Expression rewritten) {
            // Don't add a coercion for the top-level expression. That depends on the context the
            // expression is used, and it's the responsibility of the caller.
            if (original == expression) {
              return rewritten;
            }

            return QueryPlanner.coerceIfNecessary(analysis, original, rewritten);
          }

          @Override
          public Expression rewriteLikePredicate(
              LikePredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Optional<SymbolReference> mapped = tryGetMapping(node);
            if (mapped.isPresent()) {
              return mapped.get();
            }
            Expression value = treeRewriter.rewrite(node.getValue(), context);
            Expression pattern = treeRewriter.rewrite(node.getPattern(), context);
            Optional<Expression> escape =
                node.getEscape().map(e -> treeRewriter.rewrite(e, context));
            return escape.isPresent()
                ? new LikePredicate(value, pattern, escape.get())
                : new LikePredicate(value, pattern, null);
          }

          @Override
          public Expression rewriteParameter(
              Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Optional<SymbolReference> mapped = tryGetMapping(node);
            if (mapped.isPresent()) {
              return coerceIfNecessary(node, mapped.get());
            }

            checkState(
                analysis.getParameters().size() > node.getId(),
                DataNodeQueryMessages.EXCEPTION_TOO_FEW_PARAMETER_VALUES_2F7358C6);
            return coerceIfNecessary(
                node, treeRewriter.rewrite(analysis.getParameters().get(NodeRef.of(node)), null));
          }

          @Override
          public Expression rewriteGenericDataType(
              GenericDataType node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            // do not rewrite identifiers within type parameters
            return node;
          }
        },
        expression);
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
        AstUtil.preOrder(astExpression).noneMatch(SymbolReference.class::isInstance),
        DataNodeQueryMessages.EXCEPTION_SYMBOL_REFERENCES_ARE_NOT_ALLOWED_93779D6C);
  }

  public Scope getScope() {
    return scope;
  }
}
