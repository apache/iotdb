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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultExpressionTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ExpressionExtractor.extractExpressions;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanNodeSearcher.searchFrom;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup.noLookup;

public final class SymbolsExtractor {
  private SymbolsExtractor() {}

  public static Set<Symbol> extractUnique(PlanNode node) {
    ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
    extractExpressions(node).forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

    return uniqueSymbols.build();
  }

  public static Set<Symbol> extractUniqueNonRecursive(PlanNode node) {
    ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
    extractExpressionsNonRecursive(node)
        .forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

    return uniqueSymbols.build();
  }

  public static Set<Symbol> extractUnique(PlanNode node, Lookup lookup) {
    ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
    extractExpressions(node, lookup)
        .forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

    return uniqueSymbols.build();
  }

  public static Set<Symbol> extractUnique(Expression expression) {
    return ImmutableSet.copyOf(extractAll(expression));
  }

  public static Set<Symbol> extractUnique(Iterable<? extends Expression> expressions) {
    ImmutableSet.Builder<Symbol> unique = ImmutableSet.builder();
    for (Expression expression : expressions) {
      unique.addAll(extractAll(expression));
    }
    return unique.build();
  }

  public static Set<Symbol> extractUnique(AggregationNode.Aggregation aggregation) {
    return ImmutableSet.copyOf(extractAll(aggregation));
  }

  /*
  public static Set<Symbol> extractUnique(WindowNode.Function function)
  {
      return ImmutableSet.copyOf(extractAll(function));
  }*/

  public static List<Symbol> extractAll(Expression expression) {
    ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
    new SymbolBuilderVisitor().process(expression, builder);
    return builder.build();
  }

  public static List<Symbol> extractAll(AggregationNode.Aggregation aggregation) {
    ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
    for (Expression argument : aggregation.getArguments()) {
      builder.addAll(extractAll(argument));
    }
    aggregation.getFilter().ifPresent(builder::add);
    aggregation.getOrderingScheme().ifPresent(orderBy -> builder.addAll(orderBy.getOrderBy()));
    aggregation.getMask().ifPresent(builder::add);
    return builder.build();
  }

  /*
  public static List<Symbol> extractAll(WindowNode.Function function)
  {
      ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
      for (Expression argument : function.getArguments()) {
          builder.addAll(extractAll(argument));
      }
      function.getFrame().getEndValue().ifPresent(builder::add);
      function.getFrame().getSortKeyCoercedForFrameEndComparison().ifPresent(builder::add);
      function.getFrame().getStartValue().ifPresent(builder::add);
      function.getFrame().getSortKeyCoercedForFrameStartComparison().ifPresent(builder::add);
      return builder.build();
  }*/

  // to extract qualified name with prefix
  public static Set<QualifiedName> extractNames(
      Expression expression, Set<NodeRef<Expression>> columnReferences) {
    ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
    new QualifiedNameBuilderVisitor(columnReferences, true).process(expression, builder);
    return builder.build();
  }

  public static Set<QualifiedName> extractNamesNoSubqueries(
      Expression expression, Set<NodeRef<Expression>> columnReferences) {
    ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
    new QualifiedNameBuilderVisitor(columnReferences, false).process(expression, builder);
    return builder.build();
  }

  public static Set<Symbol> extractOutputSymbols(PlanNode planNode) {
    return extractOutputSymbols(planNode, noLookup());
  }

  public static Set<Symbol> extractOutputSymbols(PlanNode planNode, Lookup lookup) {
    return searchFrom(planNode, lookup).findAll().stream()
        .flatMap(node -> node.getOutputSymbols().stream())
        .collect(toImmutableSet());
  }

  private static class SymbolBuilderVisitor
      extends DefaultExpressionTraversalVisitor<ImmutableList.Builder<Symbol>> {
    @Override
    protected Void visitSymbolReference(
        SymbolReference node, ImmutableList.Builder<Symbol> builder) {
      builder.add(Symbol.from(node));
      return null;
    }

    /*@Override
    protected Void visitLambdaExpression(LambdaExpression node, ImmutableList.Builder<Symbol> context)
    {
        // Symbols in lambda expression are bound to lambda arguments, so no need to extract them
        return null;
    }*/
  }

  private static class QualifiedNameBuilderVisitor
      extends DefaultTraversalVisitor<ImmutableSet.Builder<QualifiedName>> {
    private final Set<NodeRef<Expression>> columnReferences;
    private final boolean recurseIntoSubqueries;

    private QualifiedNameBuilderVisitor(
        Set<NodeRef<Expression>> columnReferences, boolean recurseIntoSubqueries) {
      this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
      this.recurseIntoSubqueries = recurseIntoSubqueries;
    }

    @Override
    protected Void visitDereferenceExpression(
        DereferenceExpression node, ImmutableSet.Builder<QualifiedName> builder) {
      if (columnReferences.contains(NodeRef.<Expression>of(node))) {
        builder.add(DereferenceExpression.getQualifiedName(node));
      } else {
        process(node.getBase(), builder);
      }
      return null;
    }

    @Override
    protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder) {
      builder.add(QualifiedName.of(node.getValue()));
      return null;
    }

    @Override
    protected Void visitSubqueryExpression(
        SubqueryExpression node, ImmutableSet.Builder<QualifiedName> context) {
      if (!recurseIntoSubqueries) {
        return null;
      }

      return super.visitSubqueryExpression(node, context);
    }
  }
}
