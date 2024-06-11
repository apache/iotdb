/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.read.common.type.Type;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalysis {
  private final Map<NodeRef<Expression>, Type> expressionTypes;
  //  private final Map<NodeRef<Expression>, Type> expressionCoercions;
  //  private final Set<NodeRef<Expression>> typeOnlyCoercions;
  private final Map<NodeRef<Expression>, ResolvedField> columnReferences;
  private final Set<NodeRef<InPredicate>> subqueryInPredicates;
  private final Set<NodeRef<SubqueryExpression>> subqueries;
  private final Set<NodeRef<ExistsPredicate>> existsSubqueries;
  private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons;

  public ExpressionAnalysis(
      Map<NodeRef<Expression>, Type> expressionTypes,
      Set<NodeRef<InPredicate>> subqueryInPredicates,
      Set<NodeRef<SubqueryExpression>> subqueries,
      Set<NodeRef<ExistsPredicate>> existsSubqueries,
      Map<NodeRef<Expression>, ResolvedField> columnReferences,
      Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons) {
    this.expressionTypes =
        ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
    //    this.expressionCoercions =
    //        ImmutableMap.copyOf(requireNonNull(expressionCoercions, "expressionCoercions is
    // null"));
    //    this.typeOnlyCoercions =
    //        ImmutableSet.copyOf(requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null"));
    this.columnReferences =
        ImmutableMap.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
    this.subqueryInPredicates =
        ImmutableSet.copyOf(requireNonNull(subqueryInPredicates, "subqueryInPredicates is null"));
    this.subqueries = ImmutableSet.copyOf(requireNonNull(subqueries, "subqueries is null"));
    this.existsSubqueries =
        ImmutableSet.copyOf(requireNonNull(existsSubqueries, "existsSubqueries is null"));
    this.quantifiedComparisons =
        ImmutableSet.copyOf(requireNonNull(quantifiedComparisons, "quantifiedComparisons is null"));
  }

  public Type getType(Expression expression) {
    return expressionTypes.get(NodeRef.of(expression));
  }

  public Map<NodeRef<Expression>, Type> getExpressionTypes() {
    return expressionTypes;
  }

  public boolean isColumnReference(Expression node) {
    return columnReferences.containsKey(NodeRef.of(node));
  }

  public Set<NodeRef<InPredicate>> getSubqueryInPredicates() {
    return subqueryInPredicates;
  }

  public Set<NodeRef<SubqueryExpression>> getSubqueries() {
    return subqueries;
  }

  public Set<NodeRef<ExistsPredicate>> getExistsSubqueries() {
    return existsSubqueries;
  }

  public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons() {
    return quantifiedComparisons;
  }
}
