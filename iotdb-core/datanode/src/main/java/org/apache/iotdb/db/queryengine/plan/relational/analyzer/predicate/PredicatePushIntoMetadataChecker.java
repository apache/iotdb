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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateCombineIntoTableScanChecker.isInListAllLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isSymbolReference;

/**
 * only the following predicate will return true: 1. tagColumn = 'XXX' 2. 'XXX' = tagColumn 3.
 * attributeColumn = 'XXX' 4. 'XXX' = attributeColumn 5. tagColumn/attributeColumn IS NULL 6. using
 * or to combine the above expression
 */
public class PredicatePushIntoMetadataChecker extends PredicateVisitor<Boolean, Void> {

  private final Set<String> idOrAttributeColumnNames;

  public static boolean check(
      final Set<String> idOrAttributeColumnNames, final Expression expression) {
    return new PredicatePushIntoMetadataChecker(idOrAttributeColumnNames).process(expression);
  }

  public PredicatePushIntoMetadataChecker(final Set<String> idOrAttributeColumnNames) {
    this.idOrAttributeColumnNames = idOrAttributeColumnNames;
  }

  @Override
  public Boolean visitExpression(final Expression expression, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitInPredicate(final InPredicate node, final Void context) {
    return isIdOrAttributeColumn(node.getValue()) && isInListAllLiteral(node);
  }

  @Override
  protected Boolean visitIsNullPredicate(final IsNullPredicate node, final Void context) {
    return isIdOrAttributeColumn(node.getValue());
  }

  @Override
  protected Boolean visitIsNotNullPredicate(final IsNotNullPredicate node, final Void context) {
    return isIdOrAttributeColumn(node.getValue());
  }

  @Override
  protected Boolean visitLikePredicate(final LikePredicate node, final Void context) {
    return isIdOrAttributeColumn(node.getValue());
  }

  @Override
  protected Boolean visitLogicalExpression(final LogicalExpression node, final Void context) {
    final List<Expression> children = node.getTerms();
    for (final Expression child : children) {
      final Boolean result = process(child, context);
      if (result == null) {
        throw new IllegalStateException("Should never return null.");
      }
      if (!result) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  @Override
  protected Boolean visitNotExpression(final NotExpression node, final Void context) {
    return node.getValue().accept(this, context);
  }

  @Override
  protected Boolean visitComparisonExpression(final ComparisonExpression node, final Void context) {
    return isIdOrAttributeOrLiteral(node.getLeft()) && isIdOrAttributeOrLiteral(node.getRight());
  }

  private boolean isIdOrAttributeOrLiteral(final Expression expression) {
    return isIdOrAttributeColumn(expression) || isStringLiteral(expression);
  }

  private boolean isIdOrAttributeColumn(final Expression expression) {
    return isSymbolReference(expression)
        && idOrAttributeColumnNames.contains(((SymbolReference) expression).getName());
  }

  @Override
  protected Boolean visitSimpleCaseExpression(final SimpleCaseExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitSearchedCaseExpression(
      final SearchedCaseExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitIfExpression(final IfExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitNullIfExpression(final NullIfExpression node, final Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitBetweenPredicate(final BetweenPredicate node, final Void context) {
    return isIdOrAttributeOrLiteral(node.getValue())
        && isIdOrAttributeOrLiteral(node.getMin())
        && isIdOrAttributeOrLiteral(node.getMax());
  }

  public static boolean isStringLiteral(final Expression expression) {
    return expression instanceof StringLiteral;
  }
}
