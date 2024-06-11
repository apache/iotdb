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

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isSymbolReference;

public class PredicatePushIntoMetadataChecker extends PredicateVisitor<Boolean, Void> {

  private final Set<String> idOrAttributeColumnNames;

  public static boolean check(Set<String> idOrAttributeColumnNames, Expression expression) {
    return new PredicatePushIntoMetadataChecker(idOrAttributeColumnNames).process(expression);
  }

  public PredicatePushIntoMetadataChecker(Set<String> idOrAttributeColumnNames) {
    this.idOrAttributeColumnNames = idOrAttributeColumnNames;
  }

  @Override
  public Boolean visitExpression(Expression expression, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitInPredicate(InPredicate node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitIsNullPredicate(IsNullPredicate node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitLikePredicate(LikePredicate node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitLogicalExpression(LogicalExpression node, Void context) {
    if (node.getOperator() == LogicalExpression.Operator.AND) {
      throw new IllegalStateException(
          "Shouldn't have AND operator in PredicatePushIntoMetadataChecker.");
    }
    List<Expression> children = node.getTerms();
    for (Expression child : children) {
      Boolean result = process(child, context);
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
  protected Boolean visitNotExpression(NotExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitComparisonExpression(ComparisonExpression node, Void context) {
    if (node.getOperator() == ComparisonExpression.Operator.EQUAL) {
      return (isIdOrAttributeColumn(node.getLeft()) && isStringLiteral(node.getRight()))
          || (isIdOrAttributeColumn(node.getRight()) && isStringLiteral(node.getLeft()));
    } else {
      return Boolean.FALSE;
    }
  }

  private boolean isIdOrAttributeColumn(Expression expression) {
    return isSymbolReference(expression)
        && idOrAttributeColumnNames.contains(((SymbolReference) expression).getName());
  }

  @Override
  protected Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitIfExpression(IfExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitNullIfExpression(NullIfExpression node, Void context) {
    return Boolean.FALSE;
  }

  @Override
  protected Boolean visitBetweenPredicate(BetweenPredicate node, Void context) {
    return Boolean.FALSE;
  }

  public static boolean isStringLiteral(Expression expression) {
    return expression instanceof StringLiteral;
  }
}
