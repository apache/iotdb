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

import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.StringLiteral;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isSymbolReference;

public class PredicatePushIntoIndexScanChecker extends PredicateVisitor<Boolean, Void> {

  private final Set<String> idOrAttributeColumnNames;

  public PredicatePushIntoIndexScanChecker(Set<String> idOrAttributeColumnNames) {
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
      throw new IllegalStateException("Shouldn't have AND operator in index scan expression.");
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
