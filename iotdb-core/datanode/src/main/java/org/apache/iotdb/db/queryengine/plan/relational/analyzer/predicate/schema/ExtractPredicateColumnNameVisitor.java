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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.schema;

import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

public class ExtractPredicateColumnNameVisitor extends PredicateVisitor<String, Void> {

  @Override
  protected String visitInPredicate(final InPredicate node, final Void context) {
    return ((SymbolReference) node.getValue()).getName();
  }

  @Override
  protected String visitIsNullPredicate(final IsNullPredicate node, final Void context) {
    return ((SymbolReference) node.getValue()).getName();
  }

  @Override
  protected String visitIsNotNullPredicate(final IsNotNullPredicate node, final Void context) {
    return ((SymbolReference) node.getValue()).getName();
  }

  @Override
  protected String visitLikePredicate(final LikePredicate node, final Void context) {
    return ((SymbolReference) node.getValue()).getName();
  }

  @Override
  protected String visitLogicalExpression(final LogicalExpression node, final Void context) {
    throw new UnsupportedOperationException("The logical expression has no bounded column");
  }

  @Override
  protected String visitNotExpression(final NotExpression node, final Void context) {
    throw new UnsupportedOperationException("The not expression has no bounded column");
  }

  @Override
  protected String visitComparisonExpression(final ComparisonExpression node, final Void context) {
    final String columnName;
    if (node.getLeft() instanceof Literal) {
      if (!(node.getRight() instanceof SymbolReference)) {
        throw new IllegalStateException("Can only be SymbolReference, now is " + node.getRight());
      }
      columnName = ((SymbolReference) (node.getRight())).getName();
    } else {
      if (!(node.getLeft() instanceof SymbolReference)) {
        throw new IllegalStateException("Can only be SymbolReference, now is " + node.getLeft());
      }
      columnName = ((SymbolReference) (node.getLeft())).getName();
    }
    return columnName;
  }

  @Override
  protected String visitSimpleCaseExpression(final SimpleCaseExpression node, final Void context) {
    return null;
  }

  @Override
  protected String visitSearchedCaseExpression(
      final SearchedCaseExpression node, final Void context) {
    return null;
  }

  @Override
  protected String visitIfExpression(final IfExpression node, final Void context) {
    return null;
  }

  @Override
  protected String visitNullIfExpression(final NullIfExpression node, final Void context) {
    return null;
  }

  @Override
  protected String visitBetweenPredicate(final BetweenPredicate node, final Void context) {
    return null;
  }

  private static class ExtractPredicateColumnVisitorContainer {
    private static final ExtractPredicateColumnNameVisitor instance =
        new ExtractPredicateColumnNameVisitor();
  }

  public static ExtractPredicateColumnNameVisitor getInstance() {
    return ExtractPredicateColumnNameVisitor.ExtractPredicateColumnVisitorContainer.instance;
  }

  private ExtractPredicateColumnNameVisitor() {
    // Instance
  }
}
