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

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateVisitor;
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

// Return whether input expression can not match a precise id node
public class CheckSchemaPredicateVisitor
    extends PredicateVisitor<Boolean, CheckSchemaPredicateVisitor.Context> {

  @Override
  protected Boolean visitInPredicate(final InPredicate node, final Context context) {
    return processColumn(node, context);
  }

  @Override
  protected Boolean visitIsNullPredicate(final IsNullPredicate node, final Context context) {
    return processColumn(node, context);
  }

  @Override
  protected Boolean visitIsNotNullPredicate(final IsNotNullPredicate node, final Context context) {
    return processColumn(node, context);
  }

  @Override
  protected Boolean visitLikePredicate(final LikePredicate node, final Context context) {
    return processColumn(node, context);
  }

  @Override
  protected Boolean visitLogicalExpression(final LogicalExpression node, final Context context) {
    for (Expression expression : node.getTerms()) {
      if (Boolean.TRUE.equals(this.process(expression, context))) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected Boolean visitNotExpression(final NotExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitComparisonExpression(
      final ComparisonExpression node, final Context context) {
    return processColumn(node, context);
  }

  @Override
  protected Boolean visitSimpleCaseExpression(
      final SimpleCaseExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitSearchedCaseExpression(
      final SearchedCaseExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitIfExpression(final IfExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitNullIfExpression(final NullIfExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitBetweenPredicate(final BetweenPredicate node, final Context context) {
    return visitExpression(node, context);
  }

  private boolean processColumn(final Expression node, final Context context) {
    return context
        .table
        .getColumnSchema(node.accept(ExtractPredicateColumnNameVisitor.getInstance(), null))
        .getColumnCategory()
        .equals(TsTableColumnCategory.ATTRIBUTE);
  }

  public static class Context {
    private final TsTable table;

    public Context(TsTable table) {
      this.table = table;
    }
  }
}
