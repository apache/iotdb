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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableExpressionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

// Return whether input expression can not be bounded to a single ID
public class CheckSchemaPredicateVisitor
    extends PredicateVisitor<Boolean, CheckSchemaPredicateVisitor.Context> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CheckSchemaPredicateVisitor.class);
  private static final long LOG_INTERVAL_MS = 60_000L;
  private long lastLogTime = System.currentTimeMillis();

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
    if (node.getOperator().equals(LogicalExpression.Operator.AND)) {
      if (System.currentTimeMillis() - lastLogTime >= LOG_INTERVAL_MS) {
        LOGGER.info(
            "And expression encountered during id determined checking, will be classified into fuzzy expression. Sql: {}",
            context.queryContext.getSql());
        lastLogTime = System.currentTimeMillis();
      }
      return true;
    }
    // TODO: improve the distinct result set detection logic
    if (context.isDirectDeviceQuery) {
      return true;
    }
    return node.getTerms().stream().anyMatch(predicate -> predicate.accept(this, context));
  }

  @Override
  protected Boolean visitNotExpression(final NotExpression node, final Context context) {
    if (node.getValue().getExpressionType().equals(TableExpressionType.LOGICAL_EXPRESSION)) {
      if (System.currentTimeMillis() - lastLogTime >= LOG_INTERVAL_MS) {
        LOGGER.info(
            "Logical expression type encountered in not expression child during id determined checking, will be classified into fuzzy expression. Sql: {}",
            context.queryContext.getSql());
        lastLogTime = System.currentTimeMillis();
      }
      return true;
    }
    return node.getValue().accept(this, context);
  }

  @Override
  protected Boolean visitComparisonExpression(
      final ComparisonExpression node, final Context context) {
    return (node.getLeft() instanceof SymbolReference && node.getRight() instanceof SymbolReference)
        || processColumn(node, context);
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
    final TsTableColumnSchema schema =
        context.table.getColumnSchema(
            node.accept(ExtractPredicateColumnNameVisitor.getInstance(), null));
    return Objects.isNull(schema)
        || schema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE);
  }

  public static class Context {
    private final TsTable table;

    // For query performance analyze
    private final MPPQueryContext queryContext;
    private final boolean isDirectDeviceQuery;

    public Context(
        final TsTable table,
        final MPPQueryContext queryContext,
        final boolean isDirectDeviceQuery) {
      this.table = table;
      this.queryContext = queryContext;
      this.isDirectDeviceQuery = isDirectDeviceQuery;
    }
  }
}
