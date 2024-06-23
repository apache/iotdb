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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
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

// return whether input expression has attribute column predicate
public class CheckSchemaPredicateVisitor
    extends PredicateVisitor<Boolean, CheckSchemaPredicateVisitor.Context> {

  @Override
  protected Boolean visitInPredicate(InPredicate node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitIsNullPredicate(IsNullPredicate node, Context context) {
    String columnName = ((SymbolReference) node.getValue()).getName();
    return context
        .table
        .getColumnSchema(columnName)
        .getColumnCategory()
        .equals(TsTableColumnCategory.ATTRIBUTE);
  }

  @Override
  protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitLikePredicate(LikePredicate node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitLogicalExpression(LogicalExpression node, Context context) {
    return node.getTerms().get(0).accept(this, context)
        || node.getTerms().get(1).accept(this, context);
  }

  @Override
  protected Boolean visitNotExpression(NotExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitComparisonExpression(ComparisonExpression node, Context context) {
    String columnName;
    if (node.getLeft() instanceof Literal) {
      if (node.getRight() instanceof Identifier) {
        columnName = ((Identifier) (node.getRight())).getValue();
      } else {
        columnName = ((SymbolReference) (node.getRight())).getName();
      }
    } else {
      if (node.getLeft() instanceof Identifier) {
        columnName = ((Identifier) (node.getLeft())).getValue();
      } else {
        columnName = ((SymbolReference) (node.getLeft())).getName();
      }
    }
    return context
        .table
        .getColumnSchema(columnName)
        .getColumnCategory()
        .equals(TsTableColumnCategory.ATTRIBUTE);
  }

  @Override
  protected Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitSearchedCaseExpression(SearchedCaseExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitIfExpression(IfExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitNullIfExpression(NullIfExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected Boolean visitBetweenPredicate(BetweenPredicate node, Context context) {
    return visitExpression(node, context);
  }

  public static class Context {
    private final TsTable table;

    public Context(TsTable table) {
      this.table = table;
    }
  }
}
