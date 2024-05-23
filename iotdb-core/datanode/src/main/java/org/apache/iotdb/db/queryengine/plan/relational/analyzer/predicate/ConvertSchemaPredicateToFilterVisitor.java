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

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceAttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceIdFilter;
import org.apache.iotdb.commons.schema.filter.impl.OrFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.relational.sql.tree.Literal;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.StringLiteral;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConvertSchemaPredicateToFilterVisitor
    extends PredicateVisitor<SchemaFilter, ConvertSchemaPredicateToFilterVisitor.Context> {

  @Override
  protected SchemaFilter visitInPredicate(InPredicate node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitIsNullPredicate(IsNullPredicate node, Context context) {
    String columnName = ((SymbolReference) node.getValue()).getName();
    if (context
        .table
        .getColumnSchema(columnName)
        .getColumnCategory()
        .equals(TsTableColumnCategory.ID)) {
      return new DeviceIdFilter(context.idColumeIndexMap.get(columnName), null);
    } else {
      context.hasAttribute = true;
      return new DeviceAttributeFilter(columnName, null);
    }
  }

  @Override
  protected SchemaFilter visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitLikePredicate(LikePredicate node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitLogicalExpression(LogicalExpression node, Context context) {
    // the operator of the logical expression shall be OR
    return new OrFilter(
        node.getTerms().get(0).accept(this, context), node.getTerms().get(1).accept(this, context));
  }

  @Override
  protected SchemaFilter visitNotExpression(NotExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitComparisonExpression(ComparisonExpression node, Context context) {
    String columnName;
    String value;
    if (node.getLeft() instanceof Literal) {
      value = ((StringLiteral) (node.getLeft())).getValue();
      if (node.getRight() instanceof Identifier) {
        columnName = ((Identifier) (node.getRight())).getValue();
      } else {
        columnName = ((SymbolReference) (node.getRight())).getName();
      }
    } else {
      value = ((StringLiteral) (node.getRight())).getValue();
      if (node.getLeft() instanceof Identifier) {
        columnName = ((Identifier) (node.getLeft())).getValue();
      } else {
        columnName = ((SymbolReference) (node.getLeft())).getName();
      }
    }
    if (context
        .table
        .getColumnSchema(columnName)
        .getColumnCategory()
        .equals(TsTableColumnCategory.ID)) {
      return new DeviceIdFilter(context.idColumeIndexMap.get(columnName), value);
    } else {
      context.hasAttribute = true;
      return new DeviceAttributeFilter(columnName, value);
    }
  }

  @Override
  protected SchemaFilter visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitSearchedCaseExpression(SearchedCaseExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitIfExpression(IfExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitNullIfExpression(NullIfExpression node, Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitBetweenPredicate(BetweenPredicate node, Context context) {
    return visitExpression(node, context);
  }

  public static class Context {

    private final TsTable table;
    private final Map<String, Integer> idColumeIndexMap;

    private boolean hasAttribute = false;

    public Context(TsTable table) {
      this.table = table;
      this.idColumeIndexMap = getIdColumnIndex(table);
    }

    private Map<String, Integer> getIdColumnIndex(TsTable table) {
      Map<String, Integer> map = new HashMap<>();
      List<TsTableColumnSchema> columnSchemaList = table.getColumnList();
      int idIndex = 0;
      for (TsTableColumnSchema columnSchema : columnSchemaList) {
        if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
          map.put(columnSchema.getColumnName(), idIndex);
          idIndex++;
        }
      }
      return map;
    }

    public boolean hasAttribute() {
      return hasAttribute;
    }

    public void reset() {
      hasAttribute = false;
    }
  }
}
