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

package org.apache.iotdb.db.subscription.columnfilter;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.subscription.columnfilter.ColumnMetadata;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ColumnFilterBinder {

  private final ColumnFilterParser parser = new ColumnFilterParser();

  public BoundColumnFilter bind(
      final TopicConfig topicConfig, final Map<String, Map<String, TsTable>> tables)
      throws SubscriptionException {
    if (Objects.isNull(topicConfig)
        || !topicConfig.isTableTopic()
        || topicConfig.isColumnFilterTrivial()) {
      return BoundColumnFilter.matchAll();
    }

    final Expression expression = parser.parseAndValidate(topicConfig.getColumnFilter());
    final TablePattern tablePattern = buildTablePattern(topicConfig);
    final Map<BoundColumnFilter.TableKey, Set<String>> selectedColumnsByTable = new HashMap<>();
    final Map<BoundColumnFilter.TableKey, Boolean> timeSelectedByTable = new HashMap<>();
    boolean timeSelected = false;

    for (final Map.Entry<String, Map<String, TsTable>> databaseEntry : tables.entrySet()) {
      final String database = databaseEntry.getKey();
      if (!tablePattern.matchesDatabase(database)) {
        continue;
      }
      for (final TsTable table : databaseEntry.getValue().values()) {
        if (Objects.isNull(table) || !tablePattern.matchesTable(table.getTableName())) {
          continue;
        }
        final BindTableResult tableResult = bindTable(expression, database, table);
        timeSelected |= tableResult.timeSelected;
        timeSelectedByTable.put(
            BoundColumnFilter.TableKey.of(database, table.getTableName()),
            tableResult.timeSelected);
        if (!tableResult.selectedColumnNames.isEmpty()) {
          selectedColumnsByTable.put(
              BoundColumnFilter.TableKey.of(database, table.getTableName()),
              tableResult.selectedColumnNames);
        }
      }
    }

    return BoundColumnFilter.of(selectedColumnsByTable, timeSelected, timeSelectedByTable);
  }

  private static BindTableResult bindTable(
      final Expression expression, final String database, final TsTable table) {
    boolean hasMatchedColumn = false;
    boolean timeSelected = false;
    final Set<String> selectedColumnNames = new HashSet<>();
    final Set<String> tagColumnNames = new HashSet<>();

    for (final TsTableColumnSchema columnSchema : table.getColumnList()) {
      if (Objects.isNull(columnSchema)) {
        continue;
      }
      if (columnSchema.getColumnCategory() == TsTableColumnCategory.TAG) {
        tagColumnNames.add(BoundColumnFilter.normalize(sourceColumnName(table, columnSchema)));
      }
      if (!evaluate(expression, database, table.getTableName(), columnSchema)) {
        continue;
      }

      hasMatchedColumn = true;
      switch (columnSchema.getColumnCategory()) {
        case TIME:
          timeSelected = true;
          break;
        case ATTRIBUTE:
          break;
        case TAG:
        case FIELD:
          selectedColumnNames.add(
              BoundColumnFilter.normalize(sourceColumnName(table, columnSchema)));
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  DataNodeMiscMessages.UNSUPPORTED_TABLE_COLUMN_CATEGORY_FMT,
                  columnSchema.getColumnCategory()));
      }
    }

    if (hasMatchedColumn) {
      selectedColumnNames.addAll(tagColumnNames);
    }
    return new BindTableResult(selectedColumnNames, timeSelected);
  }

  private static boolean evaluate(
      final Expression expression,
      final String database,
      final String tableName,
      final TsTableColumnSchema columnSchema) {
    return ColumnFilterEvaluator.evaluate(
        expression,
        new ColumnMetadata(
            database,
            tableName,
            columnSchema.getColumnName(),
            columnSchema.getDataType().name(),
            columnSchema.getColumnCategory().name()));
  }

  private static String sourceColumnName(
      final TsTable table, final TsTableColumnSchema columnSchema) {
    return TreeViewSchema.isTreeViewTable(table)
            && columnSchema.getColumnCategory() == TsTableColumnCategory.FIELD
        ? TreeViewSchema.getSourceName(columnSchema)
        : columnSchema.getColumnName();
  }

  private static TablePattern buildTablePattern(final TopicConfig topicConfig) {
    return new TablePattern(
        true,
        topicConfig.getStringOrDefault(
            TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE),
        topicConfig.getStringOrDefault(TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE));
  }

  private static final class BindTableResult {

    private final Set<String> selectedColumnNames;
    private final boolean timeSelected;

    private BindTableResult(final Set<String> selectedColumnNames, final boolean timeSelected) {
      this.selectedColumnNames = selectedColumnNames;
      this.timeSelected = timeSelected;
    }
  }
}
