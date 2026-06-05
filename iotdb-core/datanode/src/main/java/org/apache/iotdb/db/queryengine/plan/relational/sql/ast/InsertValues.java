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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;

public class InsertValues extends InsertRows {

  private final String databaseName;
  private final String tableName;
  @Nullable private final List<String> columnNames;
  private final List<List<Expression>> rows;
  private final ZoneId zoneId;
  private boolean materialized;

  public InsertValues(
      final String databaseName,
      final String tableName,
      @Nullable final List<Identifier> columnIdentifiers,
      final Values values,
      final ZoneId zoneId) {
    super(createPlaceholderStatement(databaseName), null);
    this.databaseName = requireNonNull(databaseName, "databaseName is null");
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.columnNames =
        columnIdentifiers == null
            ? null
            : columnIdentifiers.stream().map(Identifier::getValue).collect(Collectors.toList());
    this.rows =
        requireNonNull(values, "values is null").getRows().stream()
            .map(InsertValues::normalizeRow)
            .collect(Collectors.toList());
    this.zoneId = requireNonNull(zoneId, "zoneId is null");
  }

  public void materialize(PlannerContext plannerContext, SessionInfo session) {
    if (materialized) {
      return;
    }

    InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    insertRowsStatement.setInsertRowStatementList(
        columnNames == null
            ? toInsertRowStatementsWithTableSchema(plannerContext, session)
            : toInsertRowStatementsWithColumns(plannerContext, session));
    insertRowsStatement.setWriteToTable(true);
    insertRowsStatement.setDatabaseName(databaseName);
    setInnerTreeStatement(insertRowsStatement);
    materialized = true;
  }

  private static InsertRowsStatement createPlaceholderStatement(String databaseName) {
    InsertRowsStatement statement = new InsertRowsStatement();
    statement.setInsertRowStatementList(Collections.emptyList());
    statement.setWriteToTable(true);
    statement.setDatabaseName(databaseName);
    return statement;
  }

  private static List<Expression> normalizeRow(Expression row) {
    if (row instanceof Row) {
      return ((Row) row).getItems();
    }
    return Collections.singletonList(row);
  }

  private List<InsertRowStatement> toInsertRowStatementsWithTableSchema(
      PlannerContext plannerContext, SessionInfo session) {
    final TsTable table = getTable();
    return rows.stream()
        .map(row -> toInsertRowStatement(row, table, plannerContext, session))
        .collect(Collectors.toList());
  }

  private List<InsertRowStatement> toInsertRowStatementsWithColumns(
      PlannerContext plannerContext, SessionInfo session) {
    final List<String> nonTimeColumnNames = new ArrayList<>(columnNames);
    final int timeColumnIndex = findTimeColumnIndex(nonTimeColumnNames);
    if (timeColumnIndex != -1) {
      nonTimeColumnNames.remove(timeColumnIndex);
    }
    if (timeColumnIndex == -1 && rows.size() > 1) {
      throw new SemanticException(DataNodeQueryMessages.NEED_TIMESTAMPS_WHEN_INSERT_MULTI_ROWS);
    }

    final String[] nonTimeColumnArray = nonTimeColumnNames.toArray(new String[0]);
    return rows.stream()
        .map(
            row ->
                toInsertRowStatement(
                    row, timeColumnIndex, nonTimeColumnArray, plannerContext, session))
        .collect(Collectors.toList());
  }

  private TsTable getTable() {
    TsTable table = DataNodeTableCache.getInstance().getTable(databaseName, tableName);
    if (table == null) {
      throw new SemanticException(
          "Insert target table does not exist: " + databaseName + "." + tableName);
    }
    return table;
  }

  private int findTimeColumnIndex(final List<String> columnNames) {
    List<TsTableColumnSchema> timeColumnCandidates =
        getTable().getColumnList().stream()
            .filter(column -> column.getColumnCategory() == TIME)
            .collect(Collectors.toList());
    if (timeColumnCandidates.size() != 1) {
      throw new SemanticException(
          DataNodeQueryMessages.THE_TABLE_SHOULD_ONLY_HAVE_ONE_COLUMN_FOUND);
    }

    int timeColumnIndex = -1;
    String timeColumnName = timeColumnCandidates.get(0).getColumnName();
    for (int i = 0; i < columnNames.size(); i++) {
      if (timeColumnName.equalsIgnoreCase(columnNames.get(i))) {
        if (timeColumnIndex == -1) {
          timeColumnIndex = i;
        } else {
          throw new SemanticException(
              DataNodeQueryMessages.ONE_ROW_SHOULD_ONLY_HAVE_ONE_TIME_VALUE);
        }
      }
    }
    return timeColumnIndex;
  }

  private InsertRowStatement toInsertRowStatement(
      List<Expression> expressions,
      TsTable table,
      PlannerContext plannerContext,
      SessionInfo session) {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setWriteToTable(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {table.getTableName()}));

    List<TsTableColumnSchema> columnList = table.getColumnList();
    if (expressions.size() != columnList.size()) {
      throw new SemanticException(
          "expressions and columns do not match, expressions size: "
              + expressions.size()
              + ", columns size: "
              + columnList.size());
    }

    String[] nonTimeColumnNames = new String[columnList.size() - 1];
    Object[] nonTimeValues = new Object[columnList.size() - 1];
    TsTableColumnCategory[] nonTimeColumnCategories =
        new TsTableColumnCategory[columnList.size() - 1];
    MeasurementSchema[] columnSchemas = new MeasurementSchema[columnList.size() - 1];
    TSDataType[] dataTypes = new TSDataType[columnList.size() - 1];
    int nonTimeColumnIndex = 0;
    long timestamp = -1;
    for (int i = 0; i < columnList.size(); i++) {
      TsTableColumnSchema columnSchema = columnList.get(i);
      Expression expression = expressions.get(i);

      if (columnSchema.getColumnCategory().equals(TIME)) {
        timestamp = AstUtil.expressionToTimestamp(expression, zoneId, plannerContext, session);
      } else {
        Object value = AstUtil.expressionToTsValue(expression, plannerContext, session);
        nonTimeValues[nonTimeColumnIndex] = value;
        nonTimeColumnNames[nonTimeColumnIndex] = columnSchema.getColumnName();
        dataTypes[nonTimeColumnIndex] = columnSchema.getDataType();
        nonTimeColumnCategories[nonTimeColumnIndex] = columnSchema.getColumnCategory();
        columnSchemas[nonTimeColumnIndex] =
            new MeasurementSchema(columnSchema.getColumnName(), columnSchema.getDataType());
        nonTimeColumnIndex++;
      }
    }

    TimestampPrecisionUtils.checkTimestampPrecision(timestamp);
    insertRowStatement.setTime(timestamp);
    insertRowStatement.setMeasurements(nonTimeColumnNames);
    insertRowStatement.setDataTypes(dataTypes);
    insertRowStatement.setMeasurementSchemas(columnSchemas);
    insertRowStatement.setValues(nonTimeValues);
    insertRowStatement.setColumnCategories(nonTimeColumnCategories);
    insertRowStatement.setNeedInferType(false);
    insertRowStatement.setDatabaseName(databaseName);

    try {
      insertRowStatement.transferType(zoneId);
    } catch (QueryProcessException e) {
      throw new SemanticException(e);
    }
    return insertRowStatement;
  }

  private InsertRowStatement toInsertRowStatement(
      List<Expression> expressions,
      int timeColumnIndex,
      String[] nonTimeColumnNames,
      PlannerContext plannerContext,
      SessionInfo session) {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setWriteToTable(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {tableName}));
    long timestamp;
    int nonTimeColumnCount;
    if (timeColumnIndex == -1) {
      timestamp = CommonDateTimeUtils.currentTime();
      nonTimeColumnCount = expressions.size();
    } else {
      if (timeColumnIndex >= expressions.size()) {
        throw new SemanticException(
            String.format(
                "TimeColumnIndex out of bound: %d-%d", timeColumnIndex, expressions.size()));
      }

      timestamp =
          AstUtil.expressionToTimestamp(
              expressions.get(timeColumnIndex), zoneId, plannerContext, session);
      nonTimeColumnCount = expressions.size() - 1;
    }

    if (nonTimeColumnCount != nonTimeColumnNames.length) {
      throw new SemanticException(
          String.format(
              "Inconsistent numbers of non-time column names and values: %d-%d",
              nonTimeColumnNames.length, nonTimeColumnCount));
    }

    TimestampPrecisionUtils.checkTimestampPrecision(timestamp);
    insertRowStatement.setTime(timestamp);
    insertRowStatement.setMeasurements(nonTimeColumnNames);

    Object[] values = new Object[nonTimeColumnNames.length];
    int valuePosition = 0;
    for (int i = 0; i < expressions.size(); i++) {
      if (i != timeColumnIndex) {
        values[valuePosition++] =
            AstUtil.expressionToTsValue(expressions.get(i), plannerContext, session);
      }
    }

    insertRowStatement.setValues(values);
    insertRowStatement.setNeedInferType(true);
    insertRowStatement.setDatabaseName(databaseName);
    return insertRowStatement;
  }
}
