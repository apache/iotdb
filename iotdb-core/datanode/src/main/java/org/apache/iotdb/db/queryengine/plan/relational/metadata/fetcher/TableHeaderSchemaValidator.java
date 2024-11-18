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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.ColumnCreationFailException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableAddColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class TableHeaderSchemaValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableHeaderSchemaValidator.class);

  private final ClusterConfigTaskExecutor configTaskExecutor =
      ClusterConfigTaskExecutor.getInstance();

  private TableHeaderSchemaValidator() {
    // do nothing
  }

  private static class TableHeaderSchemaValidatorHolder {

    private static final TableHeaderSchemaValidator INSTANCE = new TableHeaderSchemaValidator();
  }

  public static TableHeaderSchemaValidator getInstance() {
    return TableHeaderSchemaValidatorHolder.INSTANCE;
  }

  public Optional<TableSchema> validateTableHeaderSchema(
      final String database,
      final TableSchema tableSchema,
      final MPPQueryContext context,
      final boolean allowCreateTable) {
    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeSeries will be effective.
    DataNodeSchemaLockManager.getInstance()
        .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION);

    final List<ColumnSchema> inputColumnList = tableSchema.getColumns();
    if (inputColumnList == null || inputColumnList.isEmpty()) {
      throw new IllegalArgumentException(
          "No column other than Time present, please check the request");
    }
    // Get directly if there is a table because we do not want "addColumn" to affect
    // original writings
    TsTable table =
        DataNodeTableCache.getInstance().getTableInWrite(database, tableSchema.getTableName());
    final List<ColumnSchema> missingColumnList = new ArrayList<>();

    final boolean isAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    // first round validate, check existing schema
    if (table == null) {
      // TODO table metadata: authority check for table create
      // auto create missing table
      // it's ok that many write requests concurrently auto create same table, the thread safety
      // will be guaranteed by ProcedureManager.createTable in CN
      if (allowCreateTable && isAutoCreateSchemaEnabled) {
        autoCreateTable(database, tableSchema);
        table = DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName());
        if (table == null) {
          throw new IllegalStateException(
              "auto create table succeed, but cannot get table schema in current node's DataNodeTableCache, may be caused by concurrently auto creating table");
        }
      } else {
        throw new SemanticException("Table " + tableSchema.getTableName() + " does not exist");
      }
    }

    boolean refreshed = false;
    for (final ColumnSchema columnSchema : inputColumnList) {
      TsTableColumnSchema existingColumn = table.getColumnSchema(columnSchema.getName());
      if (Objects.isNull(existingColumn)) {
        if (!refreshed) {
          // Refresh because there may be new columns added and failed to commit
          // Allow refresh only once to avoid too much failure columns in sql when there are column
          // procedures
          refreshed = true;
          table = DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName());
          existingColumn = table.getColumnSchema(columnSchema.getName());
        }
        if (Objects.isNull(existingColumn)) {
          // check arguments for column auto creation
          if (columnSchema.getColumnCategory() == null) {
            throw new SemanticException(
                String.format(
                    "Unknown column category for %s. Cannot auto create column.",
                    columnSchema.getName()),
                TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
          }
          if (columnSchema.getType() == null) {
            throw new SemanticException(
                String.format(
                    "Unknown column data type for %s. Cannot auto create column.",
                    columnSchema.getName()),
                TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
          }
          missingColumnList.add(columnSchema);
        }
      } else {
        // leave measurement columns' dataType checking to the caller, then the caller can decide
        // whether to do partial insert

        // only check column category
        if (columnSchema.getColumnCategory() != null
            && !existingColumn.getColumnCategory().equals(columnSchema.getColumnCategory())) {
          throw new SemanticException(
              String.format("Wrong category at column %s.", columnSchema.getName()),
              TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode());
        }
      }
    }

    final List<ColumnSchema> resultColumnList = new ArrayList<>();
    if (!missingColumnList.isEmpty() && isAutoCreateSchemaEnabled) {
      // TODO table metadata: authority check for table alter
      // check id or attribute column data type in this method
      autoCreateColumn(database, tableSchema.getTableName(), missingColumnList, context);
      table = DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName());
    } else if (!missingColumnList.isEmpty()
        && !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      throw new SemanticException(
          String.format(
              "Missing columns %s.",
              missingColumnList.stream().map(ColumnSchema::getName).collect(Collectors.toList())),
          TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
    }

    table
        .getColumnList()
        .forEach(
            o ->
                resultColumnList.add(
                    new ColumnSchema(
                        o.getColumnName(),
                        TypeFactory.getType(o.getDataType()),
                        false,
                        o.getColumnCategory())));
    return Optional.of(new TableSchema(tableSchema.getTableName(), resultColumnList));
  }

  private void autoCreateTable(final String database, final TableSchema tableSchema) {
    final TsTable tsTable = new TsTable(tableSchema.getTableName());
    addColumnSchema(tableSchema.getColumns(), tsTable);
    final CreateTableTask createTableTask = new CreateTableTask(tsTable, database, true);
    try {
      final ListenableFuture<ConfigTaskResult> future = createTableTask.execute(configTaskExecutor);
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                "Auto create table column failed.", result.getStatusCode().getStatusCode()));
      }
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      /* Clean up whatever needs to be handled before interrupting  */
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void addColumnSchema(final List<ColumnSchema> columnSchemas, final TsTable tsTable) {
    for (final ColumnSchema columnSchema : columnSchemas) {
      TsTableColumnCategory category = columnSchema.getColumnCategory();
      if (category == null) {
        throw new ColumnCreationFailException(
            "Cannot create column " + columnSchema.getName() + " category is not provided");
      }
      final String columnName = columnSchema.getName();
      if (tsTable.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }
      final TSDataType dataType = getTSDataType(columnSchema.getType());
      if (dataType == null) {
        throw new ColumnCreationFailException(
            "Cannot create column " + columnSchema.getName() + " datatype is not provided");
      }
      tsTable.addColumnSchema(generateColumnSchema(category, columnName, dataType));
    }
  }

  public static TsTableColumnSchema generateColumnSchema(
      final TsTableColumnCategory category, final String columnName, final TSDataType dataType) {
    switch (category) {
      case ID:
        if (!TSDataType.STRING.equals(dataType)) {
          throw new SemanticException(
              "DataType of ID Column should only be STRING, current is " + dataType);
        }
        return new IdColumnSchema(columnName, dataType);
      case ATTRIBUTE:
        if (!TSDataType.STRING.equals(dataType)) {
          throw new SemanticException(
              "DataType of ATTRIBUTE Column should only be STRING, current is " + dataType);
        }
        return new AttributeColumnSchema(columnName, dataType);
      case TIME:
        throw new SemanticException(
            "Create table or add column statement shall not specify column category TIME");
      case MEASUREMENT:
        return new MeasurementColumnSchema(
            columnName,
            dataType,
            getDefaultEncoding(dataType),
            TSFileDescriptor.getInstance().getConfig().getCompressor());
      default:
        throw new IllegalArgumentException();
    }
  }

  private void autoCreateColumn(
      final String database,
      final String tableName,
      final List<ColumnSchema> inputColumnList,
      final MPPQueryContext context) {
    final AlterTableAddColumnTask task =
        new AlterTableAddColumnTask(
            database,
            tableName,
            parseInputColumnSchema(inputColumnList),
            context.getQueryId().getId(),
            true,
            true);
    try {
      final ListenableFuture<ConfigTaskResult> future = task.execute(configTaskExecutor);
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                String.format(
                    "Auto add table column failed: %s.%s, %s",
                    database, tableName, inputColumnList),
                result.getStatusCode().getStatusCode()));
      }
    } catch (final ExecutionException | InterruptedException e) {
      LOGGER.warn("Auto add table column failed.", e);
      throw new RuntimeException(e);
    }
  }

  private List<TsTableColumnSchema> parseInputColumnSchema(
      final List<ColumnSchema> inputColumnList) {
    final List<TsTableColumnSchema> columnSchemaList = new ArrayList<>(inputColumnList.size());
    for (final ColumnSchema inputColumn : inputColumnList) {
      switch (inputColumn.getColumnCategory()) {
        case ID:
          if (!inputColumn.getType().equals(StringType.STRING)) {
            throw new SemanticException("Id column only support data type STRING.");
          }
          columnSchemaList.add(new IdColumnSchema(inputColumn.getName(), TSDataType.STRING));
          break;
        case ATTRIBUTE:
          if (!inputColumn.getType().equals(StringType.STRING)) {
            throw new SemanticException("Attribute column only support data type STRING.");
          }
          columnSchemaList.add(new AttributeColumnSchema(inputColumn.getName(), TSDataType.STRING));
          break;
        case MEASUREMENT:
          final TSDataType dataType = InternalTypeManager.getTSDataType(inputColumn.getType());
          columnSchemaList.add(
              new MeasurementColumnSchema(
                  inputColumn.getName(),
                  dataType,
                  getDefaultEncoding(dataType),
                  TSFileDescriptor.getInstance().getConfig().getCompressor()));
          break;
        case TIME:
          throw new SemanticException(
              "Adding column for column category "
                  + inputColumn.getColumnCategory()
                  + " is not supported");
        default:
          throw new IllegalStateException(
              "Unknown ColumnCategory for adding column: " + inputColumn.getColumnCategory());
      }
    }
    return columnSchemaList;
  }
}
