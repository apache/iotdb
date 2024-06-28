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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.exception.metadata.table.TableNotExistsException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableAddColumnTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.read.common.type.UnknownType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

  // This method return all the existing column schemas in the target table.
  // When table or column is missing, this method will execute auto creation.
  // When using SQL, the columnSchemaList could be null and there won't be any validation.
  // All input column schemas will be validated and auto created when necessary.
  // When the input dataType or category of one column is null, the column cannot be auto created.
  public TableSchema validateTableHeaderSchema(
      String database, TableSchema tableSchema, MPPQueryContext context) {
    List<ColumnSchema> inputColumnList = tableSchema.getColumns();
    TsTable table = DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName());
    List<ColumnSchema> missingColumnList = new ArrayList<>();
    List<ColumnSchema> resultColumnList = new ArrayList<>();

    // first round validate, check existing schema
    if (table == null) {
      if (inputColumnList == null) {
        throw new SemanticException("Unknown column names. Cannot auto create table.");
      }
      // check arguments for table auto creation
      for (ColumnSchema columnSchema : inputColumnList) {
        if (columnSchema.getColumnCategory() == null) {
          throw new SemanticException("Unknown column category. Cannot auto create table.");
        }
        if (columnSchema.getType() == null) {
          throw new IllegalArgumentException("Unknown column data type. Cannot auto create table.");
        }
        missingColumnList.add(columnSchema);
      }
    } else if (inputColumnList == null) {
      // SQL insert without columnName, nothing to check
    } else {
      for (int i = 0; i < inputColumnList.size(); i++) {
        ColumnSchema columnSchema = inputColumnList.get(i);
        TsTableColumnSchema existingColumn = table.getColumnSchema(columnSchema.getName());
        if (existingColumn == null) {
          // check arguments for column auto creation
          if (columnSchema.getColumnCategory() == null) {
            throw new IllegalArgumentException(
                "Unknown column category. Cannot auto create column.");
          }
          if (columnSchema.getType() == null) {
            throw new IllegalArgumentException(
                "Unknown column data type. Cannot auto create column.");
          }
          missingColumnList.add(columnSchema);
        } else {
          // check and validate column data type and category
          if (!columnSchema.getType().equals(UnknownType.UNKNOWN)
              && !TypeFactory.getType(existingColumn.getDataType())
                  .equals(columnSchema.getType())) {
            throw new SemanticException(
                String.format("Wrong data type at column %s.", columnSchema.getName()));
          }
          if (columnSchema.getColumnCategory() != null
              && !existingColumn.getColumnCategory().equals(columnSchema.getColumnCategory())) {
            throw new SemanticException(
                String.format("Wrong category at column %s.", columnSchema.getName()));
          }
        }
      }
    }

    // auto create missing table or columns
    if (table == null) {
      autoCreateTable(database, tableSchema, context);
      table = DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName());
    } else if (inputColumnList == null) {
      // do nothing
    } else {
      if (!missingColumnList.isEmpty()) {
        autoCreateColumn(database, tableSchema.getTableName(), missingColumnList, context);
      }
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
    return new TableSchema(tableSchema.getTableName(), resultColumnList);
  }

  private void autoCreateTable(String database, TableSchema tableSchema, MPPQueryContext context) {
    throw new SemanticException(new TableNotExistsException(database, tableSchema.getTableName()));
  }

  private void autoCreateColumn(
      String database,
      String tableName,
      List<ColumnSchema> inputColumnList,
      MPPQueryContext context) {
    AlterTableAddColumnTask task =
        new AlterTableAddColumnTask(
            database, tableName, inputColumnList, context.getQueryId().getId());
    try {
      ListenableFuture<ConfigTaskResult> future = task.execute(configTaskExecutor);
      ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                "Auto add table column failed.", result.getStatusCode().getStatusCode()));
      }
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.warn("Auto add table column failed.", e);
      throw new RuntimeException(e);
    }
  }
}
