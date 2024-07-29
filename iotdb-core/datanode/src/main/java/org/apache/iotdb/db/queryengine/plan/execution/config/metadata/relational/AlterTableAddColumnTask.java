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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.StringType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class AlterTableAddColumnTask implements IConfigTask {

  private final String database;

  private final String tableName;

  private final List<TsTableColumnSchema> columnList;

  private final String queryId;

  public AlterTableAddColumnTask(
      String database, String tableName, List<ColumnSchema> columnList, String queryId) {
    database = PathUtils.qualifyDatabaseName(database);
    this.database = database;
    this.tableName = tableName;
    this.columnList = parseInputColumnSchema(columnList);
    this.queryId = queryId;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.alterTableAddColumn(database, tableName, columnList, queryId);
  }

  private List<TsTableColumnSchema> parseInputColumnSchema(List<ColumnSchema> inputColumnList) {
    List<TsTableColumnSchema> columnSchemaList = new ArrayList<>(inputColumnList.size());
    for (ColumnSchema inputColumn : inputColumnList) {
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
          TSDataType dataType = InternalTypeManager.getTSDataType(inputColumn.getType());
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
