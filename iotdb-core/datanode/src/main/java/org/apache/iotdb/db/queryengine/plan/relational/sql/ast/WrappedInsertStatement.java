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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;

import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class WrappedInsertStatement extends WrappedStatement
    implements ITableDeviceSchemaValidation {

  protected TableSchema tableSchema;

  public WrappedInsertStatement(InsertBaseStatement innerTreeStatement, MPPQueryContext context) {
    super(innerTreeStatement, context);
  }

  @Override
  public InsertBaseStatement getInnerTreeStatement() {
    return ((InsertBaseStatement) super.getInnerTreeStatement());
  }

  public abstract void updateAfterSchemaValidation(MPPQueryContext context)
      throws QueryProcessException;

  public TableSchema getTableSchema() {
    if (tableSchema == null) {
      InsertBaseStatement insertBaseStatement = getInnerTreeStatement();
      String tableName = insertBaseStatement.getDevicePath().getFullPath();
      List<ColumnSchema> columnSchemas =
          new ArrayList<>(insertBaseStatement.getMeasurements().length);
      for (int i = 0; i < insertBaseStatement.getMeasurements().length; i++) {
        columnSchemas.add(
            new ColumnSchema(
                insertBaseStatement.getMeasurements()[i],
                TypeFactory.getType(insertBaseStatement.getDataTypes()[i]),
                false,
                insertBaseStatement.getColumnCategories()[i]));
      }
      tableSchema = new TableSchema(tableName, columnSchemas);
    }

    return tableSchema;
  }

  public void validate(TableSchema realSchema) throws QueryProcessException {
    final TableSchema incomingTableSchema = getTableSchema();
    final List<ColumnSchema> incomingSchemaColumns = incomingTableSchema.getColumns();
    Map<String, ColumnSchema> realSchemaMap = new HashMap<>();
    realSchema.getColumns().forEach(c -> realSchemaMap.put(c.getName(), c));

    // incoming schema should be consistent with real schema
    for (ColumnSchema incomingSchemaColumn : incomingSchemaColumns) {
      final ColumnSchema realSchemaColumn = realSchemaMap.get(incomingSchemaColumn.getName());
      validate(incomingSchemaColumn, realSchemaColumn);
    }
    // incoming schema should contain all id columns in real schema and have consistent order
    final List<ColumnSchema> realIdColumns = realSchema.getIdColumns();
    final List<ColumnSchema> incomingIdColumns = incomingTableSchema.getIdColumns();
    if (realIdColumns.size() > incomingIdColumns.size()) {
      throw new QueryProcessException(
          new SemanticException(
              String.format(
                  "The incoming id columns " + "conflicts " + "with existing ones: %s v.s. %s",
                  incomingIdColumns, realIdColumns)));
    }
    for (int i = 0; i < realIdColumns.size(); i++) {
      if (!realIdColumns.get(i).equals(incomingIdColumns.get(i))) {
        throw new QueryProcessException(
            new SemanticException(
                String.format(
                    "The incoming id columns " + "conflicts " + "with existing ones: %s v.s. %s",
                    incomingIdColumns, realIdColumns)));
      }
    }
  }

  public static void validate(ColumnSchema incoming, ColumnSchema real) {
    if (real == null) {
      throw new SemanticException(
          "Column " + incoming.getName() + " does not exists or fails to be " + "created");
    }
    if (!incoming.getType().equals(real.getType())) {
      throw new SemanticException(
          String.format(
              "Inconsistent data type of column %s: %s/%s",
              incoming.getName(), incoming.getType(), real.getType()));
    }
    if (!incoming.getColumnCategory().equals(real.getColumnCategory())) {
      throw new SemanticException(
          String.format(
              "Inconsistent column category of column %s: %s/%s",
              incoming.getName(), incoming.getColumnCategory(), real.getColumnCategory()));
    }
  }
}
