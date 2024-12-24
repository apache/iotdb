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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;

import org.apache.tsfile.enums.TSDataType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class InformationSchema {
  public static final String INFORMATION_DATABASE = "information_schema";
  private static final Map<String, TsTable> schemaTables = new HashMap<>();

  public static final String QUERIES = "queries";
  public static final String DATABASES = "databases";
  public static final String TABLES = "tables";
  public static final String COLUMNS = "columns";

  static {
    final TsTable queriesTable = new TsTable(QUERIES);
    queriesTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.QUERY_ID_TABLE_MODEL, TSDataType.STRING));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.QUERY_ID_START_TIME_TABLE_MODEL, TSDataType.TIMESTAMP));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.DATA_NODE_ID_TABLE_MODEL, TSDataType.INT32));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(ColumnHeaderConstant.ELAPSED_TIME_TABLE_MODEL, TSDataType.FLOAT));
    queriesTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATEMENT.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    queriesTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(QUERIES, queriesTable);

    final TsTable databaseTable = new TsTable(DATABASES);
    databaseTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COLUMN_TTL.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SCHEMA_REPLICATION_FACTOR_TABLE_MODEL, TSDataType.INT32));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATA_REPLICATION_FACTOR_TABLE_MODEL, TSDataType.INT32));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.TIME_PARTITION_INTERVAL_TABLE_MODEL, TSDataType.INT64));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.SCHEMA_REGION_GROUP_NUM_TABLE_MODEL, TSDataType.INT32));
    databaseTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATA_REGION_GROUP_NUM_TABLE_MODEL, TSDataType.INT32));
    databaseTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(DATABASES, databaseTable);

    final TsTable tableTable = new TsTable(TABLES);
    tableTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.TABLE_NAME_TABLE_MODEL, TSDataType.STRING));
    tableTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COLUMN_TTL.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATUS.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(TABLES, tableTable);

    final TsTable columnTable = new TsTable(COLUMNS);
    columnTable.addColumnSchema(
        new TagColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.TABLE_NAME_TABLE_MODEL, TSDataType.STRING));
    columnTable.addColumnSchema(
        new TagColumnSchema(ColumnHeaderConstant.COLUMN_NAME_TABLE_MODEL, TSDataType.STRING));
    columnTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.DATATYPE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.COLUMN_CATEGORY.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new AttributeColumnSchema(
            ColumnHeaderConstant.STATUS.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(COLUMNS, columnTable);
  }

  public static Map<String, TsTable> getSchemaTables() {
    return schemaTables;
  }

  private InformationSchema() {
    // Utils
  }
}
