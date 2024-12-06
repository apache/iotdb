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
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;

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
        new MeasurementColumnSchema(ColumnHeaderConstant.TIME, TSDataType.TIMESTAMP));
    queriesTable.addColumnSchema(
        new MeasurementColumnSchema(ColumnHeaderConstant.QUERY_ID_TABLE_MODEL, TSDataType.STRING));
    queriesTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.DATA_NODE_ID_TABLE_MODEL, TSDataType.INT32));
    queriesTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.ELAPSED_TIME_TABLE_MODEL, TSDataType.FLOAT));
    queriesTable.addColumnSchema(
        new MeasurementColumnSchema(ColumnHeaderConstant.STATEMENT, TSDataType.STRING));
    queriesTable.addColumnSchema(
        new MeasurementColumnSchema(ColumnHeaderConstant.SQL_DIALECT, TSDataType.STRING));
    schemaTables.put(QUERIES, queriesTable);

    final TsTable databaseTable = new TsTable(DATABASES);
    databaseTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    databaseTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.COLUMN_TTL.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    databaseTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.SCHEMA_REPLICATION_FACTOR.toLowerCase(Locale.ENGLISH),
            TSDataType.INT32));
    databaseTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.DATA_REPLICATION_FACTOR.toLowerCase(Locale.ENGLISH),
            TSDataType.INT32));
    databaseTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.TIME_PARTITION_INTERVAL.toLowerCase(Locale.ENGLISH),
            TSDataType.INT64));
    databaseTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.MODEL.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    databaseTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(DATABASES, databaseTable);

    final TsTable tableTable = new TsTable(TABLES);
    tableTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.TABLE_NAME.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.COLUMN_TTL.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.STATUS.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    tableTable.removeColumnSchema(TsTable.TIME_COLUMN_NAME);
    schemaTables.put(TABLES, tableTable);

    final TsTable columnTable = new TsTable(COLUMNS);
    columnTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.DATABASE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.TABLE_NAME.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.COLUMN_NAME.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.DATATYPE.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new MeasurementColumnSchema(
            ColumnHeaderConstant.COLUMN_CATEGORY.toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    columnTable.addColumnSchema(
        new MeasurementColumnSchema(
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
