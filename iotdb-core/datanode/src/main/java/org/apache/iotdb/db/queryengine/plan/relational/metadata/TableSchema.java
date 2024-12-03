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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableSchema {

  private final String tableName;

  private final List<ColumnSchema> columns;

  public TableSchema(String tableName, List<ColumnSchema> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public String getTableName() {
    return tableName;
  }

  public List<ColumnSchema> getColumns() {
    return columns;
  }

  /** Get the column with the specified name and category, return null if not found. */
  public ColumnSchema getColumn(String columnName, TsTableColumnCategory columnCategory) {
    for (final ColumnSchema column : columns) {
      if (column.getName().equals(columnName) && column.getColumnCategory() == columnCategory) {
        return column;
      }
    }
    return null;
  }

  /**
   * Given the name of an ID column, return the index of this column among all ID columns, return -1
   * if not found.
   */
  public int getIndexAmongIdColumns(String idColumnName) {
    int index = 0;
    for (ColumnSchema column : getIdColumns()) {
      if (column.getName().equals(idColumnName)) {
        return index;
      }
      index++;
    }
    return -1;
  }

  public static TableSchema of(TsTable tsTable) {
    String tableName = tsTable.getTableName();
    List<ColumnSchema> columns = new ArrayList<>();
    for (TsTableColumnSchema tsTableColumnSchema : tsTable.getColumnList()) {
      columns.add(ColumnSchema.ofTsColumnSchema(tsTableColumnSchema));
    }
    return new TableSchema(tableName, columns);
  }

  public org.apache.tsfile.file.metadata.TableSchema toTsFileTableSchema() {
    // TODO-Table: unify redundant definitions
    String tableName = this.getTableName();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnCategory> columnTypes = new ArrayList<>();
    for (ColumnSchema column : columns) {
      if (column.getColumnCategory() == TsTableColumnCategory.TIME) {
        continue;
      }
      measurementSchemas.add(
          new MeasurementSchema(
              column.getName(), InternalTypeManager.getTSDataType(column.getType())));
      columnTypes.add(column.getColumnCategory().toTsFileColumnType());
    }
    return new org.apache.tsfile.file.metadata.TableSchema(
        tableName, measurementSchemas, columnTypes);
  }

  public org.apache.tsfile.file.metadata.TableSchema toTsFileTableSchemaNoAttribute() {
    // TODO-Table: unify redundant definitions
    String tableName = this.getTableName();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnCategory> columnTypes = new ArrayList<>();
    for (ColumnSchema column : columns) {
      if (column.getColumnCategory() == TsTableColumnCategory.TIME
          || column.getColumnCategory() == TsTableColumnCategory.ATTRIBUTE) {
        continue;
      }
      measurementSchemas.add(
          new MeasurementSchema(
              column.getName(), InternalTypeManager.getTSDataType(column.getType())));
      columnTypes.add(column.getColumnCategory().toTsFileColumnType());
    }
    return new org.apache.tsfile.file.metadata.TableSchema(
        tableName, measurementSchemas, columnTypes);
  }

  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TableSchema.class);

  public static TableSchema fromTsFileTableSchema(
      String tableName, org.apache.tsfile.file.metadata.TableSchema tsFileTableSchema) {
    try {
      List<ColumnSchema> columns = new ArrayList<>();
      for (int i = 0; i < tsFileTableSchema.getColumnSchemas().size(); i++) {
        final String columnName = tsFileTableSchema.getColumnSchemas().get(i).getMeasurementName();
        if (columnName == null || columnName.isEmpty()) {
          continue;
        }

        // TsFile should not contain attribute columns by design.
        final ColumnCategory columnType = tsFileTableSchema.getColumnTypes().get(i);
        if (columnType == ColumnCategory.ATTRIBUTE) {
          continue;
        }

        final TSDataType dataType = tsFileTableSchema.getColumnSchemas().get(i).getType();
        if (dataType == TSDataType.VECTOR) {
          continue;
        }

        columns.add(
            new ColumnSchema(
                columnName,
                InternalTypeManager.fromTSDataType(dataType),
                false,
                TsTableColumnCategory.fromTsFileColumnType(columnType)));
      }
      return new TableSchema(tableName, columns);
    } catch (Exception e) {
      LOGGER.warn(
          "Cannot convert tsfile table schema to iotdb table schema, table name: {}, tsfile table schema: {}",
          tableName,
          tsFileTableSchema,
          e);
      throw e;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableSchema that = (TableSchema) o;
    return Objects.equals(tableName, that.tableName) && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columns);
  }

  @Override
  public String toString() {
    return "TableSchema{" + "tableName='" + tableName + '\'' + ", columns=" + columns + '}';
  }

  public List<ColumnSchema> getIdColumns() {
    return columns.stream()
        .filter(c -> c.getColumnCategory() == TsTableColumnCategory.ID)
        .collect(Collectors.toList());
  }
}
