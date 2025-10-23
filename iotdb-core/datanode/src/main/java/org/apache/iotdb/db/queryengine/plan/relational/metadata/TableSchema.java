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
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TableSchema {

  private final String tableName;
  protected final List<ColumnSchema> columns;
  protected Map<String, String> props;

  public TableSchema(final String tableName, final List<ColumnSchema> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public String getTableName() {
    return tableName;
  }

  public List<ColumnSchema> getColumns() {
    return columns;
  }

  public Map<String, ColumnSchema> getColumnSchemaMap() {
    // return a map column name -> ColumnSchema
    return columns.stream().collect(Collectors.toMap(ColumnSchema::getName, Function.identity()));
  }

  public void setProps(final Map<String, String> props) {
    this.props = props;
  }

  public Map<String, String> getProps() {
    return props;
  }

  /** Get the column with the specified name and category, return null if not found. */
  public ColumnSchema getColumn(
      final String columnName, final TsTableColumnCategory columnCategory) {
    return columns.stream()
        .filter(
            column ->
                column.getName().equals(columnName) && column.getColumnCategory() == columnCategory)
        .findAny()
        .orElse(null);
  }

  public ColumnSchema getColumn(final String columnName) {
    List<ColumnSchema> columnScheme =
        columns.stream()
            .filter(column -> column.getName().equals(columnName))
            .collect(toImmutableList());
    if (columnScheme.isEmpty()) {
      return null;
    } else if (columnScheme.size() > 1) {
      throw new SemanticException(
          String.format("Columns in table shall not share the same name %s.", columnName));
    }
    return columnScheme.get(0);
  }

  /**
   * Given the name of an TAG column, return the index of this column among all TAG columns, return
   * -1 if not found.
   */
  public int getIndexAmongTagColumns(final String tagColumnName) {
    int index = 0;
    for (final ColumnSchema column : getTagColumns()) {
      if (column.getName().equals(tagColumnName)) {
        return index;
      }
      index++;
    }
    return -1;
  }

  public static TableSchema of(final TsTable tsTable) {
    final TableSchema schema =
        new TableSchema(
            tsTable.getTableName(),
            tsTable.getColumnList().stream()
                .map(ColumnSchema::ofTsColumnSchema)
                .collect(Collectors.toList()));
    schema.setProps(tsTable.getProps());
    return schema;
  }

  public org.apache.tsfile.file.metadata.TableSchema toTsFileTableSchema() {
    // TODO-Table: unify redundant definitions
    final String tableName = this.getTableName();
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    final List<ColumnCategory> columnTypes = new ArrayList<>();
    for (final ColumnSchema column : columns) {
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
    final String tableName = this.getTableName();
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    final List<ColumnCategory> columnTypes = new ArrayList<>();
    for (final ColumnSchema column : columns) {
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
      final String tableName, final org.apache.tsfile.file.metadata.TableSchema tsFileTableSchema) {
    try {
      final List<ColumnSchema> columns = new ArrayList<>();
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
                TsTableColumnCategory.fromTsFileColumnCategory(columnType)));
      }
      return new TableSchema(tableName, columns);
    } catch (final Exception e) {
      LOGGER.warn(
          "Cannot convert tsfile table schema to iotdb table schema, table name: {}, tsfile table schema: {}",
          tableName,
          tsFileTableSchema,
          e);
      throw e;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableSchema that = (TableSchema) o;
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

  public List<ColumnSchema> getTagColumns() {
    return columns.stream()
        .filter(c -> c.getColumnCategory() == TsTableColumnCategory.TAG)
        .collect(Collectors.toList());
  }
}
