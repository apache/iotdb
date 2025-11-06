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

import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.Objects;

/**
 * InsertNodeMeasurementInfo is a class that stores the measurement list from InsertNode and table
 * name, and provides methods to convert to TsTable.
 */
public class InsertNodeMeasurementInfo {

  /** Table name */
  private final String tableName;

  /** Column category list (TAG, ATTRIBUTE, FIELD, etc.) */
  private final TsTableColumnCategory[] columnCategories;

  /** Measurement schema list */
  private final MeasurementSchema[] measurementSchemas;

  /**
   * Constructor
   *
   * @param tableName table name
   * @param columnCategories column category list
   * @param measurementSchemas measurement schema list
   */
  public InsertNodeMeasurementInfo(
      final String tableName,
      final TsTableColumnCategory[] columnCategories,
      final MeasurementSchema[] measurementSchemas) {
    this.tableName = tableName;
    this.columnCategories = columnCategories;
    this.measurementSchemas = measurementSchemas;
  }

  /**
   * Get table name
   *
   * @return table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get column category list
   *
   * @return column category array
   */
  public TsTableColumnCategory[] getColumnCategories() {
    return columnCategories;
  }

  /**
   * Get measurement schema list
   *
   * @return measurement schema array
   */
  public MeasurementSchema[] getMeasurementSchemas() {
    return measurementSchemas;
  }

  /**
   * Convert to TsTable object
   *
   * @return converted TsTable object
   */
  public TsTable toTsTable() {
    final TsTable tsTable = new TsTable(tableName);

    if (measurementSchemas == null || measurementSchemas.length == 0) {
      return tsTable;
    }

    for (int i = 0; i < measurementSchemas.length; i++) {
      if (measurementSchemas[i] == null) {
        continue;
      }

      final String columnName = measurementSchemas[i].getMeasurementName();
      final TSDataType dataType = measurementSchemas[i].getType();
      final TSEncoding encoding = measurementSchemas[i].getEncodingType();
      final CompressionType compressor = measurementSchemas[i].getCompressor();

      // Determine column category
      final TsTableColumnCategory columnCategory =
          columnCategories != null && i < columnCategories.length && columnCategories[i] != null
              ? columnCategories[i]
              : TsTableColumnCategory.FIELD;

      // Create corresponding ColumnSchema based on column category
      switch (columnCategory) {
        case FIELD:
          tsTable.addColumnSchema(
              new FieldColumnSchema(columnName, dataType, encoding, compressor));
          break;
        case TAG:
          tsTable.addColumnSchema(new TagColumnSchema(columnName, dataType));
          break;
        case ATTRIBUTE:
          tsTable.addColumnSchema(new AttributeColumnSchema(columnName, dataType));
          break;
        case TIME:
          // TIME column is usually added during TsTable construction, skip here
          break;
        default:
          // Default to FIELD
          tsTable.addColumnSchema(
              new FieldColumnSchema(columnName, dataType, encoding, compressor));
          break;
      }
    }

    return tsTable;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InsertNodeMeasurementInfo that = (InsertNodeMeasurementInfo) o;
    return Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName);
  }

  @Override
  public String toString() {
    return "InsertNodeMeasurementInfo{"
        + "tableName='"
        + tableName
        + '\''
        + ", measurementSchemas="
        + (measurementSchemas != null ? measurementSchemas.length : 0)
        + '}';
  }
}
