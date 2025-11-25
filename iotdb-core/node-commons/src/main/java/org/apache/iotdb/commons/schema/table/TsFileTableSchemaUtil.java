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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Utility class for converting between TsTable and TSFile TableSchema */
public class TsFileTableSchemaUtil {

  private TsFileTableSchemaUtil() {
    // Utility class, prevent instantiation
  }

  /** Column category filter for efficient parsing */
  public enum ColumnCategoryFilter {
    /** Include TAG and FIELD only (exclude TIME and ATTRIBUTE) - for TsFile writing */
    NO_ATTRIBUTE(
        cat -> cat != TsTableColumnCategory.TIME && cat != TsTableColumnCategory.ATTRIBUTE);

    private final java.util.function.Predicate<TsTableColumnCategory> predicate;

    ColumnCategoryFilter(final java.util.function.Predicate<TsTableColumnCategory> predicate) {
      this.predicate = predicate;
    }

    public boolean test(final TsTableColumnCategory category) {
      return predicate.test(category);
    }
  }

  /**
   * High-performance method to convert TsTable ByteBuffer to TableSchema with column filtering.
   * Only parses and includes columns that match the filter, skipping others for efficiency. This is
   * critical for tables with many columns (e.g., 10,000+ columns).
   *
   * @param tsTableBuffer ByteBuffer containing serialized TsTable
   * @param filter Column category filter to determine which columns to include
   * @return TableSchema object
   */
  private static TableSchema tsTableBufferToTableSchemaInternal(
      final ByteBuffer tsTableBuffer, final ColumnCategoryFilter filter) {
    final String tableName = ReadWriteIOUtils.readString(tsTableBuffer);
    final int columnNum = ReadWriteIOUtils.readInt(tsTableBuffer);

    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>(columnNum);
    final List<ColumnCategory> columnTypes = new ArrayList<>(columnNum);

    for (int i = 0; i < columnNum; i++) {
      final byte categoryByte = ReadWriteIOUtils.readByte(tsTableBuffer);
      final TsTableColumnCategory category = TsTableColumnCategory.deserialize(categoryByte);

      // Early filtering: if we don't need this column, skip all its data
      if (!filter.test(category)) {
        skipColumnData(tsTableBuffer, category);
        continue;
      }

      // Only parse data for columns we need
      final String columnName = ReadWriteIOUtils.readString(tsTableBuffer);
      final TSDataType dataType = ReadWriteIOUtils.readDataType(tsTableBuffer);

      if (category == TsTableColumnCategory.FIELD) {
        ReadWriteIOUtils.readEncoding(tsTableBuffer);
        ReadWriteIOUtils.readCompressionType(tsTableBuffer);
      }
      // Skip Map
      skipMap(tsTableBuffer);

      measurementSchemas.add(new MeasurementSchema(columnName, dataType));
      columnTypes.add(category.toTsFileColumnType());
    }

    skipMap(tsTableBuffer); // Table props (skip)

    return new TableSchema(tableName, measurementSchemas, columnTypes);
  }

  /**
   * Fast skip column data without parsing. Critical for performance with many columns.
   *
   * @param buffer ByteBuffer to skip data from
   * @param category Column category to determine how much data to skip
   */
  private static void skipColumnData(
      final ByteBuffer buffer, final TsTableColumnCategory category) {
    // Skip column name
    skipString(buffer);
    // Skip data type
    ReadWriteIOUtils.readDataType(buffer);
    // Skip encoding and compression for FIELD columns
    if (category == TsTableColumnCategory.FIELD) {
      ReadWriteIOUtils.readEncoding(buffer);
      ReadWriteIOUtils.readCompressionType(buffer);
    }
    // Skip column props
    skipMap(buffer);
  }

  /**
   * Directly convert TsTable serialized ByteBuffer to TSFile TableSchema without ATTRIBUTE columns.
   * This is commonly used when writing TsFiles. Optimized for tables with many columns.
   *
   * @param tsTableBuffer ByteBuffer containing serialized TsTable
   * @return TSFile TableSchema object without ATTRIBUTE columns
   */
  public static TableSchema tsTableBufferToTableSchemaNoAttribute(final ByteBuffer tsTableBuffer) {
    return tsTableBufferToTableSchemaInternal(tsTableBuffer, ColumnCategoryFilter.NO_ATTRIBUTE);
  }

  /**
   * Convert TsTable object to TSFile TableSchema without ATTRIBUTE columns. This is commonly used
   * when writing TsFiles. Optimized for tables with many columns.
   *
   * <p>This method directly iterates through TsTable columns without serialization/deserialization,
   * providing superior performance compared to buffer-based conversion.
   *
   * @param table TsTable object to convert
   * @return TSFile TableSchema object without ATTRIBUTE columns
   */
  public static TableSchema toTsFileTableSchemaNoAttribute(final TsTable table) {
    final String tableName = table.getTableName();
    final List<TsTableColumnSchema> tsTableColumnSchemas = table.getColumnList();
    final List<IMeasurementSchema> measurementSchemas =
        new ArrayList<>(tsTableColumnSchemas.size());
    final HashMap<String, Integer> columnPosIndex = new HashMap<>(tsTableColumnSchemas.size());
    final List<ColumnCategory> columnTypes = new ArrayList<>(tsTableColumnSchemas.size());

    // Directly iterate through columns and filter out TIME and ATTRIBUTE columns
    int columnIndex = 0;
    for (final TsTableColumnSchema columnSchema : tsTableColumnSchemas) {
      final TsTableColumnCategory category = columnSchema.getColumnCategory();

      // Skip TIME and ATTRIBUTE columns (only include TAG and FIELD)
      if (category == TsTableColumnCategory.TIME || category == TsTableColumnCategory.ATTRIBUTE) {
        continue;
      }

      // Add column to schema and record its position
      final String columnName = columnSchema.getColumnName();
      measurementSchemas.add(new MeasurementSchema(columnName, columnSchema.getDataType()));
      columnTypes.add(category.toTsFileColumnType());
      columnPosIndex.put(columnName, columnIndex);
      columnIndex++;
    }

    return new TableSchema(tableName, measurementSchemas, columnTypes, columnPosIndex);
  }

  /**
   * Skip a string in ByteBuffer without reading it. This is more efficient than reading and
   * discarding the string.
   *
   * @param buffer ByteBuffer to skip string from
   */
  private static void skipString(final ByteBuffer buffer) {
    final int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      buffer.position(buffer.position() + size);
    }
  }

  /**
   * Skip a Map<String, String> in ByteBuffer without reading it. This is more efficient than
   * reading and discarding the map.
   *
   * @param buffer ByteBuffer to skip map from
   */
  private static void skipMap(final ByteBuffer buffer) {
    final int length = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < length; i++) {
      // Skip key (String)
      skipString(buffer);
      // Skip value (String)
      skipString(buffer);
    }
  }
}
