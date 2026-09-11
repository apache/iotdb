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

package org.apache.iotdb.isession;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;

import java.util.List;

/**
 * A table-model row with numeric FIELDs in {@code long[]} and STRING TAGs in {@code String[]}.
 *
 * <p>Column names, types and categories describe all supplied columns in the same order. The value
 * arrays contain only their respective FIELD or TAG values, in occurrence order in the column list.
 * TAGs may be null or empty; missing TAG columns are null. ATTRIBUTE columns, nonnumeric FIELDs and
 * null FIELD values are not supported.
 *
 * <p>BOOLEAN uses zero for false and nonzero for true. INT32 and DATE use the low 32 bits (DATE is
 * encoded as yyyyMMdd); INT64 and TIMESTAMP use the long value. FLOAT uses {@link
 * Float#floatToRawIntBits(float)} in the low 32 bits and DOUBLE uses {@link
 * Double#doubleToRawLongBits(double)}.
 *
 * <p>Metadata and arrays are retained without copying or boxing. Do not modify them while {@link
 * ITableSession#insert(NumericRow)} is in progress. Arguments are validated when inserting the row.
 */
public final class NumericRow {
  private final String tableName;
  private final long timestamp;
  private final List<String> columnNames;
  private final List<TSDataType> dataTypes;
  private final List<ColumnCategory> columnCategories;
  private final long[] values;
  private final String[] tagValues;

  /** Creates a row with numeric FIELDs and optional STRING TAGs in separate arrays. */
  public NumericRow(
      String tableName,
      long timestamp,
      List<String> columnNames,
      List<TSDataType> dataTypes,
      List<ColumnCategory> columnCategories,
      long[] values,
      String[] tagValues) {
    this.tableName = tableName;
    this.timestamp = timestamp;
    this.columnNames = columnNames;
    this.dataTypes = dataTypes;
    this.columnCategories = columnCategories;
    this.values = values;
    this.tagValues = tagValues;
  }

  public String getTableName() {
    return tableName;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<ColumnCategory> getColumnCategories() {
    return columnCategories;
  }

  public long[] getValues() {
    return values;
  }

  public String[] getTagValues() {
    return tagValues;
  }
}
