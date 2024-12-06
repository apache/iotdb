package org.apache.iotdb.commons.schema.table;

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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.Locale;

public enum InformationSchemaTable {
  QUERIES(
      new TsTable(
          "queries",
          ImmutableList.of(
              new MeasurementColumnSchema(
                  ColumnHeaderConstant.TIME,
                  TSDataType.TIMESTAMP,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED),
              new MeasurementColumnSchema(
                  ColumnHeaderConstant.QUERY_ID_TABLE_MODEL,
                  TSDataType.STRING,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED),
              new MeasurementColumnSchema(
                  ColumnHeaderConstant.DATA_NODE_ID_TABLE_MODEL,
                  TSDataType.INT32,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED),
              new MeasurementColumnSchema(
                  ColumnHeaderConstant.ELAPSED_TIME_TABLE_MODEL,
                  TSDataType.FLOAT,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED),
              new MeasurementColumnSchema(
                  ColumnHeaderConstant.STATEMENT,
                  TSDataType.STRING,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED),
              new MeasurementColumnSchema(
                  ColumnHeaderConstant.SQL_DIALECT,
                  TSDataType.STRING,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED))));

  public static final String INFORMATION_SCHEMA = "information_schema";

  private final TsTable table;

  InformationSchemaTable(TsTable table) {
    this.table = table;
  }

  public TsTable getTableMetadata() {
    return table;
  }

  public String getSchemaTableName() {
    return table.getTableName();
  }

  public InformationSchemaTable fromStringValue(String value) {
    return valueOf(value);
  }

  public static TsTable getTableFromStringValue(String value) {
    try {
      return valueOf(value.toUpperCase(Locale.ENGLISH)).table;
    } catch (IllegalArgumentException e) {
      // No matched table
      return null;
    }
  }
}
