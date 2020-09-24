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

package org.apache.iotdb.db.query.udf.core.access;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

public class RowImpl implements Row {

  private final int[] columnIndexes;
  private final TSDataType[] dataTypes;

  private RowRecord rowRecord;

  public RowImpl(int[] columnIndexes, TSDataType[] dataTypes) {
    this.columnIndexes = columnIndexes;
    this.dataTypes = dataTypes;
  }

  @Override
  public long getTime() {
    return rowRecord.getTimestamp();
  }

  @Override
  public int getInt(int columnIndex) {
    return rowRecord.getFields().get(columnIndexes[columnIndex]).getIntV();
  }

  @Override
  public long getLong(int columnIndex) {
    return rowRecord.getFields().get(columnIndexes[columnIndex]).getLongV();
  }

  @Override
  public float getFloat(int columnIndex) {
    return rowRecord.getFields().get(columnIndexes[columnIndex]).getFloatV();
  }

  @Override
  public double getDouble(int columnIndex) {
    return rowRecord.getFields().get(columnIndexes[columnIndex]).getDoubleV();
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    return rowRecord.getFields().get(columnIndexes[columnIndex]).getBoolV();
  }

  @Override
  public Binary getBinary(int columnIndex) {
    return rowRecord.getFields().get(columnIndexes[columnIndex]).getBinaryV();
  }

  @Override
  public String getString(int columnIndex) {
    return rowRecord.getFields().get(columnIndexes[columnIndex]).getBinaryV().getStringValue();
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    return dataTypes[columnIndexes[columnIndex]];
  }

  @Override
  public boolean isNull(int columnIndex) {
    Field field = rowRecord.getFields().get(columnIndexes[columnIndex]);
    return field == null || field.isNull();
  }

  public Row setRowRecord(RowRecord rowRecord) {
    this.rowRecord = rowRecord;
    return this;
  }
}
