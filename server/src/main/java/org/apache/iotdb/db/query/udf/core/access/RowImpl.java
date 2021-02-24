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
import org.apache.iotdb.tsfile.utils.Binary;

public class RowImpl implements Row {

  private final int[] columnIndexes;
  private final TSDataType[] dataTypes;

  private Object[] rowRecord;

  public RowImpl(int[] columnIndexes, TSDataType[] dataTypes) {
    this.columnIndexes = columnIndexes;
    this.dataTypes = dataTypes;
  }

  @Override
  public long getTime() {
    return (long) rowRecord[rowRecord.length - 1];
  }

  @Override
  public int getInt(int columnIndex) {
    return (int) rowRecord[columnIndexes[columnIndex]];
  }

  @Override
  public long getLong(int columnIndex) {
    return (long) rowRecord[columnIndexes[columnIndex]];
  }

  @Override
  public float getFloat(int columnIndex) {
    return (float) rowRecord[columnIndexes[columnIndex]];
  }

  @Override
  public double getDouble(int columnIndex) {
    return (double) rowRecord[columnIndexes[columnIndex]];
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    return (boolean) rowRecord[columnIndexes[columnIndex]];
  }

  @Override
  public Binary getBinary(int columnIndex) {
    return (Binary) rowRecord[columnIndexes[columnIndex]];
  }

  @Override
  public String getString(int columnIndex) {
    return ((Binary) rowRecord[columnIndexes[columnIndex]]).getStringValue();
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    return dataTypes[columnIndexes[columnIndex]];
  }

  @Override
  public boolean isNull(int columnIndex) {
    return rowRecord[columnIndexes[columnIndex]] == null;
  }

  public Row setRowRecord(Object[] rowRecord) {
    this.rowRecord = rowRecord;
    return this;
  }
}
