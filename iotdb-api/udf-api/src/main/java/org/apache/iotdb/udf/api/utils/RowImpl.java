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

package org.apache.iotdb.udf.api.utils;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.enums.TSDataType;

public class RowImpl implements Row {

  private final TSDataType[] dataTypes;
  private final int size;

  private Object[] rowRecord;

  public RowImpl(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
    size = dataTypes.length;
  }

  @Override
  public long getTime() {
    // Time column is always the last column
    return (long) rowRecord[size - 1];
  }

  @Override
  public int getInt(int columnIndex) {
    if (columnIndex >= size()) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    return (int) rowRecord[columnIndex];
  }

  @Override
  public long getLong(int columnIndex) {
    if (columnIndex >= size()) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    return (long) rowRecord[columnIndex];
  }

  @Override
  public float getFloat(int columnIndex) {
    if (columnIndex >= size()) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    return (float) rowRecord[columnIndex];
  }

  @Override
  public double getDouble(int columnIndex) {
    if (columnIndex >= size()) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    return (double) rowRecord[columnIndex];
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    if (columnIndex >= size()) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    return (boolean) rowRecord[columnIndex];
  }

  @Override
  public Binary getBinary(int columnIndex) {
    if (columnIndex >= size()) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    return transformToUDFBinary((org.apache.tsfile.utils.Binary) rowRecord[columnIndex]);
  }

  @Override
  public String getString(int columnIndex) {
    if (columnIndex >= size()) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    return rowRecord[columnIndex].toString();
  }

  @Override
  public Type getDataType(int columnIndex) {
    return transformToUDFDataType(dataTypes[columnIndex]);
  }

  @Override
  public boolean isNull(int columnIndex) {
    return rowRecord[columnIndex] == null;
  }

  // Value columns count
  // thus exclude time column
  @Override
  public int size() {
    return size - 1;
  }

  public void setRowRecord(Object[] rowRecord) {
    this.rowRecord = rowRecord;
  }

  public static Binary transformToUDFBinary(org.apache.tsfile.utils.Binary binary) {
    return binary == null ? null : new Binary(binary.getValues());
  }

  private static Type transformToUDFDataType(TSDataType tsDataType) {
    if (tsDataType == null) {
      return null;
    }
    byte type = tsDataType.getType();
    switch (type) {
      case 0:
        return Type.BOOLEAN;
      case 1:
        return Type.INT32;
      case 2:
        return Type.INT64;
      case 3:
        return Type.FLOAT;
      case 4:
        return Type.DOUBLE;
      case 5:
        return Type.TEXT;
      case 8:
        return Type.TIMESTAMP;
      case 9:
        return Type.DATE;
      case 10:
        return Type.BLOB;
      case 11:
        return Type.STRING;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }
}
