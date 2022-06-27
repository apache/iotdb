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

package org.apache.iotdb.db.mpp.transformation.dag.adapter;

import org.apache.iotdb.commons.udf.utils.UDFBinaryTransformer;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

public class ElasticSerializableRowRecordListBackedMultiColumnRow implements Row {

  private final TSDataType[] dataTypes;
  private final int size;

  private Object[] rowRecord;

  public ElasticSerializableRowRecordListBackedMultiColumnRow(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
    size = dataTypes.length;
  }

  @Override
  public long getTime() {
    return (long) rowRecord[size];
  }

  @Override
  public int getInt(int columnIndex) {
    return (int) rowRecord[columnIndex];
  }

  @Override
  public long getLong(int columnIndex) {
    return (long) rowRecord[columnIndex];
  }

  @Override
  public float getFloat(int columnIndex) {
    return (float) rowRecord[columnIndex];
  }

  @Override
  public double getDouble(int columnIndex) {
    return (double) rowRecord[columnIndex];
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    return (boolean) rowRecord[columnIndex];
  }

  @Override
  public Binary getBinary(int columnIndex) {
    return UDFBinaryTransformer.transformToUDFBinary(
        (org.apache.iotdb.tsfile.utils.Binary) rowRecord[columnIndex]);
  }

  @Override
  public String getString(int columnIndex) {
    return ((org.apache.iotdb.tsfile.utils.Binary) rowRecord[columnIndex]).getStringValue();
  }

  @Override
  public Type getDataType(int columnIndex) {
    return UDFDataTypeTransformer.transformToUDFDataType(dataTypes[columnIndex]);
  }

  @Override
  public boolean isNull(int columnIndex) {
    return rowRecord[columnIndex] == null;
  }

  @Override
  public int size() {
    return size;
  }

  public Row setRowRecord(Object[] rowRecord) {
    this.rowRecord = rowRecord;
    return this;
  }
}
