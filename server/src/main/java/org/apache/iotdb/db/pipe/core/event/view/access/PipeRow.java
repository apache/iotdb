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

package org.apache.iotdb.db.pipe.core.event.view.access;

import org.apache.iotdb.commons.pipe.utils.PipeBinaryTransformer;
import org.apache.iotdb.commons.pipe.utils.PipeDataTypeTransformer;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.pipe.api.type.Binary;
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

public class PipeRow implements Row {

  private final long timestamp;
  private final List<Path> columnNameList;
  private final List<TSDataType> columnTypeList;
  private Object[] rowRecord;
  private final int columnSize;

  public PipeRow(
      Object[] rowRecord, List<Path> columnNames, List<TSDataType> columnTypeList, long timestamp) {
    this.columnTypeList = columnTypeList;
    this.columnNameList = columnNames;
    columnSize = columnTypeList.size();
    this.timestamp = timestamp;
    this.rowRecord = rowRecord;
  }

  @Override
  public long getTime() {
    return timestamp;
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
    return PipeBinaryTransformer.transformToPipeBinary(
        (org.apache.iotdb.tsfile.utils.Binary) rowRecord[columnIndex]);
  }

  @Override
  public String getString(int columnIndex) {
    return ((org.apache.iotdb.tsfile.utils.Binary) rowRecord[columnIndex]).getStringValue();
  }

  @Override
  public Object getObject(int columnIndex) {
    return rowRecord[columnIndex];
  }

  @Override
  public Type getDataType(int columnIndex) {
    return PipeDataTypeTransformer.transformToPipeDataType(columnTypeList.get(columnIndex));
  }

  @Override
  public boolean isNull(int columnIndex) {
    return rowRecord[columnIndex] == null;
  }

  @Override
  public int size() {
    return columnSize;
  }

  @Override
  public int getColumnIndex(Path columnName) throws PipeParameterNotValidException {
    for (int i = 0; i < columnNameList.size(); i++) {
      if (columnNameList.get(i).equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public List<Path> getColumnNames() {
    return columnNameList;
  }

  @Override
  public List<Type> getColumnTypes() {
    return PipeDataTypeTransformer.transformToPipeDataTypeList(columnTypeList);
  }
}
