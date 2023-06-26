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

package org.apache.iotdb.db.pipe.event.common.row;

import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Arrays;
import java.util.List;

public class PipeRow implements Row {

  private final int rowIndex;

  private final String deviceId;
  private final boolean isAligned;
  private final MeasurementSchema[] measurementSchemaList;

  private final long[] timestampColumn;
  private final TSDataType[] valueColumnTypes;
  private final Object[] valueColumns;
  private final BitMap[] bitMaps;

  private final String[] columnNameStringList;

  public PipeRow(
      int rowIndex,
      String deviceId,
      boolean isAligned,
      MeasurementSchema[] measurementSchemaList,
      long[] timestampColumn,
      TSDataType[] valueColumnTypes,
      Object[] valueColumns,
      BitMap[] bitMaps,
      String[] columnNameStringList) {
    this.rowIndex = rowIndex;
    this.deviceId = deviceId;
    this.isAligned = isAligned;
    this.measurementSchemaList = measurementSchemaList;
    this.timestampColumn = timestampColumn;
    this.valueColumnTypes = valueColumnTypes;
    this.valueColumns = valueColumns;
    this.bitMaps = bitMaps;
    this.columnNameStringList = columnNameStringList;
  }

  @Override
  public long getTime() {
    return timestampColumn[rowIndex];
  }

  @Override
  public int getInt(int columnIndex) {
    return ((int[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public long getLong(int columnIndex) {
    return ((long[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public float getFloat(int columnIndex) {
    return ((float[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public double getDouble(int columnIndex) {
    return ((double[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    return ((boolean[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public org.apache.iotdb.pipe.api.type.Binary getBinary(int columnIndex) {
    return PipeBinaryTransformer.transformToPipeBinary(
        ((org.apache.iotdb.tsfile.utils.Binary[]) valueColumns[columnIndex])[rowIndex]);
  }

  @Override
  public String getString(int columnIndex) {
    final org.apache.iotdb.tsfile.utils.Binary binary =
        ((org.apache.iotdb.tsfile.utils.Binary[]) valueColumns[columnIndex])[rowIndex];
    return binary == null ? null : binary.getStringValue();
  }

  @Override
  public Object getObject(int columnIndex) {
    switch (getDataType(columnIndex)) {
      case INT32:
        return getInt(columnIndex);
      case INT64:
        return getLong(columnIndex);
      case FLOAT:
        return getFloat(columnIndex);
      case DOUBLE:
        return getDouble(columnIndex);
      case BOOLEAN:
        return getBoolean(columnIndex);
      case TEXT:
        return getBinary(columnIndex);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "unsupported data type %s for column %s",
                getDataType(columnIndex), columnNameStringList[columnIndex]));
    }
  }

  @Override
  public Type getDataType(int columnIndex) {
    return PipeDataTypeTransformer.transformToPipeDataType(valueColumnTypes[columnIndex]);
  }

  @Override
  public boolean isNull(int columnIndex) {
    return bitMaps[columnIndex].isMarked(rowIndex);
  }

  @Override
  public int size() {
    return valueColumns.length;
  }

  @Override
  public int getColumnIndex(Path columnName) throws PipeParameterNotValidException {
    for (int i = 0; i < columnNameStringList.length; i++) {
      if (columnNameStringList[i].equals(columnName.getFullPath())) {
        return i;
      }
    }
    throw new PipeParameterNotValidException(
        String.format("column %s not found", columnName.getFullPath()));
  }

  @Override
  public List<Type> getColumnTypes() {
    return PipeDataTypeTransformer.transformToPipeDataTypeList(Arrays.asList(valueColumnTypes));
  }

  @Override
  public String getDeviceId() {
    return deviceId;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public MeasurementSchema[] getMeasurementSchemaList() {
    return measurementSchemaList;
  }
}
