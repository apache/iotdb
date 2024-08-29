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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class PipeRow implements Row {

  protected final int rowIndex;

  protected final String deviceId;
  protected final boolean isAligned;
  protected final IMeasurementSchema[] measurementSchemaList;

  protected final long[] timestampColumn;
  protected final TSDataType[] valueColumnTypes;
  protected final Object[] valueColumns;
  protected final BitMap[] bitMaps;

  protected final String[] columnNameStringList;

  public PipeRow(
      final int rowIndex,
      final String deviceId,
      final boolean isAligned,
      final IMeasurementSchema[] measurementSchemaList,
      final long[] timestampColumn,
      final TSDataType[] valueColumnTypes,
      final Object[] valueColumns,
      final BitMap[] bitMaps,
      final String[] columnNameStringList) {
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
  public int getInt(final int columnIndex) {
    return ((int[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public LocalDate getDate(final int columnIndex) {
    return ((LocalDate[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public long getLong(final int columnIndex) {
    return ((long[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public float getFloat(final int columnIndex) {
    return ((float[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public double getDouble(final int columnIndex) {
    return ((double[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public boolean getBoolean(final int columnIndex) {
    return ((boolean[]) valueColumns[columnIndex])[rowIndex];
  }

  @Override
  public org.apache.iotdb.pipe.api.type.Binary getBinary(final int columnIndex) {
    return PipeBinaryTransformer.transformToPipeBinary(
        ((org.apache.tsfile.utils.Binary[]) valueColumns[columnIndex])[rowIndex]);
  }

  @Override
  public String getString(final int columnIndex) {
    final org.apache.tsfile.utils.Binary binary =
        ((org.apache.tsfile.utils.Binary[]) valueColumns[columnIndex])[rowIndex];
    return binary == null ? null : binary.getStringValue(TSFileConfig.STRING_CHARSET);
  }

  @Override
  public Object getObject(final int columnIndex) {
    switch (getDataType(columnIndex)) {
      case INT32:
        return getInt(columnIndex);
      case DATE:
        return getDate(columnIndex);
      case INT64:
      case TIMESTAMP:
        return getLong(columnIndex);
      case FLOAT:
        return getFloat(columnIndex);
      case DOUBLE:
        return getDouble(columnIndex);
      case BOOLEAN:
        return getBoolean(columnIndex);
      case TEXT:
      case BLOB:
      case STRING:
        return getBinary(columnIndex);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "unsupported data type %s for column %s",
                getDataType(columnIndex), columnNameStringList[columnIndex]));
    }
  }

  @Override
  public Type getDataType(final int columnIndex) {
    return PipeDataTypeTransformer.transformToPipeDataType(valueColumnTypes[columnIndex]);
  }

  @Override
  public boolean isNull(final int columnIndex) {
    return bitMaps[columnIndex].isMarked(rowIndex);
  }

  @Override
  public int size() {
    return valueColumns.length;
  }

  @Override
  public int getColumnIndex(final Path columnName) throws PipeParameterNotValidException {
    for (int i = 0; i < columnNameStringList.length; i++) {
      if (columnNameStringList[i].equals(columnName.getFullPath())) {
        return i;
      }
    }
    throw new PipeParameterNotValidException(
        String.format("column %s not found", columnName.getFullPath()));
  }

  @Override
  public String getColumnName(final int columnIndex) {
    return columnNameStringList[columnIndex];
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

  public int getCurrentRowSize() {
    int rowSize = 0;
    rowSize += 8; // timestamp
    for (int i = 0; i < valueColumnTypes.length; i++) {
      if (valueColumnTypes[i] != null) {
        if (valueColumnTypes[i].isBinary()) {
          rowSize += getBinary(i) != null ? getBinary(i).getLength() : 0;
        } else {
          rowSize += valueColumnTypes[i].getDataTypeSize();
        }
      }
    }
    return rowSize;
  }

  public IMeasurementSchema[] getMeasurementSchemaList() {
    return measurementSchemaList;
  }
}
