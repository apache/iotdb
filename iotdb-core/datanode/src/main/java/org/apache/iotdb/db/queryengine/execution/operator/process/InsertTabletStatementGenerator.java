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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Arrays;

public abstract class InsertTabletStatementGenerator implements Accountable {
  protected final int rowLimit;
  protected final PartialPath devicePath;
  protected final boolean isAligned;
  protected final String[] measurements;
  protected final TSDataType[] dataTypes;
  protected final InputLocation[] inputLocations;
  protected final Type[] typeConvertors;

  protected int rowCount = 0;

  protected long[] times;
  protected Object[] columns;
  protected BitMap[] bitMaps;

  public InsertTabletStatementGenerator(
      PartialPath devicePath,
      String[] measurements,
      TSDataType[] dataTypes,
      InputLocation[] inputLocations,
      Type[] typeConvertors,
      boolean isAligned,
      int rowLimit) {
    this.devicePath = devicePath;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.inputLocations = inputLocations;
    this.typeConvertors = typeConvertors;
    this.isAligned = isAligned;
    this.rowLimit = rowLimit;
  }

  public void reset() {
    this.rowCount = 0;
    this.times = new long[rowLimit];
    this.columns = new Object[this.measurements.length];
    for (int i = 0; i < this.measurements.length; i++) {
      switch (dataTypes[i]) {
        case BOOLEAN:
          columns[i] = new boolean[rowLimit];
          break;
        case INT32:
        case DATE:
          columns[i] = new int[rowLimit];
          break;
        case INT64:
        case TIMESTAMP:
          columns[i] = new long[rowLimit];
          break;
        case FLOAT:
          columns[i] = new float[rowLimit];
          break;
        case DOUBLE:
          columns[i] = new double[rowLimit];
          break;
        case TEXT:
        case STRING:
        case BLOB:
          columns[i] = new Binary[rowLimit];
          Arrays.fill((Binary[]) columns[i], Binary.EMPTY_VALUE);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataTypes[i]));
      }
    }
    this.bitMaps = new BitMap[this.measurements.length];
    for (int i = 0; i < this.bitMaps.length; ++i) {
      this.bitMaps[i] = new BitMap(rowLimit);
      this.bitMaps[i].markAll();
    }
  }

  public InsertTabletStatement constructInsertTabletStatement() {
    InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
    insertTabletStatement.setDevicePath(devicePath);
    insertTabletStatement.setAligned(isAligned);
    // Make sure measurements, dataTypes, typeConverters and inputLocations
    // be swapped in adjustIdColumns method
    insertTabletStatement.setMeasurements(measurements);
    insertTabletStatement.setDataTypes(dataTypes);
    insertTabletStatement.setTypeConvertors(typeConvertors);
    insertTabletStatement.setInputLocations(inputLocations);
    insertTabletStatement.setRowCount(rowCount);

    if (rowCount != rowLimit) {
      times = Arrays.copyOf(times, rowCount);
      for (int i = 0; i < columns.length; i++) {
        bitMaps[i] = bitMaps[i].getRegion(0, rowCount);
        switch (dataTypes[i]) {
          case BOOLEAN:
            columns[i] = Arrays.copyOf((boolean[]) columns[i], rowCount);
            break;
          case INT32:
          case DATE:
            columns[i] = Arrays.copyOf((int[]) columns[i], rowCount);
            break;
          case INT64:
          case TIMESTAMP:
            columns[i] = Arrays.copyOf((long[]) columns[i], rowCount);
            break;
          case FLOAT:
            columns[i] = Arrays.copyOf((float[]) columns[i], rowCount);
            break;
          case DOUBLE:
            columns[i] = Arrays.copyOf((double[]) columns[i], rowCount);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            columns[i] = Arrays.copyOf((Binary[]) columns[i], rowCount);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes[i]));
        }
      }
    }

    insertTabletStatement.setTimes(times);
    insertTabletStatement.setBitMaps(bitMaps);
    insertTabletStatement.setColumns(columns);

    return insertTabletStatement;
  }

  public boolean isFull() {
    return rowCount == rowLimit;
  }

  public boolean isEmpty() {
    return rowCount == 0;
  }

  public int getRowCount() {
    return rowCount;
  }

  public String getDevice() {
    return devicePath.toString();
  }

  // calculate used ram of time, columns & bitmaps
  protected long ramBytesUsedByTimeAndColumns() {
    return RamUsageEstimator.sizeOf(times) + getColumnsBytes() + getBitMapsBytes();
  }

  public abstract long getWrittenCount();

  public abstract long getWrittenCount(String measurement);

  public abstract int processTsBlock(TsBlock tsBlock, int lastReadIndex);

  protected void processColumn(
      Column valueColumn,
      Object columns,
      TSDataType dataType,
      Type sourceTypeConvertor,
      int rowIndex) {
    switch (dataType) {
      case INT32:
      case DATE:
        ((int[]) columns)[rowCount] = sourceTypeConvertor.getInt(valueColumn, rowIndex);
        break;
      case INT64:
      case TIMESTAMP:
        ((long[]) columns)[rowCount] = sourceTypeConvertor.getLong(valueColumn, rowIndex);
        break;
      case FLOAT:
        ((float[]) columns)[rowCount] = sourceTypeConvertor.getFloat(valueColumn, rowIndex);
        break;
      case DOUBLE:
        ((double[]) columns)[rowCount] = sourceTypeConvertor.getDouble(valueColumn, rowIndex);
        break;
      case BOOLEAN:
        ((boolean[]) columns)[rowCount] = sourceTypeConvertor.getBoolean(valueColumn, rowIndex);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        ((Binary[]) columns)[rowCount] = sourceTypeConvertor.getBinary(valueColumn, rowIndex);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "data type %s is not supported when convert data at client",
                valueColumn.getDataType()));
    }
  }

  protected long sizeOf(Object[] arr, Class<?> clazz) {
    if (arr == null) {
      return 0;
    }
    long size = RamUsageEstimator.shallowSizeOf(arr);
    long clazzSize = RamUsageEstimator.shallowSizeOfInstance(clazz);
    for (Object s : arr) {
      if (s == null) {
        continue;
      }
      size += clazzSize;
    }
    return size;
  }

  private long getBitMapsBytes() {
    if (bitMaps == null) {
      return 0;
    }
    return Arrays.stream(bitMaps).mapToLong(x -> BitMap.getSizeOfBytes(x.getSize())).sum();
  }

  private long getColumnsBytes() {
    if (columns == null) {
      return 0;
    }
    long bytes = 0L;
    for (int i = 0; i < columns.length; i++) {
      switch (dataTypes[i]) {
        case INT32:
        case DATE:
          bytes += RamUsageEstimator.sizeOf((int[]) columns[i]);
          break;
        case INT64:
        case TIMESTAMP:
          bytes += RamUsageEstimator.sizeOf((long[]) columns[i]);
          break;
        case FLOAT:
          bytes += RamUsageEstimator.sizeOf((float[]) columns[i]);
          break;
        case DOUBLE:
          bytes += RamUsageEstimator.sizeOf((double[]) columns[i]);
          break;
        case BOOLEAN:
          bytes += RamUsageEstimator.sizeOf((boolean[]) columns[i]);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          bytes += RamUsageEstimator.sizeOf((Binary[]) columns[i]);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(
                  "data type %s is not supported when convert data at client", dataTypes[i]));
      }
    }
    return bytes;
  }
}
