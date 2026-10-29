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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;

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
      columns[i] =
          TypeServices.StorageEngine.PRIMITIVE_ARRAY_ALLOCATOR_SERVICE
              .call(Type.fromTsDataType(dataTypes[i]))
              .apply(rowLimit);
      if (dataTypes[i].isBinary()) {
        Arrays.fill((Binary[]) columns[i], Binary.EMPTY_VALUE);
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
        Object copiedColumn =
            TypeServices.StorageEngine.PRIMITIVE_ARRAY_ALLOCATOR_SERVICE
                .call(Type.fromTsDataType(dataTypes[i]))
                .apply(rowCount);
        System.arraycopy(columns[i], 0, copiedColumn, 0, rowCount);
        columns[i] = copiedColumn;
      }
    }

    sortRowsByTimeIfNeeded();

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
    TypeServices.StorageEngine.SOURCE_COLUMN_TO_TABLET_VALUE_WRITER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .write(sourceTypeConvertor, valueColumn, rowIndex, columns, rowCount);
  }

  /**
   * The storage layer assumes the rows of a tablet are sorted by time: the sequence/unsequence
   * classification, the time boundaries of the tsfile resource, the ttl check, the time partition
   * routing and the last value composition all rely on it. The child operator of an {@code INSERT
   * INTO ... SELECT} does not guarantee that order, so restore it before the tablet leaves the
   * engine.
   */
  private void sortRowsByTimeIfNeeded() {
    boolean sorted = true;
    for (int i = 1; i < rowCount; i++) {
      if (times[i] < times[i - 1]) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      return;
    }

    Integer[] order = new Integer[rowCount];
    for (int i = 0; i < rowCount; i++) {
      order[i] = i;
    }
    // stable, so rows sharing a timestamp keep their relative order
    Arrays.sort(order, Comparator.comparingLong(index -> times[index]));

    int[] permutation = new int[rowCount];
    long[] sortedTimes = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      permutation[i] = order[i];
      sortedTimes[i] = times[order[i]];
    }
    times = sortedTimes;
    for (int i = 0; i < columns.length; i++) {
      columns[i] = permuteColumn(columns[i], permutation);
      bitMaps[i] = permuteBitMap(bitMaps[i], permutation);
    }
  }

  private Object permuteColumn(Object column, int[] permutation) {
    int length = Array.getLength(column);
    Object sortedColumn = Array.newInstance(column.getClass().getComponentType(), length);
    for (int i = 0; i < length; i++) {
      Array.set(sortedColumn, i, Array.get(column, permutation[i]));
    }
    return sortedColumn;
  }

  private BitMap permuteBitMap(BitMap bitMap, int[] permutation) {
    BitMap sortedBitMap = new BitMap(rowCount);
    for (int i = 0; i < rowCount; i++) {
      if (bitMap.isMarked(permutation[i])) {
        sortedBitMap.mark(i);
      }
    }
    return sortedBitMap;
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
      bytes += Type.fromTsDataType(dataTypes[i]).estimateArraySize(columns[i]);
    }
    return bytes;
  }
}
