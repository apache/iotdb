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
package org.apache.iotdb.tsfile.read.common.block.column;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.calculateBlockResetSize;

public class TimeColumnBuilder implements ColumnBuilder {

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(TimeColumnBuilder.class).instanceSize();

  private final ColumnBuilderStatus columnBuilderStatus;
  private boolean initialized;
  private final int initialEntryCount;

  private int positionCount;

  private long[] values = new long[0];

  private long retainedSizeInBytes;

  public TimeColumnBuilder(ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
    this.columnBuilderStatus = columnBuilderStatus;
    this.initialEntryCount = max(expectedEntries, 1);

    updateDataSize();
  }

  @Override
  public ColumnBuilder writeLong(long value) {
    if (values.length <= positionCount) {
      growCapacity();
    }

    values[positionCount] = value;

    positionCount++;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes(TimeColumn.SIZE_IN_BYTES_PER_POSITION);
    }
    return this;
  }

  /** Write an Object to the current entry, which should be the Long type; */
  @Override
  public ColumnBuilder writeObject(Object value) {
    if (value instanceof Long) {
      writeLong((Long) value);
      return this;
    }
    throw new UnSupportedDataTypeException("LongColumn only support Long data type");
  }

  @Override
  public ColumnBuilder write(Column column, int index) {
    return writeLong(column.getLong(index));
  }

  @Override
  public ColumnBuilder appendNull() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnBuilder appendNull(int nullCount) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public Column build() {
    return new TimeColumn(0, positionCount, values);
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.INT64;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes;
  }

  @Override
  public ColumnBuilder newColumnBuilderLike(ColumnBuilderStatus columnBuilderStatus) {
    return new TimeColumnBuilder(columnBuilderStatus, calculateBlockResetSize(positionCount));
  }

  public int getPositionCount() {
    return positionCount;
  }

  public long getTime(int position) {
    checkReadablePosition(position);
    return values[position];
  }

  private void growCapacity() {
    int newSize;
    if (initialized) {
      newSize = ColumnUtil.calculateNewArraySize(values.length);
    } else {
      newSize = initialEntryCount;
      initialized = true;
    }

    values = Arrays.copyOf(values, newSize);
    updateDataSize();
  }

  private void updateDataSize() {
    retainedSizeInBytes = INSTANCE_SIZE + sizeOf(values);
    if (columnBuilderStatus != null) {
      retainedSizeInBytes += ColumnBuilderStatus.INSTANCE_SIZE;
    }
  }

  private void checkReadablePosition(int position) {
    if (position < 0 || position >= getPositionCount()) {
      throw new IllegalArgumentException("position is not valid");
    }
  }
}
