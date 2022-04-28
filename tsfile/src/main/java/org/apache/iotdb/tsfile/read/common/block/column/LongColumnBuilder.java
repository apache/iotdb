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
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.calculateBlockResetSize;

public class LongColumnBuilder implements ColumnBuilder {

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(LongColumnBuilder.class).instanceSize();
  public static final LongColumn NULL_VALUE_BLOCK =
      new LongColumn(0, 1, new boolean[] {true}, new long[1]);

  private final ColumnBuilderStatus columnBuilderStatus;
  private boolean initialized;
  private final int initialEntryCount;

  private int positionCount;
  private boolean hasNullValue;
  private boolean hasNonNullValue;

  // it is assumed that these arrays are the same length
  private boolean[] valueIsNull = new boolean[0];
  private long[] values = new long[0];

  private long retainedSizeInBytes;

  public LongColumnBuilder(ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
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

    hasNonNullValue = true;
    positionCount++;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes(LongColumn.SIZE_IN_BYTES_PER_POSITION);
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
  public ColumnBuilder writeTsPrimitiveType(TsPrimitiveType value) {
    return writeLong(value.getLong());
  }

  @Override
  public ColumnBuilder appendNull() {
    if (values.length <= positionCount) {
      growCapacity();
    }

    valueIsNull[positionCount] = true;

    hasNullValue = true;
    positionCount++;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes(LongColumn.SIZE_IN_BYTES_PER_POSITION);
    }
    return this;
  }

  @Override
  public Column build() {
    if (!hasNonNullValue) {
      return new RunLengthEncodedColumn(NULL_VALUE_BLOCK, positionCount);
    }
    return new LongColumn(0, positionCount, hasNullValue ? valueIsNull : null, values);
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
    return new LongColumnBuilder(columnBuilderStatus, calculateBlockResetSize(positionCount));
  }

  private void growCapacity() {
    int newSize;
    if (initialized) {
      newSize = ColumnUtil.calculateNewArraySize(values.length);
    } else {
      newSize = initialEntryCount;
      initialized = true;
    }

    valueIsNull = Arrays.copyOf(valueIsNull, newSize);
    values = Arrays.copyOf(values, newSize);
    updateDataSize();
  }

  private void updateDataSize() {
    retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    if (columnBuilderStatus != null) {
      retainedSizeInBytes += ColumnBuilderStatus.INSTANCE_SIZE;
    }
  }
}
