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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.calculateBlockResetSize;

public class BinaryColumnBuilder implements ColumnBuilder {

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(BinaryColumnBuilder.class).instanceSize();

  private final ColumnBuilderStatus columnBuilderStatus;
  public static final BinaryColumn NULL_VALUE_BLOCK =
      new BinaryColumn(0, 1, new boolean[] {true}, new Binary[1]);

  private boolean initialized;
  private final int initialEntryCount;

  private int positionCount;
  private boolean hasNullValue;
  private boolean hasNonNullValue;

  // it is assumed that these arrays are the same length
  private boolean[] valueIsNull = new boolean[0];
  private Binary[] values = new Binary[0];

  private long arraysRetainedSizeInBytes;

  public BinaryColumnBuilder(ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
    this.initialEntryCount = max(expectedEntries, 1);
    this.columnBuilderStatus = columnBuilderStatus;
    updateArraysDataSize();
  }

  @Override
  public ColumnBuilder writeBinary(Binary value) {
    if (values.length <= positionCount) {
      growCapacity();
    }

    values[positionCount] = value;

    hasNonNullValue = true;
    positionCount++;
    return this;
  }

  /** Write an Object to the current entry, which should be the Binary type; */
  @Override
  public ColumnBuilder writeObject(Object value) {
    if (value instanceof Binary) {
      writeBinary((Binary) value);
      return this;
    }
    throw new UnSupportedDataTypeException("BinaryColumn only support Binary data type");
  }

  @Override
  public ColumnBuilder write(Column column, int index) {
    return writeBinary(column.getBinary(index));
  }

  @Override
  public ColumnBuilder writeTsPrimitiveType(TsPrimitiveType value) {
    return writeBinary(value.getBinary());
  }

  @Override
  public ColumnBuilder appendNull() {
    if (values.length <= positionCount) {
      growCapacity();
    }

    valueIsNull[positionCount] = true;

    hasNullValue = true;
    positionCount++;
    return this;
  }

  @Override
  public Column build() {
    if (!hasNonNullValue) {
      return new RunLengthEncodedColumn(NULL_VALUE_BLOCK, positionCount);
    }
    return new BinaryColumn(0, positionCount, hasNullValue ? valueIsNull : null, values);
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.TEXT;
  }

  @Override
  public long getRetainedSizeInBytes() {
    // TODO we need to sum up all the Binary's retainedSize here
    long size = INSTANCE_SIZE + arraysRetainedSizeInBytes;
    if (columnBuilderStatus != null) {
      size += ColumnBuilderStatus.INSTANCE_SIZE;
    }
    return size;
  }

  @Override
  public ColumnBuilder newColumnBuilderLike(ColumnBuilderStatus columnBuilderStatus) {
    // TODO we should take retain size into account here
    return new BinaryColumnBuilder(columnBuilderStatus, calculateBlockResetSize(positionCount));
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
    updateArraysDataSize();
  }

  private void updateArraysDataSize() {
    arraysRetainedSizeInBytes = sizeOf(valueIsNull) + sizeOf(values);
  }
}
