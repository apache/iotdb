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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.calculateBlockResetSize;

public class BooleanColumnBuilder implements ColumnBuilder {

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(BooleanColumnBuilder.class).instanceSize();
  private static final BooleanColumn NULL_VALUE_BLOCK =
      new BooleanColumn(0, 1, new boolean[] {true}, new boolean[1]);

  private final ColumnBuilderStatus columnBuilderStatus;
  private boolean initialized;
  private final int initialEntryCount;

  private int positionCount;
  private boolean hasNullValue;
  private boolean hasNonNullValue;

  // it is assumed that these arrays are the same length
  private boolean[] valueIsNull = new boolean[0];
  private boolean[] values = new boolean[0];

  private long retainedSizeInBytes;

  public BooleanColumnBuilder(ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
    this.columnBuilderStatus = columnBuilderStatus;
    this.initialEntryCount = max(expectedEntries, 1);

    updateDataSize();
  }

  @Override
  public ColumnBuilder writeBoolean(boolean value) {
    if (values.length <= positionCount) {
      growCapacity();
    }

    values[positionCount] = value;

    hasNonNullValue = true;
    positionCount++;
    if (columnBuilderStatus != null) {
      columnBuilderStatus.addBytes(BooleanColumn.SIZE_IN_BYTES_PER_POSITION);
    }
    return this;
  }

  @Override
  public ColumnBuilder writeTsPrimitiveType(TsPrimitiveType value) {
    return writeBoolean(value.getBoolean());
  }

  @Override
  public int appendColumn(
      TimeColumn timeColumn, Column valueColumn, int offset, TimeColumnBuilder timeBuilder) {
    int count = timeBuilder.getPositionCount();
    int index = offset;
    BooleanColumn column = (BooleanColumn) valueColumn;
    for (int i = 0; i < count; i++) {
      if (timeColumn.getLong(index) == timeBuilder.getTime(i) && !valueColumn.isNull(index)) {
        writeBoolean(column.getBoolean(index++));
      } else {
        appendNull();
      }
    }
    return index;
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
      columnBuilderStatus.addBytes(BooleanColumn.SIZE_IN_BYTES_PER_POSITION);
    }
    return this;
  }

  @Override
  public Column build() {
    if (!hasNonNullValue) {
      return new RunLengthEncodedColumn(NULL_VALUE_BLOCK, positionCount);
    }
    return new BooleanColumn(0, positionCount, hasNullValue ? valueIsNull : null, values);
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes;
  }

  @Override
  public ColumnBuilder newColumnBuilderLike(ColumnBuilderStatus columnBuilderStatus) {
    return new BooleanColumnBuilder(columnBuilderStatus, calculateBlockResetSize(positionCount));
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
