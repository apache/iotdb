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

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

/**
 * This column is used to represent columns that only contain null values. But its positionCount has
 * to be consistent with corresponding valueColumn.
 */
public class NullColumn implements Column {

  private final int positionCount;

  private final int arrayOffset;

  public NullColumn(int positionCount) {
    this.positionCount = positionCount;
    arrayOffset = 0;
  }

  public NullColumn(int arrayOffset, int positionCount) {
    if (arrayOffset < 0) {
      throw new IllegalArgumentException("arrayOffset is negative");
    }
    this.arrayOffset = positionCount;
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.positionCount = positionCount;
  }

  @Override
  public TSDataType getDataType() {
    return null;
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.BYTE_ARRAY;
  }

  @Override
  public boolean mayHaveNull() {
    return true;
  }

  @Override
  public boolean isNull(int position) {
    return true;
  }

  @Override
  public boolean[] isNull() {
    return null;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return 0;
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(getPositionCount(), positionOffset, length);
    return new NullColumn(positionOffset + arrayOffset, length);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new NullColumn(arrayOffset + fromIndex, positionCount - fromIndex);
  }

  @Override
  public void reverse() {}

  public static Column create(TSDataType dataType, int positionCount) {
    requireNonNull(dataType, "dataType is null");
    switch (dataType) {
      case BOOLEAN:
        return new RunLengthEncodedColumn(BooleanColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case INT32:
        return new RunLengthEncodedColumn(IntColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case INT64:
        return new RunLengthEncodedColumn(LongColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case FLOAT:
        return new RunLengthEncodedColumn(FloatColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case DOUBLE:
        return new RunLengthEncodedColumn(DoubleColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case TEXT:
        return new RunLengthEncodedColumn(BinaryColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      default:
        throw new IllegalArgumentException("Unknown data type: " + dataType);
    }
  }
}
