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

package org.apache.tsfile.read.common.block.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

import static java.util.Objects.requireNonNull;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

/**
 * This column is used to represent columns that only contain null values. But its positionCount has
 * to be consistent with corresponding valueColumn.
 */
public class NullColumn implements Column {

  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(BooleanColumn.class);

  private int positionCount;

  private final long retainedSizeInBytes;

  public NullColumn(int positionCount) {
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.positionCount = positionCount;
    retainedSizeInBytes = INSTANCE_SIZE;
  }

  @Override
  public TSDataType getDataType() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnEncoding getEncoding() {
    throw new UnsupportedOperationException(getClass().getName());
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
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes;
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(getPositionCount(), positionOffset, length);
    return new NullColumn(length);
  }

  @Override
  public Column getRegionCopy(int positionOffset, int length) {
    return getRegion(positionOffset, length);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new NullColumn(positionCount - fromIndex);
  }

  @Override
  public Column subColumnCopy(int fromIndex) {
    return subColumn(fromIndex);
  }

  @Override
  public void reverse() {
    // do nothing
  }

  public static Column create(TSDataType dataType, int positionCount) {
    requireNonNull(dataType, "dataType is null");
    switch (dataType) {
      case BOOLEAN:
        return new RunLengthEncodedColumn(BooleanColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case INT32:
      case DATE:
        return new RunLengthEncodedColumn(IntColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case INT64:
      case TIMESTAMP:
        return new RunLengthEncodedColumn(LongColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case FLOAT:
        return new RunLengthEncodedColumn(FloatColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case DOUBLE:
        return new RunLengthEncodedColumn(DoubleColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      case TEXT:
      case BLOB:
      case STRING:
        return new RunLengthEncodedColumn(BinaryColumnBuilder.NULL_VALUE_BLOCK, positionCount);
      default:
        throw new IllegalArgumentException("Unknown data type: " + dataType);
    }
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public void setPositionCount(int count) {
    this.positionCount = count;
  }

  @Override
  public void setNull(int start, int end) {}
}
