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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public interface ColumnBuilder {

  /** Write a boolean to the current entry; */
  default ColumnBuilder writeBoolean(boolean value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write an int to the current entry; */
  default ColumnBuilder writeInt(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a long to the current entry; */
  default ColumnBuilder writeLong(long value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a float to the current entry; */
  default ColumnBuilder writeFloat(float value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a double to the current entry; */
  default ColumnBuilder writeDouble(double value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a Binary to the current entry; */
  default ColumnBuilder writeBinary(Binary value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a TsPrimitiveType sequences to the current entry; */
  default ColumnBuilder writeTsPrimitiveType(TsPrimitiveType value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write an Object to the current entry, which should be the corresponding type; */
  default ColumnBuilder writeObject(Object value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Write value at index of passing column
   *
   * <p>Caller should make sure that value at index is not null
   *
   * @param column source column whose type should be same as ColumnBuilder
   * @param index index of source column to read from
   */
  ColumnBuilder write(Column column, int index);

  /** Appends a null value to the block. */
  ColumnBuilder appendNull();

  /** Appends nullCount null value to the block. */
  default ColumnBuilder appendNull(int nullCount) {
    for (int i = 0; i < nullCount; i++) {
      appendNull();
    }
    return this;
  }

  /** Builds the block. This method can be called multiple times. */
  Column build();

  /** Get the data type. */
  TSDataType getDataType();

  /**
   * Returns the retained size of this column in memory, including over-allocations. This method is
   * called from the inner most execution loop and must be fast.
   */
  long getRetainedSizeInBytes();

  /**
   * Creates a new column builder of the same type based on the current usage statistics of this
   * column builder.
   */
  ColumnBuilder newColumnBuilderLike(ColumnBuilderStatus columnBuilderStatus);
}
