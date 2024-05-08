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

package org.apache.iotdb.tsfile.read.common.type;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

public interface Type {

  /** Gets a boolean at {@code position}. */
  default boolean getBoolean(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a little endian int at {@code position}. */
  default int getInt(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a little endian long at {@code position}. */
  default long getLong(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a float at {@code position}. */
  default float getFloat(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a double at {@code position}. */
  default double getDouble(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a Binary at {@code position}. */
  default Binary getBinary(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }
  /** Gets a Object at {@code position}. */
  default Object getObject(Column c, int position) {
    return c.getObject(position);
  }

  /** Write a boolean to the current entry; */
  default void writeBoolean(ColumnBuilder builder, boolean value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write an int to the current entry; */
  default void writeInt(ColumnBuilder builder, int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a long to the current entry; */
  default void writeLong(ColumnBuilder builder, long value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a float to the current entry; */
  default void writeFloat(ColumnBuilder builder, float value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a double to the current entry; */
  default void writeDouble(ColumnBuilder builder, double value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a Binary to the current entry; */
  default void writeBinary(ColumnBuilder builder, Binary value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a Object to the current entry; */
  default void writeObject(ColumnBuilder builder, Object value) {
    builder.writeObject(value);
  }

  /**
   * Creates the preferred column builder for this type. This is the builder used to store values
   * after an expression projection within the query.
   */
  ColumnBuilder createColumnBuilder(int expectedEntries);

  TypeEnum getTypeEnum();
}
