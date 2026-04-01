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

package org.apache.iotdb.udf.api.relational.access;

import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.utils.Binary;

import java.io.File;
import java.time.LocalDate;
import java.util.Optional;

public interface Record {
  /**
   * Returns the int value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code TSDataType.INT32}.
   *
   * @param columnIndex index of the specified column
   * @return the int value at the specified column in this row
   */
  int getInt(int columnIndex);

  /**
   * Returns the long value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code TSDataType.INT64}
   * or {@code TSDataType.TIMESTAMP}.
   *
   * @param columnIndex index of the specified column
   * @return the long value at the specified column in this row
   */
  long getLong(int columnIndex);

  /**
   * Returns the float value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code TSDataType.FLOAT}.
   *
   * @param columnIndex index of the specified column
   * @return the float value at the specified column in this row
   */
  float getFloat(int columnIndex);

  /**
   * Returns the double value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code
   * TSDataType.DOUBLE}.
   *
   * @param columnIndex index of the specified column
   * @return the double value at the specified column in this row
   */
  double getDouble(int columnIndex);

  /**
   * Returns the boolean value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code
   * TSDataType.BOOLEAN}.
   *
   * @param columnIndex index of the specified column
   * @return the boolean value at the specified column in this row
   */
  boolean getBoolean(int columnIndex);

  /**
   * Returns the Binary value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code TSDataType.TEXT},
   * {@code TSDataType.STRING} or {@code TSDataType.BLOB}.
   *
   * @param columnIndex index of the specified column
   * @return the Binary value at the specified column in this row
   */
  Binary getBinary(int columnIndex);

  /**
   * Returns the String value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code TSDataType.TEXT}
   * or {@code TSDataType.STRING} or {@code TSDataType.OBJECT}.
   *
   * @param columnIndex index of the specified column
   * @return the String value at the specified column in this row
   */
  String getString(int columnIndex);

  /**
   * Returns the String value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code TSDataType.DATE}.
   *
   * @param columnIndex index of the specified column
   * @return the String value at the specified column in this row
   */
  LocalDate getLocalDate(int columnIndex);

  Object getObject(int columnIndex);

  /**
   * Returns the OBJECT value's real file path in current node at the specified column in this row.
   *
   * @param columnIndex index of the specified column
   * @return Optional.empty() if current node doesn't have the real file storing the object content,
   *     otherwise the File referring to the OBJECT value's real file path
   */
  Optional<File> getObjectFile(int columnIndex);

  /**
   * Returns the OBJECT value's real file length at the specified column in this row.
   *
   * @param columnIndex index of the specified column
   * @return length of the object
   */
  long objectLength(int columnIndex);

  /**
   * Returns the Binary representation of an object stored at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code
   * TSDataType.OBJECT}.
   *
   * <p>This method returns the entire binary data of the object and may require considerable memory
   * if the stored object is large.
   *
   * @param columnIndex index of the specified column
   * @return the Binary content of the object at the specified column
   */
  Binary readObject(int columnIndex);

  /**
   * Returns a partial Binary segment of an object stored at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code
   * TSDataType.OBJECT}.
   *
   * <p>This method enables reading a subset of the stored object without materializing the entire
   * binary data in memory, which is useful for large objects and streaming access patterns.
   *
   * @param columnIndex index of the specified column
   * @param offset byte offset of the subsection read
   * @param length number of bytes to read starting from the offset. If length < 0, read the entire
   *     binary data from offset.
   * @return the Binary content of the object segment at the specified column
   */
  Binary readObject(int columnIndex, long offset, int length);

  /**
   * Returns the actual data type of the value at the specified column in this row.
   *
   * @param columnIndex index of the specified column
   * @return the actual data type of the value at the specified column in this row
   */
  Type getDataType(int columnIndex);

  /**
   * Returns {@code true} if the value of the specified column is null.
   *
   * @param columnIndex index of the specified column
   * @return {@code true} if the value of the specified column is null
   */
  boolean isNull(int columnIndex);

  /**
   * Returns the number of columns.
   *
   * @return the number of columns
   */
  int size();
}
