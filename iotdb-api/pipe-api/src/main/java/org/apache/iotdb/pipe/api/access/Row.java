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

package org.apache.iotdb.pipe.api.access;

import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.pipe.api.type.Binary;
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

public interface Row {

  /**
   * Returns the timestamp of this row.
   *
   * @return timestamp
   */
  long getTime();

  /**
   * Returns the int value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code Type.INT32}.
   *
   * @param columnIndex index of the specified column
   * @return the int value at the specified column in this row
   */
  int getInt(int columnIndex);

  /**
   * Returns the long value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code Type.INT64}.
   *
   * @param columnIndex index of the specified column
   * @return the long value at the specified column in this row
   */
  long getLong(int columnIndex);

  /**
   * Returns the float value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code Type.FLOAT}.
   *
   * @param columnIndex index of the specified column
   * @return the float value at the specified column in this row
   */
  float getFloat(int columnIndex);

  /**
   * Returns the double value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code Type.DOUBLE}.
   *
   * @param columnIndex index of the specified column
   * @return the double value at the specified column in this row
   */
  double getDouble(int columnIndex);

  /**
   * Returns the boolean value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code Type.BOOLEAN}.
   *
   * @param columnIndex index of the specified column
   * @return the boolean value at the specified column in this row
   */
  boolean getBoolean(int columnIndex);

  /**
   * Returns the Binary value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code Type.TEXT}.
   *
   * @param columnIndex index of the specified column
   * @return the Binary value at the specified column in this row
   */
  Binary getBinary(int columnIndex);

  /**
   * Returns the String value at the specified column in this row.
   *
   * <p>Users need to ensure that the data type of the specified column is {@code Type.TEXT}.
   *
   * @param columnIndex index of the specified column
   * @return the String value at the specified column in this row
   */
  String getString(int columnIndex);

  /**
   * Returns the Object value at the specified column in this row.
   *
   * @param columnIndex index of the specified column
   * @return the Object value at the specified column in this row
   */
  Object getObject(int columnIndex);

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
   * Returns the number of columns (excluding the timestamp column).
   *
   * @return the number of columns (excluding the timestamp column)
   */
  int size();

  /**
   * Returns the actual column index of the given column name.
   *
   * @param columnName the column name in Path form
   * @return the actual column index of the given column name
   * @throws PipeParameterNotValidException if the given column name is not existed in the Row
   */
  int getColumnIndex(Path columnName) throws PipeParameterNotValidException;

  /**
   * Returns the column data types in the Row.
   *
   * @return the column data types in the Row
   */
  List<Type> getColumnTypes();

  /**
   * Returns the device id of the Row.
   *
   * @return the device id of the Row
   */
  String getDeviceId();
}
