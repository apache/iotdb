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

import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import java.time.LocalDate;

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
   * or {@code TSDataType.STRING}.
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
