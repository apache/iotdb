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

package org.apache.iotdb.db.query.udf.api.access;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public interface RowWindow {

  /**
   * Returns the number of rows in this window.
   *
   * @return the number of rows in this window
   */
  int windowSize();

  /**
   * Returns the row at the specified position in this window.
   *
   * <p>Note that the Row instance returned by this method each time is the same instance. In other
   * words, calling this method will only change the member variables inside the Row instance, but
   * will not generate a new Row instance.
   *
   * @param rowIndex index of the row to return
   * @return the row at the specified position in this window
   * @throws IOException if any I/O errors occur
   */
  Row getRow(int rowIndex) throws IOException;

  /**
   * Returns the actual data type of the values at the specified column in this window.
   *
   * @param columnIndex index of the specified column
   * @return the actual data type of the values at the specified column in this window
   */
  TSDataType getDataType(int columnIndex);

  /**
   * Returns an iterator used to access this window.
   *
   * @return an iterator used to access this window
   */
  RowIterator getRowIterator();
}
