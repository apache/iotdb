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
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.read.common.Path;

import java.io.IOException;
import java.util.List;

public interface RowIterator {

  /**
   * Returns {@code true} if the iteration has more rows.
   *
   * @return {@code true} if the iteration has more rows
   */
  boolean hasNextRow();

  /**
   * Returns the next row in the iteration.
   *
   * <p>Note that the Row instance returned by this method each time is the same instance. In other
   * words, calling {@code next()} will only change the member variables inside the Row instance,
   * but will not generate a new Row instance.
   *
   * @return the next element in the iteration
   * @throws IOException if any I/O errors occur
   */
  Row next() throws IOException;

  /** Resets the iteration. */
  void reset();

  /**
   * Returns the actual column index of the given column name.
   *
   * @param columnName the column name in Path form
   * @throws PipeParameterNotValidException if the given column name is not existed
   * @return the actual column index of the given column name
   */
  int getColumnIndex(Path columnName) throws PipeParameterNotValidException;

  /**
   * Returns the column names
   *
   * @return the column names
   */
  List<Path> getColumnNames();

  /**
   * Returns the column data types
   *
   * @return the column data types
   */
  List<Type> getColumnTypes();
}
