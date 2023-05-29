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

package org.apache.iotdb.udf.api.access;

import java.io.IOException;

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
}
