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

package org.apache.iotdb.db.utils.sort;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;

public interface SortReader {

  /**
   * output the cached data in sortReader, it needs to be called after hasNext() returns true.
   *
   * @return next sortKey from branch
   */
  MergeSortKey next();

  /**
   * Check if there is cached data in sortReader, cache more data if current ones are run out. This
   * method should be called before next() to ensure that there is data to read.
   *
   * @throws IoTDBException the error occurs when reading data from fileChannel
   */
  boolean hasNext() throws IoTDBException;

  /**
   * Close the sortReader and release resources.
   *
   * @throws IoTDBException the error occurs when closing fileChannel
   */
  void close() throws IoTDBException;
}
