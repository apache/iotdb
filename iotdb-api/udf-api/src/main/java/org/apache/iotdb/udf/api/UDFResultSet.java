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

package org.apache.iotdb.udf.api;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;

/** Streaming result set returned by {@link IoTDBLocal#query(String)}. */
public interface UDFResultSet extends AutoCloseable {

  /**
   * @return {@code true} if another row is available
   * @throws UDFException if underlying read fails
   */
  boolean hasNext() throws UDFException;

  /**
   * @return the next row
   * @throws UDFException if underlying read fails
   * @throws java.util.NoSuchElementException if no more rows (consistent with {@link
   *     java.util.Iterator})
   */
  Record next() throws UDFException;

  /** Release query resources. Repeated calls must be idempotent. */
  @Override
  void close() throws UDFException;
}
