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

/**
 * Entry point for UDF-embedded SQL queries and logging, injected by the framework during UDF
 * lifecycle. Reuses the outer query caller's user identity and permissions without extra client
 * dependencies.
 */
public interface IoTDBLocal {

  /**
   * Execute a single read-only table-model SQL statement and return a streaming result set.
   *
   * @param sql table-model read SQL (e.g. SELECT, SHOW CONFIGNODES)
   * @return query result set; call {@link UDFResultSet#close()} when done or rely on framework
   *     cleanup
   * @throws UDFException on parse failure, permission denied, or execution error
   */
  UDFResultSet query(String sql) throws UDFException;

  /** Log at INFO level. */
  void info(String msg);

  /** Log at INFO level with formatting. */
  void info(String format, Object... args);

  /** Log at INFO level with exception stack. */
  void info(String msg, Throwable t);

  /** Log at WARN level. */
  void warn(String msg);

  /** Log at WARN level with formatting. */
  void warn(String format, Object... args);

  /** Log at WARN level with exception stack. */
  void warn(String msg, Throwable t);

  /** Log at ERROR level. */
  void error(String msg);

  /** Log at ERROR level with formatting. */
  void error(String format, Object... args);

  /** Log at ERROR level with exception stack. */
  void error(String msg, Throwable t);

  /** Close internal session. Called by the framework after beforeDestroy method. */
  void close();
}
