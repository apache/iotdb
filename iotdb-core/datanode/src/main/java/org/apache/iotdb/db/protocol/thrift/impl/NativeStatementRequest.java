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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import java.time.ZoneId;
import java.util.function.Supplier;

/**
 * An interface that abstracts the common methods used by executeStatementInternal from different
 * types of statement requests (e.g., TSExecuteStatementReq, TSExecutePreparedReq).
 *
 * <p>This interface also extends Supplier&lt;String&gt; to provide a string representation of the
 * request content for cleanup and logging purposes.
 */
public interface NativeStatementRequest extends Supplier<String> {

  /**
   * Gets the statement ID associated with this request.
   *
   * @return the statement ID
   */
  Long getStatementId();

  /**
   * Gets the query timeout in milliseconds.
   *
   * @return the timeout value
   */
  long getTimeout();

  /**
   * Gets the fetch size for result sets.
   *
   * @return the fetch size
   */
  int getFetchSize();

  /**
   * Creates and returns the Tree model Statement for this request. This method is called lazily
   * only when the session is in Tree SQL dialect mode.
   *
   * @param zoneId the timezone for parsing
   * @return the parsed Tree model Statement, or null if not supported
   */
  Statement getTreeStatement(ZoneId zoneId);

  /**
   * Creates and returns the Table model Statement for this request. This method is called lazily
   * only when the session is in Table SQL dialect mode.
   *
   * @param parser the SQL parser for Table model
   * @param zoneId the timezone for parsing
   * @param clientSession the client session
   * @return the parsed Table model Statement, or null if not supported
   */
  org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement getTableStatement(
      SqlParser parser, ZoneId zoneId, IClientSession clientSession);

  /**
   * Returns the SQL statement string for this request.
   *
   * @return the SQL statement string
   */
  String getSql();

  /**
   * Returns the SQL statement string. This default implementation delegates to {@link #getSql()}.
   *
   * <p>This method exists to satisfy the {@link Supplier} interface, allowing this request to be
   * passed directly to methods that accept {@code Supplier<String>} (e.g., for logging/cleanup).
   *
   * @return the SQL statement string
   */
  @Override
  default String get() {
    return getSql();
  }
}
