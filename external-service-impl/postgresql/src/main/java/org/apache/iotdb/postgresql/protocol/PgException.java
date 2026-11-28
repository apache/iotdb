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

package org.apache.iotdb.postgresql.protocol;

/** An error that is sent to the client as an ErrorResponse with this SQLSTATE. */
public class PgException extends Exception {

  private static final long serialVersionUID = 1L;

  private final String sqlState;
  private final boolean fatal;
  private final String hint;

  public PgException(String sqlState, String message) {
    this(sqlState, message, null, false);
  }

  public PgException(String sqlState, String message, String hint, boolean fatal) {
    super(message);
    this.sqlState = sqlState;
    this.hint = hint;
    this.fatal = fatal;
  }

  /** A FATAL error, after which the server closes the connection. */
  public static PgException fatal(String sqlState, String message) {
    return new PgException(sqlState, message, null, true);
  }

  public String sqlState() {
    return sqlState;
  }

  public boolean isFatal() {
    return fatal;
  }

  /** A hint for the user, or {@code null}. */
  public String hint() {
    return hint;
  }
}
