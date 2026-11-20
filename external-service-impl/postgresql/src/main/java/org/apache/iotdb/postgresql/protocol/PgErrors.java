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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Maps the engine's status codes to SQLSTATEs. Unknown codes become XX000 with the engine's
 * message.
 */
public final class PgErrors {

  private static final Map<Integer, String> BY_CODE = new HashMap<>();
  private static final Pattern MISSING_TABLE =
      Pattern.compile("table\\b.*\\b(does not exist|not exists?|cannot be found)");
  private static final Pattern MISSING_DATABASE =
      Pattern.compile("database\\b.*\\b(does not exist|not exists?|cannot be found)");
  private static final Pattern MISSING_COLUMN =
      Pattern.compile("column\\b.*\\b(does not exist|not exists?|cannot be resolved)");

  static {
    BY_CODE.put(TSStatusCode.SQL_PARSE_ERROR.getStatusCode(), SqlState.SYNTAX_ERROR);
    BY_CODE.put(TSStatusCode.TABLE_NOT_EXISTS.getStatusCode(), SqlState.UNDEFINED_TABLE);
    BY_CODE.put(TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode(), SqlState.UNDEFINED_COLUMN);
    BY_CODE.put(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), SqlState.INVALID_SCHEMA_NAME);
    BY_CODE.put(TSStatusCode.NO_PERMISSION.getStatusCode(), SqlState.INSUFFICIENT_PRIVILEGE);
    BY_CODE.put(TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), SqlState.QUERY_CANCELED);
    BY_CODE.put(TSStatusCode.QUERY_TIMEOUT.getStatusCode(), SqlState.QUERY_CANCELED);
    BY_CODE.put(TSStatusCode.MPP_MEMORY_NOT_ENOUGH.getStatusCode(), SqlState.OUT_OF_MEMORY);
    BY_CODE.put(
        TSStatusCode.QUERY_EXECUTION_MEMORY_NOT_ENOUGH.getStatusCode(), SqlState.OUT_OF_MEMORY);
    BY_CODE.put(
        TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode(),
        SqlState.INSUFFICIENT_RESOURCES);
    BY_CODE.put(
        TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
        SqlState.NUMERIC_VALUE_OUT_OF_RANGE);
    BY_CODE.put(TSStatusCode.DIVISION_BY_ZERO.getStatusCode(), SqlState.DIVISION_BY_ZERO);
    BY_CODE.put(TSStatusCode.DATE_OUT_OF_RANGE.getStatusCode(), SqlState.DATETIME_FIELD_OVERFLOW);
    BY_CODE.put(TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(), SqlState.FEATURE_NOT_SUPPORTED);
  }

  private PgErrors() {}

  /** The SQLSTATE for an engine status code and message. */
  public static String sqlState(int code, String message) {
    String mapped = BY_CODE.get(code);
    if (mapped != null) {
      return mapped;
    }
    if (code == TSStatusCode.SEMANTIC_ERROR.getStatusCode()) {
      // the analyzer reports missing objects as semantic errors with a message
      String text = message == null ? "" : message.toLowerCase(Locale.ROOT);
      if (MISSING_COLUMN.matcher(text).find()) {
        return SqlState.UNDEFINED_COLUMN;
      }
      if (MISSING_TABLE.matcher(text).find()) {
        return SqlState.UNDEFINED_TABLE;
      }
      if (MISSING_DATABASE.matcher(text).find()) {
        return SqlState.INVALID_SCHEMA_NAME;
      }
      return SqlState.SYNTAX_OR_ACCESS_RULE;
    }
    return SqlState.INTERNAL_ERROR;
  }

  /** An ERROR (not FATAL) for a failed engine status. */
  public static PgException of(TSStatus status) {
    String message =
        status.getMessage() == null ? "error " + status.getCode() : status.getMessage();
    return new PgException(sqlState(status.getCode(), message), message);
  }

  /** An ERROR for an exception from the engine, with the status code it carries if any. */
  public static PgException of(int code, Throwable error) {
    String message = error.getMessage() == null ? error.toString() : error.getMessage();
    return new PgException(sqlState(code, message), message);
  }
}
