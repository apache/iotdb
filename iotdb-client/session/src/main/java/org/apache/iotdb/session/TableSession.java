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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.write.record.Tablet;

/**
 * Utility class for formatting parameter values as SQL literals for PreparedStatement. Handles
 * escaping of special characters (e.g., single quotes in strings).
 */
class ParameterFormatter {
  private ParameterFormatter() {}

  /**
   * Formats a parameter value as a SQL literal string.
   *
   * <p>Special handling:
   *
   * <ul>
   *   <li>String values: Single quotes are escaped by doubling them (e.g., "It's" becomes
   *       "'It''s'")
   *   <li>null values: Converted to "NULL"
   *   <li>Numbers: Used directly without quotes
   *   <li>Booleans: Converted to "TRUE" or "FALSE"
   * </ul>
   *
   * @param value the parameter value
   * @return SQL literal string representation
   */
  static String formatParameter(Object value) {
    if (value == null) {
      return "NULL";
    }

    if (value instanceof String) {
      // Escape single quotes by doubling them (SQL standard)
      // Example: "It's a test" -> "'It''s a test'"
      String escaped = ((String) value).replace("'", "''");
      return "'" + escaped + "'";
    }

    if (value instanceof Number) {
      // Numbers are used directly
      return value.toString();
    }

    if (value instanceof Boolean) {
      // Boolean values
      return ((Boolean) value) ? "TRUE" : "FALSE";
    }

    if (value instanceof java.sql.Timestamp) {
      // Timestamp: convert to ISO format string
      return "'" + ((java.sql.Timestamp) value).toInstant().toString() + "'";
    }

    if (value instanceof java.time.LocalDateTime) {
      // LocalDateTime: format as ISO string
      return "'"
          + ((java.time.LocalDateTime) value)
              .format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          + "'";
    }

    if (value instanceof java.time.LocalDate) {
      // LocalDate: format as ISO string
      return "'"
          + ((java.time.LocalDate) value).format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE)
          + "'";
    }

    // For other types, convert to string and quote
    String escaped = value.toString().replace("'", "''");
    return "'" + escaped + "'";
  }

  /**
   * Formats multiple parameters as a comma-separated list of SQL literals.
   *
   * @param parameters the parameter values
   * @return comma-separated SQL literal string
   */
  static String formatParameters(Object... parameters) {
    if (parameters == null || parameters.length == 0) {
      return "";
    }

    java.util.List<String> formatted = new java.util.ArrayList<>();
    for (Object param : parameters) {
      formatted.add(formatParameter(param));
    }
    return String.join(", ", formatted);
  }
}

public class TableSession implements ITableSession {

  private final Session session;

  TableSession(Session session) {
    this.session = session;
  }

  @Override
  public void insert(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException {
    session.insertRelationalTablet(tablet);
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    return session.executeQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    return session.executeQueryStatement(sql, timeoutInMs);
  }

  //  @Override
  //  public void prepare(String statementName, String sql)
  //      throws IoTDBConnectionException, StatementExecutionException {
  //    // Build PREPARE statement: PREPARE statementName FROM 'sql'
  //    // Escape single quotes in SQL by doubling them
  //    String escapedSql = sql.replace("'", "''");
  //    String prepareSql = String.format("PREPARE %s FROM '%s'", statementName, escapedSql);
  //    session.executeNonQueryStatement(prepareSql);
  //  }
  //
  //  @Override
  //  public SessionDataSet execute(String statementName, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException {
  //    return execute(statementName, 0, parameters);
  //  }
  //
  //  @Override
  //  public SessionDataSet execute(String statementName, long timeoutInMs, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException {
  //    // Build EXECUTE statement: EXECUTE statementName USING param1, param2, ...
  //    String executeSql;
  //    if (parameters == null || parameters.length == 0) {
  //      executeSql = String.format("EXECUTE %s", statementName);
  //    } else {
  //      String params = ParameterFormatter.formatParameters(parameters);
  //      executeSql = String.format("EXECUTE %s USING %s", statementName, params);
  //    }
  //
  //    // Try to execute as query first, fall back to non-query if needed
  //    try {
  //      if (timeoutInMs > 0) {
  //        return session.executeQueryStatement(executeSql, timeoutInMs);
  //      } else {
  //        return session.executeQueryStatement(executeSql);
  //      }
  //    } catch (StatementExecutionException e) {
  //      // If it's not a query statement, execute as non-query
  //      session.executeNonQueryStatement(executeSql);
  //      return null;
  //    }
  //  }
  //
  //  @Override
  //  public SessionDataSet executeImmediate(String sql, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException {
  //    return executeImmediate(sql, 0, parameters);
  //  }
  //
  //  @Override
  //  public SessionDataSet executeImmediate(String sql, long timeoutInMs, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException {
  //    // Build EXECUTE IMMEDIATE statement: EXECUTE IMMEDIATE 'sql' USING param1, param2, ...
  //    // Escape single quotes in SQL by doubling them
  //    String escapedSql = sql.replace("'", "''");
  //    String executeSql;
  //    if (parameters == null || parameters.length == 0) {
  //      executeSql = String.format("EXECUTE IMMEDIATE '%s'", escapedSql);
  //    } else {
  //      String params = ParameterFormatter.formatParameters(parameters);
  //      executeSql = String.format("EXECUTE IMMEDIATE '%s' USING %s", escapedSql, params);
  //    }
  //
  //    // Try to execute as query first, fall back to non-query if needed
  //    try {
  //      if (timeoutInMs > 0) {
  //        return session.executeQueryStatement(executeSql, timeoutInMs);
  //      } else {
  //        return session.executeQueryStatement(executeSql);
  //      }
  //    } catch (StatementExecutionException e) {
  //      // If it's not a query statement, execute as non-query
  //      session.executeNonQueryStatement(executeSql);
  //      return null;
  //    }
  //  }
  //
  //  @Override
  //  public void deallocate(String statementName)
  //      throws IoTDBConnectionException, StatementExecutionException {
  //    // Build DEALLOCATE statement: DEALLOCATE PREPARE statementName
  //    String deallocateSql = String.format("DEALLOCATE PREPARE %s", statementName);
  //    session.executeNonQueryStatement(deallocateSql);
  //  }

  @Override
  public void close() throws IoTDBConnectionException {
    session.close();
  }
}
