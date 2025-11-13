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

package org.apache.iotdb.isession;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.write.record.Tablet;

/**
 * This interface defines a session for interacting with IoTDB tables. It supports operations such
 * as data insertion, executing queries, and closing the session. Implementations of this interface
 * are expected to manage connections and ensure proper resource cleanup.
 *
 * <p>Each method may throw exceptions to indicate issues such as connection errors or execution
 * failures.
 *
 * <p>Since this interface extends {@link AutoCloseable}, it is recommended to use
 * try-with-resources to ensure the session is properly closed.
 */
public interface ITableSession extends AutoCloseable {

  /**
   * Inserts a {@link Tablet} into the database.
   *
   * @param tablet the tablet containing time-series data to be inserted.
   * @throws StatementExecutionException if an error occurs while executing the statement.
   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
   */
  void insert(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException;

  /**
   * Executes a non-query SQL statement, such as a DDL or DML command.
   *
   * @param sql the SQL statement to execute.
   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
   * @throws StatementExecutionException if an error occurs while executing the statement.
   */
  void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * Executes a query SQL statement and returns the result set.
   *
   * @param sql the SQL query statement to execute.
   * @return a {@link SessionDataSet} containing the query results.
   * @throws StatementExecutionException if an error occurs while executing the statement.
   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
   */
  SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException;

  /**
   * Executes a query SQL statement with a specified timeout and returns the result set.
   *
   * @param sql the SQL query statement to execute.
   * @param timeoutInMs the timeout duration in milliseconds for the query execution.
   * @return a {@link SessionDataSet} containing the query results.
   * @throws StatementExecutionException if an error occurs while executing the statement.
   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
   */
  SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException;

  //  /**
  //   * Prepares a SQL statement for later execution. The prepared statement is stored on the
  // server
  //   * and can be executed multiple times with different parameters.
  //   *
  //   * @param statementName the name of the prepared statement (must be a valid identifier).
  //   * @param sql the SQL statement to prepare (may contain ? placeholders for parameters).
  //   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
  //   * @throws StatementExecutionException if an error occurs while preparing the statement (e.g.,
  //   *     statement name already exists, invalid SQL syntax).
  //   */
  //  void prepare(String statementName, String sql)
  //      throws IoTDBConnectionException, StatementExecutionException;
  //
  //  /**
  //   * Executes a previously prepared statement with the given parameters.
  //   *
  //   * @param statementName the name of the prepared statement to execute.
  //   * @param parameters the parameter values to bind to the prepared statement (in order of
  //   *     appearance of ? placeholders).
  //   * @return a {@link SessionDataSet} containing the query results (if the statement is a
  // query), or
  //   *     null (if the statement is a non-query statement).
  //   * @throws StatementExecutionException if an error occurs while executing the statement (e.g.,
  //   *     prepared statement not found, parameter count mismatch, type mismatch).
  //   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
  //   */
  //  SessionDataSet execute(String statementName, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException;
  //
  //  /**
  //   * Executes a previously prepared statement with the given parameters and a specified timeout.
  //   *
  //   * @param statementName the name of the prepared statement to execute.
  //   * @param timeoutInMs the timeout duration in milliseconds for the query execution.
  //   * @param parameters the parameter values to bind to the prepared statement (in order of
  //   *     appearance of ? placeholders).
  //   * @return a {@link SessionDataSet} containing the query results (if the statement is a
  // query), or
  //   *     null (if the statement is a non-query statement).
  //   * @throws StatementExecutionException if an error occurs while executing the statement (e.g.,
  //   *     prepared statement not found, parameter count mismatch, type mismatch).
  //   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
  //   */
  //  SessionDataSet execute(String statementName, long timeoutInMs, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException;
  //
  //  /**
  //   * Executes a SQL statement immediately with the given parameters. This is equivalent to
  // parsing
  //   * and executing the SQL in one step, without storing a prepared statement.
  //   *
  //   * @param sql the SQL statement to execute (may contain ? placeholders for parameters).
  //   * @param parameters the parameter values to bind to the SQL statement (in order of appearance
  // of
  //   *     ? placeholders).
  //   * @return a {@link SessionDataSet} containing the query results (if the statement is a
  // query), or
  //   *     null (if the statement is a non-query statement).
  //   * @throws StatementExecutionException if an error occurs while executing the statement (e.g.,
  //   *     invalid SQL syntax, parameter count mismatch, type mismatch).
  //   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
  //   */
  //  SessionDataSet executeImmediate(String sql, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException;
  //
  //  /**
  //   * Executes a SQL statement immediately with the given parameters and a specified timeout.
  //   *
  //   * @param sql the SQL statement to execute (may contain ? placeholders for parameters).
  //   * @param timeoutInMs the timeout duration in milliseconds for the query execution.
  //   * @param parameters the parameter values to bind to the SQL statement (in order of appearance
  // of
  //   *     ? placeholders).
  //   * @return a {@link SessionDataSet} containing the query results (if the statement is a
  // query), or
  //   *     null (if the statement is a non-query statement).
  //   * @throws StatementExecutionException if an error occurs while executing the statement (e.g.,
  //   *     invalid SQL syntax, parameter count mismatch, type mismatch).
  //   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
  //   */
  //  SessionDataSet executeImmediate(String sql, long timeoutInMs, Object... parameters)
  //      throws StatementExecutionException, IoTDBConnectionException;
  //
  //  /**
  //   * Deallocates (removes) a previously prepared statement from the server.
  //   *
  //   * @param statementName the name of the prepared statement to deallocate.
  //   * @throws IoTDBConnectionException if there is an issue with the IoTDB connection.
  //   * @throws StatementExecutionException if an error occurs while deallocating the statement
  // (e.g.,
  //   *     prepared statement not found).
  //   */
  //  void deallocate(String statementName)
  //      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * Closes the session, releasing any held resources.
   *
   * @throws IoTDBConnectionException if there is an issue with closing the IoTDB connection.
   */
  @Override
  void close() throws IoTDBConnectionException;
}
