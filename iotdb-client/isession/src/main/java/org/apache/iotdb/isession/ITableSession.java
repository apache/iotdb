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

  /**
   * Closes the session, releasing any held resources.
   *
   * @throws IoTDBConnectionException if there is an issue with closing the IoTDB connection.
   */
  @Override
  void close() throws IoTDBConnectionException;
}
