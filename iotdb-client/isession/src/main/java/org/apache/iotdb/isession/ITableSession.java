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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;

import java.util.List;

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
   * Prepares repeated single-row writes with STRING TAGs and BOOLEAN, INT32, INT64, FLOAT, DOUBLE,
   * DATE or TIMESTAMP FIELDs. ATTRIBUTE columns and other FIELD types are unsupported. Column
   * names, types and categories must have matching lengths and order.
   *
   * <p>The returned statement belongs to this session and is not thread-safe. Close it after use.
   */
  NumericRowPreparedStatement prepareInsertNumericRow(
      String tableName,
      List<String> columnNames,
      List<TSDataType> dataTypes,
      List<ColumnCategory> columnCategories)
      throws StatementExecutionException, IoTDBConnectionException;

  /**
   * Executes one row using FIELD and TAG arrays in their respective occurrence order in the
   * prepared column list. TAGs may be null or empty.
   *
   * <p>BOOLEAN uses zero for false and nonzero for true. INT32 and DATE use the low 32 bits (DATE
   * is yyyyMMdd); INT64 and TIMESTAMP use the long value. FLOAT uses {@link
   * Float#floatToRawIntBits(float)} in the low 32 bits and DOUBLE uses {@link
   * Double#doubleToRawLongBits(double)}. Do not modify input arrays during execution.
   *
   * <p>Follows write redirection recommendations for subsequent rows. Destination handles are
   * prepared lazily and released when the statement is closed. An execution error reported by the
   * server closes the statement; prepare it again before reuse. Transport failures are propagated
   * without replaying a write whose outcome is unknown.
   */
  void executePreparedInsertNumericRow(
      NumericRowPreparedStatement prepared, long timestamp, long[] values, String[] tagValues)
      throws StatementExecutionException, IoTDBConnectionException;

  /**
   * Executes a prepared row with an optional FIELD-only null bitmap sized to {@code values.length}.
   * Marked slots are null and their values are ignored. Do not mutate the bitmap during execution.
   * Requires a server supporting numeric row null bitmaps.
   */
  void executePreparedInsertNumericRow(
      NumericRowPreparedStatement prepared,
      long timestamp,
      long[] values,
      String[] tagValues,
      BitMap nullBitmap)
      throws StatementExecutionException, IoTDBConnectionException;

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
