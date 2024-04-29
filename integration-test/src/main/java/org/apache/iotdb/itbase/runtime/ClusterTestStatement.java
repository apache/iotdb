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
package org.apache.iotdb.itbase.runtime;

import org.apache.iotdb.jdbc.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** The implementation of {@link ClusterTestStatement} in cluster test. */
public class ClusterTestStatement implements Statement {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTestStatement.class);

  private static final int DEFAULT_QUERY_TIMEOUT = 120;
  private final List<Statement> writeStatements = new ArrayList<>();
  private final List<String> writEndpoints = new ArrayList<>();
  private final List<Statement> readStatements = new ArrayList<>();
  private final List<String> readEndpoints = new ArrayList<>();
  private boolean closed = false;
  private int maxRows = Integer.MAX_VALUE;
  private int queryTimeout = DEFAULT_QUERY_TIMEOUT;
  private int fetchSize = Config.DEFAULT_FETCH_SIZE;

  public ClusterTestStatement(
      List<NodeConnection> writeConnections, List<NodeConnection> readConnections)
      throws SQLException {
    for (NodeConnection writeConnection : writeConnections) {
      try {
        Statement writeStatement = writeConnection.getUnderlyingConnecton().createStatement();
        this.writeStatements.add(writeStatement);
        this.writEndpoints.add(writeConnection.toString());
        updateConfig(writeStatement, 0);
      } catch (SQLException e) {
        LOGGER.warn("Cannot create write statement from connection {}.", writeConnection, e);
      }
    }
//    if (this.writeStatements.isEmpty()) {
//      throw new SQLException("Cannot create any write statement from connections.");
//    }
    for (NodeConnection readConnection : readConnections) {
      try {
        Statement readStatement = readConnection.getUnderlyingConnecton().createStatement();
        this.readStatements.add(readStatement);
        this.readEndpoints.add(readConnection.toString());
        updateConfig(readStatement, queryTimeout);
      } catch (SQLException e) {
        LOGGER.warn("Cannot create read statement from connection {}.", readConnection, e);
      }
    }
    if (this.readStatements.isEmpty()) {
      throw new SQLException("Cannot create any read statement from connections %s.");
    }
  }

  private void updateConfig(Statement statement, int timeout) throws SQLException {
    maxRows = Math.min(statement.getMaxRows(), maxRows);
    statement.setQueryTimeout(timeout);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return new ClusterTestResultSet(readStatements, readEndpoints, sql, queryTimeout);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return writeStatements.stream().findAny().get().executeUpdate(sql);
  }

  @Override
  public void close() throws SQLException {
    List<String> endpoints = new ArrayList<>();
    endpoints.addAll(writEndpoints);
    endpoints.addAll(readEndpoints);
    RequestDelegate<Void> delegate = new ParallelRequestDelegate<>(endpoints, queryTimeout);
    writeStatements.forEach(
        writeStatement ->
            delegate.addRequest(
                () -> {
                  if (writeStatement != null) {
                    writeStatement.close();
                  }
                  return null;
                }));

    readStatements.forEach(
        r ->
            delegate.addRequest(
                () -> {
                  if (r != null) {
                    try {
                      r.close();
                    } catch (SQLException e) {
                      // Ignore close exceptions
                    }
                  }
                  return null;
                }));
    delegate.requestAll();
    closed = true;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return writeStatements.stream().findAny().get().getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    writeStatements.stream().findAny().get().setMaxFieldSize(max);
  }

  @Override
  public int getMaxRows() {
    return maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    for (Statement readStatement : readStatements) {
      readStatement.setMaxRows(max);
    }
    maxRows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    writeStatements.stream().findAny().get().setEscapeProcessing(enable);
    for (Statement readStatement : readStatements) {
      readStatement.setEscapeProcessing(enable);
    }
  }

  @Override
  public int getQueryTimeout() {
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    if (seconds > 0) {
      queryTimeout = seconds;
    } else {
      queryTimeout = DEFAULT_QUERY_TIMEOUT;
    }
    writeStatements.stream().findAny().get().setQueryTimeout(queryTimeout);
    for (Statement readStatement : readStatements) {
      readStatement.setQueryTimeout(queryTimeout);
    }
  }

  @Override
  public void cancel() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearWarnings() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCursorName(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return writeStatements.stream().findAny().get().execute(sql);
  }

  @Override
  public ResultSet getResultSet() {
    throw new UnsupportedOperationException(
        "In integration-test you must use Statement.executeQuery() to query data");
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return writeStatements.stream().findAny().get().getUpdateCount();
  }

  @Override
  public boolean getMoreResults() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetchDirection(int direction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFetchDirection() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    this.fetchSize = rows;
    writeStatements.stream().findAny().get().setFetchSize(fetchSize);
    for (Statement readStatement : readStatements) {
      readStatement.setFetchSize(fetchSize);
    }
  }

  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  @Override
  public int getResultSetConcurrency() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    writeStatements.stream().findAny().get().addBatch(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    writeStatements.stream().findAny().get().clearBatch();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    return writeStatements.stream().findAny().get().executeBatch();
  }

  @Override
  public Connection getConnection() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getMoreResults(int current) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getGeneratedKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetHoldability() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void setPoolable(boolean poolable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPoolable() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeOnCompletion() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCloseOnCompletion() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    throw new UnsupportedOperationException();
  }
}
