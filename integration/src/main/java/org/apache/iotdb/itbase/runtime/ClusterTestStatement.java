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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ClusterTestStatement implements Statement {

  private final Statement writeStatement;
  private final List<Statement> readStatements = new ArrayList<>();
  private boolean closed = false;
  private int maxRows = Integer.MAX_VALUE;
  private int queryTimeout = Integer.MAX_VALUE;

  public ClusterTestStatement(Connection writeConnection, List<Connection> readConnections)
      throws SQLException {
    this.writeStatement = writeConnection.createStatement();
    updateConfig(writeStatement);
    for (Connection readConnection : readConnections) {
      Statement readStatement = readConnection.createStatement();
      this.readStatements.add(readStatement);
      updateConfig(readStatement);
    }
  }

  private void updateConfig(Statement statement) throws SQLException {
    maxRows = Math.min(statement.getMaxRows(), maxRows);
    queryTimeout = Math.min(statement.getQueryTimeout(), queryTimeout);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return new ClusterTestResultSet(readStatements, sql);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return writeStatement.executeUpdate(sql);
  }

  @Override
  public void close() throws SQLException {
    if (writeStatement != null) {
      writeStatement.close();
    }
    readStatements.forEach(
        r -> {
          try {
            r.close();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
    closed = true;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return writeStatement.getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    writeStatement.setMaxFieldSize(max);
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
    writeStatement.setEscapeProcessing(enable);
    for (Statement readStatement : readStatements) {
      readStatement.setEscapeProcessing(enable);
    }
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {}

  @Override
  public void cancel() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return writeStatement.execute(sql);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new UnsupportedOperationException(
        "In integration test you must use Statement.executeQuery() to query data");
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {}

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {}

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    writeStatement.addBatch(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    writeStatement.clearBatch();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    return writeStatement.executeBatch();
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
