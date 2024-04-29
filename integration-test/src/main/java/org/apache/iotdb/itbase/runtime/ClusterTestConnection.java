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

import org.apache.commons.lang3.Validate;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/** The implementation of {@link Connection} in cluster test. */
public class ClusterTestConnection implements Connection {

  private final NodeConnection writeConnection;
  private final List<NodeConnection> readConnections;
  private boolean isClosed;

  public ClusterTestConnection(
      NodeConnection writeConnection, List<NodeConnection> readConnections) {
    Validate.notNull(readConnections);
    this.writeConnection = writeConnection;
    this.readConnections = readConnections;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new ClusterTestStatement(writeConnection, readConnections);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String nativeSQL(String sql) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getAutoCommit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    writeConnection.close();
    for (NodeConnection conn : readConnections) {
      conn.close();
    }
    isClosed = true;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return writeConnection.getUnderlyingConnecton().getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadOnly() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCatalog(String catalog) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCatalog() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTransactionIsolation(int level) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getTransactionIsolation() {
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
  public Statement createStatement(int resultSetType, int resultSetConcurrency) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHoldability(int holdability) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getHoldability() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Savepoint setSavepoint() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Savepoint setSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback(Savepoint savepoint) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob createClob() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob createBlob() {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob createNClob() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLXML createSQLXML() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isValid(int timeout) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    writeConnection.getUnderlyingConnecton().setClientInfo(name, value);
    for (NodeConnection conn : readConnections) {
      conn.getUnderlyingConnecton().setClientInfo(name, value);
    }
  }

  @Override
  public void setClientInfo(Properties properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getClientInfo(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Properties getClientInfo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchema(String schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSchema() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abort(Executor executor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNetworkTimeout() {
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
