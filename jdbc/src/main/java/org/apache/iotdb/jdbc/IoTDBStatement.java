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

package org.apache.iotdb.jdbc;

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.thrift.TException;
import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class IoTDBStatement implements Statement {

  ZoneId zoneId;
  private ResultSet resultSet = null;
  private IoTDBConnection connection;
  private int fetchSize;
  private int queryTimeout = 10;
  protected TSIService.Iface client;
  private List<String> batchSQLList;
  /**
   * Keep state so we can fail certain calls made after close().
   */
  private boolean isClosed = false;

  /**
   * Keep state so we can fail certain calls made after cancel().
   */
  private boolean isCancelled = false;

  /**
   * Add SQLWarnings to the warningChain if needed.
   */
  private SQLWarning warningChain = null;

  private long sessionId;
  private long stmtId = -1;
  private long queryId = -1;

  /**
   * Constructor of IoTDBStatement.
   */
  IoTDBStatement(IoTDBConnection connection, TSIService.Iface client,
      long sessionId, int fetchSize, ZoneId zoneId) throws SQLException {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
    this.fetchSize = fetchSize;
    this.batchSQLList = new ArrayList<>();
    this.zoneId = zoneId;
    requestStmtId();
  }

  // only for test
  IoTDBStatement(IoTDBConnection connection, TSIService.Iface client,
      long sessionId, ZoneId zoneId, long statementId) {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
    this.fetchSize = Config.fetchSize;
    this.batchSQLList = new ArrayList<>();
    this.zoneId = zoneId;
    this.stmtId = statementId;
  }

  IoTDBStatement(IoTDBConnection connection, TSIService.Iface client,
      long sessionId, ZoneId zoneId) throws SQLException {
    this(connection, client, sessionId, Config.fetchSize, zoneId);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Cannot unwrap to " + iface);
  }

  @Override
  public void addBatch(String sql) {
    if (batchSQLList == null) {
      batchSQLList = new ArrayList<>();
    }
    batchSQLList.add(sql);
  }

  @Override
  public void cancel() throws SQLException {
    checkConnection("cancel");
    if (isCancelled) {
      return;
    }
    try {
      if (queryId != -1) {
        TSCancelOperationReq closeReq = new TSCancelOperationReq(sessionId, queryId);
        TSStatus closeResp = client.cancelOperation(closeReq);
        RpcUtils.verifySuccess(closeResp);
      }
    } catch (Exception e) {
      throw new SQLException("Error occurs when canceling statement.", e);
    }
    isCancelled = true;
  }

  @Override
  public void clearBatch() {
    if (batchSQLList == null) {
      batchSQLList = new ArrayList<>();
    }
    batchSQLList.clear();
  }

  @Override
  public void clearWarnings() {
    warningChain = null;
  }

  private void closeClientOperation() throws SQLException {
    try {
      if (stmtId != -1) {
        TSCloseOperationReq closeReq = new TSCloseOperationReq(sessionId);
        closeReq.setStatementId(stmtId);
        TSStatus closeResp = client.closeOperation(closeReq);
        RpcUtils.verifySuccess(closeResp);
        stmtId = -1;
      }
    } catch (Exception e) {
      throw new SQLException("Error occurs when closing statement.", e);
    }
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }

    closeClientOperation();
    isClosed = true;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLException("Not support closeOnCompletion");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    checkConnection("execute");
    isClosed = false;
    try {
      return executeSQL(sql);
    } catch (TException e) {
      if (reConnect()) {
        throw new SQLException(
                String.format("Fail to execute %s",
                        sql), e);
      } else {
        throw new SQLException(String
                .format("Fail to reconnect to server when executing %s. please check server status",
                        sql), e);
      }
    }
  }

  @Override
  public boolean execute(String arg0, int arg1) throws SQLException {
    throw new SQLException("Not support execute");
  }

  @Override
  public boolean execute(String arg0, int[] arg1) throws SQLException {
    throw new SQLException("Not support execute");
  }

  @Override
  public boolean execute(String arg0, String[] arg1) throws SQLException {
    throw new SQLException("Not support execute");
  }

  /**
   * There are four kinds of sql here: (1) show timeseries path/show timeseries (2) show storage
   * group (3) query sql (4) update sql . <p></p> (1) and (2) return new TsfileMetadataResultSet (3)
   * return new TsfileQueryResultSet (4) simply get executed
   */
  private boolean executeSQL(String sql) throws TException, SQLException {
    isCancelled = false;
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, stmtId);
    execReq.setFetchSize(fetchSize);
    TSExecuteStatementResp execResp = client.executeStatement(execReq);
    try {
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), execResp.getStatus());
    }
    if (execResp.isSetColumns()) {
      queryId = execResp.getQueryId();
      if (execResp.queryDataSet == null) {
        this.resultSet = new IoTDBNonAlignQueryResultSet(this, execResp.getColumns(),
            execResp.getDataTypeList(), execResp.columnNameIndexMap, execResp.ignoreTimeStamp, client, sql, queryId,
            sessionId, execResp.nonAlignQueryDataSet);
      }
      else {
        this.resultSet = new IoTDBQueryResultSet(this, execResp.getColumns(),
            execResp.getDataTypeList(), execResp.columnNameIndexMap, execResp.ignoreTimeStamp, client, sql, queryId,
            sessionId, execResp.queryDataSet);
      }
      return true;
    }
    return false;
  }

  @Override
  public int[] executeBatch() throws SQLException {
    checkConnection("executeBatch");
    isClosed = false;
    try {
      return executeBatchSQL();
    } catch (TException e) {
      if (reConnect()) {
        try {
          return executeBatchSQL();
        } catch (TException e2) {
          throw new SQLException(
              "Fail to execute batch sqls after reconnecting. please check server status", e2);
        }
      } else {
        throw new SQLException(
            "Fail to reconnect to server when executing batch sqls. please check server status", e);
      }
    }
  }

  private int[] executeBatchSQL() throws TException, BatchUpdateException {
    isCancelled = false;
    TSExecuteBatchStatementReq execReq = new TSExecuteBatchStatementReq(sessionId, batchSQLList);
    TSExecuteBatchStatementResp execResp = client.executeBatchStatement(execReq);
    int[] result = new int[execResp.statusList.size()];
    boolean allSuccess = true;
    String message = "";
    for (int i = 0; i < result.length; i++) {
      result[i] = execResp.statusList.get(i).code;
      if (result[i] != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        allSuccess = false;
        message = execResp.statusList.get(i).message;
      }
    }
    if (!allSuccess) {
      throw new BatchUpdateException(message, result);
    }
    return result;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    checkConnection("execute query");
    isClosed = false;
    try {
      return executeQuerySQL(sql);
    } catch (TException e) {
      if (reConnect()) {
        try {
          return executeQuerySQL(sql);
        } catch (TException e2) {
          throw new SQLException(
              "Fail to executeQuery " + sql + "after reconnecting. please check server status", e2);
        }
      } else {
        throw new SQLException(
            "Fail to reconnect to server when execute query " + sql
                + ". please check server status", e);
      }
    }
  }

  private ResultSet executeQuerySQL(String sql) throws TException, SQLException {
    isCancelled = false;
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, stmtId);
    execReq.setFetchSize(fetchSize);
    TSExecuteStatementResp execResp = client.executeQueryStatement(execReq);
    queryId = execResp.getQueryId();
    try {
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), execResp.getStatus());
    }
    if (execResp.queryDataSet == null) {
      this.resultSet = new IoTDBNonAlignQueryResultSet(this, execResp.getColumns(),
          execResp.getDataTypeList(), execResp.columnNameIndexMap, execResp.ignoreTimeStamp, client, sql, queryId,
          sessionId, execResp.nonAlignQueryDataSet);
    }
    else {
      this.resultSet = new IoTDBQueryResultSet(this, execResp.getColumns(),
          execResp.getDataTypeList(), execResp.columnNameIndexMap, execResp.ignoreTimeStamp, client, sql, queryId,
          sessionId, execResp.queryDataSet);
    }
    return resultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    checkConnection("execute update");
    isClosed = false;
    try {
      return executeUpdateSQL(sql);
    } catch (TException e) {
      if (reConnect()) {
        try {
          return executeUpdateSQL(sql);
        } catch (TException e2) {
          throw new SQLException(
              "Fail to execute update " + sql + "after reconnecting. please check server status",
              e2);
        }
      } else {
        throw new SQLException(
            "Fail to reconnect to server when execute update " + sql
                + ". please check server status", e);
      }
    }
  }

  @Override
  public int executeUpdate(String arg0, int arg1) throws SQLException {
    throw new SQLException("Not support executeUpdate");
  }

  @Override
  public int executeUpdate(String arg0, int[] arg1) throws SQLException {
    throw new SQLException("Not support executeUpdate");
  }

  @Override
  public int executeUpdate(String arg0, String[] arg1) throws SQLException {
    throw new SQLException("Not support executeUpdate");
  }

  private int executeUpdateSQL(String sql) throws TException, IoTDBSQLException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, stmtId);
    TSExecuteStatementResp execResp = client.executeUpdateStatement(execReq);
    if (execResp.isSetQueryId()) {
      queryId = execResp.getQueryId();
    }
    try {
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), execResp.getStatus());
    }
    return 0;
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkConnection("getFetchDirection");
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkConnection("setFetchDirection");
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException(String.format("direction %d is not supported!", direction));
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkConnection("getFetchSize");
    return fetchSize;
  }

  @Override
  public void setFetchSize(int fetchSize) throws SQLException {
    checkConnection("setFetchSize");
    if (fetchSize < 0) {
      throw new SQLException(String.format("fetchSize %d must be >= 0!", fetchSize));
    }
    this.fetchSize = fetchSize == 0 ? Config.fetchSize : fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException("Not support getGeneratedKeys");
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLException("Not support getMaxFieldSize");
  }

  @Override
  public void setMaxFieldSize(int arg0) throws SQLException {
    throw new SQLException("Not support getMaxFieldSize");
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new SQLException("Not support getMaxRows");
  }

  @Override
  public void setMaxRows(int num) throws SQLException {
    throw new SQLException("Not support getMaxRows" +
        ". Please use the LIMIT clause in a query instead.");
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new SQLException("Not support getMoreResults");
  }

  @Override
  public boolean getMoreResults(int arg0) throws SQLException {
    throw new SQLException("Not support getMoreResults");
  }

  @Override
  public int getQueryTimeout() {
    return this.queryTimeout;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    checkConnection("setQueryTimeout");
    if (seconds <= 0) {
      throw new SQLException(String.format("queryTimeout %d must be >= 0!", seconds));
    }
    this.queryTimeout = seconds;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    checkConnection("getResultSet");
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLException("Not support getResultSetConcurrency");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLException("Not support getResultSetHoldability");
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkConnection("getResultSetType");
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return -1;
  }

  @Override
  public SQLWarning getWarnings() {
    return warningChain;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new SQLException("Not support isCloseOnCompletion");
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new SQLException("Not support isPoolable");
  }

  @Override
  public void setPoolable(boolean arg0) throws SQLException {
    throw new SQLException("Not support setPoolable");
  }

  @Override
  public void setCursorName(String arg0) throws SQLException {
    throw new SQLException("Not support setCursorName");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLException("Not support setEscapeProcessing");
  }

  private void checkConnection(String action) throws SQLException {
    if (connection == null || connection.isClosed()) {
      throw new SQLException(String.format("Cannot %s after connection has been closed!", action));
    }
  }

  private void reInit() {
    this.client = connection.getClient();
    this.sessionId = connection.getSessionId();
  }

  private void requestStmtId() throws SQLException {
    try {
      this.stmtId = client.requestStatementId(sessionId);
    } catch (TException e) {
      if (reConnect()) {
        try {
          this.stmtId = client.requestStatementId(sessionId);
        } catch (TException e2) {
          throw new SQLException(
              "Cannot get id for statement after reconnecting. please check server status",
              e2);
        }
      } else {
        throw new SQLException(
            "Cannot get id for statement after reconnecting. please check server status", e);
      }
    }
  }


  private boolean reConnect() {
    boolean flag = connection.reconnect();
    reInit();
    return flag;
  }

  public long getSessionId() {
    return sessionId;
  }
}
