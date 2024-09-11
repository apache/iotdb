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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;

import java.nio.charset.Charset;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class IoTDBStatement implements Statement {

  private final IoTDBConnection connection;

  private ResultSet resultSet = null;
  private int fetchSize;
  private int maxRows = 0;

  protected final ZoneId zoneId;
  protected final Charset charset;

  /**
   * Timeout of query can be set by users. Unit: s. A negative number means using the default
   * configuration of server. And value 0 will disable the function of query timeout.
   */
  private int queryTimeout = -1;

  protected IClientRPCService.Iface client;
  private List<String> batchSQLList;
  private static final String NOT_SUPPORT_EXECUTE = "Not support execute";
  private static final String NOT_SUPPORT_EXECUTE_UPDATE = "Not support executeUpdate";

  /** Keep state so we can fail certain calls made after close(). */
  private boolean isClosed = false;

  /** Keep state so we can fail certain calls made after cancel(). */
  private boolean isCancelled = false;

  /** Add SQLWarnings to the warningChain if needed. */
  private SQLWarning warningChain = null;

  private long sessionId;
  private long stmtId = -1;
  private long queryId = -1;

  /** Constructor of IoTDBStatement. */
  IoTDBStatement(
      IoTDBConnection connection,
      IClientRPCService.Iface client,
      long sessionId,
      int fetchSize,
      ZoneId zoneId,
      Charset charset,
      int seconds)
      throws SQLException {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
    this.fetchSize = fetchSize;
    this.batchSQLList = new ArrayList<>();
    this.zoneId = zoneId;
    this.charset = charset;
    this.queryTimeout = seconds;
    requestStmtId();
  }

  IoTDBStatement(
      IoTDBConnection connection,
      IClientRPCService.Iface client,
      long sessionId,
      ZoneId zoneId,
      Charset charset,
      int seconds)
      throws SQLException {
    this(connection, client, sessionId, Config.DEFAULT_FETCH_SIZE, zoneId, charset, seconds);
  }

  IoTDBStatement(
      IoTDBConnection connection,
      IClientRPCService.Iface client,
      long sessionId,
      ZoneId zoneId,
      Charset charset)
      throws SQLException {
    this(connection, client, sessionId, Config.DEFAULT_FETCH_SIZE, zoneId, charset, 0);
  }

  // Only for tests
  IoTDBStatement(
      IoTDBConnection connection,
      IClientRPCService.Iface client,
      long sessionId,
      ZoneId zoneId,
      int seconds)
      throws SQLException {
    this(
        connection,
        client,
        sessionId,
        Config.DEFAULT_FETCH_SIZE,
        zoneId,
        TSFileConfig.STRING_CHARSET,
        seconds);
  }

  // Only for tests
  IoTDBStatement(
      IoTDBConnection connection,
      IClientRPCService.Iface client,
      long sessionId,
      ZoneId zoneId,
      int seconds,
      long statementId) {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
    this.fetchSize = Config.DEFAULT_FETCH_SIZE;
    this.batchSQLList = new ArrayList<>();
    this.zoneId = zoneId;
    this.charset = TSFileConfig.STRING_CHARSET;
    this.queryTimeout = seconds;
    this.stmtId = statementId;
  }

  // Only for tests
  IoTDBStatement(
      IoTDBConnection connection, IClientRPCService.Iface client, long sessionId, ZoneId zoneId)
      throws SQLException {
    this(
        connection,
        client,
        sessionId,
        Config.DEFAULT_FETCH_SIZE,
        zoneId,
        TSFileConfig.STRING_CHARSET,
        0);
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

  /**
   * Execute a SQL encoded in bytes with {@link IoTDBStatement#charset}.
   *
   * @param sql raw sql in bytes encoded with {@link IoTDBStatement#charset}
   * @return true if the first result is a {@link ResultSet} object; false if the first result is an
   *     update count or there is no result
   * @throws SQLException if a database access error occurs
   */
  public boolean execute(byte[] sql) throws SQLException {
    return execute(new String(sql, charset));
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    checkConnection("execute");
    isClosed = false;
    try {
      return executeSQL(sql);
    } catch (TException e) {
      if (reConnect()) {
        try {
          return executeSQL(sql);
        } catch (TException e2) {
          throw new SQLException(e2);
        }
      } else {
        throw new SQLException(
            String.format(
                "Fail to reconnect to server when executing %s. please check server status", sql),
            e);
      }
    }
  }

  @Override
  public boolean execute(String arg0, int arg1) throws SQLException {
    throw new SQLException(NOT_SUPPORT_EXECUTE);
  }

  @Override
  public boolean execute(String arg0, int[] arg1) throws SQLException {
    throw new SQLException(NOT_SUPPORT_EXECUTE);
  }

  @Override
  public boolean execute(String arg0, String[] arg1) throws SQLException {
    throw new SQLException(NOT_SUPPORT_EXECUTE);
  }

  /**
   * There are two kinds of sql here: (1) query sql (2) update sql.
   *
   * <p>(1) return IoTDBJDBCResultSet or IoTDBNonAlignJDBCResultSet (2) simply get executed
   */
  private boolean executeSQL(String sql) throws TException, SQLException {
    isCancelled = false;
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, stmtId);
    int rows = fetchSize;
    if (maxRows != 0 && fetchSize > maxRows) {
      rows = maxRows;
    }
    execReq.setFetchSize(rows);
    execReq.setTimeout((long) queryTimeout * 1000);
    TSExecuteStatementResp execResp = client.executeStatementV2(execReq);
    try {
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), execResp.getStatus());
    }

    if (execResp.isSetDatabase()) {
      connection.changeDefaultDatabase(execResp.getDatabase());
    }

    if (execResp.isSetColumns()) {
      queryId = execResp.getQueryId();
      if (execResp.queryResult == null) {
        throw new SQLException("execResp.queryResult should never be null.");
      } else {
        this.resultSet =
            new IoTDBJDBCResultSet(
                this,
                execResp.getColumns(),
                execResp.getDataTypeList(),
                execResp.columnNameIndexMap,
                execResp.isIgnoreTimeStamp(),
                client,
                sql,
                queryId,
                sessionId,
                execResp.queryResult,
                execResp.tracingInfo,
                execReq.timeout,
                execResp.moreData,
                zoneId,
                charset,
                execResp.isSetTableModel() && execResp.isTableModel(),
                execResp.getColumnIndex2TsBlockColumnIndexList());
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
    } finally {
      clearBatch();
    }
  }

  private int[] executeBatchSQL() throws TException, BatchUpdateException {
    isCancelled = false;
    TSExecuteBatchStatementReq execReq = new TSExecuteBatchStatementReq(sessionId, batchSQLList);
    TSStatus execResp = client.executeBatchStatement(execReq);
    int[] result = new int[batchSQLList.size()];
    boolean allSuccess = true;
    StringBuilder message = new StringBuilder(System.lineSeparator());
    for (int i = 0; i < result.length; i++) {
      if (execResp.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        result[i] = execResp.getSubStatus().get(i).code;
        if (result[i] != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && result[i] != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          allSuccess = false;
          message
              .append(execResp.getSubStatus().get(i).message)
              .append(" for SQL: \"")
              .append(batchSQLList.get(i))
              .append("\"")
              .append(System.lineSeparator());
        }
      } else {
        allSuccess =
            allSuccess
                && (execResp.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                    || execResp.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
        result[i] = execResp.getCode();
        message.setLength(0);
        message.append(execResp.getMessage());
      }
    }
    if (execResp.isSetSubStatus() && execResp.getSubStatus() != null) {
      for (TSStatus status : execResp.getSubStatus()) {
        if (status.getCode() == TSStatusCode.USE_DB.getStatusCode()
            && status.isSetMessage()
            && status.getMessage() != null
            && !status.getMessage().isEmpty()) {
          connection.changeDefaultDatabase(status.getMessage());
          break;
        }
      }
    }
    if (!allSuccess) {
      throw new BatchUpdateException(message.toString(), result);
    }
    return result;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return this.executeQuery(sql, (long) queryTimeout * 1000);
  }

  public ResultSet executeQuery(String sql, long timeoutInMS) throws SQLException {
    checkConnection("execute query");
    isClosed = false;
    try {
      return executeQuerySQL(sql, timeoutInMS);
    } catch (TException e) {
      if (reConnect()) {
        try {
          return executeQuerySQL(sql, timeoutInMS);
        } catch (TException e2) {
          throw new SQLException(
              "Fail to executeQuery " + sql + "after reconnecting. please check server status", e2);
        }
      } else {
        throw new SQLException(
            "Fail to reconnect to server when execute query "
                + sql
                + ". please check server status",
            e);
      }
    }
  }

  private ResultSet executeQuerySQL(String sql, long timeoutInMS) throws TException, SQLException {
    isCancelled = false;
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, stmtId);
    int rows = fetchSize;
    if (maxRows != 0 && fetchSize > maxRows) {
      rows = maxRows;
    }
    execReq.setFetchSize(rows);
    execReq.setTimeout(timeoutInMS);
    execReq.setJdbcQuery(true);
    TSExecuteStatementResp execResp = client.executeQueryStatementV2(execReq);
    queryId = execResp.getQueryId();
    try {
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), execResp.getStatus());
    }

    if (!execResp.isSetQueryResult()) {
      throw new SQLException("execResp.queryResult should never be null.");
    } else {
      this.resultSet =
          new IoTDBJDBCResultSet(
              this,
              execResp.getColumns(),
              execResp.getDataTypeList(),
              execResp.columnNameIndexMap,
              execResp.isIgnoreTimeStamp(),
              client,
              sql,
              queryId,
              sessionId,
              execResp.getQueryResult(),
              execResp.tracingInfo,
              execReq.timeout,
              execResp.moreData,
              zoneId,
              charset,
              execResp.isSetTableModel() && execResp.isTableModel(),
              execResp.getColumnIndex2TsBlockColumnIndexList());
    }
    return resultSet;
  }

  private BitSet listToBitSet(List<Byte> listAlias) {
    byte[] byteAlias = new byte[listAlias.size()];
    for (int i = 0; i < listAlias.size(); i++) {
      byteAlias[i] = listAlias.get(i);
    }
    return BitSet.valueOf(byteAlias);
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
            "Fail to reconnect to server when execute update "
                + sql
                + ". please check server status",
            e);
      }
    }
  }

  @Override
  public int executeUpdate(String arg0, int arg1) throws SQLException {
    throw new SQLException(NOT_SUPPORT_EXECUTE_UPDATE);
  }

  @Override
  public int executeUpdate(String arg0, int[] arg1) throws SQLException {
    throw new SQLException(NOT_SUPPORT_EXECUTE_UPDATE);
  }

  @Override
  public int executeUpdate(String arg0, String[] arg1) throws SQLException {
    throw new SQLException(NOT_SUPPORT_EXECUTE_UPDATE);
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
    this.fetchSize = fetchSize == 0 ? Config.DEFAULT_FETCH_SIZE : fetchSize;
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
    return this.maxRows;
  }

  @Override
  public void setMaxRows(int num) throws SQLException {
    checkConnection("setMaxRows");
    if (num < 0) {
      throw new SQLException(String.format("maxRows %d must be >= 0!", num));
    }
    this.maxRows = num;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
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
  public int getUpdateCount() {
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

  private boolean reInit() throws SQLException {
    this.client = connection.getClient();
    this.sessionId = connection.getSessionId();
    try {
      this.stmtId = client.requestStatementId(sessionId);
      return true;
    } catch (Exception e) {
      throw new SQLException(
          "Cannot get id for statement after reconnecting. please check server status", e);
    }
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
              "Cannot get id for statement after reconnecting. please check server status", e2);
        }
      } else {
        throw new SQLException(
            "Cannot get id for statement after reconnecting. please check server status", e);
      }
    }
  }

  private boolean reConnect() throws SQLException {
    boolean flag = connection.reconnect();
    flag = flag && reInit();
    return flag;
  }

  public long getSessionId() {
    return sessionId;
  }

  public long getStmtId() {
    return stmtId;
  }

  public long getMilliSecond(long time) {
    return RpcUtils.getMilliSecond(time, connection.getTimeFactor());
  }

  public int getNanoSecond(long time) {
    return RpcUtils.getNanoSecond(time, connection.getTimeFactor());
  }

  public int getTimeFactor() {
    return connection.getTimeFactor();
  }
}
