/**
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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSOperationHandle;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.thrift.TException;


public class IoTDBStatement implements Statement {

  private static final String SHOW_TIMESERIES_COMMAND_LOWERCASE = "show timeseries";
  private static final String SHOW_STORAGE_GROUP_COMMAND_LOWERCASE = "show storage group";
  ZoneId zoneId;
  private ResultSet resultSet = null;
  private IoTDBConnection connection;
  private int fetchSize = Config.fetchSize;
  private int queryTimeout = 10;
  private TSIService.Iface client = null;
  private TS_SessionHandle sessionHandle = null;
  private TSOperationHandle operationHandle = null;
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
   * Sets the limit for the maximum number of rows that any ResultSet object produced by this
   * Statement can contain to the given number. If the limit is exceeded, the excess rows are
   * silently dropped. The value must be >= 0, and 0 means there is not limit.
   */
  private int maxRows = 0;

  /**
   * Add SQLWarnings to the warningChain if needed.
   */
  private SQLWarning warningChain = null;

  /**
   * Constructor of IoTDBStatement.
   */
  public IoTDBStatement(IoTDBConnection connection, TSIService.Iface client,
      TS_SessionHandle sessionHandle,
      int fetchSize, ZoneId zoneId) {
    this.connection = connection;
    this.client = client;
    this.sessionHandle = sessionHandle;
    this.fetchSize = fetchSize;
    this.batchSQLList = new ArrayList<>();
    this.zoneId = zoneId;
  }

  public IoTDBStatement(IoTDBConnection connection, TSIService.Iface client,
      TS_SessionHandle sessionHandle,
      ZoneId zoneId) {
    this(connection, client, sessionHandle, Config.fetchSize, zoneId);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Cannot unwrap to " + iface);
  }

  @Override
  public void addBatch(String sql) throws SQLException {
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
      if (operationHandle != null) {
        TSCancelOperationReq closeReq = new TSCancelOperationReq(operationHandle);
        TSCancelOperationResp closeResp = client.cancelOperation(closeReq);
        Utils.verifySuccess(closeResp.getStatus());
      }
    } catch (Exception e) {
      throw new SQLException("Error occurs when canceling statement because " + e.getMessage());
    }
    isCancelled = true;
  }

  @Override
  public void clearBatch() throws SQLException {
    if (batchSQLList == null) {
      batchSQLList = new ArrayList<>();
    }
    batchSQLList.clear();
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  private void closeClientOperation() throws SQLException {
    try {
      if (operationHandle != null) {
        TSCloseOperationReq closeReq = new TSCloseOperationReq(operationHandle);
        TSCloseOperationResp closeResp = client.closeOperation(closeReq);
        Utils.verifySuccess(closeResp.getStatus());
      }
    } catch (Exception e) {
      throw new SQLException("Error occurs when closing statement because " + e.getMessage());
    }
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }

    closeClientOperation();
    // client = null;
    isClosed = true;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    checkConnection("execute");
    isClosed = false;
    try {
      return executeSQL(sql);
    } catch (TException e) {
      boolean flag = connection.reconnect();
      reInit();
      if (flag) {
        try {
          return executeSQL(sql);
        } catch (TException e2) {
          throw new SQLException(
              String.format("Fail to execute %s after reconnecting. please check server status",
                  sql));
        }
      } else {
        throw new SQLException(String
            .format("Fail to reconnect to server when executing %s. please check server status",
                sql));
      }
    }
  }

  @Override
  public boolean execute(String arg0, int arg1) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean execute(String arg0, int[] arg1) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean execute(String arg0, String[] arg1) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /**
   * There are four kinds of sql here: (1) show timeseries path (2) show storage group (3) query sql
   * (4) update sql .
   * <p></p>
   * (1) and (2) return new TsfileMetadataResultSet (3) return new TsfileQueryResultSet (4) simply
   * get executed
   */
  private boolean executeSQL(String sql) throws TException, SQLException {
    isCancelled = false;
    String sqlToLowerCase = sql.toLowerCase().trim();
    if (sqlToLowerCase.startsWith(SHOW_TIMESERIES_COMMAND_LOWERCASE)) {
      String[] cmdSplited = sql.split("\\s+");
      if (cmdSplited.length != 3) {
        throw new SQLException("Error format of \'SHOW TIMESERIES <PATH>\'");
      } else {
        String path = cmdSplited[2];
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        resultSet = databaseMetaData.getColumns(Constant.CatalogTimeseries, path, null, null);
        return true;
      }
    } else if (sqlToLowerCase.equals(SHOW_STORAGE_GROUP_COMMAND_LOWERCASE)) {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      resultSet = databaseMetaData.getColumns(Constant.CatalogStorageGroup, null, null, null);
      return true;
    } else {
      TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionHandle, sql);
      TSExecuteStatementResp execResp = client.executeStatement(execReq);
      operationHandle = execResp.getOperationHandle();
      Utils.verifySuccess(execResp.getStatus());
      if (execResp.getOperationHandle().hasResultSet) {
        resultSet = new IoTDBQueryResultSet(this, execResp.getColumns(), client, sessionHandle,
            operationHandle,
            sql, execResp.getOperationType(), getColumnsType(execResp.getColumns()));
        return true;
      }
      return false;
    }
  }

  @Override
  public int[] executeBatch() throws SQLException {
    checkConnection("executeBatch");
    isClosed = false;
    try {
      return executeBatchSQL();
    } catch (TException e) {
      boolean flag = connection.reconnect();
      reInit();
      if (flag) {
        try {
          return executeBatchSQL();
        } catch (TException e2) {
          throw new SQLException(
              "Fail to execute batch sqls after reconnecting. please check server status");
        }
      } else {
        throw new SQLException(
            "Fail to reconnect to server when executing batch sqls. please check server status");
      }
    }
  }

  private int[] executeBatchSQL() throws TException, SQLException {
    isCancelled = false;
    TSExecuteBatchStatementReq execReq = new TSExecuteBatchStatementReq(sessionHandle,
        batchSQLList);
    TSExecuteBatchStatementResp execResp = client.executeBatchStatement(execReq);
    if (execResp.getStatus().statusCode == TS_StatusCode.SUCCESS_STATUS) {
      if (execResp.getResult() == null) {
        return new int[0];
      } else {
        List<Integer> result = execResp.getResult();
        int len = result.size();
        int[] updateArray = new int[len];
        for (int i = 0; i < len; i++) {
          updateArray[i] = result.get(i);
        }
        return updateArray;
      }
    } else {
      BatchUpdateException exception;
      if (execResp.getResult() == null) {
        exception = new BatchUpdateException(execResp.getStatus().errorMessage, new int[0]);
      } else {
        List<Integer> result = execResp.getResult();
        int len = result.size();
        int[] updateArray = new int[len];
        for (int i = 0; i < len; i++) {
          updateArray[i] = result.get(i);
        }
        exception = new BatchUpdateException(execResp.getStatus().errorMessage, updateArray);
      }
      throw exception;
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    checkConnection("execute query");
    isClosed = false;
    try {
      return executeQuerySQL(sql);
    } catch (TException e) {
      boolean flag = connection.reconnect();
      reInit();
      if (flag) {
        try {
          return executeQuerySQL(sql);
        } catch (TException e2) {
          throw new SQLException(
              "Fail to executeQuery " + sql + "after reconnecting. please check server status");
        }
      } else {
        throw new SQLException(
            "Fail to reconnect to server when execute query " + sql
                + ". please check server status");
      }
    }
  }

  private ResultSet executeQuerySQL(String sql) throws TException, SQLException {
    isCancelled = false;
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionHandle, sql);
    TSExecuteStatementResp execResp = client.executeQueryStatement(execReq);
    operationHandle = execResp.getOperationHandle();
    Utils.verifySuccess(execResp.getStatus());
    resultSet = new IoTDBQueryResultSet(this, execResp.getColumns(), client, sessionHandle,
        operationHandle, sql,
        execResp.getOperationType(), getColumnsType(execResp.getColumns()));
    return resultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    checkConnection("execute update");
    isClosed = false;
    try {
      return executeUpdateSQL(sql);
    } catch (TException e) {
      boolean flag = connection.reconnect();
      reInit();
      if (flag) {
        try {
          return executeUpdateSQL(sql);
        } catch (TException e2) {
          throw new SQLException(
              "Fail to execute update " + sql + "after reconnecting. please check server status");
        }
      } else {
        throw new SQLException(
            "Fail to reconnect to server when execute update " + sql
                + ". please check server status");
      }
    }
  }

  @Override
  public int executeUpdate(String arg0, int arg1) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int executeUpdate(String arg0, int[] arg1) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int executeUpdate(String arg0, String[] arg1) throws SQLException {
    throw new SQLException("Method not supported");
  }

  private int executeUpdateSQL(String sql) throws TException, IoTDBSQLException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionHandle, sql);
    TSExecuteStatementResp execResp = client.executeUpdateStatement(execReq);
    operationHandle = execResp.getOperationHandle();
    Utils.verifySuccess(execResp.getStatus());
    return 0;
  }

  @Override
  public Connection getConnection() throws SQLException {
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
    throw new SQLException("Method not supported");
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setMaxFieldSize(int arg0) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getMaxRows() throws SQLException {
    checkConnection("getMaxRows");
    return maxRows;
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
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean getMoreResults(int arg0) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
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
    throw new SQLException("Method not supported");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkConnection("getResultSetType");
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setPoolable(boolean arg0) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setCursorName(String arg0) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLException("Method not supported");
  }

  private void checkConnection(String action) throws SQLException {
    if (connection == null || connection.isClosed()) {
      throw new SQLException(String.format("Cannot %s after connection has been closed!", action));
    }
  }

  private void reInit() {
    this.client = connection.client;
    this.sessionHandle = connection.sessionHandle;
  }

  private List<String> getColumnsType(List<String> columns) throws SQLException {
    List<String> columnTypes = new ArrayList<>();
    for (String column : columns) {
      columnTypes.add(getColumnType(column));
    }
    return columnTypes;
  }

  private String getColumnType(String columnName) throws SQLException {
    TSFetchMetadataReq req;

    req = new TSFetchMetadataReq(Constant.GLOBAL_COLUMN_REQ);
    req.setColumnPath(columnName);

    TSFetchMetadataResp resp;
    try {
      resp = client.fetchMetadata(req);
      Utils.verifySuccess(resp.getStatus());
      return resp.getDataType();
    } catch (TException | IoTDBSQLException e) {
      throw new SQLException(
          String.format("Cannot get column %s data type because %s", columnName, e.getMessage()));
    }
  }

}
