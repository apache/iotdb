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
import org.apache.iotdb.rpc.stmt.PreparedParameterSerde;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface;
import org.apache.iotdb.service.rpc.thrift.TSDeallocatePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareReq;
import org.apache.iotdb.service.rpc.thrift.TSPrepareResp;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBTablePreparedStatement extends IoTDBStatement implements PreparedStatement {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBTablePreparedStatement.class);
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";

  private final String sql;
  private final String preparedStatementName;
  private final int parameterCount;
  private final boolean serverSidePrepared;

  private final Object[] parameterValues;
  private final int[] parameterTypes;

  // retain parameters for backward compatibility
  private final Map<Integer, String> parameters = new HashMap<>();

  IoTDBTablePreparedStatement(
      IoTDBConnection connection,
      Iface client,
      Long sessionId,
      String sql,
      ZoneId zoneId,
      Charset charset)
      throws SQLException {
    super(connection, client, sessionId, zoneId, charset);
    this.sql = sql;
    this.preparedStatementName = generateStatementName();

    if (isQueryStatement(sql)) {
      // Send PREPARE request to server only for query statements
      this.serverSidePrepared = true;
      TSPrepareReq prepareReq = new TSPrepareReq();
      prepareReq.setSessionId(sessionId);
      prepareReq.setSql(sql);
      prepareReq.setStatementName(preparedStatementName);

      try {
        TSPrepareResp resp = client.prepareStatement(prepareReq);
        RpcUtils.verifySuccess(resp.getStatus());

        this.parameterCount = resp.isSetParameterCount() ? resp.getParameterCount() : 0;
        this.parameterValues = new Object[parameterCount];
        this.parameterTypes = new int[parameterCount];

        for (int i = 0; i < parameterCount; i++) {
          parameterTypes[i] = Types.NULL;
        }
      } catch (TException | StatementExecutionException e) {
        throw new SQLException("Failed to prepare statement: " + e.getMessage(), e);
      }
    } else {
      // For non-query statements, only keep text parameters for client-side substitution.
      this.serverSidePrepared = false;
      this.parameterCount = 0;
      this.parameterValues = null;
      this.parameterTypes = null;
    }
  }

  // Only for tests
  IoTDBTablePreparedStatement(
      IoTDBConnection connection, Iface client, Long sessionId, String sql, ZoneId zoneId)
      throws SQLException {
    this(connection, client, sessionId, sql, zoneId, TSFileConfig.STRING_CHARSET);
  }

  private String generateStatementName() {
    // StatementId is unique in directly connected DataNode
    return "jdbc_ps_" + getStmtId();
  }

  @Override
  public void addBatch() throws SQLException {
    super.addBatch(createCompleteSql(sql, parameters));
  }

  @Override
  public void clearParameters() {
    this.parameters.clear();
    if (serverSidePrepared) {
      for (int i = 0; i < parameterCount; i++) {
        parameterValues[i] = null;
        parameterTypes[i] = Types.NULL;
      }
    }
  }

  @Override
  public boolean execute() throws SQLException {
    if (isQueryStatement(sql)) {
      TSExecuteStatementResp resp = executeInternal();
      return resp.isSetQueryDataSet() || resp.isSetQueryResult();
    } else {
      return super.execute(createCompleteSql(sql, parameters));
    }
  }

  private boolean isQueryStatement(String sql) {
    if (sql == null) {
      return false;
    }
    String trimmedSql = sql.trim().toUpperCase();
    return trimmedSql.startsWith("SELECT") || trimmedSql.startsWith("WITH");
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    TSExecuteStatementResp resp = executeInternal();
    return processQueryResult(resp);
  }

  @Override
  public int executeUpdate() throws SQLException {
    return super.executeUpdate(createCompleteSql(sql, parameters));
  }

  private TSExecuteStatementResp executeInternal() throws SQLException {
    // Validate all parameters are set
    for (int i = 0; i < parameterCount; i++) {
      if (parameterTypes[i] == Types.NULL
          && parameterValues[i] == null
          && !parameters.containsKey(i + 1)) {
        throw new SQLException("Parameter #" + (i + 1) + " is unset");
      }
    }

    TSExecutePreparedReq req = new TSExecutePreparedReq();
    req.setSessionId(sessionId);
    req.setStatementName(preparedStatementName);
    req.setParameters(
        PreparedParameterSerde.serialize(parameterValues, parameterTypes, parameterCount));
    req.setStatementId(getStmtId());
    if (queryTimeout > 0) {
      req.setTimeout(queryTimeout * 1000L);
    }

    try {
      TSExecuteStatementResp resp = client.executePreparedStatement(req);
      RpcUtils.verifySuccess(resp.getStatus());
      return resp;
    } catch (TException | StatementExecutionException e) {
      throw new SQLException("Failed to execute prepared statement: " + e.getMessage(), e);
    }
  }

  private ResultSet processQueryResult(TSExecuteStatementResp resp) throws SQLException {
    if (resp.isSetQueryDataSet() || resp.isSetQueryResult()) {
      this.resultSet =
          new IoTDBJDBCResultSet(
              this,
              resp.getColumns(),
              resp.getDataTypeList(),
              resp.columnNameIndexMap,
              resp.ignoreTimeStamp,
              client,
              sql,
              resp.queryId,
              sessionId,
              resp.queryResult,
              resp.tracingInfo,
              (long) queryTimeout * 1000,
              resp.isSetMoreData() && resp.isMoreData(),
              zoneId);
      return resultSet;
    }
    return null;
  }

  @Override
  public void close() throws SQLException {
    if (!isClosed() && serverSidePrepared) {
      // Deallocate prepared statement on server only if it was prepared server-side
      TSDeallocatePreparedReq req = new TSDeallocatePreparedReq();
      req.setSessionId(sessionId);
      req.setStatementName(preparedStatementName);

      try {
        TSStatus status = client.deallocatePreparedStatement(req);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          logger.warn("Failed to deallocate prepared statement: {}", status.getMessage());
        }
      } catch (TException e) {
        logger.warn("Error deallocating prepared statement", e);
      }
    }
    super.close();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (resultSet != null) {
      return resultSet.getMetaData();
    }
    return null;
  }

  @Override
  public ParameterMetaData getParameterMetaData() {
    return new ParameterMetaData() {
      @Override
      public int getParameterCount() {
        return parameterCount;
      }

      @Override
      public int isNullable(int param) {
        return ParameterMetaData.parameterNullableUnknown;
      }

      @Override
      public boolean isSigned(int param) {
        if (!serverSidePrepared) {
          return false;
        }
        int type = parameterTypes[param - 1];
        return type == Types.INTEGER
            || type == Types.BIGINT
            || type == Types.FLOAT
            || type == Types.DOUBLE;
      }

      @Override
      public int getPrecision(int param) {
        return 0;
      }

      @Override
      public int getScale(int param) {
        return 0;
      }

      @Override
      public int getParameterType(int param) {
        if (!serverSidePrepared) {
          return Types.NULL;
        }
        return parameterTypes[param - 1];
      }

      @Override
      public String getParameterTypeName(int param) {
        return null;
      }

      @Override
      public String getParameterClassName(int param) {
        return null;
      }

      @Override
      public int getParameterMode(int param) {
        return ParameterMetaData.parameterModeIn;
      }

      @Override
      public <T> T unwrap(Class<T> iface) {
        return null;
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) {
        return false;
      }
    };
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, null, Types.NULL);
    this.parameters.put(parameterIndex, "NULL");
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    setNull(parameterIndex, sqlType);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, x, Types.BOOLEAN);
    this.parameters.put(parameterIndex, Boolean.toString(x));
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, x, Types.INTEGER);
    this.parameters.put(parameterIndex, Integer.toString(x));
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, x, Types.BIGINT);
    this.parameters.put(parameterIndex, Long.toString(x));
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, x, Types.FLOAT);
    this.parameters.put(parameterIndex, Float.toString(x));
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, x, Types.DOUBLE);
    this.parameters.put(parameterIndex, Double.toString(x));
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, x, Types.VARCHAR);
    if (x == null) {
      this.parameters.put(parameterIndex, null);
    } else {
      this.parameters.put(parameterIndex, "'" + escapeSingleQuotes(x) + "'");
    }
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    checkParameterIndex(parameterIndex);
    setPreparedParameterValue(parameterIndex, x, Types.BINARY);
    // Format as hexadecimal string literal for SQL: X'0A0B0C'
    this.parameters.put(parameterIndex, "X'" + PreparedParameterSerde.bytesToHex(x) + "'");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    checkParameterIndex(parameterIndex);
    // Use ISO-8601 format YYYY-MM-DD for DATE columns
    String dateStr = x.toLocalDate().toString();
    setPreparedParameterValue(parameterIndex, dateStr, Types.DATE);
    this.parameters.put(parameterIndex, "'" + dateStr + "'");
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    setDate(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    checkParameterIndex(parameterIndex);
    try {
      long time = x.getTime();
      String timeprecision = client.getProperties().getTimestampPrecision();
      switch (timeprecision.toLowerCase()) {
        case "ms":
          break;
        case "us":
          time = time * 1000;
          break;
        case "ns":
          time = time * 1000000;
          break;
        default:
          break;
      }
      setPreparedParameterValue(parameterIndex, time, Types.BIGINT);
      this.parameters.put(parameterIndex, Long.toString(time));
    } catch (TException e) {
      throw new SQLException("Failed to get time precision: " + e.getMessage(), e);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    checkParameterIndex(parameterIndex);
    // Use millisecond value for SQL compatibility with TIMESTAMP columns
    long timestampMs = x.getTime();
    setPreparedParameterValue(parameterIndex, timestampMs, Types.BIGINT);
    this.parameters.put(parameterIndex, Long.toString(timestampMs));
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    setTimestamp(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, Types.NULL);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof Date) {
      setDate(parameterIndex, (Date) x);
    } else if (x instanceof Time) {
      setTime(parameterIndex, (Time) x);
    } else if (x instanceof byte[]) {
      setBytes(parameterIndex, (byte[]) x);
    } else {
      throw new SQLException(
          String.format(
              "Can't infer the SQL type for an instance of %s. Use setObject() with explicit type.",
              x.getClass().getName()));
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object parameterObj, int targetSqlType, int scale)
      throws SQLException {
    setObject(parameterIndex, parameterObj);
  }

  private void checkParameterIndex(int index) throws SQLException {
    if (!serverSidePrepared) {
      return;
    }
    if (index < 1 || index > parameterCount) {
      throw new SQLException(
          "Parameter index out of range: " + index + " (expected 1-" + parameterCount + ")");
    }
  }

  private void setPreparedParameterValue(int parameterIndex, Object value, int sqlType) {
    if (!serverSidePrepared) {
      return;
    }
    parameterValues[parameterIndex - 1] = value;
    parameterTypes[parameterIndex - 1] = sqlType;
  }

  private String escapeSingleQuotes(String value) {
    return value.replace("'", "''");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    try {
      byte[] bytes = ReadWriteIOUtils.readBytes(x, length);
      setBytes(parameterIndex, bytes);
    } catch (IOException e) {
      throw new SQLException("Failed to read binary stream: " + e.getMessage(), e);
    }
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    setInt(parameterIndex, x);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  // ================== Helper Methods for Backward Compatibility ==================

  private String createCompleteSql(final String sql, Map<Integer, String> parameters)
      throws SQLException {
    List<String> parts = splitSqlStatement(sql);

    StringBuilder newSql = new StringBuilder(parts.get(0));
    for (int i = 1; i < parts.size(); i++) {
      if (!parameters.containsKey(i)) {
        throw new SQLException("Parameter #" + i + " is unset");
      }
      newSql.append(parameters.get(i));
      newSql.append(parts.get(i));
    }
    return newSql.toString();
  }

  private List<String> splitSqlStatement(final String sql) {
    List<String> parts = new ArrayList<>();
    int apCount = 0;
    int off = 0;
    boolean skip = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (skip) {
        skip = false;
        continue;
      }
      switch (c) {
        case '\'':
          apCount++;
          break;
        case '\\':
          skip = true;
          break;
        case '?':
          if ((apCount & 1) == 0) {
            parts.add(sql.substring(off, i));
            off = i + 1;
          }
          break;
        default:
          break;
      }
    }
    parts.add(sql.substring(off));
    return parts;
  }
}
