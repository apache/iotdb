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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.iotdb.service.rpc.thrift.TSIService.Iface;
import org.slf4j.LoggerFactory;

public class IoTDBPreparedStatement extends IoTDBStatement implements PreparedStatement {

  private String sql;
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";

  /**
   * save the SQL parameters as (paramLoc,paramValue) pairs.
   */
  private final Map<Integer, String> parameters = new LinkedHashMap<>();

  IoTDBPreparedStatement(IoTDBConnection connection, Iface client,
      Long sessionId, String sql,
      ZoneId zoneId) throws SQLException {
    super(connection, client, sessionId, zoneId);
    this.sql = sql;
  }

  @Override
  public void addBatch() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void clearParameters() {
    this.parameters.clear();
  }

  @Override
  public boolean execute() throws SQLException {
    return super.execute(createCompleteSql(sql, parameters));
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(createCompleteSql(sql, parameters));
  }

  @Override
  public int executeUpdate() throws SQLException {
    return super.executeUpdate(createCompleteSql(sql, parameters));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return  new ParameterMetaData() {
      @Override
      public int getParameterCount() throws SQLException {
        return parameters.size();
      }
      @Override
      public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown ;
      }
      @Override
      public boolean isSigned(int param) throws SQLException {
        try{
          return Integer.parseInt(parameters.get(param))<0;
        }
        catch (Exception e){
          return false;
        }
      }

      @Override
      public int getPrecision(int param) throws SQLException {
        return parameters.get(param).length();
      }

      @Override
      public int getScale(int param) throws SQLException {
        try{
          double d= Double.parseDouble(parameters.get(param));
          if (d >= 1) { //we only need the fraction digits
            d = d - (long) d;
          }
          if (d == 0) { //nothing to count
            return 0;
          }
          d *= 10; //shifts 1 digit to left
          int count = 1;
          while (d - (long) d != 0) { //keeps shifting until there are no more fractions
            d *= 10;
            count++;
          }
          return count;
        }
        catch (Exception e){
          return 0;
        }
      }

      @Override
      public int getParameterType(int param) throws SQLException {
        return 0;
      }

      @Override
      public String getParameterTypeName(int param) throws SQLException {
        return null;
      }

      @Override
      public String getParameterClassName(int param) throws SQLException {
        return null;
      }

      @Override
      public int getParameterMode(int param) throws SQLException {
        return 0;
      }

      @Override
      public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
      }
    };
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) {
    this.parameters.put(parameterIndex, Boolean.toString(x));
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setDouble(int parameterIndex, double x) {
    this.parameters.put(parameterIndex, Double.toString(x));
  }

  @Override
  public void setFloat(int parameterIndex, float x) {
    this.parameters.put(parameterIndex, Float.toString(x));
  }

  @Override
  public void setInt(int parameterIndex, int x) {
    this.parameters.put(parameterIndex, Integer.toString(x));
  }

  @Override
  public void setLong(int parameterIndex, long x) {
    this.parameters.put(parameterIndex, Long.toString(x));
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x instanceof String) {
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
    } else {
      // Can't infer a type.
      throw new SQLException(String.format(
          "Can''t infer the SQL type to use for an instance of %s. Use setObject() with"
              + " an explicit Types value to specify the type to use.",
          x.getClass().getName()));
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
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
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setString(int parameterIndex, String x) {
    this.parameters.put(parameterIndex, "'" + x.replace("'", "\\'") + "'");
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) {
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(x.getTime()),
        super.zoneId);
    this.parameters.put(parameterIndex, zonedDateTime
        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  private String createCompleteSql(final String sql, Map<Integer, String> parameters)
      throws SQLException {
    List<String> parts = splitSqlStatement(sql);

    StringBuilder newSql = new StringBuilder(parts.get(0));
    for (int i = 1; i < parts.size(); i++) {
      LoggerFactory.getLogger(IoTDBPreparedStatement.class).debug("SQL {}",sql);
      LoggerFactory.getLogger(IoTDBPreparedStatement.class).debug("parameters {}",parameters.size());
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
          // skip something like 'xxxxx'
          apCount++;
          break;
        case '\\':
          // skip something like \r\n
          skip = true;
          break;
        case '?':
          // for input like: select a from 'bc' where d, 'bc' will be skipped
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
