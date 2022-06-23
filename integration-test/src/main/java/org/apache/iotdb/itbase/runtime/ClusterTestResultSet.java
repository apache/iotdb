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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/** The implementation of {@link ClusterTestResultSet} in cluster test. */
public class ClusterTestResultSet implements ResultSet {

  private final List<ResultSet> resultSets;
  private final List<String> endpoints;
  private final int queryTimeoutSeconds;

  public ClusterTestResultSet(
      List<Statement> statements, List<String> endpoints, String sql, int queryTimeoutSeconds)
      throws SQLException {
    this.queryTimeoutSeconds = queryTimeoutSeconds;
    this.endpoints = endpoints;
    RequestDelegate<ResultSet> delegate = createRequestDelegate();
    for (Statement st : statements) {
      delegate.addRequest(() -> st.executeQuery(sql));
    }
    resultSets = delegate.requestAll();
  }

  @Override
  public boolean next() throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(rs::next);
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public void close() throws SQLException {
    RequestDelegate<Void> delegate = createRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(
          () -> {
            rs.close();
            return null;
          });
    }
    delegate.requestAll();
  }

  @Override
  public boolean wasNull() throws SQLException {
    RequestDelegate<Boolean> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(rs::wasNull);
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    RequestDelegate<String> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getString(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    RequestDelegate<Boolean> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBoolean(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    RequestDelegate<Byte> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getByte(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    RequestDelegate<Short> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getShort(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    RequestDelegate<Integer> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getInt(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    RequestDelegate<Long> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getLong(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    RequestDelegate<Float> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getFloat(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    RequestDelegate<Double> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getDouble(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    RequestDelegate<BigDecimal> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBigDecimal(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    RequestDelegate<byte[]> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBytes(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    RequestDelegate<Date> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getDate(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    RequestDelegate<Time> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getTime(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    RequestDelegate<Timestamp> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getTimestamp(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    RequestDelegate<String> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getString(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    RequestDelegate<Boolean> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBoolean(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    RequestDelegate<Byte> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getByte(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    RequestDelegate<Short> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getShort(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    RequestDelegate<Integer> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getInt(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    RequestDelegate<Long> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getLong(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    RequestDelegate<Float> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getFloat(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    RequestDelegate<Double> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getDouble(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    RequestDelegate<BigDecimal> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBigDecimal(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    RequestDelegate<byte[]> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBytes(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    RequestDelegate<Date> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getDate(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    RequestDelegate<Time> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getTime(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    RequestDelegate<Timestamp> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getTimestamp(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) {
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
  public String getCursorName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    RequestDelegate<ResultSetMetaData> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(rs::getMetaData);
    }
    List<ResultSetMetaData> metaDataList = delegate.requestAll();
    return new ClusterResultSetMetaData(metaDataList, endpoints);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    RequestDelegate<Object> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getObject(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    RequestDelegate<Object> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getObject(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    RequestDelegate<Integer> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.findColumn(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public Reader getCharacterStream(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getCharacterStream(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    RequestDelegate<BigDecimal> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBigDecimal(columnIndex));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    RequestDelegate<BigDecimal> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(() -> rs.getBigDecimal(columnLabel));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isBeforeFirst() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAfterLast() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isFirst() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isLast() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void beforeFirst() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void afterLast() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean first() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean last() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean absolute(int row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean relative(int rows) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean previous() {
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
  public void setFetchSize(int rows) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFetchSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getConcurrency() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rowUpdated() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rowInserted() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rowDeleted() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNull(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateByte(int columnIndex, byte x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateShort(int columnIndex, short x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateInt(int columnIndex, int x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateLong(int columnIndex, long x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateFloat(int columnIndex, float x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDouble(int columnIndex, double x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateString(int columnIndex, String x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDate(int columnIndex, Date x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTime(int columnIndex, Time x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(int columnIndex, Object x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNull(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateByte(String columnLabel, byte x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateShort(String columnLabel, short x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateInt(String columnLabel, int x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateLong(String columnLabel, long x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateFloat(String columnLabel, float x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDouble(String columnLabel, double x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateString(String columnLabel, String x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDate(String columnLabel, Date x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTime(String columnLabel, Time x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(String columnLabel, Object x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void insertRow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteRow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void refreshRow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancelRowUpdates() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void moveToInsertRow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void moveToCurrentRow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement getStatement() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Ref getRef(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob getBlob(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob getClob(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array getArray(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Ref getRef(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob getBlob(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob getClob(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array getArray(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL getURL(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URL getURL(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRef(int columnIndex, Ref x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRef(String columnLabel, Ref x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateArray(int columnIndex, Array x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateArray(String columnLabel, Array x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RowId getRowId(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RowId getRowId(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getHoldability() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    RequestDelegate<Boolean> delegate = createLocalRequestDelegate();
    for (ResultSet rs : resultSets) {
      delegate.addRequest(rs::isClosed);
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public void updateNString(int columnIndex, String nString) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNString(String columnLabel, String nString) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob getNClob(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob getNClob(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getNString(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getNString(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) {
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

  /**
   * As each query operation in one ResultSet may send another RPCs to server, the request should be
   * executed in parallel in order to accelerate the test.
   */
  private <T> RequestDelegate<T> createRequestDelegate() {
    return new ParallelRequestDelegate<>(endpoints, queryTimeoutSeconds);
  }

  private <T> RequestDelegate<T> createLocalRequestDelegate() {
    return new SerialRequestDelegate<>(endpoints);
  }
}
