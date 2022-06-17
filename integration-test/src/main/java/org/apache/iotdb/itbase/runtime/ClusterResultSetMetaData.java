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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/** The implementation of {@link ResultSetMetaData} in cluster test. */
public class ClusterResultSetMetaData implements ResultSetMetaData {

  private final List<ResultSetMetaData> metaDataList;
  private final List<String> endpoints;

  public ClusterResultSetMetaData(List<ResultSetMetaData> metadataList, List<String> endpoints) {
    this.metaDataList = metadataList;
    this.endpoints = endpoints;
  }

  @Override
  public int getColumnCount() throws SQLException {
    RequestDelegate<Integer> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(rs::getColumnCount);
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isAutoIncrement(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isCaseSensitive(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isSearchable(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isCurrency(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int isNullable(int column) throws SQLException {
    RequestDelegate<Integer> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isNullable(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isSigned(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    RequestDelegate<Integer> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getColumnDisplaySize(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    RequestDelegate<String> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getColumnLabel(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    RequestDelegate<String> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getColumnName(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    RequestDelegate<String> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getSchemaName(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    RequestDelegate<Integer> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getPrecision(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int getScale(int column) throws SQLException {
    RequestDelegate<Integer> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getScale(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getTableName(int column) throws SQLException {
    RequestDelegate<String> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getTableName(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    RequestDelegate<String> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getCatalogName(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    RequestDelegate<Integer> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getColumnType(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    RequestDelegate<String> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getColumnTypeName(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isReadOnly(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isWritable(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    RequestDelegate<Boolean> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.isDefinitelyWritable(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    RequestDelegate<String> delegate = createRequestDelegate();
    for (ResultSetMetaData rs : metaDataList) {
      delegate.addRequest(() -> rs.getColumnClassName(column));
    }
    return delegate.requestAllAndCompare();
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    throw new UnsupportedOperationException();
  }

  /** As all the ResultSetMetaData are on local, there's no need to request in parallel */
  private <T> RequestDelegate<T> createRequestDelegate() {
    return new SerialRequestDelegate<>(endpoints);
  }
}
