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

import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

public class IoTDBResultMetadata implements ResultSetMetaData {

  private List<String> columnInfoList;
  private List<String> columnTypeList;
  private boolean ignoreTimestamp;
  private List<String> sgColumns;
  private String operationType = "";
  private boolean nonAlign = false;

  private static final String INT64 = "INT64";
  private static final String BOOLEAN = "BOOLEAN";
  private static final String INT32 = "INT32";
  private static final String FLOAT = "FLOAT";
  private static final String DOUBLE = "DOUBLE";

  private static final String TEXT = "TEXT";

  private static final String STRING = "STRING";

  private static final String BLOB = "BLOB";

  private static final String DATE = "DATE";
  private static final String TIMESTAMP = "TIMESTAMP";

  /** Constructor of IoTDBResultMetadata. */
  public IoTDBResultMetadata(
      Boolean nonAlign,
      List<String> sgColumns,
      String operationType,
      List<String> columnInfoList,
      List<String> columnTypeList,
      boolean ignoreTimestamp) {
    this.sgColumns = sgColumns;
    this.operationType = operationType;
    this.columnInfoList = columnInfoList;
    this.columnTypeList = columnTypeList;
    this.ignoreTimestamp = ignoreTimestamp;
    this.nonAlign = nonAlign;
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @SuppressWarnings({
    "squid:S3776"
  }) // ignore Cognitive Complexity of methods should not be too high
  @Override
  public String getCatalogName(int column) throws SQLException {
    String systemSchmea = "_system_schmea";
    String system = "_system";
    String systemUser = "_system_user";
    String systemRole = "_system_role";
    String systemAuths = "_system_auths";
    String systemDatabase = "_system_database";
    String systemNull = "";
    String columnName = columnInfoList.get(column - 1);
    List<String> listColumns = columnInfoList;
    if (column < 1 || column > columnInfoList.size()) {
      throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }
    if ("SHOW".equals(operationType)) {
      if ("count".equals(listColumns.get(0))) {
        return systemDatabase;
      } else if ("database".equals(listColumns.get(0))
          && listColumns.size() > 1
          && "ttl".equals(listColumns.get(1))) {
        return "";
      } else if ("version".equals(listColumns.get(0).trim()) && listColumns.size() == 1) {
        return system;
      } else if ("database".equals(listColumns.get(0))
          || "devices".equals(listColumns.get(0))
          || "child paths".equals(listColumns.get(0))
          || "child nodes".equals(listColumns.get(0))
          || "timeseries".equals(listColumns.get(0))) {
        return systemSchmea;
      }
    } else if ("LIST_USER".equals(operationType)) {
      return systemUser;
    } else if ("LIST_ROLE".equals(operationType)) {
      return systemRole;
    } else if ("LIST_USER_PRIVILEGE".equals(operationType)) {
      return systemAuths;
    } else if ("LIST_ROLE_PRIVILEGE".equals(operationType)) {
      return systemAuths;
    } else if ("LIST_USER_ROLES".equals(operationType)) {
      return systemRole;
    } else if ("LIST_ROLE_USERS".equals(operationType)) {
      return systemUser;
    } else if ("QUERY".equals(operationType)) {
      if ((("time".equalsIgnoreCase(columnName) && columnInfoList.size() != 2)
              || "timeseries".equalsIgnoreCase(columnName)
              || "device".equalsIgnoreCase(columnName))
          || (columnInfoList.size() >= 2
              && "time".equalsIgnoreCase(columnInfoList.get(0))
              && "device".equalsIgnoreCase(columnInfoList.get(1)))) {
        return systemNull;
      }
    } else if (!"FILL".equals(operationType)) {
      return systemNull;
    }
    if (nonAlign) {
      return sgColumns.get(column - 1);
    } else {
      return sgColumns.get(column - 2);
    }
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    String columnTypeName = getColumnTypeName(column);
    switch (columnTypeName) {
      case TIMESTAMP:
        return Timestamp.class.getName();
      case INT64:
        return Long.class.getName();
      case BOOLEAN:
        return Boolean.class.getName();
      case INT32:
        return Integer.class.getName();
      case FLOAT:
        return Float.class.getName();
      case DOUBLE:
        return Double.class.getName();
      case TEXT:
      case STRING:
        return String.class.getName();
      case DATE:
        return Date.class.getName();
      case BLOB:
        return "byte[]";
      default:
        break;
    }
    return null;
  }

  @Override
  public int getColumnCount() {
    return columnInfoList == null ? 0 : columnInfoList.size();
  }

  @Override
  public int getColumnDisplaySize(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    checkColumnIndex(column);
    return columnInfoList.get(column - 1);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  private void checkColumnIndex(int column) throws SQLException {
    if (columnInfoList == null || columnInfoList.isEmpty()) {
      throw new SQLException("No column exists");
    }
    if (column > columnInfoList.size()) {
      throw new SQLException(String.format("column %d does not exist", column));
    }
    if (column <= 0) {
      throw new SQLException("column index should start from 1");
    }
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && !ignoreTimestamp) {
      return Types.TIMESTAMP;
    }
    String columnType = columnTypeList.get(column - 1);

    switch (columnType.toUpperCase()) {
      case BOOLEAN:
        return Types.BOOLEAN;
      case INT32:
        return Types.INTEGER;
      case INT64:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case TEXT:
      case STRING:
        return Types.VARCHAR;
      case DATE:
        return Types.DATE;
      case TIMESTAMP:
        return Types.TIMESTAMP;
      case BLOB:
        return Types.BLOB;
      default:
        break;
    }
    return 0;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && !ignoreTimestamp) {
      return TIMESTAMP;
    }
    String columnType = columnTypeList.get(column - 1);
    String typeString = columnType.toUpperCase();
    if (BOOLEAN.equals(typeString)
        || INT32.equals(typeString)
        || INT64.equals(typeString)
        || FLOAT.equals(typeString)
        || DOUBLE.equals(typeString)
        || TEXT.equals(typeString)
        || STRING.equals(typeString)
        || BLOB.equals(typeString)
        || DATE.equals(typeString)
        || TIMESTAMP.equals(typeString)) {
      return typeString;
    }
    return null;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && !ignoreTimestamp) {
      return 3;
    }
    String columnType = columnTypeList.get(column - 1);
    switch (columnType.toUpperCase()) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 10;
      case INT64:
        return 19;
      case FLOAT:
        return 38;
      case DOUBLE:
        return 308;
      case TEXT:
      case BLOB:
      case STRING:
        return Integer.MAX_VALUE;
      case TIMESTAMP:
        return 3;
      case DATE:
        return 0;
      default:
        break;
    }
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && !ignoreTimestamp) {
      return 0;
    }
    String columnType = columnTypeList.get(column - 1);
    switch (columnType.toUpperCase()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case TEXT:
      case BLOB:
      case STRING:
      case TIMESTAMP:
      case DATE:
        return 0;
      case FLOAT:
        return 6;
      case DOUBLE:
        return 15;
      default:
        break;
    }
    return 0;
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    checkColumnIndex(column);
    return getCatalogName(column);
  }

  @Override
  public String getTableName(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && !ignoreTimestamp) {
      return "TIME";
    }
    // Temporarily use column names as table names
    return columnInfoList.get(column - 1);
  }

  @Override
  public boolean isAutoIncrement(int arg0) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int arg0) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int arg0) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int arg0) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int arg0) throws SQLException {
    return 1;
  }

  @Override
  public boolean isReadOnly(int arg0) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int arg0) throws SQLException {
    return true;
  }

  @Override
  public boolean isSigned(int arg0) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int arg0) throws SQLException {
    return false;
  }
}
