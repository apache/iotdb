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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class IoTDBResultMetadata implements ResultSetMetaData {

  private List<String> columnInfoList;
  private List<String> columnTypeList;
  private boolean ignoreTimestamp;
  private List<String> sgColumns;
  private String operationType = "";
  private boolean nonAlign = false;

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

  @Override
  public String getCatalogName(int column) throws SQLException {
    String system_schmea = "_system_schmea";
    String system = "_system";
    String system_user = "_system_user";
    String system_role = "_system_role";
    String system_auths = "_system_auths";
    String system_database = "_system_database";
    String system_null = "";
    String columnName = columnInfoList.get(column - 1);
    List<String> listColumns = columnInfoList;
    if (column < 1 || column > columnInfoList.size()) {
      throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }
    if (operationType.equals("SHOW")) {
      if (listColumns.get(0).equals("count")) {
        return system_database;
      } else if (listColumns.get(0).equals("storage group")
          && listColumns.size() > 1
          && listColumns.get(1).equals("ttl")) {
        return "";
      } else if (listColumns.get(0).trim().equals("version") && listColumns.size() == 1) {
        return system;
      } else if (listColumns.get(0).equals("storage group")
          || listColumns.get(0).equals("devices")
          || listColumns.get(0).equals("child paths")
          || listColumns.get(0).equals("child nodes")
          || listColumns.get(0).equals("timeseries")) {
        return system_schmea;
      }
    } else if (operationType.equals("LIST_USER")) {
      return system_user;
    } else if (operationType.equals("LIST_ROLE")) {
      return system_role;
    } else if (operationType.equals("LIST_USER_PRIVILEGE")) {
      return system_auths;
    } else if (operationType.equals("LIST_ROLE_PRIVILEGE")) {
      return system_auths;
    } else if (operationType.equals("LIST_USER_ROLES")) {
      return system_role;
    } else if (operationType.equals("LIST_ROLE_USERS")) {
      return system_user;
    } else if (operationType.equals("QUERY")) {
      if ((columnName.toLowerCase().equals("time") && columnInfoList.size() != 2)
          || columnName.toLowerCase().equals("timeseries")
          || columnName.toLowerCase().equals("device")) {
        return system_null;
      } else if (columnInfoList.size() >= 2
          && columnInfoList.get(0).toLowerCase().equals("time")
          && columnInfoList.get(1).toLowerCase().equals("device")) {
        return system_null;
      }
    } else if (!operationType.equals("FILL")) {
      return system_null;
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
      case "TIMESTAMP":
      case "INT64":
        return Long.class.getName();
      case "BOOLEAN":
        return Boolean.class.getName();
      case "INT32":
        return Integer.class.getName();
      case "FLOAT":
        return Float.class.getName();
      case "DOUBLE":
        return Double.class.getName();
      case "TEXT":
        return String.class.getName();
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
    if (operationType.equals("QUERY") && nonAlign) {
      if (column % 2 == 1
          && columnInfoList.get(column - 1).length() >= 4
          && columnInfoList.get(column - 1).substring(0, 4).equals("Time")) {
        return "Time";
      } else {
        return columnInfoList.get(column - 1);
      }
    } else {
      return columnInfoList.get(column - 1);
    }
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
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT,
    String columnType = columnTypeList.get(column - 1);

    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
        return Types.BOOLEAN;
      case "INT32":
        return Types.INTEGER;
      case "INT64":
        return Types.BIGINT;
      case "FLOAT":
        return Types.FLOAT;
      case "DOUBLE":
        return Types.DOUBLE;
      case "TEXT":
        return Types.VARCHAR;
      default:
        break;
    }
    return 0;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && !ignoreTimestamp) {
      return "TIME";
    }
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT,
    String columnType;
    if (!ignoreTimestamp) {
      columnType = columnTypeList.get(column - 2);
    } else {
      columnType = columnTypeList.get(column - 1);
    }
    String typeString = columnType.toUpperCase();
    if (typeString.equals("BOOLEAN")
        || typeString.equals("INT32")
        || typeString.equals("INT64")
        || typeString.equals("FLOAT")
        || typeString.equals("DOUBLE")
        || typeString.equals("TEXT")) {
      return typeString;
    }
    return null;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && !ignoreTimestamp) {
      return 13;
    }
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT,
    String columnType;
    if (!ignoreTimestamp) {
      columnType = columnTypeList.get(column - 2);
    } else {
      columnType = columnTypeList.get(column - 1);
    }
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
        return 1;
      case "INT32":
        return 10;
      case "INT64":
        return 19;
      case "FLOAT":
        return 38;
      case "DOUBLE":
        return 308;
      case "TEXT":
        return Integer.MAX_VALUE;
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
    String columnType;
    if (!ignoreTimestamp) {
      columnType = columnTypeList.get(column - 2);
    } else {
      columnType = columnTypeList.get(column - 1);
    }
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
      case "INT32":
      case "INT64":
      case "TEXT":
        return 0;
      case "FLOAT":
        return 6;
      case "DOUBLE":
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
    String columName;
    if (!ignoreTimestamp) {
      columName = columnInfoList.get(column - 2);
    } else {
      columName = columnInfoList.get(column - 1);
    }
    return columName;
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
