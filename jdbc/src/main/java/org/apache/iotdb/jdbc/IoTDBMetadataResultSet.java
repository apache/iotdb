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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;

public class IoTDBMetadataResultSet extends IoTDBQueryResultSet {

  private static final String GET_STRING_COLUMN = "COLUMN";
  private static final String GET_STRING_STORAGE_GROUP = "STORAGE_GROUP";
  private static final String GET_STRING_TIMESERIES_NUM = "TIMESERIES_NUM";
  private static final String GET_STRING_NODES_NUM = "NODE_NUM";
  private static final String GET_STRING_NODE_PATH = "NODE_PATH";
  private static final String GET_STRING_NODE_TIMESERIES_NUM = "NODE_TIMESERIES_NUM";
  private static final String GET_STRING_TIMESERIES_NAME = "Timeseries";
  private static final String GET_STRING_DEVICES = "DEVICES";
  private static final String GET_STRING_TIMESERIES_STORAGE_GROUP = "Storage Group";

  public static final String GET_STRING_TIMESERIES_DATATYPE = "DataType";
  private static final String GET_STRING_TIMESERIES_ENCODING = "Encoding";
  private Iterator<?> columnItr;
  private MetadataType type;
  private String currentColumn;
  private String currentStorageGroup;
  private String currentDevice;
  private String currentVersion;
  private List<String> currentTimeseries;
  private List<String> timeseriesNumList;
  private List<String> nodesNumList;
  private Map<String, String> nodeTimeseriesNumMap;
  private String timeseriesNum;
  private String nodesNum;
  private String currentNode;
  private String currentNodeTimeseriesNum;
  // for display
  private int colCount; // the number of columns for show
  private String[] showLabels; // headers for show

  private static final String METHOD_NOT_SUPPORTED = "Method not supported";

  /**
   * Constructor used for the result of DatabaseMetadata.getColumns()
   */
  IoTDBMetadataResultSet(Object object, MetadataType type) throws SQLException {
    this.type = type;
    switch (type) {
      case COLUMN:
        List<String> columns = (List<String>) object;
        colCount = 1;
        showLabels = new String[]{"column"};
        columnItr = columns.iterator();
        break;
      case STORAGE_GROUP:
        Set<String> storageGroupSet = (Set<String>) object;
        colCount = 1;
        showLabels = new String[]{"Storage Group"};
        columnItr = storageGroupSet.iterator();
        break;
      case DEVICES:
        Set<String> devicesSet = (Set<String>) object;
        colCount = 1;
        showLabels = new String[]{"Device"};
        columnItr = devicesSet.iterator();
        break;
      case TIMESERIES:
        List<List<String>> showTimeseriesList = (List<List<String>>) object;
        colCount = 4;
        showLabels = new String[]{GET_STRING_TIMESERIES_NAME, GET_STRING_TIMESERIES_STORAGE_GROUP,
          GET_STRING_TIMESERIES_DATATYPE, GET_STRING_TIMESERIES_ENCODING};
        columnItr = showTimeseriesList.iterator();
        break;
      case COUNT_TIMESERIES:
        String tsNum = object.toString();
        timeseriesNumList = new ArrayList<>();
        timeseriesNumList.add(tsNum);
        colCount = 1;
        showLabels = new String[]{"count"};
        columnItr = timeseriesNumList.iterator();
        break;
      case COUNT_NODES:
        String ndNum = object.toString();
        nodesNumList = new ArrayList<>();
        nodesNumList.add(ndNum);
        colCount = 1;
        showLabels = new String[]{"count"};
        columnItr = nodesNumList.iterator();
        break;
      case COUNT_NODE_TIMESERIES:
        nodeTimeseriesNumMap = (Map<String, String>) object;
        colCount = 2;
        showLabels = new String[]{"column", "count"};
        columnItr = nodeTimeseriesNumMap.entrySet().iterator();
        break;
      case VERSION:
        String version = (String) object;
        colCount = 1;
        showLabels = new String[]{version};
        Set<String> versionSet = new HashSet<>();
        versionSet.add(version);
        columnItr = versionSet.iterator();
        break;
      default:
        throw new SQLException("TsfileMetadataResultSet constructor is wrongly used.");
    }
  }

  @Override
  public int findColumn(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean getBoolean(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte getByte(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte[] getBytes(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public Date getDate(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public double getDouble(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public float getFloat(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return new IoTDBMetadataResultMetadata(showLabels);
  }

  @Override
  public boolean next() {
    boolean hasNext = columnItr.hasNext();
    if (hasNext) {
      switch (type) {
        case STORAGE_GROUP:
          currentStorageGroup = (String) columnItr.next();
          break;
        case TIMESERIES:
          currentTimeseries = (List<String>) columnItr.next();
          break;
        case COLUMN:
          currentColumn = (String) columnItr.next();
          break;
        case DEVICES:
          currentDevice = (String) columnItr.next();
          break;
        case COUNT_TIMESERIES:
          timeseriesNum = (String) columnItr.next();
          break;
        case COUNT_NODES:
          nodesNum = (String) columnItr.next();
          break;
        case COUNT_NODE_TIMESERIES:
          Map.Entry pair = (Map.Entry) columnItr.next();
          currentNode = (String) pair.getKey();
          currentNodeTimeseriesNum = (String) pair.getValue();
          break;
        case VERSION:
          currentVersion = (String) columnItr.next();
          hasNext = false;
          break;
        default:
          break;
      }
    }
    return hasNext;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public Object getObject(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public short getShort(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public Statement getStatement() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    switch (type) {
      case STORAGE_GROUP:
        if (columnIndex == 1) {
          return getString(GET_STRING_STORAGE_GROUP);
        }
        break;
      case TIMESERIES:
        if (columnIndex >= 1 && columnIndex <= colCount) {
          return getString(showLabels[columnIndex - 1]);
        }
        break;
      case COLUMN:
        if (columnIndex == 1) {
          return getString(GET_STRING_COLUMN);
        }
        break;
      case DEVICES:
        if (columnIndex == 1) {
          return getString(GET_STRING_DEVICES);
        }
      case COUNT_TIMESERIES:
        if (columnIndex == 1) {
          return getString(GET_STRING_TIMESERIES_NUM);
        }
        break;
      case COUNT_NODES:
        if (columnIndex == 1) {
          return getString(GET_STRING_NODES_NUM);
        }
      case COUNT_NODE_TIMESERIES:
        return columnIndex == 1 ? getString(GET_STRING_NODE_PATH) : getString(GET_STRING_NODE_TIMESERIES_NUM);
      default:
        break;
    }
    throw new SQLException(String.format("select column index %d does not exists", columnIndex));
  }

  @Override
  public String getString(String columnName) {
    // use special key word to judge return content
    switch (columnName) {
      case GET_STRING_STORAGE_GROUP:
        return currentStorageGroup;
      case GET_STRING_TIMESERIES_NAME:
        return currentTimeseries.get(0);
      case GET_STRING_TIMESERIES_STORAGE_GROUP:
        return currentTimeseries.get(1);
      case GET_STRING_TIMESERIES_DATATYPE:
        return currentTimeseries.get(2);
      case GET_STRING_TIMESERIES_ENCODING:
        return currentTimeseries.get(3);
      case GET_STRING_COLUMN:
        return currentColumn;
      case GET_STRING_DEVICES:
        return currentDevice;
      case GET_STRING_TIMESERIES_NUM:
        return timeseriesNum;
      case GET_STRING_NODES_NUM:
        return nodesNum;
      case GET_STRING_NODE_PATH:
        return currentNode;
      case GET_STRING_NODE_TIMESERIES_NUM:
        return currentNodeTimeseriesNum;
      default:
        break;
    }
    return null;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public Time getTime(String columnName) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getType() {
    return type.ordinal();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isClosed() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean wasNull() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED);
  }

  public enum MetadataType {
    STORAGE_GROUP, TIMESERIES, COLUMN, DEVICES, COUNT_TIMESERIES, COUNT_NODES, COUNT_NODE_TIMESERIES, VERSION
  }
}
