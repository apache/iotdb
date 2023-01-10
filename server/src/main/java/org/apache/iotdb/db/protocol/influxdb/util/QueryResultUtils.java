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
package org.apache.iotdb.db.protocol.influxdb.util;

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManagerFactory;
import org.apache.iotdb.rpc.IoTDBJDBCDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryResultUtils {
  /**
   * update the new values to the query results of influxdb
   *
   * @param queryResult influxdb query results to be updated
   * @param columns columns to be updated
   * @param updateValues values to be updated
   */
  public static void updateQueryResultColumnValue(
      QueryResult queryResult, List<String> columns, List<List<Object>> updateValues) {
    List<QueryResult.Result> results = queryResult.getResults();
    QueryResult.Result result = results.get(0);
    List<QueryResult.Series> series = results.get(0).getSeries();
    QueryResult.Series serie = series.get(0);

    serie.setValues(updateValues);
    serie.setColumns(columns);
    series.set(0, serie);
    result.setSeries(series);
    results.set(0, result);
  }

  /**
   * get a null query result
   *
   * @return null queryResult
   */
  public static QueryResult getNullQueryResult() {
    QueryResult queryResult = new QueryResult();
    QueryResult.Result result = new QueryResult.Result();
    queryResult.setResults(Arrays.asList(result));
    return queryResult;
  }

  /**
   * update the new values to the query results of influxdb
   *
   * @param queryResult influxdb query results to be updated
   * @param updateValues values to be updated
   */
  private static void updateQueryResultValue(
      QueryResult queryResult, List<List<Object>> updateValues) {
    List<QueryResult.Result> results = queryResult.getResults();
    QueryResult.Result result = results.get(0);
    List<QueryResult.Series> series = results.get(0).getSeries();
    QueryResult.Series serie = series.get(0);

    serie.setValues(updateValues);
    series.set(0, serie);
    result.setSeries(series);
    results.set(0, result);
  }

  /**
   * check whether the query results of two influxdb belong to the same query, that is, whether the
   * measurement and columns are consistent
   *
   * @param queryResult1 query result1
   * @param queryResult2 query result2
   * @return belong to the same query
   */
  private static boolean checkSameQueryResult(QueryResult queryResult1, QueryResult queryResult2) {
    return queryResult1
            .getResults()
            .get(0)
            .getSeries()
            .get(0)
            .getName()
            .equals(queryResult2.getResults().get(0).getSeries().get(0).getName())
        && StringUtils.checkSameStringList(
            queryResult1.getResults().get(0).getSeries().get(0).getColumns(),
            queryResult2.getResults().get(0).getSeries().get(0).getColumns());
  }

  /**
   * union the query results of two influxdb
   *
   * @param queryResult1 query result 1
   * @param queryResult2 query result 2
   * @return union of two query results
   */
  public static QueryResult orQueryResultProcess(
      QueryResult queryResult1, QueryResult queryResult2) {
    if (checkQueryResultNull(queryResult1)) {
      return queryResult2;
    } else if (checkQueryResultNull(queryResult2)) {
      return queryResult1;
    }
    if (!checkSameQueryResult(queryResult1, queryResult2)) {
      System.out.println("QueryResult1 and QueryResult2 is not same attribute");
      return queryResult1;
    }
    List<List<Object>> values1 = queryResult1.getResults().get(0).getSeries().get(0).getValues();
    List<List<Object>> values2 = queryResult2.getResults().get(0).getSeries().get(0).getValues();
    List<List<Object>> notSameValuesInValues1 = new ArrayList<>();
    for (List<Object> value1 : values1) {
      boolean allNotEqual = true;
      for (List<Object> value2 : values2) {
        boolean notEqual = false;
        for (int t = 0; t < value1.size(); t++) {
          // if there is any inequality, skip the current J
          if (!CommonUtils.checkEqualsContainNull(value1.get(t), value2.get(t))) {
            notEqual = true;
            break;
          }
        }
        if (!notEqual) {
          allNotEqual = false;
          break;
        }
      }
      if (allNotEqual) {
        notSameValuesInValues1.add(value1);
      }
    }
    // values2 plus a different valueList
    values2.addAll(notSameValuesInValues1);
    updateQueryResultValue(queryResult1, values2);
    return queryResult1;
  }

  /**
   * take the intersection of the query results of two influxdb
   *
   * @param queryResult1 query result 1
   * @param queryResult2 query result 2
   * @return intersection of two query results
   */
  public static QueryResult andQueryResultProcess(
      QueryResult queryResult1, QueryResult queryResult2) {
    if (checkQueryResultNull(queryResult1) || checkQueryResultNull(queryResult2)) {
      return getNullQueryResult();
    }
    if (!checkSameQueryResult(queryResult1, queryResult2)) {
      System.out.println("QueryResult1 and QueryResult2 is not same attribute");
      return queryResult1;
    }
    List<List<Object>> values1 = queryResult1.getResults().get(0).getSeries().get(0).getValues();
    List<List<Object>> values2 = queryResult2.getResults().get(0).getSeries().get(0).getValues();
    List<List<Object>> sameValues = new ArrayList<>();
    for (List<Object> value1 : values1) {
      for (List<Object> value2 : values2) {
        boolean allEqual = true;
        for (int t = 0; t < value1.size(); t++) {
          // if there is any inequality, skip the current J
          if (!CommonUtils.checkEqualsContainNull(value1.get(t), value2.get(t))) {
            allEqual = false;
            break;
          }
        }
        // at this time, the matching is completed. If it is completely equal
        if (allEqual) {
          sameValues.add(value1);
        }
      }
    }
    updateQueryResultValue(queryResult1, sameValues);
    return queryResult1;
  }

  /**
   * checks whether query result is null in the specified way
   *
   * @param queryResult query result to be checked
   * @return is null
   */
  public static boolean checkQueryResultNull(QueryResult queryResult) {
    return queryResult.getResults().get(0).getSeries() == null;
  }

  /**
   * parse time series paths from query results
   *
   * @param tsExecuteStatementResp query results
   * @return time series paths
   */
  public static List<String> getFullPaths(TSExecuteStatementResp tsExecuteStatementResp) {
    List<String> res = new ArrayList<>();
    IoTDBJDBCDataSet ioTDBJDBCDataSet = creatIoTJDBCDataset(tsExecuteStatementResp);
    try {
      while (ioTDBJDBCDataSet.hasCachedResults()) {
        ioTDBJDBCDataSet.constructOneRow();
        String path = ioTDBJDBCDataSet.getValueByName(ColumnHeaderConstant.TIMESERIES);
        res.add(path);
      }
    } catch (StatementExecutionException e) {
      throw new InfluxDBException(e.getMessage());
    }
    return res;
  }

  /**
   * Convert align by device query result of NewIoTDB to the query result of influxdb,used for
   * Memory and schema_file schema region
   *
   * @param tsExecuteStatementResp NewIoTDB execute statement resp to be converted
   * @return query results in influxdb format
   */
  public static QueryResult iotdbResultConvertInfluxResult(
      TSExecuteStatementResp tsExecuteStatementResp,
      String database,
      String measurement,
      Map<String, Integer> fieldOrders) {
    if (tsExecuteStatementResp == null) {
      return getNullQueryResult();
    }
    // generate series
    QueryResult.Series series = new QueryResult.Series();
    series.setName(measurement);
    // gets the reverse map of the tag
    Map<String, Integer> tagOrders =
        InfluxDBMetaManagerFactory.getInstance().getTagOrders(database, measurement, -1);
    Map<Integer, String> tagOrderReversed =
        tagOrders.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    Map<Integer, String> fieldOrdersReversed =
        fieldOrders.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    int tagSize = tagOrderReversed.size();
    ArrayList<String> tagList = new ArrayList<>();
    for (int i = 1; i <= tagSize; i++) {
      tagList.add(tagOrderReversed.get(i));
    }

    ArrayList<String> fieldList = new ArrayList<>();
    for (int i = 1 + tagSize; i < 1 + tagSize + fieldOrders.size(); i++) {
      fieldList.add(fieldOrdersReversed.get(i));
    }
    ArrayList<String> columns = new ArrayList<>();
    columns.add("time");
    columns.addAll(tagList);
    columns.addAll(fieldList);
    // insert columns into series
    series.setColumns(columns);
    List<List<Object>> values = new ArrayList<>();
    IoTDBJDBCDataSet ioTDBJDBCDataSet = creatIoTJDBCDataset(tsExecuteStatementResp);
    try {
      while (ioTDBJDBCDataSet.hasCachedResults()) {
        Object[] value = new Object[columns.size()];
        ioTDBJDBCDataSet.constructOneRow();
        value[0] = Long.valueOf(ioTDBJDBCDataSet.getValueByName(ColumnHeaderConstant.TIME));
        String deviceName = ioTDBJDBCDataSet.getValueByName(ColumnHeaderConstant.DEVICE);
        String[] deviceNameList = deviceName.split("\\.");
        for (int i = 3; i < deviceNameList.length; i++) {
          if (!deviceNameList[i].equals(InfluxConstant.PLACE_HOLDER)) {
            value[i - 2] = deviceNameList[i];
          }
        }
        for (int i = 3; i <= ioTDBJDBCDataSet.columnNameList.size(); i++) {
          Object o = ioTDBJDBCDataSet.getObject(ioTDBJDBCDataSet.findColumnNameByIndex(i));
          if (o != null) {
            // insert the value of filed into it
            value[fieldOrders.get(ioTDBJDBCDataSet.findColumnNameByIndex(i))] = o;
          }
        }
        values.add(Arrays.asList(value));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    series.setValues(values);

    QueryResult queryResult = new QueryResult();
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(new ArrayList<>(Arrays.asList(series)));
    queryResult.setResults(new ArrayList<>(Arrays.asList(result)));

    return queryResult;
  }

  /**
   * Convert align by device query result of NewIoTDB to the query result of influxdb,used for tag
   * schema region
   *
   * @param tsExecuteStatementResp NewIoTDB execute statement resp to be converted
   * @return query results in influxdb format
   */
  public static QueryResult iotdbResultConvertInfluxResult(
      TSExecuteStatementResp tsExecuteStatementResp,
      String database,
      String measurement,
      Map<String, Integer> tagOrders,
      Map<String, Integer> fieldOrders) {
    if (tsExecuteStatementResp == null) {
      return getNullQueryResult();
    }
    // generate series
    QueryResult.Series series = new QueryResult.Series();
    series.setName(measurement);
    Map<Integer, String> tagOrderReversed =
        tagOrders.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    int tagSize = tagOrderReversed.size();
    Map<Integer, String> fieldOrdersReversed =
        fieldOrders.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    ArrayList<String> tagList = new ArrayList<>();
    for (int i = 0; i < tagSize; i++) {
      tagList.add(tagOrderReversed.get(i));
    }

    ArrayList<String> fieldList = new ArrayList<>();
    for (int i = 0; i < fieldOrders.size(); i++) {
      fieldList.add(fieldOrdersReversed.get(i));
    }

    ArrayList<String> columns = new ArrayList<>();
    columns.add("time");
    columns.addAll(tagList);
    columns.addAll(fieldList);
    // insert columns into series
    series.setColumns(columns);
    List<List<Object>> values = new ArrayList<>();
    IoTDBJDBCDataSet ioTDBJDBCDataSet = creatIoTJDBCDataset(tsExecuteStatementResp);
    try {
      while (ioTDBJDBCDataSet.hasCachedResults()) {
        Object[] value = new Object[columns.size()];
        ioTDBJDBCDataSet.constructOneRow();
        value[0] = Long.valueOf(ioTDBJDBCDataSet.getValueByName(ColumnHeaderConstant.TIME));
        String deviceName = ioTDBJDBCDataSet.getValueByName(ColumnHeaderConstant.DEVICE);
        String[] deviceNameList = deviceName.split("\\.");
        for (int i = 2; i < deviceNameList.length; i += 2) {
          if (tagOrders.containsKey(deviceNameList[i])) {
            int position = tagOrders.get(deviceNameList[i]) + 1;
            value[position] = deviceNameList[i + 1];
          }
        }
        for (int i = 3; i <= ioTDBJDBCDataSet.columnNameList.size(); i++) {
          Object o = ioTDBJDBCDataSet.getObject(ioTDBJDBCDataSet.findColumnNameByIndex(i));
          if (o != null) {
            // insert the value of filed into it
            int position = fieldOrders.get(ioTDBJDBCDataSet.findColumnNameByIndex(i)) + tagSize + 1;
            value[position] = o;
          }
        }
        values.add(Arrays.asList(value));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    series.setValues(values);

    QueryResult queryResult = new QueryResult();
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(new ArrayList<>(Arrays.asList(series)));
    queryResult.setResults(new ArrayList<>(Arrays.asList(result)));

    return queryResult;
  }

  public static List<InfluxFunctionValue> getInfluxFunctionValues(
      TSExecuteStatementResp tsExecuteStatementResp) {
    IoTDBJDBCDataSet ioTDBJDBCDataSet = creatIoTJDBCDataset(tsExecuteStatementResp);
    List<InfluxFunctionValue> result = new ArrayList<>(ioTDBJDBCDataSet.columnSize);
    try {
      while (ioTDBJDBCDataSet.hasCachedResults()) {
        ioTDBJDBCDataSet.constructOneRow();
        Long timestamp = null;
        for (String columnName : ioTDBJDBCDataSet.columnNameList) {
          if ("Time".equals(columnName)) {
            timestamp = ioTDBJDBCDataSet.getTimestamp(columnName).getTime();
            continue;
          }
          Object o = ioTDBJDBCDataSet.getObject(columnName);
          result.add(new InfluxFunctionValue(o, timestamp));
        }
      }
    } catch (StatementExecutionException e) {
      throw new InfluxDBException(e.getMessage());
    }
    return result;
  }

  public static Map<String, Object> getColumnNameAndValue(
      TSExecuteStatementResp tsExecuteStatementResp) {
    IoTDBJDBCDataSet ioTDBJDBCDataSet = creatIoTJDBCDataset(tsExecuteStatementResp);
    Map<String, Object> result = new HashMap<>();
    try {
      while (ioTDBJDBCDataSet.hasCachedResults()) {
        ioTDBJDBCDataSet.constructOneRow();
        for (String columnName : ioTDBJDBCDataSet.columnNameList) {
          Object o = ioTDBJDBCDataSet.getObject(columnName);
          result.put(columnName, o);
        }
      }
    } catch (StatementExecutionException e) {
      throw new InfluxDBException(e.getMessage());
    }
    return result;
  }

  public static IoTDBJDBCDataSet creatIoTJDBCDataset(
      TSExecuteStatementResp tsExecuteStatementResp) {
    return new IoTDBJDBCDataSet(
        null,
        tsExecuteStatementResp.getColumns(),
        tsExecuteStatementResp.getDataTypeList(),
        tsExecuteStatementResp.columnNameIndexMap,
        tsExecuteStatementResp.ignoreTimeStamp,
        tsExecuteStatementResp.queryId,
        0,
        null,
        0,
        tsExecuteStatementResp.queryDataSet,
        0,
        0,
        tsExecuteStatementResp.sgColumns,
        null);
  }
}
