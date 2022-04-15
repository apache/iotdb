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

import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManager;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
   * Convert align by device query result of iotdb to the query result of influxdb
   *
   * @param queryDataSet iotdb query results to be converted
   * @return query results in influxdb format
   */
  public static QueryResult iotdbResultConvertInfluxResult(
      QueryDataSet queryDataSet,
      String database,
      String measurement,
      Map<String, Integer> fieldOrders)
      throws IOException {

    if (queryDataSet == null) {
      return getNullQueryResult();
    }
    // generate series
    QueryResult.Series series = new QueryResult.Series();
    series.setName(measurement);
    // gets the reverse map of the tag
    Map<String, Integer> tagOrders = InfluxDBMetaManager.getTagOrders(database, measurement);
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

    while (queryDataSet.hasNext()) {
      Object[] value = new Object[columns.size()];

      RowRecord record = queryDataSet.next();
      List<Field> fields = record.getFields();

      value[0] = record.getTimestamp();

      String deviceName = fields.get(0).getStringValue();
      String[] deviceNameList = deviceName.split("\\.");
      for (int i = 3; i < deviceNameList.length; i++) {
        if (!deviceNameList[i].equals(InfluxConstant.PLACE_HOLDER)) {
          value[i - 2] = deviceNameList[i];
        }
      }
      for (int i = 1; i < fields.size(); i++) {
        Object o = FieldUtils.iotdbFieldConvert(fields.get(i));
        if (o != null) {
          // insert the value of filed into it
          value[
                  fieldOrders.get(
                      ((AlignByDeviceDataSet) queryDataSet).getMeasurements().get(i - 1))] =
              o;
        }
      }
      // insert actual value
      values.add(Arrays.asList(value));
    }
    series.setValues(values);

    QueryResult queryResult = new QueryResult();
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(new ArrayList<>(Arrays.asList(series)));
    queryResult.setResults(new ArrayList<>(Arrays.asList(result)));

    return queryResult;
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
}
