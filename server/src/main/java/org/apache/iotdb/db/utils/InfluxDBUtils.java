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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunctionFactory;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxQueryOperator;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxSelectComponent;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSQueryResult;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSQueryRsp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSSeries;
import org.apache.iotdb.tsfile.read.common.Field;

import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InfluxDBUtils {
  public static void checkInfluxDBQueryOperator(Operator operator) {
    if (!(operator instanceof InfluxQueryOperator)) {
      throw new IllegalArgumentException("not query sql");
    }
    InfluxSelectComponent selectComponent = ((InfluxQueryOperator) operator).getSelectComponent();
    if (selectComponent.isHasMoreSelectorFunction() && selectComponent.isHasCommonQuery()) {
      throw new IllegalArgumentException(
          "ERR: mixing multiple selector functions with tags or fields is not supported");
    }
    if (selectComponent.isHasAggregationFunction() && selectComponent.isHasCommonQuery()) {
      throw new IllegalArgumentException(
          "ERR: mixing aggregate and non-aggregate queries is not supported");
    }
  }

  /**
   * get the last node through the path in iotdb
   *
   * @param path path to process
   * @return last node
   */
  public static String getFieldByPath(String path) {
    String[] tmpList = path.split("\\.");
    return tmpList[tmpList.length - 1];
  }

  public static QueryResult queryExpr(FilterOperator filterOperator) {
    return null;
  }

  public static void ProcessSelectComponent(
      QueryResult queryResult, InfluxSelectComponent selectComponent) {}

  /**
   * Query the select result. By default, there are no filter conditions. The functions to be
   * queried use the built-in iotdb functions
   *
   * @param selectComponent select data to query
   * @return select query result
   */
  public static QueryResult queryFuncWithoutFilter(
      InfluxSelectComponent selectComponent,
      String database,
      String measurement,
      ServiceProvider serviceProvider) {
    // columns
    List<String> columns = new ArrayList<>();
    columns.add(InfluxSQLConstant.RESERVED_TIME);

    List<InfluxDBFunction> functions = new ArrayList<>();
    String path = "root." + database + "." + measurement;
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (expression instanceof FunctionExpression) {
        String functionName = ((FunctionExpression) expression).getFunctionName();
        functions.add(
            InfluxDBFunctionFactory.generateFunctionBySession(
                functionName, expression.getExpressions(), path, serviceProvider));
        columns.add(functionName);
      }
    }

    List<Object> value = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (InfluxDBFunction function : functions) {
      InfluxDBFunctionValue functionValue = function.calculateByIoTDBFunc();
      if (value.size() == 0) {
        value.add(functionValue.getTimestamp());
      } else {
        value.set(0, functionValue.getTimestamp());
      }
      value.add(functionValue.getValue());
    }
    if (selectComponent.isHasAggregationFunction() || selectComponent.isHasMoreFunction()) {
      value.set(0, 0);
    }
    values.add(value);

    // generate series
    QueryResult queryResult = new QueryResult();
    QueryResult.Series series = new QueryResult.Series();
    series.setColumns(columns);
    series.setValues(values);
    series.setName(measurement);
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(new ArrayList<>(Arrays.asList(series)));
    queryResult.setResults(new ArrayList<>(Arrays.asList(result)));
    return queryResult;
  }

  public static QueryResult convertTSQueryResult(TSQueryRsp tsQueryRsp) {
    QueryResult queryResult = new QueryResult();
    List<QueryResult.Result> results = new ArrayList<>();
    List<QueryResult.Series> serieList = new ArrayList<>();
    for (TSQueryResult tsQueryResult : tsQueryRsp.results) {
      QueryResult.Result result = new QueryResult.Result();
      for (TSSeries tsSeries : tsQueryResult.series) {
        QueryResult.Series series = new QueryResult.Series();
        // TODO buffer to object
        //        series.setValues(tsSeries.values);
        series.setColumns(tsSeries.columns);
        series.setName(tsSeries.name);
        series.setTags(tsSeries.tags);
        serieList.add(series);
      }
      result.setSeries(serieList);
      result.setError(tsQueryResult.error);
      results.add(result);
    }
    queryResult.setResults(results);
    queryResult.setError(tsQueryRsp.error);
    return queryResult;
  }

  public static String generateFunctionSql(String first_value, String parmaName, String path) {
    return null;
  }

  /**
   * convert the value of field in iotdb to object
   *
   * @param field filed to be converted
   * @return value stored in field
   */
  public static Object iotdbFiledConvert(Field field) {
    if (field.getDataType() == null) {
      return null;
    }
    switch (field.getDataType()) {
      case TEXT:
        return field.getStringValue();
      case INT64:
        return field.getLongV();
      case INT32:
        return field.getIntV();
      case DOUBLE:
        return field.getDoubleV();
      case FLOAT:
        return field.getFloatV();
      case BOOLEAN:
        return field.getBoolV();
      default:
        return null;
    }
  }

  public static String parserFunctionPath(String path) {
    String splitStrings = path.split("[(]", 1)[1];
    return splitStrings.substring(0, splitStrings.length() - 1);
  }
}
