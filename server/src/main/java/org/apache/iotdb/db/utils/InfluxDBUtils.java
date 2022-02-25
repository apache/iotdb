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
import org.apache.iotdb.db.protocol.influxdb.expression.InfluxResultColumn;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.InfluxFunctionExpression;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunctionFactory;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxDBFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxQueryOperator;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxSelectComponent;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSQueryRsp;

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

  public static TSQueryRsp queryExpr(FilterOperator filterOperator) {
    return null;
  }

  public static void ProcessSelectComponent(
      TSQueryRsp tsQueryRsp, InfluxSelectComponent selectComponent) {}

  /**
   * Query the select result. By default, there are no filter conditions. The functions to be
   * queried use the built-in iotdb functions
   *
   * @param selectComponent select data to query
   * @return select query result
   */
  public static TSQueryRsp queryFuncWithoutFilter(
      InfluxSelectComponent selectComponent, String database, String measurement) {
    // columns
    List<String> columns = new ArrayList<>();
    columns.add(InfluxSQLConstant.RESERVED_TIME);

    List<InfluxDBFunction> functions = new ArrayList<>();
    String path = "root." + database + "." + measurement;
    for (InfluxResultColumn resultColumn : selectComponent.getInfluxResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (expression instanceof InfluxFunctionExpression) {
        String functionName = ((FunctionExpression) expression).getFunctionName();
        functions.add(
            InfluxDBFunctionFactory.generateFunctionBySession(
                functionName, expression.getExpressions(), path));
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
    return null;
  }
}
