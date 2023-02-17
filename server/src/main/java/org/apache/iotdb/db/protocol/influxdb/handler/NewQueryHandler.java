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
package org.apache.iotdb.db.protocol.influxdb.handler;

import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSqlConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.service.thrift.impl.NewInfluxDBServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.influxdb.dto.QueryResult;

import java.util.List;
import java.util.Map;

/** query handler for NewIoTDB */
public class NewQueryHandler extends AbstractQueryHandler {

  /**
   * If the function in the influxdb query request is also supported by IoTDB, use the IoTDB syntax
   * to get the result
   *
   * @param path database path
   * @param function influxdb function
   * @param sessionid session id
   * @return influxdb function value
   */
  public final InfluxFunctionValue updateByIoTDBFunc(
      String path, InfluxFunction function, long sessionid) {
    switch (function.getFunctionName()) {
      case InfluxSqlConstant.COUNT:
        {
          String functionSql =
              StringUtils.generateFunctionSql(
                  function.getFunctionName(), function.getParmaName(), path);
          TSExecuteStatementResp tsExecuteStatementResp =
              NewInfluxDBServiceImpl.executeStatement(functionSql, sessionid);
          List<InfluxFunctionValue> list =
              QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(influxFunctionValue);
          }
          break;
        }
      case InfluxSqlConstant.MEAN:
        {
          String functionSqlCount =
              StringUtils.generateFunctionSql("count", function.getParmaName(), path);
          TSExecuteStatementResp tsExecuteStatementResp =
              NewInfluxDBServiceImpl.executeStatement(functionSqlCount, sessionid);
          List<InfluxFunctionValue> list =
              QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(influxFunctionValue);
          }
          String functionSqlSum =
              StringUtils.generateFunctionSql("sum", function.getParmaName(), path);
          tsExecuteStatementResp =
              NewInfluxDBServiceImpl.executeStatement(functionSqlSum, sessionid);
          list = QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(null, influxFunctionValue);
          }
          break;
        }
      case InfluxSqlConstant.SUM:
        {
          String functionSql =
              StringUtils.generateFunctionSql("sum", function.getParmaName(), path);
          TSExecuteStatementResp tsExecuteStatementResp =
              NewInfluxDBServiceImpl.executeStatement(functionSql, sessionid);
          List<InfluxFunctionValue> list =
              QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(influxFunctionValue);
          }
          break;
        }
      case InfluxSqlConstant.FIRST:
      case InfluxSqlConstant.LAST:
        {
          String functionSql;
          String functionName;
          if (function.getFunctionName().equals(InfluxSqlConstant.FIRST)) {
            functionSql =
                StringUtils.generateFunctionSql("first_value", function.getParmaName(), path);
            functionName = "first_value";
          } else {
            functionSql =
                StringUtils.generateFunctionSql("last_value", function.getParmaName(), path);
            functionName = "last_value";
          }
          TSExecuteStatementResp tsExecuteStatementResp =
              NewInfluxDBServiceImpl.executeStatement(functionSql, sessionid);
          Map<String, Object> map = QueryResultUtils.getColumnNameAndValue(tsExecuteStatementResp);
          for (Map.Entry<String, Object> entry : map.entrySet()) {
            String colume = entry.getKey();
            Object o = entry.getValue();
            String fullPath = colume.substring(functionName.length() + 1, colume.length() - 1);
            String devicePath = StringUtils.getDeviceByPath(fullPath);
            String specificSql =
                String.format(
                    "select %s from %s where %s=%s",
                    function.getParmaName(), devicePath, fullPath, o);
            TSExecuteStatementResp resp =
                NewInfluxDBServiceImpl.executeStatement(specificSql, sessionid);
            List<InfluxFunctionValue> list = QueryResultUtils.getInfluxFunctionValues(resp);
            for (InfluxFunctionValue influxFunctionValue : list) {
              function.updateValueIoTDBFunc(influxFunctionValue);
            }
          }
          break;
        }
      case InfluxSqlConstant.MAX:
      case InfluxSqlConstant.MIN:
        {
          String functionSql;
          if (function.getFunctionName().equals(InfluxSqlConstant.MAX)) {
            functionSql =
                StringUtils.generateFunctionSql("max_value", function.getParmaName(), path);
          } else {
            functionSql =
                StringUtils.generateFunctionSql("min_value", function.getParmaName(), path);
          }
          TSExecuteStatementResp tsExecuteStatementResp =
              NewInfluxDBServiceImpl.executeStatement(functionSql, sessionid);
          List<InfluxFunctionValue> list =
              QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(influxFunctionValue);
          }
          break;
        }
      default:
        throw new IllegalStateException("Unexpected value: " + function.getFunctionName());
    }
    return function.calculateByIoTDBFunc();
  }

  /**
   * If the function in the influxdb query request is also supported by IoTDB, use the IoTDB syntax
   * to get the result
   *
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param function influxdb function
   * @param sessionid session id
   * @return influxdb function value
   */
  @Override
  public InfluxFunctionValue updateByIoTDBFunc(
      String database, String measurement, InfluxFunction function, long sessionid) {
    String path = "root." + database + "." + measurement;
    return updateByIoTDBFunc(path, function, sessionid);
  }

  /**
   * Query the result according to the query SQL supported by IoTDB
   *
   * @param querySql query sql
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param tagOrders tag orders
   * @param fieldOrders field orders
   * @param sessionId session id
   * @return query result
   */
  @Override
  public QueryResult queryByConditions(
      String querySql,
      String database,
      String measurement,
      Map<String, Integer> tagOrders,
      Map<String, Integer> fieldOrders,
      long sessionId) {
    TSExecuteStatementResp executeStatementResp =
        NewInfluxDBServiceImpl.executeStatement(querySql, sessionId);
    return QueryResultUtils.iotdbResultConvertInfluxResult(
        executeStatementResp, database, measurement, fieldOrders);
  }
}
