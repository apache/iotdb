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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.meta.NewInfluxDBMetaManager;
import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.service.thrift.impl.NewInfluxDBServiceImpl;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewQueryHandler extends AbstractQueryHandler {

  public static TSExecuteStatementResp executeStatement(String sql, long sessionId) {
    TSExecuteStatementReq tsExecuteStatementReq = new TSExecuteStatementReq();
    tsExecuteStatementReq.setStatement(sql);
    tsExecuteStatementReq.setSessionId(sessionId);
    tsExecuteStatementReq.setStatementId(
        NewInfluxDBServiceImpl.getClientRPCService().requestStatementId(sessionId));
    tsExecuteStatementReq.setFetchSize(InfluxConstant.DEFAULT_FETCH_SIZE);
    TSExecuteStatementResp executeStatementResp =
        NewInfluxDBServiceImpl.getClientRPCService().executeStatement(tsExecuteStatementReq);
    TSStatus tsStatus = executeStatementResp.getStatus();
    if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new InfluxDBException(tsStatus.getMessage());
    }
    return executeStatementResp;
  }

  @Override
  public Map<String, Integer> getFieldOrders(
      String database, String measurement, ServiceProvider serviceProvider, long sessionID) {
    Map<String, Integer> fieldOrders = new HashMap<>();
    String showTimeseriesSql = "show timeseries root." + database + '.' + measurement + ".**";
    TSExecuteStatementResp executeStatementResp = executeStatement(showTimeseriesSql, sessionID);
    List<String> paths = QueryResultUtils.getFullPaths(executeStatementResp);
    Map<String, Integer> tagOrders = NewInfluxDBMetaManager.getTagOrders(database, measurement);
    int tagOrderNums = tagOrders.size();
    int fieldNums = 0;
    for (String path : paths) {
      String filed = StringUtils.getFieldByPath(path);
      if (!fieldOrders.containsKey(filed)) {
        // The corresponding order of fields is 1 + tagNum (the first is timestamp, then all tags,
        // and finally all fields)
        fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
        fieldNums++;
      }
    }
    return fieldOrders;
  }

  @Override
  public InfluxFunctionValue updateByIoTDBFunc(
      InfluxFunction function, ServiceProvider serviceProvider, String path, long sessionid) {
    switch (function.getFunctionName()) {
      case InfluxSQLConstant.COUNT:
        {
          String functionSql =
              StringUtils.generateFunctionSql(
                  function.getFunctionName(), function.getParmaName(), path);
          TSExecuteStatementResp tsExecuteStatementResp = executeStatement(functionSql, sessionid);
          List<InfluxFunctionValue> list =
              QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(influxFunctionValue);
          }
          break;
        }
      case InfluxSQLConstant.MEAN:
        {
          String functionSqlCount =
              StringUtils.generateFunctionSql("count", function.getParmaName(), path);
          TSExecuteStatementResp tsExecuteStatementResp =
              executeStatement(functionSqlCount, sessionid);
          List<InfluxFunctionValue> list =
              QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(influxFunctionValue);
          }
          String functionSqlSum =
              StringUtils.generateFunctionSql("sum", function.getParmaName(), path);
          tsExecuteStatementResp = executeStatement(functionSqlSum, sessionid);
          list = QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(null, influxFunctionValue);
          }
          break;
        }
      case InfluxSQLConstant.SUM:
        {
          String functionSql =
              StringUtils.generateFunctionSql("sum", function.getParmaName(), path);
          TSExecuteStatementResp tsExecuteStatementResp = executeStatement(functionSql, sessionid);
          List<InfluxFunctionValue> list =
              QueryResultUtils.getInfluxFunctionValues(tsExecuteStatementResp);
          for (InfluxFunctionValue influxFunctionValue : list) {
            function.updateValueIoTDBFunc(influxFunctionValue);
          }
          break;
        }
      case InfluxSQLConstant.FIRST:
      case InfluxSQLConstant.LAST:
        {
          String functionSql;
          String functionName;
          if (function.getFunctionName().equals(InfluxSQLConstant.FIRST)) {
            functionSql =
                StringUtils.generateFunctionSql("first_value", function.getParmaName(), path);
            functionName = "first_value";
          } else {
            functionSql =
                StringUtils.generateFunctionSql("last_value", function.getParmaName(), path);
            functionName = "last_value";
          }
          TSExecuteStatementResp tsExecuteStatementResp = executeStatement(functionSql, sessionid);
          Map<String, Object> map = QueryResultUtils.getColumnNameAndValue(tsExecuteStatementResp);
          for (String colume : map.keySet()) {
            Object o = map.get(colume);
            String fullPath = colume.substring(functionName.length() + 1, colume.length() - 1);
            String devicePath = StringUtils.getDeviceByPath(fullPath);
            String specificSql =
                String.format(
                    "select %s from %s where %s=%s",
                    function.getParmaName(), devicePath, fullPath, o);
            TSExecuteStatementResp resp = executeStatement(specificSql, sessionid);
            List<InfluxFunctionValue> list = QueryResultUtils.getInfluxFunctionValues(resp);
            for (InfluxFunctionValue influxFunctionValue : list) {
              function.updateValueIoTDBFunc(influxFunctionValue);
            }
          }
          break;
        }
      case InfluxSQLConstant.MAX:
      case InfluxSQLConstant.MIN:
        {
          String functionSql;
          if (function.getFunctionName().equals(InfluxSQLConstant.MAX)) {
            functionSql =
                StringUtils.generateFunctionSql("max_value", function.getParmaName(), path);
          } else {
            functionSql =
                StringUtils.generateFunctionSql("min_value", function.getParmaName(), path);
          }
          TSExecuteStatementResp tsExecuteStatementResp = executeStatement(functionSql, sessionid);
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

  @Override
  public QueryResult queryByConditions(
      String querySql,
      String database,
      String measurement,
      ServiceProvider serviceProvider,
      Map<String, Integer> fieldOrders,
      long sessionId) {
    TSExecuteStatementResp executeStatementResp = executeStatement(querySql, sessionId);
    return QueryResultUtils.iotdbResultConvertInfluxResult(
        executeStatementResp, database, measurement, fieldOrders);
  }
}
