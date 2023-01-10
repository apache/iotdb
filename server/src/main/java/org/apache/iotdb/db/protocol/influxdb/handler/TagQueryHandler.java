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

import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManagerFactory;
import org.apache.iotdb.db.protocol.influxdb.util.FilterUtils;
import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.service.thrift.impl.NewInfluxDBServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;

import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Query Handler for NewIoTDB When schema region is tag schema region */
public class TagQueryHandler extends NewQueryHandler {

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
    String path = "root." + database + ".measurement." + measurement;
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
        executeStatementResp, database, measurement, tagOrders, fieldOrders);
  }

  @Override
  public QueryResult queryByConditions(
      List<IExpression> expressions,
      String database,
      String measurement,
      Map<String, Integer> fieldOrders,
      Long sessionId) {
    List<SingleSeriesExpression> fieldExpressions = new ArrayList<>();
    List<SingleSeriesExpression> tagExpressions = new ArrayList<>();
    Map<String, Integer> tagOrders =
        InfluxDBMetaManagerFactory.getInstance().getTagOrders(database, measurement, sessionId);
    for (IExpression expression : expressions) {
      SingleSeriesExpression singleSeriesExpression = ((SingleSeriesExpression) expression);
      // the current condition is in tag
      if (tagOrders.containsKey(singleSeriesExpression.getSeriesPath().getFullPath())) {
        tagExpressions.add(singleSeriesExpression);
      } else {
        fieldExpressions.add(singleSeriesExpression);
      }
    }
    // construct the actual query path
    StringBuilder curQueryPath =
        new StringBuilder("root." + database + ".measurement." + measurement);
    for (SingleSeriesExpression singleSeriesExpression : tagExpressions) {
      String tagKey = singleSeriesExpression.getSeriesPath().getFullPath();
      String tagValue =
          StringUtils.removeQuotation(
              FilterUtils.getFilterStringValue(singleSeriesExpression.getFilter()));
      curQueryPath.append(".").append(tagKey).append(".").append(tagValue);
    }
    curQueryPath.append(".**");

    // construct actual query condition
    StringBuilder realIotDBCondition = new StringBuilder();
    for (int i = 0; i < fieldExpressions.size(); i++) {
      SingleSeriesExpression singleSeriesExpression = fieldExpressions.get(i);
      if (i != 0) {
        realIotDBCondition.append(" and ");
      }
      realIotDBCondition
          .append(singleSeriesExpression.getSeriesPath().getFullPath())
          .append(" ")
          .append((FilterUtils.getFilerSymbol(singleSeriesExpression.getFilter())))
          .append(" ")
          .append(FilterUtils.getFilterStringValue(singleSeriesExpression.getFilter()));
    }
    // actual query SQL statement
    String realQuerySql;

    realQuerySql = "select * from " + curQueryPath;
    if (realIotDBCondition.length() != 0) {
      realQuerySql += " where " + realIotDBCondition;
    }
    realQuerySql += " align by device";
    return queryByConditions(
        realQuerySql, database, measurement, tagOrders, fieldOrders, sessionId);
  }
}
