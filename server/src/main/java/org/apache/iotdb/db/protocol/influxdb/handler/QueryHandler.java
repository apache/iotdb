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
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.util.FieldUtils;
import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryHandler extends AbstractQueryHandler {

  ServiceProvider serviceProvider = IoTDB.serviceProvider;

  @Override
  public InfluxFunctionValue updateByIoTDBFunc(
      String database, String measurement, InfluxFunction function, long sessionid) {
    String path = "root." + database + "." + measurement;
    switch (function.getFunctionName()) {
      case InfluxSQLConstant.COUNT:
        {
          long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
          String functionSql =
              StringUtils.generateFunctionSql(
                  function.getFunctionName(), function.getParmaName(), path);
          try {
            QueryPlan queryPlan =
                (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSql);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryId,
                    true,
                    System.currentTimeMillis(),
                    functionSql,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Field> fields = queryDataSet.next().getFields();
              for (Field field : fields) {
                function.updateValueIoTDBFunc(new InfluxFunctionValue(field.getLongV(), null));
              }
            }
          } catch (QueryProcessException
              | QueryFilterOptimizationException
              | StorageEngineException
              | IOException
              | MetadataException
              | SQLException
              | TException
              | InterruptedException e) {
            e.printStackTrace();
            throw new InfluxDBException(e.getMessage());
          } finally {
            ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
          }
          break;
        }
      case InfluxSQLConstant.MEAN:
        {
          long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
          try {
            String functionSqlCount =
                StringUtils.generateFunctionSql("count", function.getParmaName(), path);
            QueryPlan queryPlan =
                (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSqlCount);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryId,
                    true,
                    System.currentTimeMillis(),
                    functionSqlCount,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Field> fields = queryDataSet.next().getFields();
              for (Field field : fields) {
                function.updateValueIoTDBFunc(new InfluxFunctionValue(field.getLongV(), null));
              }
            }
          } catch (QueryProcessException
              | TException
              | StorageEngineException
              | SQLException
              | IOException
              | InterruptedException
              | QueryFilterOptimizationException
              | MetadataException e) {
            throw new InfluxDBException(e.getMessage());
          } finally {
            ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
          }
          long queryId1 = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
          try {
            String functionSqlSum =
                StringUtils.generateFunctionSql("sum", function.getParmaName(), path);
            QueryPlan queryPlan =
                (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSqlSum);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryId,
                    true,
                    System.currentTimeMillis(),
                    functionSqlSum,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Field> fields = queryDataSet.next().getFields();
              for (Field field : fields) {
                function.updateValueIoTDBFunc(
                    null, new InfluxFunctionValue(field.getDoubleV(), null));
              }
            }
          } catch (QueryProcessException
              | TException
              | StorageEngineException
              | SQLException
              | IOException
              | InterruptedException
              | QueryFilterOptimizationException
              | MetadataException e) {
            e.printStackTrace();
            throw new InfluxDBException(e.getMessage());
          } finally {
            ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId1);
          }
          break;
        }
      case InfluxSQLConstant.SPREAD:
        {
          long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
          try {
            String functionSqlMaxValue =
                StringUtils.generateFunctionSql("max_value", function.getParmaName(), path);
            QueryPlan queryPlan =
                (QueryPlan)
                    serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSqlMaxValue);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryId,
                    true,
                    System.currentTimeMillis(),
                    functionSqlMaxValue,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Path> paths = queryDataSet.getPaths();
              List<Field> fields = queryDataSet.next().getFields();
              for (int i = 0; i < paths.size(); i++) {
                Object o = FieldUtils.iotdbFieldConvert(fields.get(i));
                if (o instanceof Number) {
                  function.updateValueIoTDBFunc(
                      new InfluxFunctionValue(((Number) o).doubleValue(), null));
                }
              }
            }
          } catch (QueryProcessException
              | TException
              | StorageEngineException
              | SQLException
              | IOException
              | InterruptedException
              | QueryFilterOptimizationException
              | MetadataException e) {
            throw new InfluxDBException(e.getMessage());
          } finally {
            ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
          }
          long queryId1 = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
          try {
            String functionSqlMinValue =
                StringUtils.generateFunctionSql("min_value", function.getParmaName(), path);
            QueryPlan queryPlan =
                (QueryPlan)
                    serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSqlMinValue);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryId,
                    true,
                    System.currentTimeMillis(),
                    functionSqlMinValue,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Path> paths = queryDataSet.getPaths();
              List<Field> fields = queryDataSet.next().getFields();
              for (int i = 0; i < paths.size(); i++) {
                Object o = FieldUtils.iotdbFieldConvert(fields.get(i));
                if (o instanceof Number) {
                  function.updateValueIoTDBFunc(
                      null, new InfluxFunctionValue(((Number) o).doubleValue(), null));
                }
              }
            }
          } catch (QueryProcessException
              | TException
              | StorageEngineException
              | SQLException
              | IOException
              | InterruptedException
              | QueryFilterOptimizationException
              | MetadataException e) {
            throw new InfluxDBException(e.getMessage());
          } finally {
            ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId1);
          }
          break;
        }
      case InfluxSQLConstant.SUM:
        {
          long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
          try {
            String functionSql =
                StringUtils.generateFunctionSql("sum", function.getParmaName(), path);
            QueryPlan queryPlan =
                (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSql);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryId,
                    true,
                    System.currentTimeMillis(),
                    functionSql,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Field> fields = queryDataSet.next().getFields();
              if (fields.get(1).getDataType() != null) {
                function.updateValueIoTDBFunc(
                    new InfluxFunctionValue(fields.get(1).getDoubleV(), null));
              }
            }
          } catch (QueryProcessException
              | TException
              | StorageEngineException
              | SQLException
              | IOException
              | InterruptedException
              | QueryFilterOptimizationException
              | MetadataException e) {
            throw new InfluxDBException(e.getMessage());
          } finally {
            ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
          }
          break;
        }
      case InfluxSQLConstant.FIRST:
      case InfluxSQLConstant.LAST:
        {
          String functionSql;
          if (function.getFunctionName().equals(InfluxSQLConstant.FIRST)) {
            functionSql =
                StringUtils.generateFunctionSql("first_value", function.getParmaName(), path);
          } else {
            functionSql =
                StringUtils.generateFunctionSql("last_value", function.getParmaName(), path);
          }
          List<Long> queryIds = new ArrayList<>();
          queryIds.add(ServiceProvider.SESSION_MANAGER.requestQueryId(true));
          try {
            QueryPlan queryPlan =
                (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSql);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryIds.get(0),
                    true,
                    System.currentTimeMillis(),
                    functionSql,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Path> paths = queryDataSet.getPaths();
              List<Field> fields = queryDataSet.next().getFields();
              for (int i = 0; i < paths.size(); i++) {
                Object o = FieldUtils.iotdbFieldConvert(fields.get(i));
                long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
                queryIds.add(queryId);
                if (o != null) {
                  String specificSql =
                      String.format(
                          "select %s from %s where %s=%s",
                          function.getParmaName(),
                          paths.get(i).getDevice(),
                          paths.get(i).getFullPath(),
                          o);
                  QueryPlan queryPlanNew =
                      (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(specificSql);
                  QueryContext queryContextNew =
                      serviceProvider.genQueryContext(
                          queryId,
                          true,
                          System.currentTimeMillis(),
                          specificSql,
                          InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
                  QueryDataSet queryDataSetNew =
                      serviceProvider.createQueryDataSet(
                          queryContextNew, queryPlanNew, InfluxConstant.DEFAULT_FETCH_SIZE);
                  while (queryDataSetNew.hasNext()) {
                    RowRecord recordNew = queryDataSetNew.next();
                    List<Field> newFields = recordNew.getFields();
                    long time = recordNew.getTimestamp();
                    function.updateValueIoTDBFunc(
                        new InfluxFunctionValue(
                            FieldUtils.iotdbFieldConvert(newFields.get(0)), time));
                  }
                }
              }
            }
          } catch (QueryProcessException
              | TException
              | StorageEngineException
              | SQLException
              | IOException
              | InterruptedException
              | QueryFilterOptimizationException
              | MetadataException e) {
            throw new InfluxDBException(e.getMessage());
          } finally {
            for (long queryId : queryIds) {
              ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
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
          long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
          try {
            QueryPlan queryPlan =
                (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(functionSql);
            QueryContext queryContext =
                serviceProvider.genQueryContext(
                    queryId,
                    true,
                    System.currentTimeMillis(),
                    functionSql,
                    InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
            QueryDataSet queryDataSet =
                serviceProvider.createQueryDataSet(
                    queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
            while (queryDataSet.hasNext()) {
              List<Path> paths = queryDataSet.getPaths();
              List<Field> fields = queryDataSet.next().getFields();
              for (int i = 0; i < paths.size(); i++) {
                Object o = FieldUtils.iotdbFieldConvert(fields.get(i));
                function.updateValueIoTDBFunc(new InfluxFunctionValue(o, null));
              }
            }
          } catch (QueryProcessException
              | TException
              | StorageEngineException
              | SQLException
              | IOException
              | InterruptedException
              | QueryFilterOptimizationException
              | MetadataException e) {
            throw new InfluxDBException(e.getMessage());
          } finally {
            ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
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
      Map<String, Integer> tagOrders,
      Map<String, Integer> fieldOrders,
      long sessionId)
      throws AuthException {
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      QueryPlan queryPlan =
          (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(querySql);
      TSStatus tsStatus = SessionManager.getInstance().checkAuthority(queryPlan, sessionId);
      if (tsStatus != null) {
        throw new AuthException(tsStatus.getMessage());
      }
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              querySql,
              InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, queryPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
      return QueryResultUtils.iotdbResultConvertInfluxResult(
          queryDataSet, database, measurement, fieldOrders);
    } catch (QueryProcessException
        | TException
        | StorageEngineException
        | SQLException
        | IOException
        | InterruptedException
        | QueryFilterOptimizationException
        | MetadataException e) {
      throw new InfluxDBException(e.getMessage());
    } finally {
      ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
    }
  }
}
