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
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ResultColumn;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSQLConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionFactory;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxAggregator;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxSelector;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManager;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxQueryOperator;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxSelectComponent;
import org.apache.iotdb.db.protocol.influxdb.util.FieldUtils;
import org.apache.iotdb.db.protocol.influxdb.util.FilterUtils;
import org.apache.iotdb.db.protocol.influxdb.util.JacksonUtils;
import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxQueryResultRsp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryHandler {

  public static InfluxQueryResultRsp queryInfluxDB(
      String database,
      InfluxQueryOperator queryOperator,
      long sessionId,
      ServiceProvider serviceProvider) {
    String measurement = queryOperator.getFromComponent().getPrefixPaths().get(0).getFullPath();
    // The list of fields under the current measurement and the order of the specified rules
    Map<String, Integer> fieldOrders = getFieldOrders(database, measurement, serviceProvider);
    QueryResult queryResult;
    InfluxQueryResultRsp tsQueryResultRsp = new InfluxQueryResultRsp();
    try {
      // contain filter condition or have common query the result of by traversal.
      if (queryOperator.getWhereComponent() != null
          || queryOperator.getSelectComponent().isHasCommonQuery()
          || queryOperator.getSelectComponent().isHasOnlyTraverseFunction()) {
        // step1 : generate query results
        queryResult =
            queryExpr(
                queryOperator.getWhereComponent() != null
                    ? queryOperator.getWhereComponent().getFilterOperator()
                    : null,
                database,
                measurement,
                serviceProvider,
                fieldOrders,
                sessionId);
        // step2 : select filter
        ProcessSelectComponent(queryResult, queryOperator.getSelectComponent());
      }
      // don't contain filter condition and only have function use iotdb function.
      else {
        queryResult =
            queryFuncWithoutFilter(
                queryOperator.getSelectComponent(), database, measurement, serviceProvider);
      }
      return tsQueryResultRsp
          .setResultJsonString(JacksonUtils.bean2Json(queryResult))
          .setStatus(RpcUtils.getInfluxDBStatus(TSStatusCode.SUCCESS_STATUS));
    } catch (AuthException e) {
      return tsQueryResultRsp.setStatus(
          RpcUtils.getInfluxDBStatus(
              TSStatusCode.UNINITIALIZED_AUTH_ERROR.getStatusCode(), e.getMessage()));
    }
  }

  /**
   * conditions are generated from subtrees of unique conditions
   *
   * @param basicFunctionOperator subtree to generate condition
   * @return corresponding conditions
   */
  public static IExpression getIExpressionForBasicFunctionOperator(
      BasicFunctionOperator basicFunctionOperator) {
    return new SingleSeriesExpression(
        basicFunctionOperator.getSinglePath(),
        FilterUtils.filterTypeToFilter(
            basicFunctionOperator.getFilterType(), basicFunctionOperator.getValue()));
  }

  /**
   * further process the obtained query result through the query criteria of select
   *
   * @param queryResult query results to be processed
   * @param selectComponent select conditions to be filtered
   */
  public static void ProcessSelectComponent(
      QueryResult queryResult, InfluxSelectComponent selectComponent) {

    // get the row order map of the current data result first
    List<String> columns = queryResult.getResults().get(0).getSeries().get(0).getColumns();
    Map<String, Integer> columnOrders = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      columnOrders.put(columns.get(i), i);
    }
    // get current values
    List<List<Object>> values = queryResult.getResults().get(0).getSeries().get(0).getValues();
    // new columns
    List<String> newColumns = new ArrayList<>();
    newColumns.add(InfluxSQLConstant.RESERVED_TIME);

    // when have function
    if (selectComponent.isHasFunction()) {
      List<InfluxFunction> functions = new ArrayList<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        if (expression instanceof FunctionExpression) {
          String functionName = ((FunctionExpression) expression).getFunctionName();
          functions.add(
              InfluxFunctionFactory.generateFunction(functionName, expression.getExpressions()));
          newColumns.add(functionName);
        } else if (expression instanceof TimeSeriesOperand) {
          String columnName = ((TimeSeriesOperand) expression).getPath().getFullPath();
          if (!columnName.equals(InfluxSQLConstant.STAR)) {
            newColumns.add(columnName);
          } else {
            newColumns.addAll(columns.subList(1, columns.size()));
          }
        }
      }
      for (List<Object> value : values) {
        for (InfluxFunction function : functions) {
          List<Expression> expressions = function.getExpressions();
          if (expressions == null) {
            throw new IllegalArgumentException("not support param");
          }
          TimeSeriesOperand parmaExpression = (TimeSeriesOperand) expressions.get(0);
          String parmaName = parmaExpression.getPath().getFullPath();
          if (columnOrders.containsKey(parmaName)) {
            Object selectedValue = value.get(columnOrders.get(parmaName));
            Long selectedTimestamp = (Long) value.get(0);
            if (selectedValue != null) {
              // selector function
              if (function instanceof InfluxSelector) {
                ((InfluxSelector) function)
                    .updateValueAndRelateValues(
                        new InfluxFunctionValue(selectedValue, selectedTimestamp), value);
              } else {
                // aggregate function
                ((InfluxAggregator) function)
                    .updateValueBruteForce(
                        new InfluxFunctionValue(selectedValue, selectedTimestamp));
              }
            }
          }
        }
      }
      List<Object> value = new ArrayList<>();
      values = new ArrayList<>();
      // after the data is constructed, the final results are generated
      // First, judge whether there are common queries. If there are, a selector function is allowed
      // without aggregate functions
      if (selectComponent.isHasCommonQuery()) {
        InfluxSelector selector = (InfluxSelector) functions.get(0);
        List<Object> relatedValue = selector.getRelatedValues();
        for (String column : newColumns) {
          if (InfluxSQLConstant.getNativeSelectorFunctionNames().contains(column)) {
            value.add(selector.calculateBruteForce().getValue());
          } else {
            if (relatedValue != null) {
              value.add(relatedValue.get(columnOrders.get(column)));
            }
          }
        }
      } else {
        // If there are no common queries, they are all function queries
        for (InfluxFunction function : functions) {
          if (value.size() == 0) {
            value.add(function.calculateBruteForce().getTimestamp());
          } else {
            value.set(0, function.calculateBruteForce().getTimestamp());
          }
          value.add(function.calculateBruteForce().getValue());
        }
        if (selectComponent.isHasAggregationFunction() || selectComponent.isHasMoreFunction()) {
          value.set(0, 0);
        }
      }
      values.add(value);
    }
    // if it is not a function query, it is only a common query
    else if (selectComponent.isHasCommonQuery()) {
      // start traversing the scope of the select
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        if (expression instanceof TimeSeriesOperand) {
          // not star case
          if (!((TimeSeriesOperand) expression)
              .getPath()
              .getFullPath()
              .equals(InfluxSQLConstant.STAR)) {
            newColumns.add(((TimeSeriesOperand) expression).getPath().getFullPath());
          } else {
            newColumns.addAll(columns.subList(1, columns.size()));
          }
        }
      }
      List<List<Object>> newValues = new ArrayList<>();
      for (List<Object> value : values) {
        List<Object> tmpValue = new ArrayList<>();
        for (String newColumn : newColumns) {
          tmpValue.add(value.get(columnOrders.get(newColumn)));
        }
        newValues.add(tmpValue);
      }
      values = newValues;
    }
    QueryResultUtils.updateQueryResultColumnValue(
        queryResult, StringUtils.removeDuplicate(newColumns), values);
  }

  public static Map<String, Integer> getFieldOrders(
      String database, String measurement, ServiceProvider serviceProvider) {
    Map<String, Integer> fieldOrders = new HashMap<>();
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      String showTimeseriesSql = "show timeseries root." + database + '.' + measurement + ".**";
      PhysicalPlan physicalPlan =
          serviceProvider.getPlanner().parseSQLToPhysicalPlan(showTimeseriesSql);
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              showTimeseriesSql,
              InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, physicalPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
      int fieldNums = 0;
      Map<String, Integer> tagOrders = InfluxDBMetaManager.getTagOrders(database, measurement);
      int tagOrderNums = tagOrders.size();
      while (queryDataSet.hasNext()) {
        List<Field> fields = queryDataSet.next().getFields();
        String filed = StringUtils.getFieldByPath(fields.get(0).getStringValue());
        if (!fieldOrders.containsKey(filed)) {
          // The corresponding order of fields is 1 + tagNum (the first is timestamp, then all tags,
          // and finally all fields)
          fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
          fieldNums++;
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
    return fieldOrders;
  }

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

    List<InfluxFunction> functions = new ArrayList<>();
    String path = "root." + database + "." + measurement;
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (expression instanceof FunctionExpression) {
        String functionName = ((FunctionExpression) expression).getFunctionName();
        functions.add(
            InfluxFunctionFactory.generateFunction(functionName, expression.getExpressions()));
        columns.add(functionName);
      }
    }

    List<Object> value = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (InfluxFunction function : functions) {
      InfluxFunctionValue functionValue = updateByIoTDBFunc(function, serviceProvider, path);
      //      InfluxFunctionValue functionValue = function.calculateByIoTDBFunc();
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

  private static InfluxFunctionValue updateByIoTDBFunc(
      InfluxFunction function, ServiceProvider serviceProvider, String path) {
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
                    function.updateValueIoTDBFunc(new InfluxFunctionValue(newFields.get(0), time));
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

  public static QueryResult queryExpr(
      FilterOperator operator,
      String database,
      String measurement,
      ServiceProvider serviceProvider,
      Map<String, Integer> fieldOrders,
      Long sessionId)
      throws AuthException {
    if (operator == null) {
      List<IExpression> expressions = new ArrayList<>();
      return queryByConditions(
          expressions, database, measurement, serviceProvider, fieldOrders, sessionId);
    } else if (operator instanceof BasicFunctionOperator) {
      List<IExpression> iExpressions = new ArrayList<>();
      iExpressions.add(getIExpressionForBasicFunctionOperator((BasicFunctionOperator) operator));
      return queryByConditions(
          iExpressions, database, measurement, serviceProvider, fieldOrders, sessionId);
    } else {
      FilterOperator leftOperator = operator.getChildren().get(0);
      FilterOperator rightOperator = operator.getChildren().get(1);
      if (operator.getFilterType() == FilterConstant.FilterType.KW_OR) {
        return QueryResultUtils.orQueryResultProcess(
            queryExpr(leftOperator, database, measurement, serviceProvider, fieldOrders, sessionId),
            queryExpr(
                rightOperator, database, measurement, serviceProvider, fieldOrders, sessionId));
      } else if (operator.getFilterType() == FilterConstant.FilterType.KW_AND) {
        if (canMergeOperator(leftOperator) && canMergeOperator(rightOperator)) {
          List<IExpression> iExpressions1 = getIExpressionByFilterOperatorOperator(leftOperator);
          List<IExpression> iExpressions2 = getIExpressionByFilterOperatorOperator(rightOperator);
          iExpressions1.addAll(iExpressions2);
          return queryByConditions(
              iExpressions1, database, measurement, serviceProvider, fieldOrders, sessionId);
        } else {
          return QueryResultUtils.andQueryResultProcess(
              queryExpr(
                  leftOperator, database, measurement, serviceProvider, fieldOrders, sessionId),
              queryExpr(
                  rightOperator, database, measurement, serviceProvider, fieldOrders, sessionId));
        }
      }
    }
    throw new IllegalArgumentException("unknown operator " + operator);
  }

  /**
   * get query results in the format of influxdb through conditions
   *
   * @param expressions list of conditions, including tag and field condition
   * @return returns the results of the influxdb query
   */
  private static QueryResult queryByConditions(
      List<IExpression> expressions,
      String database,
      String measurement,
      ServiceProvider serviceProvider,
      Map<String, Integer> fieldOrders,
      Long sessionId)
      throws AuthException {
    // used to store the actual order according to the tag
    Map<Integer, SingleSeriesExpression> realTagOrders = new HashMap<>();
    // stores a list of conditions belonging to the field
    List<SingleSeriesExpression> fieldExpressions = new ArrayList<>();
    // maximum number of tags in the current query criteria
    int currentQueryMaxTagNum = 0;
    Map<String, Integer> tagOrders = InfluxDBMetaManager.getTagOrders(database, measurement);
    for (IExpression expression : expressions) {
      SingleSeriesExpression singleSeriesExpression = ((SingleSeriesExpression) expression);
      // the current condition is in tag
      if (tagOrders.containsKey(singleSeriesExpression.getSeriesPath().getFullPath())) {
        int curOrder = tagOrders.get(singleSeriesExpression.getSeriesPath().getFullPath());
        // put it into the map according to the tag
        realTagOrders.put(curOrder, singleSeriesExpression);
        // update the maximum tag order of the current query criteria
        currentQueryMaxTagNum = Math.max(currentQueryMaxTagNum, curOrder);
      } else {
        fieldExpressions.add(singleSeriesExpression);
      }
    }
    // construct the actual query path
    StringBuilder curQueryPath = new StringBuilder("root." + database + "." + measurement);
    // the maximum number of traversals from 1 to the current query condition
    for (int i = 1; i <= currentQueryMaxTagNum; i++) {
      if (realTagOrders.containsKey(i)) {
        // since it is the value in the path, you need to remove the quotation marks at the
        // beginning and end
        curQueryPath
            .append(".")
            .append(
                StringUtils.removeQuotation(
                    FilterUtils.getFilterStringValue(realTagOrders.get(i).getFilter())));
      } else {
        curQueryPath.append(".").append("*");
      }
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
    if (!(realIotDBCondition.length() == 0)) {
      realQuerySql += " where " + realIotDBCondition;
    }
    realQuerySql += " align by device";

    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      QueryPlan queryPlan =
          (QueryPlan) serviceProvider.getPlanner().parseSQLToPhysicalPlan(realQuerySql);
      TSStatus tsStatus = SessionManager.getInstance().checkAuthority(queryPlan, sessionId);
      if (tsStatus != null) {
        throw new AuthException(tsStatus.getMessage());
      }
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              realQuerySql,
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

  /**
   * generate query conditions through the syntax tree (if you enter this function, it means that it
   * must be a syntax tree that can be merged, and there is no or)
   *
   * @param filterOperator the syntax tree of query criteria needs to be generated
   * @return condition list
   */
  public static List<IExpression> getIExpressionByFilterOperatorOperator(
      FilterOperator filterOperator) {
    if (filterOperator instanceof BasicFunctionOperator) {
      // It must be a non-or situation
      List<IExpression> expressions = new ArrayList<>();
      expressions.add(
          getIExpressionForBasicFunctionOperator((BasicFunctionOperator) filterOperator));
      return expressions;
    } else {
      FilterOperator leftOperator = filterOperator.getChildren().get(0);
      FilterOperator rightOperator = filterOperator.getChildren().get(1);
      List<IExpression> expressions1 = getIExpressionByFilterOperatorOperator(leftOperator);
      List<IExpression> expressions2 = getIExpressionByFilterOperatorOperator(rightOperator);
      expressions1.addAll(expressions2);
      return expressions1;
    }
  }

  /**
   * judge whether the subtrees of the syntax tree have or operations. If not, the query can be
   * merged
   *
   * @param operator subtree to judge
   * @return can merge queries
   */
  public static boolean canMergeOperator(FilterOperator operator) {
    if (operator instanceof BasicFunctionOperator) {
      return true;
    } else {
      if (operator.getFilterType() == FilterConstant.FilterType.KW_OR) {
        return false;
      } else {
        FilterOperator leftOperator = operator.getChildren().get(0);
        FilterOperator rightOperator = operator.getChildren().get(1);
        return canMergeOperator(leftOperator) && canMergeOperator(rightOperator);
      }
    }
  }
}
