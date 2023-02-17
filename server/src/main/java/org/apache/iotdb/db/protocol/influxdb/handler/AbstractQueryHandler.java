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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxSqlConstant;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunction;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionFactory;
import org.apache.iotdb.db.protocol.influxdb.function.InfluxFunctionValue;
import org.apache.iotdb.db.protocol.influxdb.function.aggregator.InfluxAggregator;
import org.apache.iotdb.db.protocol.influxdb.function.selector.InfluxSelector;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManagerFactory;
import org.apache.iotdb.db.protocol.influxdb.statement.InfluxQueryStatement;
import org.apache.iotdb.db.protocol.influxdb.statement.InfluxSelectComponent;
import org.apache.iotdb.db.protocol.influxdb.util.FilterUtils;
import org.apache.iotdb.db.protocol.influxdb.util.JacksonUtils;
import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxQueryResultRsp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;

import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Used to process influxdb query requests, this abstract class defines some template methods */
public abstract class AbstractQueryHandler {

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
  abstract InfluxFunctionValue updateByIoTDBFunc(
      String database, String measurement, InfluxFunction function, long sessionid);

  /**
   * The method that needs to be implemented is to query the result according to the query SQL
   * supported by IoTDB
   *
   * @param querySql query sql
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param tagOrders tag orders
   * @param fieldOrders field orders
   * @param sessionId session id
   * @return query result
   * @throws AuthException
   */
  abstract QueryResult queryByConditions(
      String querySql,
      String database,
      String measurement,
      Map<String, Integer> tagOrders,
      Map<String, Integer> fieldOrders,
      long sessionId)
      throws AuthException;

  public final InfluxQueryResultRsp queryInfluxDB(
      String database, InfluxQueryStatement queryStatement, long sessionId) {
    String measurement = queryStatement.getFromComponent().getPrefixPaths().get(0).getFullPath();
    // The list of fields under the current measurement and the order of the specified rules
    Map<String, Integer> fieldOrders =
        InfluxDBMetaManagerFactory.getInstance().getFieldOrders(database, measurement, sessionId);
    QueryResult queryResult;
    InfluxQueryResultRsp tsQueryResultRsp = new InfluxQueryResultRsp();
    try {
      // contain filter condition or have common query the result of by traversal.
      if (queryStatement.hasWhere()
          || queryStatement.getSelectComponent().isHasCommonQuery()
          || queryStatement.getSelectComponent().isHasOnlyTraverseFunction()) {
        // step1 : generate query results
        queryResult =
            queryExpr(
                queryStatement.hasWhere()
                    ? queryStatement.getWhereCondition().getPredicate()
                    : null,
                database,
                measurement,
                fieldOrders,
                sessionId);
        // step2 : select filter
        ProcessSelectComponent(queryResult, queryStatement.getSelectComponent());
      }
      // don't contain filter condition and only have function use iotdb function.
      else {
        queryResult =
            queryFuncWithoutFilter(
                queryStatement.getSelectComponent(), database, measurement, sessionId);
      }
      return tsQueryResultRsp
          .setResultJsonString(JacksonUtils.bean2Json(queryResult))
          .setStatus(RpcUtils.getInfluxDBStatus(TSStatusCode.SUCCESS_STATUS));
    } catch (AuthException e) {
      return tsQueryResultRsp.setStatus(
          RpcUtils.getInfluxDBStatus(e.getCode().getStatusCode(), e.getMessage()));
    }
  }

  /**
   * further process the obtained query result through the query criteria of select
   *
   * @param queryResult query results to be processed
   * @param selectComponent select conditions to be filtered
   */
  public void ProcessSelectComponent(
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
    newColumns.add(InfluxSqlConstant.RESERVED_TIME);

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
          if (!columnName.equals(InfluxSqlConstant.STAR)) {
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
          if (InfluxSqlConstant.getNativeSelectorFunctionNames().contains(column)) {
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
          if (value.isEmpty()) {
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
              .equals(InfluxSqlConstant.STAR)) {
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

  /**
   * Query the select result. By default, there are no filter conditions. The functions to be
   * queried use the built-in iotdb functions
   *
   * @param selectComponent select data to query
   * @return select query result
   */
  public QueryResult queryFuncWithoutFilter(
      InfluxSelectComponent selectComponent, String database, String measurement, long sessionid) {
    // columns
    List<String> columns = new ArrayList<>();
    columns.add(InfluxSqlConstant.RESERVED_TIME);

    List<InfluxFunction> functions = new ArrayList<>();

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
      InfluxFunctionValue functionValue =
          updateByIoTDBFunc(database, measurement, function, sessionid);
      //      InfluxFunctionValue functionValue = function.calculateByIoTDBFunc();
      if (value.isEmpty()) {
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

  public QueryResult queryExpr(
      Expression predicate,
      String database,
      String measurement,
      Map<String, Integer> fieldOrders,
      Long sessionId)
      throws AuthException {
    if (predicate == null) {
      return queryByConditions(
          Collections.emptyList(), database, measurement, fieldOrders, sessionId);
    } else if (predicate instanceof CompareBinaryExpression) {
      return queryByConditions(
          convertToIExpressions(predicate), database, measurement, fieldOrders, sessionId);
    } else if (predicate instanceof LogicOrExpression) {
      return QueryResultUtils.orQueryResultProcess(
          queryExpr(
              ((LogicOrExpression) predicate).getLeftExpression(),
              database,
              measurement,
              fieldOrders,
              sessionId),
          queryExpr(
              ((LogicOrExpression) predicate).getRightExpression(),
              database,
              measurement,
              fieldOrders,
              sessionId));
    } else if (predicate instanceof LogicAndExpression) {
      if (canMergePredicate(predicate)) {
        return queryByConditions(
            convertToIExpressions(predicate), database, measurement, fieldOrders, sessionId);
      } else {
        return QueryResultUtils.andQueryResultProcess(
            queryExpr(
                ((LogicAndExpression) predicate).getLeftExpression(),
                database,
                measurement,
                fieldOrders,
                sessionId),
            queryExpr(
                ((LogicAndExpression) predicate).getRightExpression(),
                database,
                measurement,
                fieldOrders,
                sessionId));
      }
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  /**
   * get query results in the format of influxdb through conditions
   *
   * @param expressions list of conditions, including tag and field condition
   * @return returns the results of the influxdb query
   */
  public QueryResult queryByConditions(
      List<IExpression> expressions,
      String database,
      String measurement,
      Map<String, Integer> fieldOrders,
      Long sessionId)
      throws AuthException {
    // used to store the actual order according to the tag
    Map<Integer, SingleSeriesExpression> realTagOrders = new HashMap<>();
    // stores a list of conditions belonging to the field
    List<SingleSeriesExpression> fieldExpressions = new ArrayList<>();
    // maximum number of tags in the current query criteria
    int currentQueryMaxTagNum = 0;
    Map<String, Integer> tagOrders =
        InfluxDBMetaManagerFactory.getInstance().getTagOrders(database, measurement, sessionId);
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
    if (currentQueryMaxTagNum < tagOrders.size()) {
      curQueryPath.append(".**");
    }
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
    return queryByConditions(realQuerySql, database, measurement, null, fieldOrders, sessionId);
  }

  /**
   * generate query conditions through the syntax tree (if you enter this function, it means that it
   * must be a syntax tree that can be merged, and there is no or)
   *
   * @param predicate the syntax tree of query criteria needs to be generated
   * @return condition list
   */
  public List<IExpression> convertToIExpressions(Expression predicate) {
    if (predicate instanceof CompareBinaryExpression) {
      Expression leftExpression = ((CompareBinaryExpression) predicate).getLeftExpression();
      Expression rightExpression = ((CompareBinaryExpression) predicate).getRightExpression();
      if (!(leftExpression instanceof TimeSeriesOperand
          && rightExpression instanceof ConstantOperand)) {
        throw new SemanticException("Unsupported predicate: " + predicate);
      }
      SingleSeriesExpression singleSeriesExpression =
          new SingleSeriesExpression(
              ((TimeSeriesOperand) leftExpression).getPath(),
              FilterUtils.expressionTypeToFilter(
                  predicate.getExpressionType(),
                  ((ConstantOperand) rightExpression).getValueString()));
      return Collections.singletonList(singleSeriesExpression);
    } else if (predicate instanceof LogicBinaryExpression) {
      List<IExpression> iExpressions = new ArrayList<>();
      iExpressions.addAll(
          convertToIExpressions(((LogicBinaryExpression) predicate).getLeftExpression()));
      iExpressions.addAll(
          convertToIExpressions(((LogicBinaryExpression) predicate).getRightExpression()));
      return iExpressions;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  /**
   * judge whether the subtrees of the syntax tree have or operations. If not, the query can be
   * merged
   *
   * @param predicate subtree to judge
   * @return can merge queries
   */
  public boolean canMergePredicate(Expression predicate) {
    if (predicate instanceof CompareBinaryExpression) {
      return true;
    } else if (predicate instanceof LogicOrExpression) {
      return false;
    } else if (predicate instanceof LogicAndExpression) {
      return canMergePredicate(((LogicAndExpression) predicate).getLeftExpression())
          && canMergePredicate(((LogicAndExpression) predicate).getRightExpression());
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }
}
