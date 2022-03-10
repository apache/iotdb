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

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
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
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
      iExpressions.add(
          InfluxDBUtils.getIExpressionForBasicFunctionOperator((BasicFunctionOperator) operator));
      return queryByConditions(
          iExpressions, database, measurement, serviceProvider, fieldOrders, sessionId);
    } else {
      FilterOperator leftOperator = operator.getChildren().get(0);
      FilterOperator rightOperator = operator.getChildren().get(1);
      if (operator.getFilterType() == FilterConstant.FilterType.KW_OR) {
        return InfluxDBUtils.orQueryResultProcess(
            queryExpr(leftOperator, database, measurement, serviceProvider, fieldOrders, sessionId),
            queryExpr(
                rightOperator, database, measurement, serviceProvider, fieldOrders, sessionId));
      } else if (operator.getFilterType() == FilterConstant.FilterType.KW_AND) {
        if (InfluxDBUtils.canMergeOperator(leftOperator)
            && InfluxDBUtils.canMergeOperator(rightOperator)) {
          List<IExpression> iExpressions1 =
              InfluxDBUtils.getIExpressionByFilterOperatorOperator(leftOperator);
          List<IExpression> iExpressions2 =
              InfluxDBUtils.getIExpressionByFilterOperatorOperator(rightOperator);
          iExpressions1.addAll(iExpressions2);
          return queryByConditions(
              iExpressions1, database, measurement, serviceProvider, fieldOrders, sessionId);
        } else {
          return InfluxDBUtils.andQueryResultProcess(
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
          if (!checkEqualsContainNull(value1.get(t), value2.get(t))) {
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
   * judge whether two instances are equal, and allow null check
   *
   * @param o1 object1
   * @param o2 object2
   * @return are they equal
   */
  public static boolean checkEqualsContainNull(Object o1, Object o2) {
    if (o1 == null && o2 == null) {
      return true;
    } else if (o1 == null) {
      return false;
    } else if (o2 == null) {
      return false;
    } else {
      return o1.equals(o2);
    }
  }

  /**
   * determine whether the two string lists are the same
   *
   * @param list1 first list to compare
   * @param list2 second list to compare
   * @return Is it the same
   */
  private static boolean checkSameStringList(List<String> list1, List<String> list2) {
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (int i = 0; i < list1.size(); i++) {
        if (!list1.get(i).equals(list2.get(i))) {
          return false;
        }
      }
    }
    return true;
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
        && checkSameStringList(
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
          if (!checkEqualsContainNull(value1.get(t), value2.get(t))) {
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
    Map<String, Integer> tagOrders =
        InfluxDBMetaManager.getDatabase2Measurement2TagOrders().get(database).get(measurement);
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
                InfluxDBUtils.removeQuotation(
                    getFilterStringValue(realTagOrders.get(i).getFilter())));
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
          .append((getFilerSymbol(singleSeriesExpression.getFilter())))
          .append(" ")
          .append(getFilterStringValue(singleSeriesExpression.getFilter()));
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
      TSStatus tsStatus = serviceProvider.checkAuthority(queryPlan, sessionId);
      if (tsStatus != null) {
        throw new AuthException(tsStatus.getMessage());
      }
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              realQuerySql,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, queryPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
      return iotdbResultConvertInfluxResult(queryDataSet, database, measurement, fieldOrders);
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
   * Convert align by device query result of iotdb to the query result of influxdb
   *
   * @param queryDataSet iotdb query results to be converted
   * @return query results in influxdb format
   */
  private static QueryResult iotdbResultConvertInfluxResult(
      QueryDataSet queryDataSet,
      String database,
      String measurement,
      Map<String, Integer> fieldOrders)
      throws IOException {

    if (queryDataSet == null) {
      return InfluxDBUtils.getNullQueryResult();
    }
    // generate series
    QueryResult.Series series = new QueryResult.Series();
    series.setName(measurement);
    // gets the reverse map of the tag
    Map<String, Integer> tagOrders =
        InfluxDBMetaManager.getDatabase2Measurement2TagOrders().get(database).get(measurement);
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
        Object o = InfluxDBUtils.iotdbFiledConvert(fields.get(i));
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
   * if the first and last of the current string are quotation marks, they are removed
   *
   * @param str string to process
   * @return string after processing
   */
  public static String removeQuotation(String str) {
    if (str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"') {
      return str.substring(1, str.length() - 1);
    }
    return str;
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
        filterTypeToFilter(
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
                    .updateValue(new InfluxFunctionValue(selectedValue, selectedTimestamp));
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
    InfluxDBUtils.updateQueryResultColumnValue(
        queryResult, InfluxDBUtils.removeDuplicate(newColumns), values);
  }

  /**
   * remove string list duplicate names
   *
   * @param strings the list of strings with duplicate names needs to be removed
   * @return list of de duplicated strings
   */
  public static List<String> removeDuplicate(List<String> strings) {
    Map<String, Integer> nameNums = new HashMap<>();
    List<String> result = new ArrayList<>();
    for (String tmpString : strings) {
      if (!nameNums.containsKey(tmpString)) {
        nameNums.put(tmpString, 1);
        result.add(tmpString);
      } else {
        int nums = nameNums.get(tmpString);
        result.add(tmpString + "_" + nums);
        nameNums.put(tmpString, nums + 1);
      }
    }
    return result;
  }

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
            InfluxFunctionFactory.generateFunctionByProvider(
                functionName, expression.getExpressions(), path, serviceProvider));
        columns.add(functionName);
      }
    }

    List<Object> value = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (InfluxFunction function : functions) {
      // if(){
      //
      // }
      // select
      //
      InfluxFunctionValue functionValue = function.calculateByIoTDBFunc();
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

  public static String generateFunctionSql(String functionName, String parameter, String path) {
    return String.format("select %s(%s) from %s.**", functionName, parameter, path);
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

  public static Filter filterTypeToFilter(FilterConstant.FilterType filterType, String value) {
    switch (filterType) {
      case EQUAL:
        return ValueFilter.eq(value);
      case NOTEQUAL:
        return ValueFilter.notEq(value);
      case LESSTHANOREQUALTO:
        return ValueFilter.ltEq(value);
      case LESSTHAN:
        return ValueFilter.lt(value);
      case GREATERTHANOREQUALTO:
        return ValueFilter.gtEq(value);
      case GREATERTHAN:
        return ValueFilter.gt(value);
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + filterType);
    }
  }

  public static String getFilterStringValue(Filter filter) {
    String filterString = filter.toString();
    if (filter instanceof ValueFilter.ValueEq) {
      return filterString.split("== ")[1];
    } else if (filter instanceof ValueFilter.ValueNotEq) {
      return filterString.split("!= ")[1];
    } else if (filter instanceof ValueFilter.ValueLtEq) {
      return filterString.split("<= ")[1];
    } else if (filter instanceof ValueFilter.ValueLt) {
      return filterString.split("< ")[1];
    } else if (filter instanceof ValueFilter.ValueGtEq) {
      return filterString.split(">= ")[1];
    } else if (filter instanceof ValueFilter.ValueGt) {
      return filterString.split("> ")[1];
    } else {
      throw new UnSupportedDataTypeException("Unsupported filter :" + filter);
    }
  }

  public static String getFilerSymbol(Filter filter) {
    if (filter instanceof ValueFilter.ValueEq) {
      return "=";
    } else if (filter instanceof ValueFilter.ValueNotEq) {
      return "!=";
    } else if (filter instanceof ValueFilter.ValueLtEq) {
      return "<=";
    } else if (filter instanceof ValueFilter.ValueLt) {
      return "<";
    } else if (filter instanceof ValueFilter.ValueGtEq) {
      return ">=";
    } else if (filter instanceof ValueFilter.ValueGt) {
      return ">";
    } else {
      throw new UnSupportedDataTypeException("Unsupported filter :" + filter);
    }
  }
}
