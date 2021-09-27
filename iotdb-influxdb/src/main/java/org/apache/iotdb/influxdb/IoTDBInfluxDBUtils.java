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

package org.apache.iotdb.influxdb;

import org.apache.iotdb.influxdb.qp.constant.FilterConstant;
import org.apache.iotdb.influxdb.qp.logical.Operator;
import org.apache.iotdb.influxdb.qp.logical.crud.*;
import org.apache.iotdb.tsfile.read.common.Field;

import org.influxdb.dto.QueryResult;

import java.util.*;

public final class IoTDBInfluxDBUtils {

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
   * sum the query results of two influxdb (that is, by default, they will not have duplicate data
   * and will be directly added together)
   *
   * @param queryResult1 query result1
   * @param queryResult2 query result2
   * @return sum of two query results
   */
  public static QueryResult addQueryResultProcess(
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
    values1.addAll(values2);
    updateQueryResultValue(queryResult1, values1);
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
        FilterOperator leftOperator = operator.getChildOperators().get(0);
        FilterOperator rightOperator = operator.getChildOperators().get(1);
        return canMergeOperator(leftOperator) && canMergeOperator(rightOperator);
      }
    }
  }

  /**
   * generate query conditions through the syntax tree (if you enter this function, it means that it
   * must be a syntax tree that can be merged, and there is no or)
   *
   * @param filterOperator the syntax tree of query criteria needs to be generated
   * @return condition list
   */
  public static List<Condition> getConditionsByFilterOperatorOperator(
      FilterOperator filterOperator) {
    if (filterOperator instanceof BasicFunctionOperator) {
      // It must be a non-or situation
      List<Condition> conditions = new ArrayList<>();
      conditions.add(getConditionForBasicFunctionOperator((BasicFunctionOperator) filterOperator));
      return conditions;
    } else {
      FilterOperator leftOperator = filterOperator.getChildOperators().get(0);
      FilterOperator rightOperator = filterOperator.getChildOperators().get(1);
      List<Condition> conditions1 = getConditionsByFilterOperatorOperator(leftOperator);
      List<Condition> conditions2 = getConditionsByFilterOperatorOperator(rightOperator);
      conditions1.addAll(conditions2);
      return conditions1;
    }
  }

  /**
   * conditions are generated from subtrees of unique conditions
   *
   * @param basicFunctionOperator subtree to generate condition
   * @return corresponding conditions
   */
  public static Condition getConditionForBasicFunctionOperator(
      BasicFunctionOperator basicFunctionOperator) {
    Condition condition = new Condition();
    condition.setFilterType(basicFunctionOperator.getFilterType());
    condition.setValue(basicFunctionOperator.getKeyName());
    condition.setLiteral(basicFunctionOperator.getValue());
    return condition;
  }

  /**
   * get the last node through the path in iotdb
   *
   * @param path path to process
   * @return last node
   */
  public static String getFiledByPath(String path) {
    String[] tmpList = path.split("\\.");
    return tmpList[tmpList.length - 1];
  }

  /**
   * generate a list of the same layer path from a list of rows example: input:{root.d.s.x.s.field1
   * , root.d.s.x.s.field2 , root.d.s.xx.ss.filed3 , root.d.s.xx.ss.filed1 ,
   * root.d.s.xxx.sss.field4} output:{1,3,4} corresponding to the subscripts 1, 3 and 4 of the list.
   * That is, the last subscript of the current same layer path. It is easy to distinguish the same
   * set of data
   *
   * @param columnNames list of row names to be generated
   * @return the collection of the last subscript in the same layer path
   */
  public static ArrayList<Integer> getSamePathForList(List<String> columnNames) {
    ArrayList<Integer> list = new ArrayList<>();
    // record the last result to determine whether it is repeated with the current result
    String lastPath = null;
    for (int i = 1; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      String path = columnName.substring(0, columnName.lastIndexOf("."));
      if (i == 1) {
        lastPath = path;
      } else {
        if (!lastPath.equals(path)) {
          list.add(i - 1);
          lastPath = path;
        }
      }
    }
    list.add(columnNames.size() - 1);
    return list;
  }

  /**
   * convert the value of field in iotdb to object
   *
   * @param field filed to be converted
   * @return value stored in field
   */
  public static Object iotdbFiledCvt(Field field) {
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
   * check whether the field is empty. If it is empty, an error will be thrown
   *
   * @param string string to check
   * @param name prompt information in error throwing
   */
  public static void checkNonEmptyString(String string, String name)
      throws IllegalArgumentException {
    if (string == null || string.isEmpty()) {
      throw new IllegalArgumentException("Expecting a non-empty string for " + name);
    }
  }

  /**
   * check whether the operator generated by SQL is a valid query statement. If it is not, an error
   * will be thrown
   *
   * @param operator operator to check
   */
  public static void checkQueryOperator(Operator operator) {
    if (!(operator instanceof QueryOperator)) {
      throw new IllegalArgumentException("not query sql");
    }
    SelectComponent selectComponent = ((QueryOperator) operator).getSelectComponent();
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

  public static String generateFunctionSql(String functionName, String functionParam, String path) {
    return String.format(
        "select %s(%s) from %s align by device", functionName, functionParam, path);
  }
}
