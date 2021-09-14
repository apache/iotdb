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

package org.apache.iotdb.infludb;

import org.apache.iotdb.infludb.qp.constant.FilterConstant;
import org.apache.iotdb.infludb.qp.logical.Operator;
import org.apache.iotdb.infludb.qp.logical.crud.*;
import org.apache.iotdb.tsfile.read.common.Field;
import org.influxdb.dto.QueryResult;

import java.util.*;

public final class IotDBInfluxDBUtils {


    /**
     * 如果当前字符串的第一个和最后一个是引号，则将其去除
     *
     * @param str 需要处理的字符串
     * @return 处理之后的字符串
     */
    public static String removeQuotation(String str) {
        if (str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"') {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }


    /**
     * 将两个influxdb的查询结果取交集
     *
     * @param queryResult1 第一个查询结果
     * @param queryResult2 第二个查询结果
     * @return 两个查询结果取交集
     */
    public static QueryResult andQueryResultProcess(QueryResult queryResult1, QueryResult queryResult2) {
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
                    //如果有不相等的话，那么跳过当前j
                    if (!checkEqualsContainNull(value1.get(t), value2.get(t))) {
                        allEqual = false;
                        break;
                    }
                }
                //此时匹配完，如果完全相等
                if (allEqual) {
                    sameValues.add(value1);
                }
            }
        }
        updateQueryResultValue(queryResult1, sameValues);
        return queryResult1;
    }


    /**
     * 将两个influxdb的查询结果取并集
     *
     * @param queryResult1 第一个查询结果
     * @param queryResult2 第二个查询结果
     * @return 两个查询结果取并集
     */
    public static QueryResult orQueryResultProcess(QueryResult queryResult1, QueryResult queryResult2) {
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
                    //如果有不相等的话，那么跳过当前j
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
        //values2加上 不相同的valueList
        values2.addAll(notSameValuesInValues1);
        updateQueryResultValue(queryResult1, values2);
        return queryResult1;
    }


    /**
     * 将两个influxdb的查询结果取和（即默认二者不会有重复的数据，直接加在一起）
     *
     * @param queryResult1 第一个查询结果
     * @param queryResult2 第二个查询结果
     * @return 两个查询结果取和
     */
    public static QueryResult addQueryResultProcess(QueryResult queryResult1, QueryResult queryResult2) {
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
        //values相加
        values1.addAll(values2);
        updateQueryResultValue(queryResult1, values1);
        return queryResult1;
    }

    /**
     * 将新的values更新到influxdb的查询结果中
     *
     * @param queryResult  待更新的influxdb查询结果
     * @param updateValues 待更新的values
     */
    private static void updateQueryResultValue(QueryResult queryResult, List<List<Object>> updateValues) {
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
     * 将新的values更新到influxdb的查询结果中
     *
     * @param queryResult  待更新的influxdb查询结果
     * @param columns      待更新的columns
     * @param updateValues 待更新的values
     */
    public static void updateQueryResultColumnValue(QueryResult queryResult, List<String> columns, List<List<Object>> updateValues) {
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
     * 检查两个influxdb的查询结果是否属于同一个查询，即measurement和columns是否一致
     *
     * @param queryResult1 待检查结果1
     * @param queryResult2 待检查结果2
     * @return 是否属于同一个查询
     */
    private static boolean checkSameQueryResult(QueryResult queryResult1, QueryResult queryResult2) {
        return queryResult1.getResults().get(0).getSeries().get(0).getName().
                equals(queryResult2.getResults().get(0).getSeries().get(0).getName()) &&
                checkSameStringList(queryResult1.getResults().get(0).getSeries().get(0).getColumns(),
                        queryResult2.getResults().get(0).getSeries().get(0).getColumns());
    }

    /**
     * 判断两个字符串列表是否相同
     *
     * @param list1 需要比对的第一个列表
     * @param list2 需要比对的第二个列表
     * @return 是否相同
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
     * 判断该语法树的子树是否有or操作，如果没有均为and操作的话，则可以合并查询
     *
     * @param operator 需要判断的子树
     * @return 是否可以合并查询
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
     * 通过语法树来生成查询条件（如果进入这个函数，说明一定是可以合并的语法树，不存在or的情况）
     *
     * @param filterOperator 需要生成查询条件的语法树
     * @return 条件列表
     */
    public static List<Condition> getConditionsByFilterOperatorOperator(FilterOperator filterOperator) {
        if (filterOperator instanceof BasicFunctionOperator) {
            //一定会是非or的情况
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
     * 通过唯一条件的子树来生成条件
     *
     * @param basicFunctionOperator 需要生成条件的子树
     * @return 对应的条件
     */
    public static Condition getConditionForBasicFunctionOperator(BasicFunctionOperator basicFunctionOperator) {
        Condition condition = new Condition();
        condition.setFilterType(basicFunctionOperator.getFilterType());
        condition.setValue(basicFunctionOperator.getKeyName());
        condition.setLiteral(basicFunctionOperator.getValue());
        return condition;
    }

    /**
     * 通过iotdb中的path来获取最后一个节点
     *
     * @param path 需要处理的path
     * @return 最后一个节点
     */
    //截取最后一个点
    public static String getFiledByPath(String path) {
        String[] tmpList = path.split("\\.");
        return tmpList[tmpList.length - 1];
    }

    /**
     * 通过一个行列表来生成相同layerPath的列表
     * example:
     * input:{root.d.s.x.s.field1 , root.d.s.x.s.field2 , root.d.s.xx.ss.filed3 , root.d.s.xx.ss.filed1 , root.d.s.xxx.sss.field4}
     * output:{1,3,4}
     * 对应的是列表下标的1，3和4。也就是当前同一layerPath的最后一个下标。便于区分同一组数据
     *
     * @param columnNames 需要生成列表的行名称列表
     * @return 相同layerPath中最后一个下标的集合
     */
    public static ArrayList<Integer> getSamePathForList(List<String> columnNames) {
        ArrayList<Integer> list = new ArrayList<>();
        //记录上一个结果么，用来判断是否和当前重复
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
     * 将iotdb中field的值转换成object
     *
     * @param field 需要转换的filed
     * @return field中存储的值
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
     * 获得一个空值的QueryResult
     *
     * @return 空值QueryResult
     */
    public static QueryResult getNullQueryResult() {
        QueryResult queryResult = new QueryResult();
        QueryResult.Result result = new QueryResult.Result();
        queryResult.setResults(Arrays.asList(result));
        return queryResult;
    }

    /**
     * 以指定的方式来检查QueryResult是否为null
     *
     * @param queryResult 待检查的QueryResult
     * @return 是否为null
     */
    public static boolean checkQueryResultNull(QueryResult queryResult) {
        return queryResult.getResults().get(0).getSeries() == null;
    }

    /**
     * 判断两个实例是否相等，允许null检查
     *
     * @param o1 实例1
     * @param o2 实例2
     * @return 是否相等
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
     * 检查该字段是否为空,如果为空，则抛出错误
     *
     * @param string 需要检查的字段
     * @param name   抛错中的提示信息
     */
    public static void checkNonEmptyString(String string, String name) throws IllegalArgumentException {
        if (string == null || string.isEmpty()) {
            throw new IllegalArgumentException("Expecting a non-empty string for " + name);
        }
    }

    /**
     * 检查sql生成的operator是否为合法的查询语句，如果不合法，则抛出错误
     *
     * @param operator 需要检查的operator
     */
    public static void checkQueryOperator(Operator operator) {
        if (!(operator instanceof QueryOperator)) {
            throw new IllegalArgumentException("not query sql");
        }
        SelectComponent selectComponent = ((QueryOperator) operator).getSelectComponent();
        if (selectComponent.isHasMoreSelectorFunction() && selectComponent.isHasCommonQuery()) {
            throw new IllegalArgumentException("ERR: mixing multiple selector functions with tags or fields is not supported");
        }
        if (selectComponent.isHasAggregationFunction() && selectComponent.isHasCommonQuery()) {
            throw new IllegalArgumentException("ERR: mixing aggregate and non-aggregate queries is not supported");
        }
    }

    /**
     * 去除字符串列表重名
     *
     * @param strings 需要去除重名的字符串列表
     * @return 去重后的字符串列表
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
}
