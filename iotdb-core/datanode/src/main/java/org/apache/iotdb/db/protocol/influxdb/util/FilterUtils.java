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
package org.apache.iotdb.db.protocol.influxdb.util;

import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class FilterUtils {

  public static String getFilterStringValue(Filter filter) {
    if (filter == null) {
      throw new UnSupportedDataTypeException("Null filter");
    }
    String filterString = filter.toString();
    if (filter.getOperatorType().equals(OperatorType.VALUE_EQ)) {
      return filterString.split("== ")[1];
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_NEQ)) {
      return filterString.split("!= ")[1];
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_LTEQ)) {
      return filterString.split("<= ")[1];
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_LT)) {
      return filterString.split("< ")[1];
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_GTEQ)) {
      return filterString.split(">= ")[1];
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_GT)) {
      return filterString.split("> ")[1];
    } else {
      throw new UnSupportedDataTypeException("Unsupported filter :" + filter);
    }
  }

  public static String getFilerSymbol(Filter filter) {
    if (filter == null) {
      throw new UnSupportedDataTypeException("Null filter");
    }
    if (filter.getOperatorType().equals(OperatorType.VALUE_EQ)) {
      return "=";
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_NEQ)) {
      return "!=";
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_LTEQ)) {
      return "<=";
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_LT)) {
      return "<";
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_GTEQ)) {
      return ">=";
    } else if (filter.getOperatorType().equals(OperatorType.VALUE_GT)) {
      return ">";
    } else {
      throw new UnSupportedDataTypeException("Unsupported filter :" + filter);
    }
  }

  public static Filter expressionTypeToFilter(ExpressionType expressionType, String value) {
    // MeasurementIndex and DataType are not used here.
    switch (expressionType) {
      case EQUAL_TO:
        return ValueFilterApi.eq(0, value, TSDataType.INT32);
      case NON_EQUAL:
        return ValueFilterApi.notEq(0, value, TSDataType.INT32);
      case LESS_EQUAL:
        return ValueFilterApi.ltEq(0, value, TSDataType.INT32);
      case LESS_THAN:
        return ValueFilterApi.lt(0, value, TSDataType.INT32);
      case GREATER_EQUAL:
        return ValueFilterApi.gtEq(0, value, TSDataType.INT32);
      case GREATER_THAN:
        return ValueFilterApi.gt(0, value, TSDataType.INT32);
      default:
        throw new IllegalArgumentException("Unsupported expression type:" + expressionType);
    }
  }
}
