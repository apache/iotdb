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

import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class FilterUtils {

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

  public static Filter expressionTypeToFilter(ExpressionType expressionType, String value) {
    switch (expressionType) {
      case EQUAL_TO:
        return ValueFilter.eq(value);
      case NON_EQUAL:
        return ValueFilter.notEq(value);
      case LESS_EQUAL:
        return ValueFilter.ltEq(value);
      case LESS_THAN:
        return ValueFilter.lt(value);
      case GREATER_EQUAL:
        return ValueFilter.gtEq(value);
      case GREATER_THAN:
        return ValueFilter.gt(value);
      default:
        throw new IllegalArgumentException("Unsupported expression type:" + expressionType);
    }
  }
}
