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
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;

public class FilterUtils {

  public static String getFilterStringValue(Filter filter) {
    return filter.toString().split(filter.getOperatorType().getSymbol())[1].trim();
  }

  public static String getFilterSymbol(Filter filter) {
    return filter.getOperatorType().getSymbol();
  }

  public static Filter expressionTypeToFilter(ExpressionType expressionType, String value) {
    // MeasurementIndex and DataType are not used here.
    switch (expressionType) {
      case EQUAL_TO:
        return ValueFilterApi.eq(0, new Binary(value.getBytes()), TSDataType.STRING);
      case NON_EQUAL:
        return ValueFilterApi.notEq(0, new Binary(value.getBytes()), TSDataType.STRING);
      case LESS_EQUAL:
        return ValueFilterApi.ltEq(0, new Binary(value.getBytes()), TSDataType.STRING);
      case LESS_THAN:
        return ValueFilterApi.lt(0, new Binary(value.getBytes()), TSDataType.STRING);
      case GREATER_EQUAL:
        return ValueFilterApi.gtEq(0, new Binary(value.getBytes()), TSDataType.STRING);
      case GREATER_THAN:
        return ValueFilterApi.gt(0, new Binary(value.getBytes()), TSDataType.STRING);
      default:
        throw new IllegalArgumentException("Unsupported expression type:" + expressionType);
    }
  }
}
