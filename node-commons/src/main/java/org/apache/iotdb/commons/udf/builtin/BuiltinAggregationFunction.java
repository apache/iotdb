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

package org.apache.iotdb.commons.udf.builtin;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum BuiltinAggregationFunction {
  MIN_TIME("min_time"),
  MAX_TIME("max_time"),
  MAX_VALUE("max_value"),
  MIN_VALUE("min_value"),
  EXTREME("extreme"),
  FIRST_VALUE("first_value"),
  LAST_VALUE("last_value"),
  COUNT("count"),
  AVG("avg"),
  SUM("sum"),
  COUNT_IF("count_if"),
  TIME_DURATION("time_duration");

  private final String functionName;

  BuiltinAggregationFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.stream(BuiltinAggregationFunction.values())
              .map(BuiltinAggregationFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }

  /** @return if the Aggregation can use statistics to optimize */
  public static boolean canUseStatistics(String name) {
    final String functionName = name.toLowerCase();
    switch (functionName) {
      case "min_time":
      case "max_time":
      case "max_value":
      case "min_value":
      case "extreme":
      case "first_value":
      case "last_value":
      case "count":
      case "avg":
      case "sum":
      case "time_duration":
        return true;
      case "count_if":
        return false;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + name);
    }
  }

  // TODO Maybe we can merge this method with canUseStatistics(),
  //  new method returns three level push-down: No push-down, DataRegion, SeriesScan
  /** @return if the Aggregation can split to multi phases */
  public static boolean canSplitToMultiPhases(String name) {
    final String functionName = name.toLowerCase();
    switch (functionName) {
      case "min_time":
      case "max_time":
      case "max_value":
      case "min_value":
      case "extreme":
      case "first_value":
      case "last_value":
      case "count":
      case "avg":
      case "sum":
      case "time_duration":
        return true;
      case "count_if":
        return false;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + name);
    }
  }
}
