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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.Type;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;

public enum TableBuiltinAggregationFunction {
  SUM("sum"),
  COUNT("count"),
  AVG("avg"),
  EXTREME("extreme"),
  MAX("max"),
  MIN("min"),
  FIRST("first"),
  FIRST_BY("first_by"),
  LAST("last"),
  LAST_BY("last_by"),
  MODE("mode"),
  MAX_BY("max_by"),
  MIN_BY("min_by"),
  STDDEV("stddev"),
  STDDEV_POP("stddev_pop"),
  STDDEV_SAMP("stddev_samp"),
  VARIANCE("variance"),
  VAR_POP("var_pop"),
  VAR_SAMP("var_samp"),
  ;

  private final String functionName;

  TableBuiltinAggregationFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.stream(org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction.values())
              .map(org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }

  /**
   * @return if the Aggregation can use statistics to optimize
   */
  public static boolean canUseStatistics(String name) {
    final String functionName = name.toLowerCase();
    switch (functionName) {
      case "sum":
      case "count":
      case "avg":
      case "extreme":
      case "max":
      case "min":
      case "first":
      case "last":
      case "time_duration":
        return true;
      case "first_by":
      case "last_by":
      case "mode":
      case "max_by":
      case "min_by":
      case "stddev":
      case "stddev_pop":
      case "stddev_samp":
      case "variance":
      case "var_pop":
      case "var_samp":
        return false;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + name);
    }
  }

  public static List<Type> getIntermediateTypes(String name, Type originalType) {
    if (AVG.functionName.equalsIgnoreCase(name)) {
      return ImmutableList.of(DOUBLE, INT32);
    } else {
      return ImmutableList.of(originalType);
    }
  }
}
