/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.itbase.constant;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum BuiltinAggregationFunctionEnum {
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
  ;

  private final String functionName;

  BuiltinAggregationFunctionEnum(String functionName) {
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
}
