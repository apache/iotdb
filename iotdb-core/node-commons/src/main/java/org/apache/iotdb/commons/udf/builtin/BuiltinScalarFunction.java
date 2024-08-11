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

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum BuiltinScalarFunction {
  DIFF("diff"),
  CAST("cast"),
  ROUND("round"),
  REPLACE("replace"),
  SUBSTRING("substring"),
  LENGTH("length"),
  UPPER("upper"),
  LOWER("lower"),
  TRIM("trim"),
  STRING_CONTAINS("string_contains"),
  STRING_MATCHES("string_matches"),
  LOCATE("locate"),
  STARTS_WITH("startswith"),
  ENDS_WITH("endswith"),
  CONCAT("concat"),
  STRCMP("strcmp"),
  SIN("sin"),
  COS("cos"),
  TAN("tan"),
  ASIN("asin"),
  ACOS("acos"),
  ATAN("atan"),
  SINH("sinh"),
  COSH("cosh"),
  TANH("tanh"),
  DEGREES("degrees"),
  RADIANS("radians"),
  ABS("abs"),
  SIGN("sign"),
  CEIL("ceil"),
  FLOOR("floor"),
  EXP("exp"),
  LN("ln"),
  LOG10("log10"),
  SQRT("sqrt"),
  ;

  private final String functionName;

  BuiltinScalarFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.stream(BuiltinScalarFunction.values())
              .map(BuiltinScalarFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static boolean contains(String functionName) {
    return NATIVE_FUNCTION_NAMES.contains(functionName.toLowerCase());
  }

  /**
   * We shouldn't apply these functions to each DataRegion for one device, because these functions
   * need all data to calculate correct result. So we need to collect all data for one device in one
   * DataRegion, and then apply these functions to only that one DataRegion.
   */
  public static final Set<String> DEVICE_VIEW_SPECIAL_PROCESS_FUNCTIONS = ImmutableSet.of("diff");

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }
}
