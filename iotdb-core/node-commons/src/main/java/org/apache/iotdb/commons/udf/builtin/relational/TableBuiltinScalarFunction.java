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

package org.apache.iotdb.commons.udf.builtin.relational;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableBuiltinScalarFunction {
  DIFF("diff"),
  CAST("cast"),
  ROUND("round"),
  REPLACE("replace"),
  SUBSTRING("substring"),
  LENGTH("length"),
  UPPER("upper"),
  LOWER("lower"),
  TRIM("trim"),
  LTRIM("ltrim"),
  RTRIM("rtrim"),
  REGEXP_LIKE("regexp_like"),
  STRPOS("strpos"),
  STARTS_WITH("starts_with"),
  ENDS_WITH("ends_with"),
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
  PI("pi"),
  E("e"),
  DATE_BIN("date_bin"),
  ;

  private final String functionName;

  TableBuiltinScalarFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> BUILT_IN_SCALAR_FUNCTION_NAME =
      new HashSet<>(
          Arrays.stream(TableBuiltinScalarFunction.values())
              .map(TableBuiltinScalarFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getBuiltInScalarFunctionName() {
    return BUILT_IN_SCALAR_FUNCTION_NAME;
  }
}
