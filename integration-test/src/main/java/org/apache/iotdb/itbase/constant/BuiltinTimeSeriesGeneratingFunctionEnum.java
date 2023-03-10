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

/** All built-in UDFs need to register their function names and classes here. */
public enum BuiltinTimeSeriesGeneratingFunctionEnum {
  CONST("CONST"),
  E("E"),
  PI("PI"),
  SIN("SIN"),
  COS("COS"),
  TAN("TAN"),
  ASIN("ASIN"),
  ACOS("ACOS"),
  ATAN("ATAN"),
  SINH("SINH"),
  COSH("COSH"),
  TANH("TANH"),
  DEGREES("DEGREES"),
  RADIANS("RADIANS"),
  ABS("ABS"),
  SIGN("SIGN"),
  CEIL("CEIL"),
  FLOOR("FLOOR"),
  ROUND("ROUND"),
  EXP("EXP"),
  LN("LN"),
  LOG10("LOG10"),
  SQRT("SQRT"),
  STRING_CONTAINS("STRING_CONTAINS"),
  STRING_MATCHES("STRING_MATCHES"),
  STRING_LENGTH("LENGTH"),
  STRING_LOCATE("LOCATE"),
  STRING_STARTS_WITH("STARTSWITH"),
  STRING_ENDS_WITH("ENDSWITH"),
  STRING_CONCAT("CONCAT"),
  STRING_SUBSTR("SUBSTR"),
  STRING_UPPER("UPPER"),
  STRING_LOWER("LOWER"),
  STRING_TRIM("TRIM"),
  STRING_CMP("STRCMP"),
  CHANGE_POINTS(" CHANGE_POINTS"),
  DIFFERENCE("DIFFERENCE"),
  NON_NEGATIVE_DIFFERENCE("NON_NEGATIVE_DIFFERENCE"),
  TIME_DIFFERENCE("TIME_DIFFERENCE"),
  DERIVATIVE("DERIVATIVE"),
  NON_NEGATIVE_DERIVATIVE("NON_NEGATIVE_DERIVATIVE"),
  TOP_K("TOP_K"),
  BOTTOM_K("BOTTOM_K"),
  IN_RANGE("IN_RANGE"),
  ON_OFF("ON_OFF"),
  ZERO_DURATION("ZERO_DURATION"),
  NON_ZERO_DURATION("NON_ZERO_DURATION"),
  ZERO_COUNT("ZERO_COUNT"),
  NON_ZERO_COUNT("NON_ZERO_COUNT"),
  EQUAL_SIZE_BUCKET_RANDOM_SAMPLE("EQUAL_SIZE_BUCKET_RANDOM_SAMPLE"),
  EQUAL_SIZE_BUCKET_AGG_SAMPLE("EQUAL_SIZE_BUCKET_AGG_SAMPLE"),
  EQUAL_SIZE_BUCKET_M4_SAMPLE("EQUAL_SIZE_BUCKET_M4_SAMPLE"),
  EQUAL_SIZE_BUCKET_OUTLIER_SAMPLE("EQUAL_SIZE_BUCKET_OUTLIER_SAMPLE"),
  JEXL("JEXL"),
  MASTER_REPAIR("MASTER_REPAIR"),
  M4("M4");

  private final String functionName;

  BuiltinTimeSeriesGeneratingFunctionEnum(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }
}
