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

package org.apache.iotdb.db.query.udf.builtin;

/** All built-in UDFs need to register their function names and classes here. */
public enum BuiltinFunction {
  CONST("CONST", UDTFConst.class),
  E("E", UDTFConstE.class),
  PI("PI", UDTFConstPi.class),
  SIN("SIN", UDTFSin.class),
  COS("COS", UDTFCos.class),
  TAN("TAN", UDTFTan.class),
  ASIN("ASIN", UDTFAsin.class),
  ACOS("ACOS", UDTFAcos.class),
  ATAN("ATAN", UDTFAtan.class),
  SINH("SINH", UDTFSinh.class),
  COSH("COSH", UDTFCosh.class),
  TANH("TANH", UDTFTanh.class),
  DEGREES("DEGREES", UDTFDegrees.class),
  RADIANS("RADIANS", UDTFRadians.class),
  ABS("ABS", UDTFAbs.class),
  SIGN("SIGN", UDTFSign.class),
  CEIL("CEIL", UDTFCeil.class),
  FLOOR("FLOOR", UDTFFloor.class),
  ROUND("ROUND", UDTFRound.class),
  EXP("EXP", UDTFExp.class),
  LN("LN", UDTFLog.class),
  LOG10("LOG10", UDTFLog10.class),
  SQRT("SQRT", UDTFSqrt.class),
  STRING_CONTAINS("STRING_CONTAINS", UDTFContains.class),
  STRING_MATCHES("STRING_MATCHES", UDTFMatches.class),
  DIFFERENCE("DIFFERENCE", UDTFCommonValueDifference.class),
  NON_NEGATIVE_DIFFERENCE("NON_NEGATIVE_DIFFERENCE", UDTFNonNegativeValueDifference.class),
  TIME_DIFFERENCE("TIME_DIFFERENCE", UDTFTimeDifference.class),
  DERIVATIVE("DERIVATIVE", UDTFCommonDerivative.class),
  NON_NEGATIVE_DERIVATIVE("NON_NEGATIVE_DERIVATIVE", UDTFNonNegativeDerivative.class),
  TOP_K("TOP_K", UDTFTopK.class),
  BOTTOM_K("BOTTOM_K", UDTFBottomK.class),
  ;

  private final String functionName;
  private final Class<?> functionClass;
  private final String className;

  BuiltinFunction(String functionName, Class<?> functionClass) {
    this.functionName = functionName;
    this.functionClass = functionClass;
    this.className = functionClass.getName();
  }

  public String getFunctionName() {
    return functionName;
  }

  public Class<?> getFunctionClass() {
    return functionClass;
  }

  public String getClassName() {
    return className;
  }
}
