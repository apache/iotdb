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

import org.apache.iotdb.commons.udf.builtin.String.UDTFConcat;
import org.apache.iotdb.commons.udf.builtin.String.UDTFEndsWith;
import org.apache.iotdb.commons.udf.builtin.String.UDTFLower;
import org.apache.iotdb.commons.udf.builtin.String.UDTFStartsWith;
import org.apache.iotdb.commons.udf.builtin.String.UDTFStrCompare;
import org.apache.iotdb.commons.udf.builtin.String.UDTFStrLength;
import org.apache.iotdb.commons.udf.builtin.String.UDTFStrLocate;
import org.apache.iotdb.commons.udf.builtin.String.UDTFTrim;
import org.apache.iotdb.commons.udf.builtin.String.UDTFUpper;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** All built-in UDFs need to register their function names and classes here. */
public enum BuiltinTimeSeriesGeneratingFunction {
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
  EXP("EXP", UDTFExp.class),
  LN("LN", UDTFLog.class),
  LOG10("LOG10", UDTFLog10.class),
  SQRT("SQRT", UDTFSqrt.class),
  STRING_CONTAINS("STRING_CONTAINS", UDTFContains.class),
  STRING_MATCHES("STRING_MATCHES", UDTFMatches.class),
  STRING_LENGTH("LENGTH", UDTFStrLength.class),
  STRING_LOCATE("LOCATE", UDTFStrLocate.class),
  STRING_STARTS_WITH("STARTSWITH", UDTFStartsWith.class),
  STRING_ENDS_WITH("ENDSWITH", UDTFEndsWith.class),
  STRING_CONCAT("CONCAT", UDTFConcat.class),
  STRING_UPPER("UPPER", UDTFUpper.class),
  STRING_LOWER("LOWER", UDTFLower.class),
  STRING_TRIM("TRIM", UDTFTrim.class),
  STRING_CMP("STRCMP", UDTFStrCompare.class),
  CHANGE_POINTS("CHANGE_POINTS", UDTFChangePoints.class),
  DIFFERENCE("DIFFERENCE", UDTFCommonValueDifference.class),
  NON_NEGATIVE_DIFFERENCE("NON_NEGATIVE_DIFFERENCE", UDTFNonNegativeValueDifference.class),
  TIME_DIFFERENCE("TIME_DIFFERENCE", UDTFTimeDifference.class),
  DERIVATIVE("DERIVATIVE", UDTFCommonDerivative.class),
  NON_NEGATIVE_DERIVATIVE("NON_NEGATIVE_DERIVATIVE", UDTFNonNegativeDerivative.class),
  TOP_K("TOP_K", UDTFTopK.class),
  BOTTOM_K("BOTTOM_K", UDTFBottomK.class),
  IN_RANGE("IN_RANGE", UDTFInRange.class),
  ON_OFF("ON_OFF", UDTFOnOff.class),
  ZERO_DURATION("ZERO_DURATION", UDTFZeroDuration.class),
  NON_ZERO_DURATION("NON_ZERO_DURATION", UDTFNonZeroDuration.class),
  ZERO_COUNT("ZERO_COUNT", UDTFZeroCount.class),
  NON_ZERO_COUNT("NON_ZERO_COUNT", UDTFNonZeroCount.class),
  EQUAL_SIZE_BUCKET_RANDOM_SAMPLE(
      "EQUAL_SIZE_BUCKET_RANDOM_SAMPLE", UDTFEqualSizeBucketRandomSample.class),
  EQUAL_SIZE_BUCKET_AGG_SAMPLE("EQUAL_SIZE_BUCKET_AGG_SAMPLE", UDTFEqualSizeBucketAggSample.class),
  EQUAL_SIZE_BUCKET_M4_SAMPLE("EQUAL_SIZE_BUCKET_M4_SAMPLE", UDTFEqualSizeBucketM4Sample.class),
  EQUAL_SIZE_BUCKET_OUTLIER_SAMPLE(
      "EQUAL_SIZE_BUCKET_OUTLIER_SAMPLE", UDTFEqualSizeBucketOutlierSample.class),
  JEXL("JEXL", UDTFJexl.class),
  MASTER_REPAIR("MASTER_REPAIR", UDTFMasterRepair.class),
  M4("M4", UDTFM4.class);

  private final String functionName;
  private final Class<?> functionClass;
  private final String className;

  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.stream(BuiltinTimeSeriesGeneratingFunction.values())
              .map(BuiltinTimeSeriesGeneratingFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }

  /**
   * Set of functions are mappable but DeviceView of them also need special process. Now there is no
   * function satisfies this.
   */
  public static final Set<String> DEVICE_VIEW_SPECIAL_PROCESS_FUNCTIONS = ImmutableSet.of();

  BuiltinTimeSeriesGeneratingFunction(String functionName, Class<?> functionClass) {
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
