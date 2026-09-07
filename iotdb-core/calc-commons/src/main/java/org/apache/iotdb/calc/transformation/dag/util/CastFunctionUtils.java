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

package org.apache.iotdb.calc.transformation.dag.util;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;

import org.apache.tsfile.read.common.type.Type;

import javax.annotation.Nonnull;

import java.time.ZoneId;

public class CastFunctionUtils {
  public static final String ERROR_MSG = "Unsupported target dataType: %s";

  public static int castLongToInt(long value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new SemanticException(
          String.format("long value %d is out of range of integer value.", value));
    }
    return (int) value;
  }

  public static int castFloatToInt(float value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new SemanticException(
          String.format("Float value %f is out of range of integer value.", value));
    }
    return Math.round(value);
  }

  public static long castFloatToLong(float value) {
    if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
      throw new SemanticException(
          String.format("Float value %f is out of range of long value.", value));
    }
    return Math.round((double) value);
  }

  public static int castDoubleToInt(double value) {
    if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new SemanticException(
          String.format("Double value %f is out of range of integer value.", value));
    }
    return Math.round((float) value);
  }

  public static long castDoubleToLong(double value) {
    if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
      throw new SemanticException(
          String.format("Double value %f is out of range of long value.", value));
    }
    return Math.round(value);
  }

  public static float castDoubleToFloat(double value) {
    if (value > Float.MAX_VALUE || value < -Float.MAX_VALUE) {
      throw new SemanticException(
          String.format("Double value %f is out of range of float value.", value));
    }
    return (float) value;
  }

  public static float castTextToFloat(String value) {
    float f = Float.parseFloat(value);
    if (f == Float.POSITIVE_INFINITY || f == Float.NEGATIVE_INFINITY) {
      throw new SemanticException(
          String.format("Text value %s is out of range of float value.", value));
    }
    return f;
  }

  public static double castTextToDouble(String value) {
    double d = Double.parseDouble(value);
    if (d == Double.POSITIVE_INFINITY || d == Double.NEGATIVE_INFINITY) {
      throw new SemanticException(
          String.format("Text value %s is out of range of double value.", value));
    }
    return d;
  }

  public static boolean castTextToBoolean(String value) {
    String lowerCase = value.toLowerCase();
    if (lowerCase.equals("true")) {
      return true;
    } else if (lowerCase.equals("false")) {
      return false;
    } else {
      throw new SemanticException(
          String.format(CalcMessages.INVALID_TEXT_INPUT_FOR_BOOLEAN, value));
    }
  }

  // used by IrExpressionInterpreter to do constant folding
  public static Object cast(
      @Nonnull Object value, Type sourceType, Type targetType, SessionInfo session) {
    TypeServices.CastObjectInputService inputService =
        TypeServices.CAST_OBJECT_INPUT_SERVICE.call(sourceType);
    TypeServices.CastObjectValueService valueService =
        TypeServices.CAST_OBJECT_VALUE_SERVICE.call(targetType);
    ZoneId zoneId = session == null ? null : session.getZoneId();
    return inputService.cast(value, valueService, zoneId);
  }
}
