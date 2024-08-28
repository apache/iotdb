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

package org.apache.iotdb.db.pipe.receiver.transform.converter;

import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class ValueConverter {

  @FunctionalInterface
  private interface Converter {
    Object convert(
        final TSDataType sourceDataType, final TSDataType targetDataType, final Object sourceValue);
  }

  private static final Converter[][] CONVERTER =
      new Converter[TSDataType.values().length][TSDataType.values().length];

  private static final Converter DO_NOTHING_CONVERTER =
      (sourceDataType, targetDataType, sourceValue) -> sourceValue;

  static {
    for (final TSDataType sourceDataType : TSDataType.values()) {
      for (final TSDataType targetDataType : TSDataType.values()) {
        CONVERTER[sourceDataType.ordinal()][targetDataType.ordinal()] = DO_NOTHING_CONVERTER;
      }
    }

    // BOOLEAN
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.BOOLEAN.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToInt32((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToInt64((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToFloat((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToDouble((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToText((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToTimestamp((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToDate((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToBlob((boolean) sourceValue);
    CONVERTER[TSDataType.BOOLEAN.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBooleanToString((boolean) sourceValue);

    // INT32
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToBoolean((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.INT32.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToInt64((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToFloat((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToDouble((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToText((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToTimestamp((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToDate((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToBlob((int) sourceValue);
    CONVERTER[TSDataType.INT32.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt32ToString((int) sourceValue);

    // INT64
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToBoolean((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToInt32((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.INT64.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToFloat((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToDouble((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToText((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertInt64ToTimestamp((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToDate((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToBlob((long) sourceValue);
    CONVERTER[TSDataType.INT64.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertInt64ToString((long) sourceValue);

    // FLOAT
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToBoolean((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToInt32((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToInt64((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.FLOAT.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToDouble((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToText((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertFloatToTimestamp((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToDate((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToBlob((float) sourceValue);
    CONVERTER[TSDataType.FLOAT.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertFloatToString((float) sourceValue);

    // DOUBLE
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertDoubleToBoolean((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDoubleToInt32((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDoubleToInt64((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDoubleToFloat((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.DOUBLE.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDoubleToText((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertDoubleToTimestamp((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDoubleToDate((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDoubleToBlob((double) sourceValue);
    CONVERTER[TSDataType.DOUBLE.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertDoubleToString((double) sourceValue);

    // TEXT
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToBoolean((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToInt32((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToInt64((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToFloat((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToDouble((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.TEXT.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertTextToTimestamp((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToDate((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToBlob((Binary) sourceValue);
    CONVERTER[TSDataType.TEXT.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTextToString((Binary) sourceValue);

    // TIMESTAMP
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertTimestampToBoolean((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertTimestampToInt32((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertTimestampToInt64((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertTimestampToFloat((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertTimestampToDouble((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTimestampToText((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTimestampToDate((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertTimestampToBlob((long) sourceValue);
    CONVERTER[TSDataType.TIMESTAMP.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertTimestampToString((long) sourceValue);

    // DATE
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToBoolean((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToInt32((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToInt64((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToFloat((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToDouble((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToText((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToTimestamp((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.DATE.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToBlob((int) sourceValue);
    CONVERTER[TSDataType.DATE.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertDateToString((int) sourceValue);

    // BLOB
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToBoolean((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToInt32((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToInt64((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToFloat((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToDouble((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToText((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertBlobToTimestamp((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToDate((Binary) sourceValue);
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.BLOB.ordinal()] = DO_NOTHING_CONVERTER;
    CONVERTER[TSDataType.BLOB.ordinal()][TSDataType.STRING.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertBlobToString((Binary) sourceValue);

    // STRING
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.BOOLEAN.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertStringToBoolean((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.INT32.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertStringToInt32((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.INT64.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertStringToInt64((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.FLOAT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertStringToFloat((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.DOUBLE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertStringToDouble((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.TEXT.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertStringToText((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.TIMESTAMP.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) ->
            convertStringToTimestamp((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.DATE.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertStringToDate((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.BLOB.ordinal()] =
        (sourceDataType, targetDataType, sourceValue) -> convertStringToBlob((Binary) sourceValue);
    CONVERTER[TSDataType.STRING.ordinal()][TSDataType.STRING.ordinal()] = DO_NOTHING_CONVERTER;
  }

  public static Object convert(
      final TSDataType sourceDataType, final TSDataType targetDataType, final Object sourceValue) {
    return sourceValue == null
        ? null
        : CONVERTER[sourceDataType.ordinal()][targetDataType.ordinal()].convert(
            sourceDataType, targetDataType, sourceValue);
  }

  ////////////// BOOLEAN //////////////

  private static final Binary BINARY_TRUE = parseString(Boolean.TRUE.toString());
  private static final Binary BINARY_FALSE = parseString(Boolean.FALSE.toString());
  private static final int TRUE_DATE = DateUtils.parseDateExpressionToInt(LocalDate.of(1970, 1, 2));
  private static final int FALSE_DATE =
      DateUtils.parseDateExpressionToInt(LocalDate.of(1970, 1, 1));
  private static final int DEFAULT_DATE =
      DateUtils.parseDateExpressionToInt(LocalDate.of(1970, 1, 1));

  public static int convertBooleanToInt32(final boolean value) {
    return value ? 1 : 0;
  }

  public static long convertBooleanToInt64(final boolean value) {
    return value ? 1L : 0L;
  }

  public static float convertBooleanToFloat(final boolean value) {
    return value ? 1.0f : 0.0f;
  }

  public static double convertBooleanToDouble(final boolean value) {
    return value ? 1.0 : 0.0;
  }

  public static Binary convertBooleanToText(final boolean value) {
    return value ? BINARY_TRUE : BINARY_FALSE;
  }

  public static long convertBooleanToTimestamp(final boolean value) {
    return value ? 1L : 0L;
  }

  public static int convertBooleanToDate(final boolean value) {
    return value ? TRUE_DATE : FALSE_DATE;
  }

  public static Binary convertBooleanToBlob(final boolean value) {
    return value ? BINARY_TRUE : BINARY_FALSE;
  }

  public static Binary convertBooleanToString(final boolean value) {
    return value ? BINARY_TRUE : BINARY_FALSE;
  }

  ///////////// INT32 //////////////

  public static boolean convertInt32ToBoolean(final int value) {
    return value != 0;
  }

  public static long convertInt32ToInt64(final int value) {
    return value;
  }

  public static float convertInt32ToFloat(final int value) {
    return value;
  }

  public static double convertInt32ToDouble(final int value) {
    return value;
  }

  public static Binary convertInt32ToText(final int value) {
    return parseText(Integer.toString(value));
  }

  public static long convertInt32ToTimestamp(final int value) {
    return value;
  }

  public static int convertInt32ToDate(final int value) {
    try {
      DateUtils.parseIntToLocalDate(value);
      return value;
    } catch (Exception e) {
      return DEFAULT_DATE;
    }
  }

  public static Binary convertInt32ToBlob(final int value) {
    return parseBlob(Integer.toString(value));
  }

  public static Binary convertInt32ToString(final int value) {
    return parseString(Integer.toString(value));
  }

  ///////////// INT64 //////////////

  public static boolean convertInt64ToBoolean(final long value) {
    return value != 0;
  }

  public static int convertInt64ToInt32(final long value) {
    return (int) value;
  }

  public static float convertInt64ToFloat(final long value) {
    return value;
  }

  public static double convertInt64ToDouble(final long value) {
    return value;
  }

  public static Binary convertInt64ToText(final long value) {
    return parseText(Long.toString(value));
  }

  public static long convertInt64ToTimestamp(final long value) {
    return value;
  }

  public static int convertInt64ToDate(final long value) {
    try {
      int data = (int) value;
      DateUtils.parseIntToLocalDate(data);
      return data;
    } catch (Exception e) {
      return DEFAULT_DATE;
    }
  }

  public static Binary convertInt64ToBlob(final long value) {
    return parseBlob(Long.toString(value));
  }

  public static Binary convertInt64ToString(final long value) {
    return parseString(Long.toString(value));
  }

  ///////////// FLOAT //////////////

  public static boolean convertFloatToBoolean(final float value) {
    return value != 0;
  }

  public static int convertFloatToInt32(final float value) {
    return (int) value;
  }

  public static long convertFloatToInt64(final float value) {
    return (long) value;
  }

  public static double convertFloatToDouble(final float value) {
    return value;
  }

  public static Binary convertFloatToText(final float value) {
    return parseText(Float.toString(value));
  }

  public static long convertFloatToTimestamp(final float value) {
    return (long) value;
  }

  public static int convertFloatToDate(final float value) {
    try {
      int data = (int) value;
      DateUtils.parseIntToLocalDate(data);
      return data;
    } catch (Exception e) {
      return DEFAULT_DATE;
    }
  }

  public static Binary convertFloatToBlob(final float value) {
    return parseBlob(Float.toString(value));
  }

  public static Binary convertFloatToString(final float value) {
    return parseString(Float.toString(value));
  }

  ///////////// DOUBLE //////////////

  public static boolean convertDoubleToBoolean(final double value) {
    return value != 0;
  }

  public static int convertDoubleToInt32(final double value) {
    return (int) value;
  }

  public static long convertDoubleToInt64(final double value) {
    return (long) value;
  }

  public static float convertDoubleToFloat(final double value) {
    return (float) value;
  }

  public static Binary convertDoubleToText(final double value) {
    return parseText(Double.toString(value));
  }

  public static long convertDoubleToTimestamp(final double value) {
    return (long) value;
  }

  public static int convertDoubleToDate(final double value) {
    try {
      int data = (int) value;
      DateUtils.parseIntToLocalDate(data);
      return data;
    } catch (Exception e) {
      return DEFAULT_DATE;
    }
  }

  public static Binary convertDoubleToBlob(final double value) {
    return parseBlob(Double.toString(value));
  }

  public static Binary convertDoubleToString(final double value) {
    return parseString(Double.toString(value));
  }

  ///////////// TEXT //////////////

  public static boolean convertTextToBoolean(final Binary value) {
    return Boolean.parseBoolean(value.toString());
  }

  public static int convertTextToInt32(final Binary value) {
    return parseInteger(value.toString());
  }

  public static long convertTextToInt64(final Binary value) {
    return parseLong(value.toString());
  }

  public static float convertTextToFloat(final Binary value) {
    return parseFloat(value.toString());
  }

  public static double convertTextToDouble(final Binary value) {
    return parseDouble(value.toString());
  }

  public static long convertTextToTimestamp(final Binary value) {
    return parseTimestamp(value.toString());
  }

  public static int convertTextToDate(final Binary value) {
    return parseDate(value.toString());
  }

  public static Binary convertTextToBlob(final Binary value) {
    return parseBlob(value.toString());
  }

  public static Binary convertTextToString(final Binary value) {
    return value;
  }

  ///////////// TIMESTAMP //////////////

  public static boolean convertTimestampToBoolean(final long value) {
    return value != 0;
  }

  public static int convertTimestampToInt32(final long value) {
    return (int) value;
  }

  public static long convertTimestampToInt64(final long value) {
    return value;
  }

  public static float convertTimestampToFloat(final long value) {
    return value;
  }

  public static double convertTimestampToDouble(final long value) {
    return value;
  }

  public static Binary convertTimestampToText(final long value) {
    return parseText(Long.toString(value));
  }

  public static int convertTimestampToDate(final long value) {
    try {
      Instant instant = Instant.ofEpochMilli(value);
      return DateUtils.parseDateExpressionToInt(instant.atZone(ZoneOffset.UTC).toLocalDate());
    } catch (Exception e) {
      return DEFAULT_DATE;
    }
  }

  public static Binary convertTimestampToBlob(final long value) {
    return parseBlob(Long.toString(value));
  }

  public static Binary convertTimestampToString(final long value) {
    return parseString(Long.toString(value));
  }

  ///////////// DATE //////////////

  public static boolean convertDateToBoolean(final int value) {
    return value != FALSE_DATE;
  }

  public static int convertDateToInt32(final int value) {
    return value;
  }

  public static long convertDateToInt64(final int value) {
    return value;
  }

  public static float convertDateToFloat(final int value) {
    return value;
  }

  public static double convertDateToDouble(final int value) {
    return value;
  }

  public static Binary convertDateToText(final int value) {
    return parseText(Integer.toString(value));
  }

  public static long convertDateToTimestamp(final int value) {
    try {
      LocalDate date = DateUtils.parseIntToLocalDate(value);
      ZonedDateTime dateTime = date.atStartOfDay(ZoneOffset.UTC);
      Instant instant = dateTime.toInstant();
      return instant.toEpochMilli();
    } catch (Exception e) {
      return 0L;
    }
  }

  public static Binary convertDateToBlob(final int value) {
    return parseBlob(Integer.toString(value));
  }

  public static Binary convertDateToString(final int value) {
    return parseString(Integer.toString(value));
  }

  ///////////// BLOB //////////////

  public static boolean convertBlobToBoolean(final Binary value) {
    return Boolean.parseBoolean(value.toString());
  }

  public static int convertBlobToInt32(final Binary value) {
    return parseInteger(value.toString());
  }

  public static long convertBlobToInt64(final Binary value) {
    return parseLong(value.toString());
  }

  public static float convertBlobToFloat(final Binary value) {
    return parseFloat(value.toString());
  }

  public static double convertBlobToDouble(final Binary value) {
    return parseDouble(value.toString());
  }

  public static long convertBlobToTimestamp(final Binary value) {
    return parseTimestamp(value.toString());
  }

  public static int convertBlobToDate(final Binary value) {
    return parseDate(value.toString());
  }

  public static Binary convertBlobToString(final Binary value) {
    return value;
  }

  public static Binary convertBlobToText(final Binary value) {
    return value;
  }

  ///////////// STRING //////////////

  public static boolean convertStringToBoolean(final Binary value) {
    return Boolean.parseBoolean(value.toString());
  }

  public static int convertStringToInt32(final Binary value) {
    return parseInteger(value.toString());
  }

  public static long convertStringToInt64(final Binary value) {
    return parseLong(value.toString());
  }

  public static float convertStringToFloat(final Binary value) {
    return parseFloat(value.toString());
  }

  public static double convertStringToDouble(final Binary value) {
    return parseDouble(value.toString());
  }

  public static long convertStringToTimestamp(final Binary value) {
    return parseTimestamp(value.toString());
  }

  public static int convertStringToDate(final Binary value) {
    return parseDate(value.toString());
  }

  public static Binary convertStringToBlob(final Binary value) {
    return parseBlob(value.toString());
  }

  public static Binary convertStringToText(final Binary value) {
    return value;
  }

  ///////////// UTILS //////////////

  public static Object parse(final String value, final TSDataType dataType) {
    if (value == null) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case INT32:
        return parseInteger(value);
      case INT64:
        return parseLong(value);
      case FLOAT:
        return parseFloat(value);
      case DOUBLE:
        return parseDouble(value);
      case TEXT:
        return parseText(value);
      case TIMESTAMP:
        return parseTimestamp(value);
      case DATE:
        return parseDate(value);
      case BLOB:
        return parseBlob(value);
      case STRING:
        return parseString(value);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static Binary parseBlob(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  private static int parseInteger(final String value) {
    try {
      return Integer.parseInt(value);
    } catch (Exception e) {
      return 0;
    }
  }

  private static long parseLong(final String value) {
    try {
      return Long.parseLong(value);
    } catch (Exception e) {
      return 0L;
    }
  }

  private static float parseFloat(final String value) {
    try {
      return Float.parseFloat(value);
    } catch (Exception e) {
      return 0.0f;
    }
  }

  private static double parseDouble(final String value) {
    try {
      return Double.parseDouble(value);
    } catch (Exception e) {
      return 0.0d;
    }
  }

  private static long parseTimestamp(final String value) {
    if (value == null || value.isEmpty()) {
      return 0L;
    }
    try {
      return TypeInferenceUtils.isNumber(value)
          ? Long.parseLong(value)
          : DateTimeUtils.parseDateTimeExpressionToLong(StringUtils.trim(value), ZoneOffset.UTC);
    } catch (final Exception e) {
      return 0L;
    }
  }

  private static int parseDate(final String value) {
    if (value == null || value.isEmpty()) {
      return DEFAULT_DATE;
    }
    try {
      if (TypeInferenceUtils.isNumber(value)) {
        int date = Integer.parseInt(value);
        DateUtils.parseIntToLocalDate(date);
        return date;
      }
      return DateTimeUtils.parseDateExpressionToInt(StringUtils.trim(value));
    } catch (final Exception e) {
      return DEFAULT_DATE;
    }
  }

  private static Binary parseString(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  private static Binary parseText(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  private ValueConverter() {
    // forbidden to construct
  }
}
