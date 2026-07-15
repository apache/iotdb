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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import com.sun.jna.platform.win32.Variant;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.function.Function;

public class TypeServices {

  public static final int DEFAULT_DATE =
      DateUtils.parseDateExpressionToInt(LocalDate.of(1970, 1, 1));

  public static final TypeService<Function<String, Object>> VALUE_PARSER_NO_EXCEPTION_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> Boolean::parseBoolean;
            case INT32 -> TypeServices::parseInteger;
            case INT64 -> TypeServices::parseLong;
            case FLOAT -> TypeServices::parseFloat;
            case DOUBLE -> TypeServices::parseDouble;
            case TEXT -> TypeServices::parseText;
            case TIMESTAMP -> TypeServices::parseTimestamp;
            case DATE -> TypeServices::parseDate;
            case BLOB -> TypeServices::parseBlob;
            case STRING -> TypeServices::parseString;
            default -> throw new UnSupportedDataTypeException(CalcMessages.UNKNOWN_DATATYPE + type);
          };

  public static final TypeService<Short> OPC_DA_VARIANT_TYPE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> Variant.VT_BOOL;
            case INT32 -> Variant.VT_I4;
            case INT64 -> Variant.VT_I8;
            case DATE, TIMESTAMP -> Variant.VT_DATE;
            case FLOAT -> Variant.VT_R4;
            case DOUBLE -> Variant.VT_R8;
            // Note that "Variant" does not support "VT_BLOB" data, and not all the DA servers
            // support this, thus we use "VT_BSTR" to substitute.
            case TEXT, STRING, BLOB, OBJECT -> Variant.VT_BSTR;
            default ->
                throw new UnSupportedDataTypeException(
                    DataNodePipeMessages.UNSUPPORTED_DATATYPE + type.getTypeEnum());
          };

  public static final TypeService<Function<Object, String>> OPC_UA_VALUE_STRINGIFIER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, BLOB, STRING -> Object::toString;
            case DATE ->
                value -> ((LocalDate) value).atStartOfDay(ZoneId.systemDefault()).toString();
            case TIMESTAMP -> value -> DateTimeUtils.convertLongToDate((long) value);
            default ->
                value -> {
                  throw new PipeRuntimeNonCriticalException(
                      DataNodePipeMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  public static final TypeService<Function<Boolean, Type>>
      PIPE_INSERT_EVENT_VALUE_LIST_TYPE_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, TEXT, BLOB, OBJECT, STRING ->
                    ignored -> type;
                case DATE ->
                    isDateStoredAsLocalDate ->
                        isDateStoredAsLocalDate ? type : Type.fromTsDataType(TSDataType.INT32);
                default ->
                    ignored -> {
                      throw new UnSupportedDataTypeException(
                          DataNodePipeMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                    };
              };

  static {
    OPC_UA_VALUE_STRINGIFIER_SERVICE.check();
    PIPE_INSERT_EVENT_VALUE_LIST_TYPE_SERVICE.check();
  }

  public static int parseInteger(final String value) {
    try {
      return Integer.parseInt(value);
    } catch (final Exception e) {
      return 0;
    }
  }

  public static long parseLong(final String value) {
    try {
      return Long.parseLong(value);
    } catch (final Exception e) {
      return 0L;
    }
  }

  public static float parseFloat(final String value) {
    try {
      return Float.parseFloat(value);
    } catch (final Exception e) {
      return 0.0f;
    }
  }

  public static double parseDouble(final String value) {
    try {
      return Double.parseDouble(value);
    } catch (final Exception e) {
      return 0.0d;
    }
  }

  public static Binary parseBlob(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static Binary parseString(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static Binary parseText(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }

  public static long parseTimestamp(final String value) {
    if (value == null || value.isEmpty()) {
      return 0L;
    }
    try {
      return TypeInferenceUtils.isNumber(value)
          ? Long.parseLong(value)
          : DataNodeDateTimeUtils.parseDateTimeExpressionToLong(
              StringUtils.trim(value), ZoneOffset.UTC);
    } catch (final Exception e) {
      return 0L;
    }
  }

  public static int parseDate(final String value) {
    if (value == null) {
      return DEFAULT_DATE;
    }
    final String trimmedValue = StringUtils.trim(value);
    if (trimmedValue.isEmpty()) {
      return DEFAULT_DATE;
    }
    if (TypeInferenceUtils.isNumber(trimmedValue)) {
      try {
        int date = Integer.parseInt(trimmedValue);
        DateUtils.parseIntToLocalDate(date);
        return date;
      } catch (final Exception e) {
        return DEFAULT_DATE;
      }
    }
    try {
      return DateTimeUtils.parseDateExpressionToInt(trimmedValue);
    } catch (final Exception e) {
      return parseDateTimeToDate(trimmedValue);
    }
  }

  public static int parseDateTimeToDate(final String value) {
    try {
      return DateUtils.parseDateExpressionToInt(
          Instant.ofEpochMilli(
                  DateTimeUtils.convertDatetimeStrToLong(value, ZoneOffset.UTC, 0, "ms"))
              .atZone(ZoneOffset.UTC)
              .toLocalDate());
    } catch (final Exception e) {
      return DEFAULT_DATE;
    }
  }
}
