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

package org.apache.iotdb.pipe.it.dual;

import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BytesUtils;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

public class TypeConversionSemanticCase {

  public static final int ROW_COUNT = 3;

  public static final List<TypeConversionSemanticCase> CASES =
      Arrays.asList(
          c(
              "bool_to_int32",
              TSDataType.BOOLEAN,
              TSDataType.INT32,
              values("true", "false", "true"),
              values("1", "0", "1")),
          c(
              "bool_to_int64",
              TSDataType.BOOLEAN,
              TSDataType.INT64,
              values("true", "false", "true"),
              values("1", "0", "1")),
          c(
              "bool_to_float",
              TSDataType.BOOLEAN,
              TSDataType.FLOAT,
              values("true", "false", "true"),
              values("1.0", "0.0", "1.0")),
          c(
              "bool_to_double",
              TSDataType.BOOLEAN,
              TSDataType.DOUBLE,
              values("true", "false", "true"),
              values("1.0", "0.0", "1.0")),
          c(
              "bool_to_text",
              TSDataType.BOOLEAN,
              TSDataType.TEXT,
              values("true", "false", "true"),
              values("true", "false", "true")),
          c(
              "bool_to_blob",
              TSDataType.BOOLEAN,
              TSDataType.BLOB,
              values("true", "false", "true"),
              values(blobValue("true"), blobValue("false"), blobValue("true"))),
          c(
              "bool_to_string",
              TSDataType.BOOLEAN,
              TSDataType.STRING,
              values("true", "false", "true"),
              values("true", "false", "true")),
          c(
              "bool_to_date",
              TSDataType.BOOLEAN,
              TSDataType.DATE,
              values("true", "false", "true"),
              values("1970-01-02", "1970-01-01", "1970-01-02")),
          c(
              "bool_to_timestamp",
              TSDataType.BOOLEAN,
              TSDataType.TIMESTAMP,
              values("true", "false", "true"),
              values(timestampValue(1), timestampValue(0), timestampValue(1))),
          c(
              "int32_to_boolean",
              TSDataType.INT32,
              TSDataType.BOOLEAN,
              values("0", "2", "-1"),
              values("false", "true", "true")),
          c(
              "int32_to_timestamp",
              TSDataType.INT32,
              TSDataType.TIMESTAMP,
              values("0", "1", "86400000"),
              values(timestampValue(0), timestampValue(1), timestampValue(86400000))),
          c(
              "int32_to_date",
              TSDataType.INT32,
              TSDataType.DATE,
              values("19700102", "20240229", "42"),
              values("1970-01-02", "2024-02-29", "1970-01-01")),
          c(
              "int64_to_int32",
              TSDataType.INT64,
              TSDataType.INT32,
              values("2147483648", "-2147483649", "42"),
              values("-2147483648", "2147483647", "42")),
          c(
              "int64_to_boolean",
              TSDataType.INT64,
              TSDataType.BOOLEAN,
              values("0", "2", "-1"),
              values("false", "true", "true")),
          c(
              "int64_to_date",
              TSDataType.INT64,
              TSDataType.DATE,
              values("19700102", "2147483648", "19700103"),
              values("1970-01-02", "1970-01-01", "1970-01-03")),
          c(
              "float_to_int32",
              TSDataType.FLOAT,
              TSDataType.INT32,
              values("2.9", "-2.9", "0.0"),
              values("2", "-2", "0")),
          c(
              "float_to_boolean",
              TSDataType.FLOAT,
              TSDataType.BOOLEAN,
              values("0.0", "0.1", "-0.1"),
              values("false", "true", "true")),
          c(
              "float_to_date",
              TSDataType.FLOAT,
              TSDataType.DATE,
              values("19700102.9", "19700103.1", "42.9"),
              values("1970-01-02", "1970-01-03", "1970-01-01")),
          c(
              "double_to_int64",
              TSDataType.DOUBLE,
              TSDataType.INT64,
              values("3.9", "-3.9", "0.0"),
              values("3", "-3", "0")),
          c(
              "double_to_boolean",
              TSDataType.DOUBLE,
              TSDataType.BOOLEAN,
              values("0.0", "0.1", "-0.1"),
              values("false", "true", "true")),
          c(
              "double_to_timestamp",
              TSDataType.DOUBLE,
              TSDataType.TIMESTAMP,
              values("1.9", "86400000.9", "0.0"),
              values(timestampValue(1), timestampValue(86400000), timestampValue(0))),
          c(
              "text_to_int32",
              TSDataType.TEXT,
              TSDataType.INT32,
              values("'123.9'", "'bad'", "'-123.9'"),
              values("123", "0", "-123")),
          c(
              "string_to_int64",
              TSDataType.STRING,
              TSDataType.INT64,
              values("'456.9'", "'bad'", "'-456.9'"),
              values("456", "0", "-456")),
          c(
              "blob_to_float",
              TSDataType.BLOB,
              TSDataType.FLOAT,
              values(blobSql("7.5"), blobSql("bad"), blobSql("-7.5")),
              values("7.5", "0.0", "-7.5")),
          c(
              "text_to_double",
              TSDataType.TEXT,
              TSDataType.DOUBLE,
              values("'8.25'", "'bad'", "'-8.25'"),
              values("8.25", "0.0", "-8.25")),
          c(
              "text_to_boolean",
              TSDataType.TEXT,
              TSDataType.BOOLEAN,
              values("'true'", "'1'", "'TrUe'"),
              values("true", "false", "true")),
          c(
              "string_to_boolean",
              TSDataType.STRING,
              TSDataType.BOOLEAN,
              values("'TRUE'", "'false'", "'yes'"),
              values("true", "false", "false")),
          c(
              "blob_to_boolean",
              TSDataType.BLOB,
              TSDataType.BOOLEAN,
              values(blobSql("true"), blobSql("FALSE"), blobSql("0")),
              values("true", "false", "false")),
          c(
              "text_to_timestamp",
              TSDataType.TEXT,
              TSDataType.TIMESTAMP,
              values("'86400000'", "'1970-01-02T00:00:00.000'", "'bad'"),
              values(timestampValue(86400000), timestampValue(86400000), timestampValue(0))),
          c(
              "string_to_timestamp",
              TSDataType.STRING,
              TSDataType.TIMESTAMP,
              values("'1970-01-03T00:00:00.000'", "'bad'", "'86400000'"),
              values(timestampValue(172800000), timestampValue(0), timestampValue(86400000))),
          c(
              "blob_to_timestamp",
              TSDataType.BLOB,
              TSDataType.TIMESTAMP,
              values(blobSql("bad"), blobSql("1"), blobSql("1970-01-02T00:00:00.000")),
              values(timestampValue(0), timestampValue(1), timestampValue(86400000))),
          c(
              "text_to_date",
              TSDataType.TEXT,
              TSDataType.DATE,
              values("'19700102'", "'1970-01-04'", "'bad'"),
              values("1970-01-02", "1970-01-04", "1970-01-01")),
          c(
              "string_to_date",
              TSDataType.STRING,
              TSDataType.DATE,
              values("'1970-01-03'", "'19700105'", "'1970-01-07'"),
              values("1970-01-03", "1970-01-05", "1970-01-07")),
          c(
              "blob_to_date",
              TSDataType.BLOB,
              TSDataType.DATE,
              values(blobSql("bad"), blobSql("1970-01-06"), blobSql("19700108")),
              values("1970-01-01", "1970-01-06", "1970-01-08")),
          c(
              "timestamp_to_date",
              TSDataType.TIMESTAMP,
              TSDataType.DATE,
              values("0", "86399999", "86400000"),
              values("1970-01-01", "1970-01-01", "1970-01-02")),
          c(
              "date_to_timestamp",
              TSDataType.DATE,
              TSDataType.TIMESTAMP,
              values("'1970-01-01'", "'1970-01-02'", "'1970-01-03'"),
              values(timestampValue(0), timestampValue(86400000), timestampValue(172800000))),
          c(
              "timestamp_to_boolean",
              TSDataType.TIMESTAMP,
              TSDataType.BOOLEAN,
              values("0", "-1", "1"),
              values("false", "true", "true")),
          c(
              "date_to_boolean",
              TSDataType.DATE,
              TSDataType.BOOLEAN,
              values("'1970-01-01'", "'1970-01-02'", "'1969-12-31'"),
              values("false", "true", "true")));

  public final String measurement;
  public final TSDataType sourceType;
  public final TSDataType targetType;
  public final String[] sourceSqlValues;
  public final String[] expectedValues;

  private TypeConversionSemanticCase(
      final String measurement,
      final TSDataType sourceType,
      final TSDataType targetType,
      final String[] sourceSqlValues,
      final String[] expectedValues) {
    this.measurement = measurement;
    this.sourceType = sourceType;
    this.targetType = targetType;
    this.sourceSqlValues = sourceSqlValues;
    this.expectedValues = expectedValues;
  }

  private static TypeConversionSemanticCase c(
      final String measurement,
      final TSDataType sourceType,
      final TSDataType targetType,
      final String[] sourceSqlValues,
      final String[] expectedValues) {
    return new TypeConversionSemanticCase(
        measurement, sourceType, targetType, sourceSqlValues, expectedValues);
  }

  private static String[] values(final String... values) {
    return values;
  }

  public static String timestampValue(final long timestamp) {
    return RpcUtils.formatDatetime("default", "ms", timestamp, ZoneOffset.UTC);
  }

  private static String blobSql(final String value) {
    final StringBuilder builder = new StringBuilder("X'");
    for (final byte b : value.getBytes(StandardCharsets.UTF_8)) {
      builder.append(String.format("%02x", b & 0xFF));
    }
    return builder.append("'").toString();
  }

  private static String blobValue(final String value) {
    return BytesUtils.parseBlobByteArrayToString(value.getBytes(StandardCharsets.UTF_8));
  }
}
