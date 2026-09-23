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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rpc;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.function.Function;

final class TypeServices {

  static final TypeService<Function<byte[], String>> JDBC_STRING_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> value -> String.valueOf(BytesUtils.bytesToBool(value));
            case INT32 -> value -> String.valueOf(BytesUtils.bytesToInt(value));
            case INT64, TIMESTAMP -> value -> String.valueOf(BytesUtils.bytesToLong(value));
            case FLOAT -> value -> String.valueOf(BytesUtils.bytesToFloat(value));
            case DOUBLE -> value -> String.valueOf(BytesUtils.bytesToDouble(value));
            case TEXT, STRING -> value -> new String(value, StandardCharsets.UTF_8);
            case OBJECT -> BytesUtils::parseObjectByteArrayToString;
            case BLOB -> BytesUtils::parseBlobByteArrayToString;
            case DATE -> value -> DateUtils.formatDate(BytesUtils.bytesToInt(value));
            case ROW, UNKNOWN, VECTOR -> ignored -> null;
          };

  static final TypeService<Function<byte[], Object>> JDBC_OBJECT_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BytesUtils::bytesToBool;
            case INT32 -> BytesUtils::bytesToInt;
            case INT64 -> BytesUtils::bytesToLong;
            case FLOAT -> BytesUtils::bytesToFloat;
            case DOUBLE -> BytesUtils::bytesToDouble;
            case TEXT, STRING -> value -> new String(value, StandardCharsets.UTF_8);
            case OBJECT, BLOB -> Binary::new;
            case TIMESTAMP -> value -> new Timestamp(BytesUtils.bytesToLong(value));
            case DATE -> value -> DateUtils.parseIntToDate(BytesUtils.bytesToInt(value));
            case ROW, UNKNOWN, VECTOR -> ignored -> null;
          };

  static final TypeService<RpcObjectReader> RPC_OBJECT_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE ->
                (actualType, column, position, timeFactor) ->
                    actualType.getObject(column, position);
            case TIMESTAMP ->
                (actualType, column, position, timeFactor) ->
                    RpcUtils.convertToTimestamp(actualType.getLong(column, position), timeFactor);
            case TEXT, STRING ->
                (actualType, column, position, timeFactor) ->
                    actualType
                        .getBinary(column, position)
                        .getStringValue(TSFileConfig.STRING_CHARSET);
            case OBJECT ->
                (actualType, column, position, timeFactor) ->
                    BytesUtils.parseObjectByteArrayToString(
                        actualType.getBinary(column, position).getValues());
            case BLOB ->
                (actualType, column, position, timeFactor) ->
                    BytesUtils.parseBlobByteArrayToString(
                        actualType.getBinary(column, position).getValues());
            case DATE ->
                (actualType, column, position, timeFactor) ->
                    DateUtils.formatDate(actualType.getInt(column, position));
            case ROW, UNKNOWN, VECTOR -> (actualType, column, position, timeFactor) -> null;
          };

  static final TypeService<RpcStringReader> RPC_STRING_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    String.valueOf(actualType.getBoolean(column, position));
            case INT32 ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    String.valueOf(actualType.getInt(column, position));
            case INT64 ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    String.valueOf(actualType.getLong(column, position));
            case FLOAT ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    String.valueOf(actualType.getFloat(column, position));
            case DOUBLE ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    String.valueOf(actualType.getDouble(column, position));
            case TIMESTAMP ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    RpcUtils.formatDatetime(
                        timeFormat, timePrecision, actualType.getLong(column, position), zoneId);
            case TEXT, STRING ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    actualType
                        .getBinary(column, position)
                        .getStringValue(TSFileConfig.STRING_CHARSET);
            case OBJECT ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    BytesUtils.parseObjectByteArrayToString(
                        actualType.getBinary(column, position).getValues());
            case BLOB ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    BytesUtils.parseBlobByteArrayToString(
                        actualType.getBinary(column, position).getValues());
            case DATE ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) ->
                    DateUtils.formatDate(actualType.getInt(column, position));
            case ROW, UNKNOWN, VECTOR ->
                (actualType, column, position, timeFormat, timePrecision, zoneId) -> null;
          };

  static {
    JDBC_STRING_READER_SERVICE.check();
    JDBC_OBJECT_READER_SERVICE.check();
    RPC_OBJECT_READER_SERVICE.check();
    RPC_STRING_READER_SERVICE.check();
  }

  private TypeServices() {}

  @FunctionalInterface
  interface RpcObjectReader {

    Object read(Type type, Column column, int position, int timeFactor);
  }

  @FunctionalInterface
  interface RpcStringReader {

    String read(
        Type type,
        Column column,
        int position,
        String timeFormat,
        String timePrecision,
        ZoneId zoneId);
  }
}
