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

import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
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

  static {
    JDBC_STRING_READER_SERVICE.check();
    JDBC_OBJECT_READER_SERVICE.check();
  }

  private TypeServices() {}
}
