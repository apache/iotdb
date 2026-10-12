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

package org.apache.iotdb.pipe.plugin.sink.opcua.server;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.function.Function;
import java.util.function.Supplier;

// Keep Milo-dependent type services in the external plugin so core can load without Milo.
final class OpcUaTypeServices {

  public static final TypeService<Function<Object, Object>> OPC_UA_LAST_VALUE_CONVERTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case DATE ->
                value ->
                    new DateTime(
                        new Date(DateUtils.parseIntToDate(((Number) value).intValue()).getTime()));
            case TIMESTAMP ->
                value ->
                    new DateTime(
                        TimestampPrecisionUtils.currPrecision.toNanos(((Number) value).longValue())
                                / 100L
                            + 116444736000000000L);
            case TEXT, BLOB, STRING -> String::valueOf;
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> Function.identity();
            case ROW, VECTOR, OBJECT, UNKNOWN ->
                value -> {
                  throw new UnSupportedDataTypeException(
                      DataNodePipeMessages.UNSUPPORTED_DATATYPE + type.getTypeEnum());
                };
          };

  public static final TypeService<Function<Object, String>> OPC_UA_VALUE_STRINGIFIER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, BLOB, STRING -> Object::toString;
            case DATE ->
                value -> ((LocalDate) value).atStartOfDay(ZoneId.systemDefault()).toString();
            case TIMESTAMP -> value -> DateTimeUtils.convertLongToDate((long) value);
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                value -> {
                  throw new PipeRuntimeNonCriticalException(
                      DataNodePipeMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  public static final TypeService<TabletObjectValueGetter>
      OPC_UA_TABLET_OBJECT_VALUE_GETTER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN -> (column, rowIndex) -> ((boolean[]) column)[rowIndex];
                case INT32 -> (column, rowIndex) -> ((int[]) column)[rowIndex];
                // Milo calls Date.toInstant(), which java.sql.Date does not support.
                case DATE ->
                    (column, rowIndex) ->
                        new DateTime(
                            Date.from(
                                ((LocalDate[]) column)
                                    [rowIndex].atStartOfDay(ZoneId.systemDefault())
                                    .toInstant()));
                case INT64 -> (column, rowIndex) -> ((long[]) column)[rowIndex];
                case TIMESTAMP ->
                    (column, rowIndex) ->
                        new DateTime(
                            TimestampPrecisionUtils.currPrecision.toNanos(
                                        ((long[]) column)[rowIndex])
                                    / 100L
                                + 116444736000000000L);
                case FLOAT -> (column, rowIndex) -> ((float[]) column)[rowIndex];
                case DOUBLE -> (column, rowIndex) -> ((double[]) column)[rowIndex];
                case TEXT, BLOB, STRING ->
                    (column, rowIndex) -> ((Binary[]) column)[rowIndex].toString();
                case OBJECT, ROW, UNKNOWN, VECTOR ->
                    (column, rowIndex) -> {
                      throw new UnSupportedDataTypeException(
                          DataNodePipeMessages.UNSUPPORTED_DATATYPE + type.getTypeEnum());
                    };
              };

  public static final TypeService<Supplier<NodeId>> OPC_UA_DATA_TYPE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> () -> Identifiers.Boolean;
            case INT32 -> () -> Identifiers.Int32;
            case DATE, TIMESTAMP -> () -> Identifiers.DateTime;
            case INT64 -> () -> Identifiers.Int64;
            case FLOAT -> () -> Identifiers.Float;
            case DOUBLE -> () -> Identifiers.Double;
            case TEXT, BLOB, STRING -> () -> Identifiers.String;
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                () -> {
                  throw new PipeRuntimeNonCriticalException(
                      DataNodePipeMessages.UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  @FunctionalInterface
  public interface TabletObjectValueGetter {
    Object get(Object column, int rowIndex);
  }

  static {
    OPC_UA_LAST_VALUE_CONVERTER_SERVICE.check();
    OPC_UA_VALUE_STRINGIFIER_SERVICE.check();
    OPC_UA_TABLET_OBJECT_VALUE_GETTER_SERVICE.check();
    OPC_UA_DATA_TYPE_SERVICE.check();
  }

  private OpcUaTypeServices() {
    // Utility class
  }
}
