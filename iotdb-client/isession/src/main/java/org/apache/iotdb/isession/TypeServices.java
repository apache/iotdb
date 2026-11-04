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

package org.apache.iotdb.isession;

import org.apache.iotdb.isession.i18n.ISessionMessages;
import org.apache.iotdb.rpc.IoTDBRpcDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.write.UnSupportedDataTypeException;

/** Type-specific RPC result readers that preserve primitive access without intermediate boxing. */
final class TypeServices {

  static final TypeService<FieldValueReader> FIELD_VALUE_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (dataSet, columnIndex, field) -> field.setBoolV(dataSet.getBoolean(columnIndex));
            case INT32, DATE ->
                (dataSet, columnIndex, field) -> field.setIntV(dataSet.getInt(columnIndex));
            case INT64, TIMESTAMP ->
                (dataSet, columnIndex, field) -> field.setLongV(dataSet.getLong(columnIndex));
            case FLOAT ->
                (dataSet, columnIndex, field) -> field.setFloatV(dataSet.getFloat(columnIndex));
            case DOUBLE ->
                (dataSet, columnIndex, field) -> field.setDoubleV(dataSet.getDouble(columnIndex));
            case TEXT, BLOB, STRING, OBJECT ->
                (dataSet, columnIndex, field) -> field.setBinaryV(dataSet.getBinary(columnIndex));
            case ROW, UNKNOWN, VECTOR ->
                (dataSet, columnIndex, field) -> {
                  throw new UnSupportedDataTypeException(
                      String.format(
                          ISessionMessages.EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160,
                          type.getTypeEnum()));
                };
          };

  static {
    FIELD_VALUE_READER_SERVICE.check();
  }

  private TypeServices() {}

  @FunctionalInterface
  interface FieldValueReader {

    void read(IoTDBRpcDataSet dataSet, int columnIndex, Field field)
        throws StatementExecutionException;
  }
}
