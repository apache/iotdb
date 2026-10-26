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

package org.apache.iotdb.session.subscription.payload;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.TSRecord;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Type-specific Tablet readers that preserve primitive-array access without intermediate boxing.
 */
final class TypeServices {

  static final TypeService<TSRecordValueAppender> TS_RECORD_VALUE_APPENDER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (record, measurement, values, rowIndex) ->
                    record.addPoint(measurement, ((boolean[]) values)[rowIndex]);
            case INT32 ->
                (record, measurement, values, rowIndex) ->
                    record.addPoint(measurement, ((int[]) values)[rowIndex]);
            case DATE ->
                (record, measurement, values, rowIndex) ->
                    record.addPoint(measurement, ((LocalDate[]) values)[rowIndex]);
            case INT64, TIMESTAMP ->
                (record, measurement, values, rowIndex) ->
                    record.addPoint(measurement, ((long[]) values)[rowIndex]);
            case FLOAT ->
                (record, measurement, values, rowIndex) ->
                    record.addPoint(measurement, ((float[]) values)[rowIndex]);
            case DOUBLE ->
                (record, measurement, values, rowIndex) ->
                    record.addPoint(measurement, ((double[]) values)[rowIndex]);
            case TEXT, STRING, BLOB, OBJECT ->
                (record, measurement, values, rowIndex) -> {
                  Binary binary = ((Binary[]) values)[rowIndex];
                  if (Objects.nonNull(binary)) {
                    record.addPoint(measurement, binary.getValues());
                  }
                };
            case ROW, UNKNOWN, VECTOR ->
                (record, measurement, values, rowIndex) -> {
                  throw unsupportedDataType(type.getTypeEnum());
                };
          };

  static final TypeService<FieldValueReader> FIELD_VALUE_READER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> (field, values, index) -> field.setBoolV(((boolean[]) values)[index]);
            case INT32 -> (field, values, index) -> field.setIntV(((int[]) values)[index]);
            case DATE ->
                (field, values, index) ->
                    field.setIntV(
                        DateUtils.parseDateExpressionToInt(((LocalDate[]) values)[index]));
            case INT64, TIMESTAMP ->
                (field, values, index) -> field.setLongV(((long[]) values)[index]);
            case FLOAT -> (field, values, index) -> field.setFloatV(((float[]) values)[index]);
            case DOUBLE -> (field, values, index) -> field.setDoubleV(((double[]) values)[index]);
            case TEXT, STRING, BLOB, OBJECT ->
                (field, values, index) ->
                    field.setBinaryV(new Binary(((Binary[]) values)[index].getValues()));
            case ROW, UNKNOWN, VECTOR ->
                (field, values, index) -> {
                  throw unsupportedDataType(type.getTypeEnum());
                };
          };

  static {
    TS_RECORD_VALUE_APPENDER_SERVICE.check();
    FIELD_VALUE_READER_SERVICE.check();
  }

  private TypeServices() {}

  private static UnSupportedDataTypeException unsupportedDataType(Object dataType) {
    return new UnSupportedDataTypeException(
        String.format("Data type %s is not supported.", dataType));
  }

  @FunctionalInterface
  interface TSRecordValueAppender {

    void append(TSRecord record, String measurement, Object values, int rowIndex);
  }

  @FunctionalInterface
  interface FieldValueReader {

    void read(Field field, Object values, int index);
  }
}
