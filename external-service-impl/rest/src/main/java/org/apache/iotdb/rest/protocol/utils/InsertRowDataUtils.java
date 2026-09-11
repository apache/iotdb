/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rest.protocol.utils;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.NoValidValueException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.session.Session.MSG_UNSUPPORTED_DATA_TYPE;

public class InsertRowDataUtils {

  private static final TypeService<ValueConverter> VALUE_CONVERTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (value, index, mismatchedInfo) -> {
                  if (!(value instanceof Boolean)) {
                    mismatchedInfo.put(index, value);
                  }
                  return value;
                };
            case INT32, DATE ->
                (value, index, mismatchedInfo) -> {
                  if (value instanceof Number number) {
                    return number.intValue();
                  }
                  mismatchedInfo.put(index, value);
                  return value;
                };
            case INT64, TIMESTAMP ->
                (value, index, mismatchedInfo) -> {
                  if (value instanceof Number number) {
                    return number.longValue();
                  }
                  mismatchedInfo.put(index, value);
                  return value;
                };
            case FLOAT ->
                (value, index, mismatchedInfo) -> {
                  if (value instanceof Number number) {
                    return number.floatValue();
                  }
                  mismatchedInfo.put(index, value);
                  return value;
                };
            case DOUBLE ->
                (value, index, mismatchedInfo) -> {
                  if (value instanceof Number number) {
                    return number.doubleValue();
                  }
                  mismatchedInfo.put(index, value);
                  return value;
                };
            case TEXT, BLOB, STRING ->
                (value, index, mismatchedInfo) ->
                    new Binary(value.toString().getBytes(StandardCharsets.UTF_8));
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (value, index, mismatchedInfo) -> {
                  throw new IoTDBConnectionException(
                      MSG_UNSUPPORTED_DATA_TYPE + type.getTypeEnum());
                };
          };

  private static final String ALL_INSERT_DATA_IS_NULL = "All inserted data is null.";

  public static void filterNullValueAndMeasurement(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<Object>> valuesList,
      List<List<TSDataType>> typesList) {
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      List<Object> values = valuesList.get(i);
      List<String> measurements = measurementsList.get(i);
      List<TSDataType> types = typesList.get(i);
      boolean isAllValuesNull = getValuesIsEmpty(measurements, types, values);
      if (isAllValuesNull) {
        valuesList.remove(i);
        measurementsList.remove(i);
        deviceIds.remove(i);
        times.remove(i);
        typesList.remove(i);
      }
    }
    if (valuesList.isEmpty()) {
      throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
    }
  }

  private static boolean getValuesIsEmpty(
      List<String> measurementsList, List<TSDataType> types, List<Object> valuesList) {
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      if (valuesList.get(i) == null) {
        valuesList.remove(i);
        measurementsList.remove(i);
        types.remove(i);
      }
    }
    return valuesList.isEmpty();
  }

  public static List<Object> reGenValues(
      List<TSDataType> types, List<Object> values, Map<Integer, Object> mismatchedInfo)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        continue;
      }
      Object val = values.get(i);
      values.set(
          i,
          VALUE_CONVERTER_SERVICE
              .call(Type.fromTsDataType(types.get(i)))
              .convert(val, i, mismatchedInfo));
    }

    return values;
  }

  @FunctionalInterface
  private interface ValueConverter {
    Object convert(Object value, int index, Map<Integer, Object> mismatchedInfo)
        throws IoTDBConnectionException;
  }
}
