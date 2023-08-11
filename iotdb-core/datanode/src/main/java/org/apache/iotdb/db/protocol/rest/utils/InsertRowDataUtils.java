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

package org.apache.iotdb.db.protocol.rest.utils;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.NoValidValueException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.session.Session.MSG_UNSUPPORTED_DATA_TYPE;

public class InsertRowDataUtils {

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
      boolean isAllValuesNull = filterNullValueAndMeasurement(measurements, types, values);
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

  private static boolean filterNullValueAndMeasurement(
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

  public static List<ByteBuffer> objectValuesListToByteBufferList(
      List<List<Object>> valuesList, List<List<TSDataType>> typesList)
      throws IoTDBConnectionException {
    List<ByteBuffer> buffersList = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      ByteBuffer buffer = getValueBuffer(typesList.get(i), valuesList.get(i));
      buffersList.add(buffer);
    }
    return buffersList;
  }

  public static ByteBuffer getValueBuffer(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    ByteBuffer buffer = ByteBuffer.allocate(calculateLength(types, values));
    putValues(types, values, buffer);
    return buffer;
  }

  private static int calculateLength(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    int res = 0;
    for (int i = 0; i < types.size(); i++) {
      // types
      res += Byte.BYTES;
      switch (types.get(i)) {
        case BOOLEAN:
          res += 1;
          break;
        case INT32:
          res += Integer.BYTES;
          break;
        case INT64:
          res += Long.BYTES;
          break;
        case FLOAT:
          res += Float.BYTES;
          break;
        case DOUBLE:
          res += Double.BYTES;
          break;
        case TEXT:
          res += Integer.BYTES;
          if (values.get(i) instanceof Binary) {
            res += ((Binary) values.get(i)).getValues().length;
          } else {
            res += ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET).length;
          }
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    return res;
  }

  /**
   * put value in buffer
   *
   * @param types types list
   * @param values values list
   * @param buffer buffer to insert
   */
  private static void putValues(List<TSDataType> types, List<Object> values, ByteBuffer buffer)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        ReadWriteIOUtils.write(-2, buffer);
        continue;
      }
      ReadWriteIOUtils.write(types.get(i), buffer);
      switch (types.get(i)) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) values.get(i), buffer);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) values.get(i), buffer);
          break;
        case INT64:
          Object object = values.get(i);
          if (object instanceof Integer) {
            int value = (Integer) values.get(i);
            ReadWriteIOUtils.write((long) value, buffer);
          } else {
            ReadWriteIOUtils.write((long) values.get(i), buffer);
          }
          break;
        case FLOAT:
          if (values.get(i) instanceof Double) {
            double value = (Double) values.get(i);
            ReadWriteIOUtils.write((float) value, buffer);
          } else if (values.get(i) instanceof Integer) {
            int value = (Integer) values.get(i);
            ReadWriteIOUtils.write((float) value, buffer);
          } else if (values.get(i) instanceof Long) {
            long value = (Long) values.get(i);
            ReadWriteIOUtils.write((float) value, buffer);
          } else {
            ReadWriteIOUtils.write((float) values.get(i), buffer);
          }
          break;
        case DOUBLE:
          if (values.get(i) instanceof Float) {
            float value = (Float) values.get(i);
            ReadWriteIOUtils.write((double) value, buffer);
          } else if (values.get(i) instanceof Integer) {
            int value = (Integer) values.get(i);
            ReadWriteIOUtils.write((double) value, buffer);
          } else if (values.get(i) instanceof Long) {
            long value = (Long) values.get(i);
            ReadWriteIOUtils.write((double) value, buffer);
          } else {
            ReadWriteIOUtils.write((double) values.get(i), buffer);
          }
          break;
        case TEXT:
          byte[] bytes;
          if (values.get(i) instanceof Binary) {
            bytes = ((Binary) values.get(i)).getValues();
          } else {
            bytes = ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET);
          }
          ReadWriteIOUtils.write(bytes.length, buffer);
          buffer.put(bytes);
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    buffer.flip();
  }
}
