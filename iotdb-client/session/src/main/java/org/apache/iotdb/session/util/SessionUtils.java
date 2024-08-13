/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.session.util;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.UrlUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.session.Session.MSG_UNSUPPORTED_DATA_TYPE;

public class SessionUtils {

  private static final byte TYPE_NULL = -2;

  public static ByteBuffer getTimeBuffer(Tablet tablet) {
    ByteBuffer timeBuffer = ByteBuffer.allocate(tablet.getTimeBytesSize());
    for (int i = 0; i < tablet.rowSize; i++) {
      timeBuffer.putLong(tablet.timestamps[i]);
    }
    timeBuffer.flip();
    return timeBuffer;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static ByteBuffer getValueBuffer(Tablet tablet) {
    ByteBuffer valueBuffer = ByteBuffer.allocate(tablet.getTotalValueOccupation());
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      getValueBufferOfDataType(schema.getType(), tablet, i, valueBuffer);
    }
    if (tablet.bitMaps != null) {
      for (BitMap bitMap : tablet.bitMaps) {
        boolean columnHasNull = bitMap != null && !bitMap.isAllUnmarked();
        valueBuffer.put(BytesUtils.boolToByte(columnHasNull));
        if (columnHasNull) {
          byte[] bytes = bitMap.getByteArray();
          for (int j = 0; j < tablet.rowSize / Byte.SIZE + 1; j++) {
            valueBuffer.put(bytes[j]);
          }
        }
      }
    }
    valueBuffer.flip();
    return valueBuffer;
  }

  public static ByteBuffer getValueBuffer(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    ByteBuffer buffer = ByteBuffer.allocate(SessionUtils.calculateLength(types, values));
    SessionUtils.putValues(types, values, buffer);
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
        case DATE:
          res += Integer.BYTES;
          break;
        case INT64:
        case TIMESTAMP:
          res += Long.BYTES;
          break;
        case FLOAT:
          res += Float.BYTES;
          break;
        case DOUBLE:
          res += Double.BYTES;
          break;
        case TEXT:
        case STRING:
          res += Integer.BYTES;
          if (values.get(i) instanceof Binary) {
            res += ((Binary) values.get(i)).getValues().length;
          } else {
            res += ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET).length;
          }
          break;
        case BLOB:
          res += Integer.BYTES;
          res += ((Binary) values.get(i)).getValues().length;
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
   * @throws IoTDBConnectionException
   */
  private static void putValues(List<TSDataType> types, List<Object> values, ByteBuffer buffer)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        ReadWriteIOUtils.write(TYPE_NULL, buffer);
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
        case DATE:
          ReadWriteIOUtils.write(
              DateUtils.parseDateExpressionToInt((LocalDate) values.get(i)), buffer);
          break;
        case INT64:
        case TIMESTAMP:
          ReadWriteIOUtils.write((Long) values.get(i), buffer);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) values.get(i), buffer);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) values.get(i), buffer);
          break;
        case TEXT:
        case STRING:
          byte[] bytes;
          if (values.get(i) instanceof Binary) {
            bytes = ((Binary) values.get(i)).getValues();
          } else {
            bytes = ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET);
          }
          ReadWriteIOUtils.write(bytes.length, buffer);
          buffer.put(bytes);
          break;
        case BLOB:
          bytes = ((Binary) values.get(i)).getValues();
          ReadWriteIOUtils.write(bytes.length, buffer);
          buffer.put(bytes);
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    buffer.flip();
  }

  @SuppressWarnings({
    "squid:S6541",
    "squid:S3776"
  }) /// ignore Cognitive Complexity of methods should not be too high
  // ignore Methods should not perform too many tasks (aka Brain method)
  private static void getValueBufferOfDataType(
      TSDataType dataType, Tablet tablet, int i, ByteBuffer valueBuffer) {

    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putInt(intValues[index]);
          } else {
            valueBuffer.putInt(Integer.MIN_VALUE);
          }
        }
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putLong(longValues[index]);
          } else {
            valueBuffer.putLong(Long.MIN_VALUE);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putFloat(floatValues[index]);
          } else {
            valueBuffer.putFloat(Float.MIN_VALUE);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putDouble(doubleValues[index]);
          } else {
            valueBuffer.putDouble(Double.MIN_VALUE);
          }
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.put(BytesUtils.boolToByte(boolValues[index]));
          } else {
            valueBuffer.put(BytesUtils.boolToByte(false));
          }
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        Binary[] binaryValues = (Binary[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          valueBuffer.putInt(binaryValues[index].getLength());
          valueBuffer.put(binaryValues[index].getValues());
        }
        break;
      case DATE:
        LocalDate[] dateValues = (LocalDate[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          valueBuffer.putInt(DateUtils.parseDateExpressionToInt(dateValues[index]));
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  public static List<TEndPoint> parseSeedNodeUrls(List<String> nodeUrls) {
    if (nodeUrls == null) {
      throw new NumberFormatException("nodeUrls is null");
    }
    List<TEndPoint> endPointsList = new ArrayList<>();
    for (String nodeUrl : nodeUrls) {
      TEndPoint endPoint = UrlUtils.parseTEndPointIpv4AndIpv6Url(nodeUrl);
      endPointsList.add(endPoint);
    }
    return endPointsList;
  }

  private SessionUtils() {}
}
