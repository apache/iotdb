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
import org.apache.tsfile.file.metadata.IDeviceID;
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
  private static final int EMPTY_DATE_INT = 10000101;

  public static ByteBuffer getTimeBuffer(Tablet tablet) {
    ByteBuffer timeBuffer = ByteBuffer.allocate(getTimeBytesSize(tablet));
    for (int i = 0; i < tablet.getRowSize(); i++) {
      timeBuffer.putLong(tablet.timestamps[i]);
    }
    timeBuffer.flip();
    return timeBuffer;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static ByteBuffer getValueBuffer(Tablet tablet) {
    ByteBuffer valueBuffer = ByteBuffer.allocate(getTotalValueOccupation(tablet));
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      getValueBufferOfDataType(schema.getType(), tablet, i, valueBuffer);
    }
    if (tablet.bitMaps != null) {
      for (BitMap bitMap : tablet.bitMaps) {
        boolean columnHasNull = bitMap != null && !bitMap.isAllUnmarked(tablet.getRowSize());
        valueBuffer.put(BytesUtils.boolToByte(columnHasNull));
        if (columnHasNull) {
          valueBuffer.put(bitMap.getTruncatedByteArray(tablet.getRowSize()));
        }
      }
    }
    valueBuffer.flip();
    return valueBuffer;
  }

  private static int getTimeBytesSize(Tablet tablet) {
    return tablet.getRowSize() * 8;
  }

  /**
   * @return Total bytes of values
   */
  private static int getTotalValueOccupation(Tablet tablet) {
    int valueOccupation = 0;
    int columnIndex = 0;
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    int rowSize = tablet.getRowSize();
    for (IMeasurementSchema schema : schemas) {
      valueOccupation +=
          calOccupationOfOneColumn(schema.getType(), tablet.values, columnIndex, rowSize);
      columnIndex++;
    }

    // Add bitmap size if the tablet has bitMaps
    BitMap[] bitMaps = tablet.bitMaps;
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        // Marker byte
        valueOccupation++;
        if (bitMap != null && !bitMap.isAllUnmarked()) {
          valueOccupation += rowSize / Byte.SIZE + 1;
        }
      }
    }
    return valueOccupation;
  }

  private static int calOccupationOfOneColumn(
      TSDataType dataType, Object[] values, int columnIndex, int rowSize) {
    int valueOccupation = 0;
    switch (dataType) {
      case BOOLEAN:
        valueOccupation += rowSize;
        break;
      case INT32:
      case FLOAT:
      case DATE:
        valueOccupation += rowSize * 4;
        break;
      case INT64:
      case DOUBLE:
      case TIMESTAMP:
        valueOccupation += rowSize * 8;
        break;
      case TEXT:
      case BLOB:
      case STRING:
        valueOccupation += rowSize * 4;
        Binary[] binaries = (Binary[]) values[columnIndex];
        for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
          valueOccupation +=
              binaries[rowIndex] != null
                  ? binaries[rowIndex].getLength()
                  : Binary.EMPTY_VALUE.getLength();
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
    return valueOccupation;
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
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            valueBuffer.putInt(intValues[index]);
          } else {
            valueBuffer.putInt(Integer.MIN_VALUE);
          }
        }
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) tablet.values[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            valueBuffer.putLong(longValues[index]);
          } else {
            valueBuffer.putLong(Long.MIN_VALUE);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) tablet.values[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            valueBuffer.putFloat(floatValues[index]);
          } else {
            valueBuffer.putFloat(Float.MIN_VALUE);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) tablet.values[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            valueBuffer.putDouble(doubleValues[index]);
          } else {
            valueBuffer.putDouble(Double.MIN_VALUE);
          }
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) tablet.values[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
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
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i) && binaryValues[index] != null) {
            valueBuffer.putInt(binaryValues[index].getLength());
            valueBuffer.put(binaryValues[index].getValues());
          } else {
            valueBuffer.putInt(Binary.EMPTY_VALUE.getLength());
            valueBuffer.put(Binary.EMPTY_VALUE.getValues());
          }
        }
        break;
      case DATE:
        LocalDate[] dateValues = (LocalDate[]) tablet.values[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i) && dateValues[index] != null) {
            valueBuffer.putInt(DateUtils.parseDateExpressionToInt(dateValues[index]));
          } else {
            valueBuffer.putInt(EMPTY_DATE_INT);
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  /* Used for table model insert only. */
  public static boolean isTabletContainsSingleDevice(Tablet tablet) {
    if (tablet.getRowSize() == 1) {
      return true;
    }
    IDeviceID firstDeviceId = tablet.getDeviceID(0);
    for (int i = 1; i < tablet.getRowSize(); ++i) {
      if (!firstDeviceId.equals(tablet.getDeviceID(i))) {
        return false;
      }
    }
    return true;
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
