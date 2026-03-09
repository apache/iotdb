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
import org.apache.tsfile.encoding.encoder.Encoder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.session.Session.MSG_UNSUPPORTED_DATA_TYPE;

public class SessionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionUtils.class);
  private static final byte TYPE_NULL = -2;
  private static final int EMPTY_DATE_INT = 10000101;

  public static ByteBuffer getTimeBuffer(Tablet tablet) {
    ByteBuffer timeBuffer = ByteBuffer.allocate(getTimeBytesSize(tablet));
    for (int i = 0; i < tablet.getRowSize(); i++) {
      timeBuffer.putLong(tablet.getTimestamp(i));
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
    BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
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
          calOccupationOfOneColumn(schema.getType(), tablet.getValues(), columnIndex, rowSize);
      columnIndex++;
    }

    // Add bitmap size if the tablet has bitMaps
    BitMap[] bitMaps = tablet.getBitMaps();
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
      case OBJECT:
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

  public static ByteBuffer getValueBuffer(
      List<TSDataType> types, List<Object> values, List<String> measurements)
      throws IoTDBConnectionException {
    ByteBuffer buffer = ByteBuffer.allocate(SessionUtils.calculateLength(types, values));
    SessionUtils.putValues(types, values, buffer, measurements);
    return buffer;
  }

  public static int calculateLength(List<TSDataType> types, List<? extends Object> values)
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
        case OBJECT:
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
  public static void putValues(
      List<TSDataType> types,
      List<? extends Object> values,
      ByteBuffer buffer,
      List<String> measurements)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
      try {
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
      } catch (Throwable e) {
        LOGGER.error(
            "Cannot put values for measurement {}, type={}", measurements.get(i), types.get(i), e);
        throw e;
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
        int[] intValues = (int[]) tablet.getValues()[i];
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
        long[] longValues = (long[]) tablet.getValues()[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            valueBuffer.putLong(longValues[index]);
          } else {
            valueBuffer.putLong(Long.MIN_VALUE);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) tablet.getValues()[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            valueBuffer.putFloat(floatValues[index]);
          } else {
            valueBuffer.putFloat(Float.MIN_VALUE);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) tablet.getValues()[i];
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            valueBuffer.putDouble(doubleValues[index]);
          } else {
            valueBuffer.putDouble(Double.MIN_VALUE);
          }
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) tablet.getValues()[i];
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
      case OBJECT:
        Binary[] binaryValues = (Binary[]) tablet.getValues()[i];
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
        LocalDate[] dateValues = (LocalDate[]) tablet.getValues()[i];
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

  @SuppressWarnings({"java:S3776", "java:S6541"})
  public static void encodeValue(
      TSDataType dataType,
      Tablet tablet,
      int i,
      Encoder encoder,
      ByteArrayOutputStream outputStream) {

    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) tablet.getValues()[i];
        int lastNonNullIntValue = 0;
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            lastNonNullIntValue = intValues[index];
          }
          encoder.encode(lastNonNullIntValue, outputStream);
        }
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) tablet.getValues()[i];
        long lastNonNullLongValue = 0;
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            lastNonNullLongValue = longValues[index];
          }
          encoder.encode(lastNonNullLongValue, outputStream);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) tablet.getValues()[i];
        float lastNonNullFloatValue = 0.0f;
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            lastNonNullFloatValue = floatValues[index];
          }
          encoder.encode(lastNonNullFloatValue, outputStream);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) tablet.getValues()[i];
        double lastNonNullDoubleValue = 0.0;
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            lastNonNullDoubleValue = doubleValues[index];
          }
          encoder.encode(lastNonNullDoubleValue, outputStream);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) tablet.getValues()[i];
        boolean lastNonNullBooleanValue = false;
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            lastNonNullBooleanValue = boolValues[index];
          }
          encoder.encode(lastNonNullBooleanValue, outputStream);
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        Binary[] binaryValues = (Binary[]) tablet.getValues()[i];
        Binary lastNonNullBinaryValue = Binary.EMPTY_VALUE;
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i) && binaryValues[index] != null) {
            lastNonNullBinaryValue = binaryValues[index];
          }
          encoder.encode(lastNonNullBinaryValue, outputStream);
        }
        break;
      case DATE:
        LocalDate[] dateValues = (LocalDate[]) tablet.getValues()[i];
        int lastNonNullDateValue = EMPTY_DATE_INT;
        for (int index = 0; index < tablet.getRowSize(); index++) {
          if (!tablet.isNull(index, i)) {
            lastNonNullDateValue = DateUtils.parseDateExpressionToInt(dateValues[index]);
          }
          // use the previous value as the placeholder of nulls to increase encoding performance
          encoder.encode(lastNonNullDateValue, outputStream);
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
    try {
      encoder.flush(outputStream);
    } catch (IOException e) {
      throw new IllegalStateException(e);
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
