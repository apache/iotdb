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
import org.apache.iotdb.session.i18n.SessionMessages;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SessionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionUtils.class);
  private static final byte TYPE_NULL = -2;

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
    return SessionTypeServices.tabletColumnOccupationCalculator(dataType)
        .calculate(values, columnIndex, rowSize);
  }

  public static ByteBuffer getValueBuffer(
      List<TSDataType> types, List<Object> values, List<String> measurements)
      throws IoTDBConnectionException {
    ByteBuffer buffer = ByteBuffer.allocate(SessionUtils.calculateLength(types, values));
    SessionUtils.putValues(types, values, buffer, measurements);
    return buffer;
  }

  public static Object sortValueList(Object valueList, TSDataType dataType, int[] index) {
    return SessionTypeServices.valueListSorter(dataType).sort(valueList, index);
  }

  public static int calculateLength(List<TSDataType> types, List<? extends Object> values)
      throws IoTDBConnectionException {
    int res = 0;
    for (int i = 0; i < types.size(); i++) {
      // types
      res += Byte.BYTES;
      res += SessionTypeServices.valueLengthCalculator(types.get(i)).calculate(values.get(i));
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
        TSDataType type = types.get(i);
        ReadWriteIOUtils.write(type, buffer);
        SessionTypeServices.valueWriter(type).write(values.get(i), buffer);
      } catch (Throwable e) {
        LOGGER.error(
            "Cannot put values for measurement {}, type={}", measurements.get(i), types.get(i), e);
        throw e;
      }
    }
    buffer.flip();
  }

  private static void getValueBufferOfDataType(
      TSDataType dataType, Tablet tablet, int i, ByteBuffer valueBuffer) {
    SessionTypeServices.tabletValueWriter(dataType).write(tablet, i, valueBuffer);
  }

  public static void encodeValue(
      TSDataType dataType,
      Tablet tablet,
      int i,
      Encoder encoder,
      ByteArrayOutputStream outputStream) {

    SessionTypeServices.tabletValueEncoder(dataType).encode(tablet, i, encoder, outputStream);
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
      throw new NumberFormatException(SessionMessages.NODE_URLS_IS_NULL);
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
