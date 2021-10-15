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

import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SessionUtils {

  private static final Logger logger = LoggerFactory.getLogger(SessionUtils.class);

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
    int indexOfValues = 0;
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema instanceof UnaryMeasurementSchema) {
        getValueBufferOfDataType(schema.getType(), tablet, indexOfValues, valueBuffer);
        indexOfValues++;
      } else {
        for (int j = 0; j < schema.getSubMeasurementsTSDataTypeList().size(); j++) {
          getValueBufferOfDataType(
              schema.getSubMeasurementsTSDataTypeList().get(j), tablet, indexOfValues, valueBuffer);
          indexOfValues++;
        }
      }
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
        Binary[] binaryValues = (Binary[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          valueBuffer.putInt(binaryValues[index].getLength());
          valueBuffer.put(binaryValues[index].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  public static List<EndPoint> parseSeedNodeUrls(List<String> nodeUrls) {
    if (nodeUrls == null) {
      throw new NumberFormatException("nodeUrls is null");
    }
    List<EndPoint> endPointsList = new ArrayList<>();
    for (String nodeUrl : nodeUrls) {
      EndPoint endPoint = parseNodeUrl(nodeUrl);
      endPointsList.add(endPoint);
    }
    return endPointsList;
  }

  private static EndPoint parseNodeUrl(String nodeUrl) {
    EndPoint endPoint = new EndPoint();
    String[] split = nodeUrl.split(":");
    if (split.length != 2) {
      throw new NumberFormatException("NodeUrl Incorrect format");
    }
    String ip = split[0];
    try {
      int rpcPort = Integer.parseInt(split[1]);
      return endPoint.setIp(ip).setPort(rpcPort);
    } catch (Exception e) {
      throw new NumberFormatException("NodeUrl Incorrect format");
    }
  }
}
