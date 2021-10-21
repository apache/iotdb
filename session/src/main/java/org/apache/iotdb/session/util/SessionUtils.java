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
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

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
    ByteBuffer valueBuffer = ByteBuffer.allocate(tablet.getValueBytesSize());
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      TSDataType dataType = tablet.getSchemas().get(i).getType();
      switch (dataType) {
        case INT32:
          int[] intValues = (int[]) tablet.values[i];
          for (int index = 0; index < tablet.rowSize; index++) {
            valueBuffer.putInt(intValues[index]);
          }
          break;
        case INT64:
          long[] longValues = (long[]) tablet.values[i];
          for (int index = 0; index < tablet.rowSize; index++) {
            valueBuffer.putLong(longValues[index]);
          }
          break;
        case FLOAT:
          float[] floatValues = (float[]) tablet.values[i];
          for (int index = 0; index < tablet.rowSize; index++) {
            valueBuffer.putFloat(floatValues[index]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = (double[]) tablet.values[i];
          for (int index = 0; index < tablet.rowSize; index++) {
            valueBuffer.putDouble(doubleValues[index]);
          }
          break;
        case BOOLEAN:
          boolean[] boolValues = (boolean[]) tablet.values[i];
          for (int index = 0; index < tablet.rowSize; index++) {
            valueBuffer.put(BytesUtils.boolToByte(boolValues[index]));
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
    valueBuffer.flip();
    return valueBuffer;
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
