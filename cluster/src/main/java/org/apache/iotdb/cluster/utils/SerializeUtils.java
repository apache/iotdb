/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class SerializeUtils {

  private SerializeUtils() {
    // util class
  }

  public static void serialize(String str, DataOutputStream dataOutputStream) {
    try {
      byte[] strBytes = str.getBytes(TSFileConfig.STRING_CHARSET);
      dataOutputStream.writeInt(strBytes.length);
      dataOutputStream.write(strBytes);
    } catch (IOException e) {
      // unreachable
    }
  }

  public static String deserializeString(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] strBytes = new byte[length];
    buffer.get(strBytes);
    return new String(strBytes);
  }

  public static void serialize(List<Integer> ints, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(ints.size());
      for (Integer anInt : ints) {
        dataOutputStream.writeInt(anInt);
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserialize(List<Integer> ints, ByteBuffer buffer) {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      ints.add(buffer.getInt());
    }
  }

  public static void serialize(Node node, DataOutputStream dataOutputStream) {
    try {
      byte[] ipBytes = node.ip.getBytes();
      dataOutputStream.writeInt(ipBytes.length);
      dataOutputStream.write(ipBytes);
      dataOutputStream.writeInt(node.metaPort);
      dataOutputStream.writeInt(node.nodeIdentifier);
      dataOutputStream.writeInt(node.dataPort);
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserialize(Node node, ByteBuffer buffer) {
    int ipLength = buffer.getInt();
    byte[] ipBytes = new byte[ipLength];
    buffer.get(ipBytes);
    node.setIp(new String(ipBytes));
    node.setMetaPort(buffer.getInt());
    node.setNodeIdentifier(buffer.getInt());
    node.setDataPort(buffer.getInt());
  }

  public static void serializeBatchData(BatchData batchData, DataOutputStream outputStream) {
    try {
      int length = batchData.length();
      TSDataType dataType = batchData.getDataType();
      outputStream.writeInt(length);
      outputStream.write(dataType.ordinal());
      switch (dataType) {
        case BOOLEAN:
          for (int i = 0; i < length; i++) {
            outputStream.writeLong(batchData.getTimeByIndex(i));
            outputStream.writeBoolean(batchData.getBooleanByIndex(i));
          }
          break;
        case DOUBLE:
          for (int i = 0; i < length; i++) {
            outputStream.writeLong(batchData.getTimeByIndex(i));
            outputStream.writeDouble(batchData.getDoubleByIndex(i));
          }
          break;
        case FLOAT:
          for (int i = 0; i < length; i++) {
            outputStream.writeLong(batchData.getTimeByIndex(i));
            outputStream.writeFloat(batchData.getFloatByIndex(i));
          }
          break;
        case TEXT:
          for (int i = 0; i < length; i++) {
            outputStream.writeLong(batchData.getTimeByIndex(i));
            Binary binary = batchData.getBinaryByIndex(i);
            outputStream.writeInt(binary.getLength());
            outputStream.write(binary.getValues());
          }
          break;
        case INT64:
          for (int i = 0; i < length; i++) {
            outputStream.writeLong(batchData.getTimeByIndex(i));
            outputStream.writeLong(batchData.getLongByIndex(i));
          }
          break;
        case INT32:
          for (int i = 0; i < length; i++) {
            outputStream.writeLong(batchData.getTimeByIndex(i));
            outputStream.writeInt(batchData.getIntByIndex(i));
          }
          break;
      }
    } catch (IOException ignored) {
      // ignored
    }
  }

  public static BatchData deserializeBatchData(ByteBuffer buffer) {
    int length = buffer.getInt();
    TSDataType dataType = TSDataType.values()[buffer.get()];
    BatchData batchData = new BatchData(dataType);
    switch (dataType) {
      case INT32:
        for (int i = 0; i < length; i++) {
          batchData.putInt(buffer.getLong(), buffer.getInt());
        }
        break;
      case INT64:
        for (int i = 0; i < length; i++) {
          batchData.putLong(buffer.getLong(), buffer.getLong());
        }
        break;
      case TEXT:
        for (int i = 0; i < length; i++) {
          long time = buffer.getLong();
          int len = buffer.getInt();
          byte[] bytes = new byte[len];
          buffer.get(bytes);
          batchData.putBinary(time, new Binary(bytes));
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          batchData.putFloat(buffer.getLong(), buffer.getFloat());
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          batchData.putDouble(buffer.getLong(), buffer.getDouble());
        }
        break;
      case BOOLEAN:
        for (int i = 0; i < length; i++) {
          batchData.putBoolean(buffer.getLong(), buffer.get() == 1);
        }
        break;
    }
    return batchData;
  }

  public static void serializeTVPairs(List<TimeValuePair> timeValuePairs,
      DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(timeValuePairs.size());
      switch (timeValuePairs.get(0).getValue().getDataType()) {
        case TEXT:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeInt(timeValuePair.getValue().getBinary().getLength());
            dataOutputStream.write(timeValuePair.getValue().getBinary().getValues());
          }
          break;
        case BOOLEAN:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeBoolean(timeValuePair.getValue().getBoolean());
          }
        case INT64:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeLong(timeValuePair.getValue().getLong());
          }
        case INT32:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeInt(timeValuePair.getValue().getInt());
          }
        case FLOAT:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeFloat(timeValuePair.getValue().getFloat());
          }
        case DOUBLE:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeDouble(timeValuePair.getValue().getDouble());
          }
      }
    } catch (IOException ignored) {
      // unreachable
    }
  }

  public static List<TimeValuePair> deserializeTVPairs(ByteBuffer buffer) {
    if (buffer == null || buffer.limit() == 0) {
      return Collections.emptyList();
    }
    TSDataType dataType = TSDataType.values()[buffer.get()];
    int size = buffer.getInt();
    List<TimeValuePair> ret = new ArrayList<>(size);
    switch (dataType) {
      case DOUBLE:
        for (int i = 0; i < size; i++) {
          TimeValuePair pair = new TimeValuePair(buffer.getLong(),
              TsPrimitiveType.getByType(dataType, buffer.getDouble()));
          ret.add(pair);
        }
        break;
      case FLOAT:
        for (int i = 0; i < size; i++) {
          TimeValuePair pair = new TimeValuePair(buffer.getLong(),
              TsPrimitiveType.getByType(dataType, buffer.getDouble()));
          ret.add(pair);
        }
        break;
      case INT32:
        for (int i = 0; i < size; i++) {
          TimeValuePair pair = new TimeValuePair(buffer.getLong(),
              TsPrimitiveType.getByType(dataType, buffer.getInt()));
          ret.add(pair);
        }
        break;
      case INT64:
        for (int i = 0; i < size; i++) {
          TimeValuePair pair = new TimeValuePair(buffer.getLong(),
              TsPrimitiveType.getByType(dataType, buffer.getLong()));
          ret.add(pair);
        }
        break;
      case BOOLEAN:
        for (int i = 0; i < size; i++) {
          TimeValuePair pair = new TimeValuePair(buffer.getLong(),
              TsPrimitiveType.getByType(dataType, buffer.get() == 1));
          ret.add(pair);
        }
        break;
      case TEXT:
        for (int i = 0; i < size; i++) {
          long time = buffer.getLong();
          int bytesLen = buffer.getInt();
          byte[] bytes = new byte[bytesLen];
          buffer.get(bytes);
          TsPrimitiveType primitiveType = TsPrimitiveType.getByType(dataType, bytes);
          TimeValuePair pair = new TimeValuePair(time, primitiveType);
          ret.add(pair);
        }
        break;
    }
    return ret;
  }

  public static ByteBuffer serializeFilter(Filter filter) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public static void serializeObject(Object object, DataOutputStream dataOutputStream) {
    ReadWriteIOUtils.writeObject(object, dataOutputStream);
  }

  public static Object deserializeObject(ByteBuffer buffer) {
    if (buffer == null || buffer.limit() == 0) {
      return null;
    }
    return ReadWriteIOUtils.readObject(buffer);
  }
}
