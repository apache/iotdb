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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.activation.UnsupportedDataTypeException;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
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

  public static void serializeStringList(List<String> strs, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(strs.size());
    } catch (IOException e) {
      // unreachable
    }
    for (String str : strs) {
      serialize(str, dataOutputStream);
    }
  }

  public static List<String> deserializeStringList(ByteBuffer buffer) {
    int length = buffer.getInt();
    List<String> result = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      result.add(deserializeString(buffer));
    }
    return result;
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

  public static void serialize(Set<Integer> ints, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(ints.size());
      for (Integer anInt : ints) {
        dataOutputStream.writeInt(anInt);
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserialize(Set<Integer> ints, ByteBuffer buffer) {
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

  public static void deserialize(Node node, DataInputStream stream) throws IOException {
    int ipLength = stream.readInt();
    byte[] ipBytes = new byte[ipLength];
    int readSize = stream.read(ipBytes);
    if (readSize != ipLength) {
      throw new IOException(String.format("No sufficient bytes read when deserializing the ip of "
          + "a "
          + "node: %d/%d", readSize, ipLength));
    }
    node.setIp(new String(ipBytes));
    node.setMetaPort(stream.readInt());
    node.setNodeIdentifier(stream.readInt());
    node.setDataPort(stream.readInt());
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
    if (buffer == null || (buffer.limit() - buffer.position()) == 0) {
      return null;
    }

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
          break;
        case INT64:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeLong(timeValuePair.getValue().getLong());
          }
          break;
        case INT32:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeInt(timeValuePair.getValue().getInt());
          }
          break;
        case FLOAT:
          for (TimeValuePair timeValuePair : timeValuePairs) {
            dataOutputStream.writeLong(timeValuePair.getTimestamp());
            dataOutputStream.writeFloat(timeValuePair.getValue().getFloat());
          }
          break;
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

  public static void serializeTVPair(TimeValuePair timeValuePair,
      DataOutputStream dataOutputStream) {
    if (timeValuePair.getValue() == null) {
      return;
    }
    TSDataType dataType = timeValuePair.getValue().getDataType();
    try {
      dataOutputStream.write(dataType.ordinal());
      switch (dataType) {
        case TEXT:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          dataOutputStream.writeInt(timeValuePair.getValue().getBinary().getLength());
          dataOutputStream.write(timeValuePair.getValue().getBinary().getValues());
          break;
        case BOOLEAN:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          dataOutputStream.writeBoolean(timeValuePair.getValue().getBoolean());
          break;
        case INT64:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          dataOutputStream.writeLong(timeValuePair.getValue().getLong());
          break;
        case INT32:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          dataOutputStream.writeInt(timeValuePair.getValue().getInt());
          break;
        case FLOAT:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          dataOutputStream.writeFloat(timeValuePair.getValue().getFloat());
          break;
        case DOUBLE:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          dataOutputStream.writeDouble(timeValuePair.getValue().getDouble());
      }
    } catch (IOException e) {
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
              TsPrimitiveType.getByType(dataType, buffer.getFloat()));
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

  public static TimeValuePair deserializeTVPair(ByteBuffer buffer)
      throws UnsupportedDataTypeException {
    if (buffer == null || (buffer.limit() - buffer.position() == 0)) {
      return null;
    }
    TSDataType dataType = TSDataType.values()[buffer.get()];
    switch (dataType) {
      case DOUBLE:
        return new TimeValuePair(buffer.getLong(),
            TsPrimitiveType.getByType(dataType, buffer.getDouble()));
      case FLOAT:
        return new TimeValuePair(buffer.getLong(),
            TsPrimitiveType.getByType(dataType, buffer.getFloat()));
      case INT32:
        return new TimeValuePair(buffer.getLong(),
            TsPrimitiveType.getByType(dataType, buffer.getInt()));
      case INT64:
        return new TimeValuePair(buffer.getLong(),
            TsPrimitiveType.getByType(dataType, buffer.getLong()));
      case BOOLEAN:
        return new TimeValuePair(buffer.getLong(),
            TsPrimitiveType.getByType(dataType, buffer.get() == 1));
      case TEXT:
        long time = buffer.getLong();
        int bytesLen = buffer.getInt();
        byte[] bytes = new byte[bytesLen];
        buffer.get(bytes);
        TsPrimitiveType primitiveType = TsPrimitiveType.getByType(dataType, bytes);
        return new TimeValuePair(time, primitiveType);
    }
    throw new UnsupportedDataTypeException(dataType.toString());
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

  public static void serializeObjects(Object[] object, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(object.length);
      for (Object o : object) {
        ReadWriteIOUtils.writeObject(o, dataOutputStream);
      }
    } catch (IOException e) {
      // ignore
    }
  }

  public static Object deserializeObject(ByteBuffer buffer) {
    if (buffer == null || buffer.limit() == 0) {
      return null;
    }
    return ReadWriteIOUtils.readObject(buffer);
  }

  public static Object[] deserializeObjects(ByteBuffer buffer) {
    if (buffer == null || buffer.limit() == 0) {
      return new Object[0];
    }
    int size = buffer.getInt();
    Object[] ret = new Object[size];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = ReadWriteIOUtils.readObject(buffer);
    }
    return ret;
  }

  public static ByteBuffer serializeLongs(long[] longs) {
    //TODO-Cluster: replace with a no-copy method
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      for (long aLong : longs) {
        dataOutputStream.writeLong(aLong);
      }
    } catch (IOException e) {
      // ignore
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public static long[] deserializeLongs(ByteBuffer buffer) {
    int size = (buffer.limit() - buffer.position()) / Long.BYTES;
    long[] ret = new long[size];
    for (int i = 0; i < size; i++) {
      ret[i] = buffer.getLong();
    }
    return ret;
  }

  /**
   * Convert a string representation of a Node to an object.
   * @param str A string that is generated by Node.getFullPath()
   * @return a Node object
   */
  public static Node stringToNode(String str) {
    int ipFirstPos = str.indexOf("ip:", 0) + "ip:".length();
    int ipLastPos = str.indexOf(',', ipFirstPos);
    int metaPortFirstPos = str.indexOf("metaPort:", ipLastPos) + "metaPort:".length();
    int metaPortLastPos = str.indexOf(',', metaPortFirstPos);
    int idFirstPos = str.indexOf("nodeIdentifier:", metaPortLastPos) + "nodeIdentifier:".length();
    int idLastPos = str.indexOf(',', idFirstPos);
    int dataPortFirstPos = str.indexOf("dataPort:", idLastPos) + "dataPort:".length();
    int dataPortLastPos = str.indexOf(')', dataPortFirstPos);

    String ip = str.substring(ipFirstPos, ipLastPos);
    int metaPort = Integer.parseInt(str.substring(metaPortFirstPos, metaPortLastPos));
    int id = Integer.parseInt(str.substring(idFirstPos, idLastPos));
    int dataPort = Integer.parseInt(str.substring(dataPortFirstPos, dataPortLastPos));
    return new Node(ip, metaPort, id, dataPort);
  }
}
