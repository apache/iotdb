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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchData.BatchDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@SuppressWarnings("java:S1135") // ignore todos
public class SerializeUtils {

  private SerializeUtils() {
    // util class
  }

  public static byte serializeNullable(final TSEncoding encoding) {
    return encoding == null ? -1 : encoding.serialize();
  }

  public static TSEncoding deserializeEncodingNullable(final byte encoding) {
    return encoding == -1 ? null : TSEncoding.deserialize(encoding);
  }

  public static byte serializeNullable(final CompressionType compressor) {
    return compressor == null ? -1 : compressor.serialize();
  }

  public static CompressionType deserializeCompressorNullable(final byte compressor) {
    return compressor == -1 ? null : CompressionType.deserialize(compressor);
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
    return new String(strBytes, TSFileConfig.STRING_CHARSET);
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

  public static void serializeIntList(List<Integer> ints, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(ints.size());
      for (Integer anInt : ints) {
        dataOutputStream.writeInt(anInt);
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserializeIntList(List<Integer> ints, ByteBuffer buffer) {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      ints.add(buffer.getInt());
    }
  }

  public static void serializeIntSet(Set<Integer> ints, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(ints.size());
      for (Integer anInt : ints) {
        dataOutputStream.writeInt(anInt);
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserializeIntSet(Set<Integer> ints, ByteBuffer buffer) {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      ints.add(buffer.getInt());
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void serializeBatchData(BatchData batchData, DataOutputStream outputStream) {
    try {
      int length = batchData.length();
      TSDataType dataType = batchData.getDataType();
      outputStream.writeInt(length);
      outputStream.write(dataType.ordinal());
      outputStream.write(batchData.getBatchDataType().ordinal());
      batchData.serializeData(outputStream);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static BatchData deserializeBatchData(ByteBuffer buffer) {
    if (buffer == null || (buffer.limit() - buffer.position()) == 0) {
      return null;
    }

    int length = buffer.getInt();
    TSDataType dataType = TSDataType.values()[buffer.get()];
    BatchData batchData = BatchDataType.deserialize(buffer.get(), dataType);
    Type.fromTsDataType(dataType).deserialize(buffer, batchData, length);
    batchData.resetBatchData();
    return batchData;
  }

  public static void serializeTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) {
    try {
      TSDataType dataType = timeValuePairs.get(0).getValue().getDataType();
      dataOutputStream.write(dataType.ordinal());
      dataOutputStream.writeInt(timeValuePairs.size());
      Type type = Type.fromTsDataType(dataType);
      for (TimeValuePair timeValuePair : timeValuePairs) {
        serializeTVPair(type, timeValuePair, dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable
    }
  }

  public static void serializeTVPair(
      TimeValuePair timeValuePair, DataOutputStream dataOutputStream) {
    if (timeValuePair.getValue() == null) {
      return;
    }
    TSDataType dataType = timeValuePair.getValue().getDataType();
    try {
      dataOutputStream.write(dataType.ordinal());
      serializeTVPair(Type.fromTsDataType(dataType), timeValuePair, dataOutputStream);
    } catch (IOException e) {
      // unreachable
    }
  }

  private static void serializeTVPair(
      Type type, TimeValuePair timeValuePair, DataOutputStream dataOutputStream)
      throws IOException {
    if (timeValuePair.getTimestamp() == Long.MIN_VALUE) {
      dataOutputStream.writeLong(Long.MIN_VALUE);
    } else {
      type.serialize(timeValuePair, dataOutputStream);
    }
  }

  public static List<TimeValuePair> deserializeTVPairs(ByteBuffer buffer) {
    if (buffer == null || buffer.limit() == 0) {
      return Collections.emptyList();
    }
    TSDataType dataType = TSDataType.values()[buffer.get()];
    int size = buffer.getInt();
    List<TimeValuePair> ret = new ArrayList<>(size);
    Type type = Type.fromTsDataType(dataType);
    for (int i = 0; i < size; i++) {
      ret.add(deserializeTVPair(type, buffer));
    }
    return ret;
  }

  public static TimeValuePair deserializeTVPair(ByteBuffer buffer) {
    if (buffer == null || (buffer.limit() - buffer.position() == 0)) {
      return null;
    }
    Type type = Type.fromTsDataType(TSDataType.values()[buffer.get()]);
    return deserializeTVPair(type, buffer);
  }

  private static TimeValuePair deserializeTVPair(Type type, ByteBuffer buffer) {
    long time = buffer.getLong();
    return new TimeValuePair(time, time == Long.MIN_VALUE ? null : type.deserialize(buffer));
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
    // TODO-Cluster: replace with a no-copy method
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

  public static void serializePrivilegeTypeSet(
      Set<PrivilegeType> types, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(types.size());
      for (PrivilegeType type : types) {
        dataOutputStream.writeInt(type.ordinal());
      }
    } catch (IOException e) {
      //
    }
  }

  public static void deserializePrivilegeTypeSet(Set<PrivilegeType> types, ByteBuffer buffer) {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      types.add(PrivilegeType.values()[buffer.getInt()]);
    }
  }
}
