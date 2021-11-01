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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchData.BatchDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

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
        case VECTOR:
          for (int i = 0; i < length; i++) {
            outputStream.writeLong(batchData.getTimeByIndex(i));
            TsPrimitiveType[] values = batchData.getVectorByIndex(i);
            outputStream.writeInt(values.length);
            for (TsPrimitiveType value : values) {
              if (value == null) {
                outputStream.write(0);
              } else {
                outputStream.write(1);
                outputStream.write(value.getDataType().serialize());
                switch (value.getDataType()) {
                  case BOOLEAN:
                    outputStream.writeBoolean(value.getBoolean());
                    break;
                  case DOUBLE:
                    outputStream.writeDouble(value.getDouble());
                    break;
                  case FLOAT:
                    outputStream.writeFloat(value.getFloat());
                    break;
                  case TEXT:
                    Binary binary = value.getBinary();
                    outputStream.writeInt(binary.getLength());
                    outputStream.write(binary.getValues());
                    break;
                  case INT64:
                    outputStream.writeLong(value.getLong());
                    break;
                  case INT32:
                    outputStream.writeInt(value.getInt());
                    break;
                }
              }
            }
          }
      }
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
      case VECTOR:
        for (int i = 0; i < length; i++) {
          long time = buffer.getLong();
          int valuesLength = buffer.getInt();
          TsPrimitiveType[] values = new TsPrimitiveType[valuesLength];
          for (int j = 0; j < valuesLength; j++) {
            boolean notNull = (buffer.get() == 1);
            if (notNull) {
              switch (TSDataType.values()[buffer.get()]) {
                case BOOLEAN:
                  values[j] = new TsPrimitiveType.TsBoolean(buffer.get() == 1);
                  break;
                case DOUBLE:
                  values[j] = new TsPrimitiveType.TsDouble(buffer.getDouble());
                  break;
                case FLOAT:
                  values[j] = new TsPrimitiveType.TsFloat(buffer.getFloat());
                  break;
                case TEXT:
                  int len = buffer.getInt();
                  byte[] bytes = new byte[len];
                  buffer.get(bytes);
                  values[j] = new TsPrimitiveType.TsBinary(new Binary(bytes));
                  break;
                case INT64:
                  values[j] = new TsPrimitiveType.TsLong(buffer.getLong());
                  break;
                case INT32:
                  values[j] = new TsPrimitiveType.TsInt(buffer.getInt());
                  break;
              }
            }
          }
          batchData.putVector(time, values);
        }
        break;
    }
    batchData.resetBatchData();
    return batchData;
  }

  private static void serializeTextTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) throws IOException {
    for (TimeValuePair timeValuePair : timeValuePairs) {
      dataOutputStream.writeLong(timeValuePair.getTimestamp());
      if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
        dataOutputStream.writeInt(timeValuePair.getValue().getBinary().getLength());
        dataOutputStream.write(timeValuePair.getValue().getBinary().getValues());
      }
    }
  }

  private static void serializeBooleanTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) throws IOException {
    for (TimeValuePair timeValuePair : timeValuePairs) {
      dataOutputStream.writeLong(timeValuePair.getTimestamp());
      if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
        dataOutputStream.writeBoolean(timeValuePair.getValue().getBoolean());
      }
    }
  }

  private static void serializeIntTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) throws IOException {
    for (TimeValuePair timeValuePair : timeValuePairs) {
      dataOutputStream.writeLong(timeValuePair.getTimestamp());
      if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
        dataOutputStream.writeInt(timeValuePair.getValue().getInt());
      }
    }
  }

  private static void serializeLongTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) throws IOException {
    for (TimeValuePair timeValuePair : timeValuePairs) {
      dataOutputStream.writeLong(timeValuePair.getTimestamp());
      if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
        dataOutputStream.writeLong(timeValuePair.getValue().getLong());
      }
    }
  }

  private static void serializeFloatTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) throws IOException {
    for (TimeValuePair timeValuePair : timeValuePairs) {
      dataOutputStream.writeLong(timeValuePair.getTimestamp());
      if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
        dataOutputStream.writeFloat(timeValuePair.getValue().getFloat());
      }
    }
  }

  private static void serializeDoubleTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) throws IOException {
    for (TimeValuePair timeValuePair : timeValuePairs) {
      dataOutputStream.writeLong(timeValuePair.getTimestamp());
      if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
        dataOutputStream.writeDouble(timeValuePair.getValue().getDouble());
      }
    }
  }

  public static void serializeTVPairs(
      List<TimeValuePair> timeValuePairs, DataOutputStream dataOutputStream) {
    try {
      TSDataType dataType = timeValuePairs.get(0).getValue().getDataType();
      dataOutputStream.write(dataType.ordinal());
      dataOutputStream.writeInt(timeValuePairs.size());
      switch (timeValuePairs.get(0).getValue().getDataType()) {
        case TEXT:
          serializeTextTVPairs(timeValuePairs, dataOutputStream);
          break;
        case BOOLEAN:
          serializeBooleanTVPairs(timeValuePairs, dataOutputStream);
          break;
        case INT64:
          serializeLongTVPairs(timeValuePairs, dataOutputStream);
          break;
        case INT32:
          serializeIntTVPairs(timeValuePairs, dataOutputStream);
          break;
        case FLOAT:
          serializeFloatTVPairs(timeValuePairs, dataOutputStream);
          break;
        case DOUBLE:
          serializeDoubleTVPairs(timeValuePairs, dataOutputStream);
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
      switch (dataType) {
        case TEXT:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
            dataOutputStream.writeInt(timeValuePair.getValue().getBinary().getLength());
            dataOutputStream.write(timeValuePair.getValue().getBinary().getValues());
          }
          break;
        case BOOLEAN:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
            dataOutputStream.writeBoolean(timeValuePair.getValue().getBoolean());
          }
          break;
        case INT64:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
            dataOutputStream.writeLong(timeValuePair.getValue().getLong());
          }
          break;
        case INT32:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
            dataOutputStream.writeInt(timeValuePair.getValue().getInt());
          }
          break;
        case FLOAT:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
            dataOutputStream.writeFloat(timeValuePair.getValue().getFloat());
          }
          break;
        case DOUBLE:
          dataOutputStream.writeLong(timeValuePair.getTimestamp());
          if (timeValuePair.getTimestamp() != Long.MIN_VALUE) {
            dataOutputStream.writeDouble(timeValuePair.getValue().getDouble());
          }
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  private static void deserializeDoubleTVPairs(
      ByteBuffer buffer, List<TimeValuePair> ret, int size, TSDataType dataType) {
    for (int i = 0; i < size; i++) {
      long time = buffer.getLong();
      TimeValuePair pair =
          time != Long.MIN_VALUE
              ? new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getDouble()))
              : new TimeValuePair(time, null);
      ret.add(pair);
    }
  }

  private static void deserializeFloatTVPairs(
      ByteBuffer buffer, List<TimeValuePair> ret, int size, TSDataType dataType) {
    for (int i = 0; i < size; i++) {
      long time = buffer.getLong();
      TimeValuePair pair =
          time != Long.MIN_VALUE
              ? new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getFloat()))
              : new TimeValuePair(time, null);
      ret.add(pair);
    }
  }

  private static void deserializeIntTVPairs(
      ByteBuffer buffer, List<TimeValuePair> ret, int size, TSDataType dataType) {
    for (int i = 0; i < size; i++) {
      long time = buffer.getLong();
      TimeValuePair pair =
          time != Long.MIN_VALUE
              ? new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getInt()))
              : new TimeValuePair(time, null);
      ret.add(pair);
    }
  }

  private static void deserializeLongTVPairs(
      ByteBuffer buffer, List<TimeValuePair> ret, int size, TSDataType dataType) {
    for (int i = 0; i < size; i++) {
      long time = buffer.getLong();
      TimeValuePair pair =
          time != Long.MIN_VALUE
              ? new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getLong()))
              : new TimeValuePair(time, null);
      ret.add(pair);
    }
  }

  private static void deserializeBooleanTVPairs(
      ByteBuffer buffer, List<TimeValuePair> ret, int size, TSDataType dataType) {
    for (int i = 0; i < size; i++) {
      long time = buffer.getLong();
      TimeValuePair pair =
          time != Long.MIN_VALUE
              ? new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.get() == 1))
              : new TimeValuePair(time, null);
      ret.add(pair);
    }
  }

  private static void deserializeTextTVPairs(
      ByteBuffer buffer, List<TimeValuePair> ret, int size, TSDataType dataType) {
    for (int i = 0; i < size; i++) {
      long time = buffer.getLong();
      TimeValuePair pair;
      if (time != Long.MIN_VALUE) {
        int bytesLen = buffer.getInt();
        byte[] bytes = new byte[bytesLen];
        buffer.get(bytes);
        TsPrimitiveType primitiveType = TsPrimitiveType.getByType(dataType, new Binary(bytes));
        pair = new TimeValuePair(time, primitiveType);
      } else {
        pair = new TimeValuePair(time, null);
      }
      ret.add(pair);
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
        deserializeDoubleTVPairs(buffer, ret, size, dataType);
        break;
      case FLOAT:
        deserializeFloatTVPairs(buffer, ret, size, dataType);
        break;
      case INT32:
        deserializeIntTVPairs(buffer, ret, size, dataType);
        break;
      case INT64:
        deserializeLongTVPairs(buffer, ret, size, dataType);
        break;
      case BOOLEAN:
        deserializeBooleanTVPairs(buffer, ret, size, dataType);
        break;
      case TEXT:
        deserializeTextTVPairs(buffer, ret, size, dataType);
        break;
    }
    return ret;
  }

  public static TimeValuePair deserializeTVPair(ByteBuffer buffer) {
    if (buffer == null || (buffer.limit() - buffer.position() == 0)) {
      return null;
    }
    TSDataType dataType = TSDataType.values()[buffer.get()];
    long time = buffer.getLong();
    if (time == Long.MIN_VALUE) {
      return new TimeValuePair(time, null);
    }
    switch (dataType) {
      case DOUBLE:
        return new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getDouble()));
      case FLOAT:
        return new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getFloat()));
      case INT32:
        return new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getInt()));
      case INT64:
        return new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.getLong()));
      case BOOLEAN:
        return new TimeValuePair(time, TsPrimitiveType.getByType(dataType, buffer.get() == 1));
      case TEXT:
        int bytesLen = buffer.getInt();
        byte[] bytes = new byte[bytesLen];
        buffer.get(bytes);
        TsPrimitiveType primitiveType = TsPrimitiveType.getByType(dataType, new Binary(bytes));
        return new TimeValuePair(time, primitiveType);
    }
    return null;
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
}
