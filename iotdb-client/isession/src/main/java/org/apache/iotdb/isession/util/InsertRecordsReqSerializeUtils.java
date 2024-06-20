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
package org.apache.iotdb.isession.util;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.req.InsertRecordsRequest;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class InsertRecordsReqSerializeUtils {
  private static final Logger logger = LoggerFactory.getLogger(InsertRecordsReqSerializeUtils.class);

  /**
   * Serialize InsertRecordsRequest to ByteBuffer array.
   *
   * @param deviceIds device ids
   * @param measurementIdsList measurement ids list
   * @param timestamps timestamps
   * @param typesList data types list
   * @param valuesList values list
   * @param writeInfo write info
   * @return ByteBuffer array, the first element is schema buffer, the second element is data
   *     buffer, the third element is info buffer
   * @throws IOException if an I/O error occurs
   */
  public static ByteBuffer[] serializeInsertRecordsReq(
      List<String> deviceIds,
      List<List<String>> measurementIdsList,
      List<Long> timestamps,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      Map<String, Object> writeInfo)
      throws IOException {
    ByteBuffer schemaBuffer = serializeSchema(deviceIds, measurementIdsList);
    ByteBuffer dataBuffer = serializeValue(timestamps, typesList, valuesList);
    ByteBuffer infoBuffer = serializeInfo(writeInfo);
    return new ByteBuffer[] {schemaBuffer, dataBuffer, infoBuffer};
  }

  /**
   * Deserialize InsertRecordsRequest from ByteBuffer array.
   *
   * @param schemaBuffer schema buffer, containing device ids and measurement ids
   * @param valueBuffer value buffer, containing timestamps, data types and values
   * @param infoBuffer info buffer, containing write info
   * @return deserialized InsertRecordsRequest
   * @throws IOException if an I/O error occurs
   */
  public static InsertRecordsRequest deserializeInsertRecordsReq(
      ByteBuffer schemaBuffer, ByteBuffer valueBuffer, ByteBuffer infoBuffer) throws IOException {
    List<String> deviceIds = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<String>> measurementIds = new ArrayList<>();
    List<List<TSDataType>> dataTypes = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    deserializeSchema(schemaBuffer, deviceIds, measurementIds);
    deserializeValueBuffer(valueBuffer, timestamps, dataTypes, values);
    Map<String, Object> info = deserializeInfo(infoBuffer);
    return new InsertRecordsRequest(deviceIds, measurementIds, timestamps, dataTypes, values, info);
  }

  private static ByteBuffer serializeSchema(
      List<String> deviceIds, List<List<String>> measurementIdsList) throws IOException {
    // creating dictionary
    Map<String, Integer> dictionary = getDictionary(deviceIds, measurementIdsList);
    // serializing dictionary
    PublicBAOS schemaBufferOS = new PublicBAOS();
    serializeDictionary(dictionary, schemaBufferOS);
    dictionaryEncoding(dictionary, deviceIds, measurementIdsList, schemaBufferOS);
    ByteBuffer schemaBuffer;
    if (SessionConfig.enableRPCCompression) {
      ICompressor compressor = ICompressor.getCompressor(SessionConfig.rpcCompressionType);
      byte[] compressedData =
          compressor.compress(schemaBufferOS.getBuf(), 0, schemaBufferOS.size());
      schemaBuffer = ByteBuffer.allocate(compressedData.length + 5);
      schemaBuffer.put((byte) 1);
      ReadWriteIOUtils.write(schemaBufferOS.size(), schemaBuffer);
      schemaBuffer.put(compressedData);
    } else {
      schemaBuffer = ByteBuffer.allocate(schemaBufferOS.size() + 1);
      schemaBuffer.put((byte) 0);
      schemaBuffer.put(schemaBufferOS.getBuf(), 0, schemaBufferOS.size());
    }
    schemaBuffer.flip();
    return schemaBuffer;
  }

  private static void deserializeSchema(
      ByteBuffer schemaBuffer,
      List<String> decodedDeviceIds,
      List<List<String>> decodedMeasurementIdsList)
      throws IOException {
    boolean compressed = ReadWriteIOUtils.readBool(schemaBuffer);
    ByteBuffer buffer = schemaBuffer;
    if (compressed) {
      int uncompressedLength = ReadWriteIOUtils.readInt(schemaBuffer);
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(SessionConfig.rpcCompressionType);
      buffer = ByteBuffer.allocate(uncompressedLength);
      unCompressor.uncompress(schemaBuffer, buffer);
      buffer.flip();
    }

    String[] dictionary = getDictionary(buffer);
    IntRleDecoder decoder = new IntRleDecoder();
    deserializeDevices(buffer, decoder, dictionary, decodedDeviceIds);
    deserializeMeasurementIds(buffer, decoder, dictionary, decodedMeasurementIdsList);
  }

  private static String[] getDictionary(ByteBuffer schemaBuffer) {
    int dictionarySize = ReadWriteIOUtils.readInt(schemaBuffer);
    String[] dictionary = new String[dictionarySize];
    for (int i = 0; i < dictionarySize; ++i) {
      dictionary[i] = ReadWriteIOUtils.readString(schemaBuffer);
    }
    return dictionary;
  }

  private static void deserializeDevices(
      ByteBuffer buffer,
      IntRleDecoder decoder,
      String[] dictionary,
      List<String> decodedDeviceIds) {
    int deviceSize = decoder.readInt(buffer);
    for (int i = 0; i < deviceSize; ++i) {
      StringBuilder builder = new StringBuilder();
      int wordSize = decoder.readInt(buffer);
      for (int j = 0; j < wordSize; ++j) {
        builder.append(dictionary[decoder.readInt(buffer)]);
        if (j != wordSize - 1) {
          builder.append(".");
        }
      }
      decodedDeviceIds.add(builder.toString());
    }
  }

  private static void deserializeMeasurementIds(
      ByteBuffer buffer,
      IntRleDecoder decoder,
      String[] dictionary,
      List<List<String>> decodedMeasurementIdsList) {
    int listNum = decoder.readInt(buffer);
    for (int i = 0; i < listNum; ++i) {
      int currListSize = decoder.readInt(buffer);
      List<String> currList = new ArrayList<>(currListSize);
      for (int j = 0; j < currListSize; ++j) {
        currList.add(dictionary[decoder.readInt(buffer)]);
      }
      decodedMeasurementIdsList.add(currList);
    }
  }

  private static Map<String, Integer> getDictionary(
      List<String> deviceId, List<List<String>> measurementIdsList) {
    Map<String, Integer> dictionary = new LinkedHashMap<>();
    AtomicInteger count = new AtomicInteger(0);
    deviceId.forEach(
        s -> {
          Arrays.stream(s.split("\\."))
              .forEach(
                  word -> {
                    dictionary.computeIfAbsent(word, k -> count.getAndIncrement());
                  });
        });
    measurementIdsList.forEach(
        measurementIds -> {
          measurementIds.forEach(
              measurementId -> {
                dictionary.computeIfAbsent(measurementId, k -> count.getAndIncrement());
              });
        });
    return dictionary;
  }

  private static void serializeDictionary(Map<String, Integer> dictionary, PublicBAOS buffer)
      throws IOException {
    ReadWriteIOUtils.write(dictionary.size(), buffer);
    for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), buffer);
    }
  }

  private static void dictionaryEncoding(
      Map<String, Integer> dictionary,
      List<String> deviceIds,
      List<List<String>> measurementIdsList,
      PublicBAOS buffer)
      throws IOException {
    IntRleEncoder rleEncoder = new IntRleEncoder();
    rleEncoder.encode(deviceIds.size(), buffer);
    for (String deviceId : deviceIds) {
      String[] words = deviceId.split("\\.");
      rleEncoder.encode(words.length, buffer);
      Arrays.stream(words)
          .forEach(
              word -> {
                rleEncoder.encode(dictionary.get(word), buffer);
              });
    }
    rleEncoder.encode(measurementIdsList.size(), buffer);
    for (List<String> measurements : measurementIdsList) {
      rleEncoder.encode(measurements.size(), buffer);
      for (String measurement : measurements) {
        rleEncoder.encode(dictionary.get(measurement), buffer);
      }
    }
    rleEncoder.flush(buffer);
  }

  private static ByteBuffer serializeValue(
      List<Long> timestamps, List<List<TSDataType>> typesList, List<List<Object>> valuesList)
      throws IOException {
    PublicBAOS valueSerializeBuffer = new PublicBAOS();
    serializeTimestamps(timestamps, valueSerializeBuffer);
    serializeTypesAndValues(typesList, valuesList, valueSerializeBuffer);
    ByteBuffer outBuffer = null;
    if (SessionConfig.enableRPCCompression) {
      ICompressor compressor = ICompressor.getCompressor(SessionConfig.rpcCompressionType);
      byte[] compressed =
          compressor.compress(valueSerializeBuffer.getBuf(), 0, valueSerializeBuffer.size());
      outBuffer = ByteBuffer.allocate(compressed.length + 5);
      outBuffer.put((byte) 1);
      ReadWriteIOUtils.write(valueSerializeBuffer.size(), outBuffer);
      outBuffer.put(compressed);
    } else {
      outBuffer = ByteBuffer.allocate(valueSerializeBuffer.size() + 1);
      outBuffer.put((byte) 0);
      outBuffer.put(valueSerializeBuffer.getBuf(), 0, valueSerializeBuffer.size());
    }
    outBuffer.flip();
    return outBuffer;
  }

  private static void serializeTimestamps(List<Long> timestamps, PublicBAOS valueBuffer)
      throws IOException {
    Encoder timeEncoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.GORILLA).getEncoder(TSDataType.INT64);
    PublicBAOS timeBuffer = new PublicBAOS();
    for (long timestamp : timestamps) {
      timeEncoder.encode(timestamp, timeBuffer);
    }
    timeEncoder.flush(timeBuffer);
    valueBuffer.write(timeBuffer.getBuf(), 0, timeBuffer.size());
  }

  private static void serializeTypesAndValues(
      List<List<TSDataType>> typesList, List<List<Object>> valuesList, PublicBAOS buffer)
      throws IOException {
    int recordCount = typesList.size();
    ReadWriteIOUtils.write(recordCount, buffer);
    List<Integer> intList = new ArrayList<>();
    List<Long> longList = new ArrayList<>();
    List<Float> floatList = new ArrayList<>();
    List<Double> doubleList = new ArrayList<>();
    List<Boolean> booleanList = new ArrayList<>();
    List<String> stringList = new ArrayList<>();
    PublicBAOS typeBuffer = new PublicBAOS();
    for (int i = 0; i < recordCount; ++i) {
      List<TSDataType> types = typesList.get(i);
      List<Object> values = valuesList.get(i);
      int size = types.size();
      ReadWriteIOUtils.write(size, buffer);
      for (int j = 0; j < size; ++j) {
        switch (types.get(j)) {
          case INT32:
            intList.add((Integer) values.get(j));
            break;
          case INT64:
            longList.add((Long) values.get(j));
            break;
          case FLOAT:
            floatList.add((Float) values.get(j));
            break;
          case DOUBLE:
            doubleList.add((Double) values.get(j));
            break;
          case BOOLEAN:
            booleanList.add((Boolean) values.get(j));
            break;
          case TEXT:
            stringList.add((String) values.get(j));
            break;
          default:
            throw new IOException("Unsupported data type " + types.get(j));
        }
        ReadWriteIOUtils.write(types.get(j), typeBuffer);
      }
    }
    serializeIntList(intList, buffer);
    serializeLongList(longList, buffer);
    serializeFloatList(floatList, buffer);
    serializeDoubleList(doubleList, buffer);
    serializeBooleanList(booleanList, buffer);
    serializeStringList(stringList, buffer);
    buffer.write(typeBuffer.getBuf(), 0, typeBuffer.size());
  }

  private static void serializeIntList(List<Integer> values, PublicBAOS buffer) throws IOException {
    ReadWriteIOUtils.write(values.size(), buffer);
    if (values.isEmpty()) {
      return;
    }
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.GORILLA).getEncoder(TSDataType.INT32);
    PublicBAOS intBuffer = new PublicBAOS();
    for (int value : values) {
      encoder.encode(value, intBuffer);
    }
    encoder.flush(intBuffer);
    buffer.write(intBuffer.getBuf(), 0, intBuffer.size());
  }

  private static void serializeLongList(List<Long> values, PublicBAOS buffer) throws IOException {
    ReadWriteIOUtils.write(values.size(), buffer);
    if (values.isEmpty()) {
      return;
    }
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.GORILLA).getEncoder(TSDataType.INT64);
    PublicBAOS longBuffer = new PublicBAOS();
    for (long value : values) {
      encoder.encode(value, longBuffer);
    }
    encoder.flush(longBuffer);
    buffer.write(longBuffer.getBuf(), 0, longBuffer.size());
  }

  private static void serializeFloatList(List<Float> values, PublicBAOS buffer) throws IOException {
    ReadWriteIOUtils.write(values.size(), buffer);
    if (values.isEmpty()) {
      return;
    }
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.GORILLA).getEncoder(TSDataType.FLOAT);
    PublicBAOS floatBuffer = new PublicBAOS();
    for (float value : values) {
      encoder.encode(value, floatBuffer);
    }
    encoder.flush(floatBuffer);
    buffer.write(floatBuffer.getBuf(), 0, floatBuffer.size());
  }

  private static void serializeDoubleList(List<Double> values, PublicBAOS buffer)
      throws IOException {
    ReadWriteIOUtils.write(values.size(), buffer);
    if (values.isEmpty()) {
      return;
    }
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.GORILLA).getEncoder(TSDataType.DOUBLE);
    PublicBAOS doubleBuffer = new PublicBAOS();
    for (double value : values) {
      encoder.encode(value, doubleBuffer);
    }
    encoder.flush(doubleBuffer);
    buffer.write(doubleBuffer.getBuf(), 0, doubleBuffer.size());
  }

  private static void serializeBooleanList(List<Boolean> values, PublicBAOS buffer)
      throws IOException {
    ReadWriteIOUtils.write(values.size(), buffer);
    if (values.isEmpty()) {
      return;
    }
    final int bitsPerByte = 8;

    int size = (values.size() + bitsPerByte - 1) / bitsPerByte;
    byte[] byteArray = new byte[size];

    for (int i = 0; i < values.size(); i++) {
      if (values.get(i)) {
        int arrayIndex = i / bitsPerByte;
        int bitPosition = i % bitsPerByte;
        byteArray[arrayIndex] |= (byte) (1 << bitPosition);
      }
    }

    ReadWriteIOUtils.write(size, buffer);
    buffer.write(byteArray);
  }

  private static void serializeStringList(List<String> values, PublicBAOS buffer)
      throws IOException {
    ReadWriteIOUtils.write(values.size(), buffer);
    if (values.isEmpty()) {
      return;
    }
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.DICTIONARY).getEncoder(TSDataType.TEXT);
    PublicBAOS stringBuffer = new PublicBAOS();
    for (String value : values) {
      encoder.encode(new Binary(value.getBytes()), stringBuffer);
    }
    encoder.flush(stringBuffer);
    buffer.write(stringBuffer.getBuf(), 0, stringBuffer.size());
  }

  private static void deserializeValueBuffer(
      ByteBuffer buffer,
      List<Long> outputTimestamps,
      List<List<TSDataType>> outputTypesList,
      List<List<Object>> outputValuesList)
      throws IOException {
    boolean compressed = ReadWriteIOUtils.readBool(buffer);
    ByteBuffer dataBuffer = buffer;
    if (compressed) {
      int uncompressedSize = ReadWriteIOUtils.readInt(buffer);
      dataBuffer = ByteBuffer.allocate(uncompressedSize);
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(SessionConfig.rpcCompressionType);
      unCompressor.uncompress(buffer, dataBuffer);
      dataBuffer.flip();
    }
    deserializeTime(dataBuffer, outputTimestamps);
    deserializeTypesAndValues(dataBuffer, outputTypesList, outputValuesList);
  }

  private static void deserializeTime(ByteBuffer buffer, List<Long> timestamps) throws IOException {
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.GORILLA, TSDataType.INT64);
    while (decoder.hasNext(buffer)) {
      timestamps.add(decoder.readLong(buffer));
    }
  }

  private static void deserializeTypesAndValues(
      ByteBuffer buffer, List<List<TSDataType>> typesList, List<List<Object>> valuesList)
      throws IOException {
    int recordCount = ReadWriteIOUtils.readInt(buffer);
    int[] recordSizeArray = new int[recordCount];
    for (int i = 0; i < recordCount; ++i) {
      recordSizeArray[i] = ReadWriteIOUtils.readInt(buffer);
    }
    int[] intArray = deserializeIntList(buffer);
    long[] longArray = deserializeLongList(buffer);
    float[] floatArray = deserializeFloatList(buffer);
    double[] doubleArray = deserializeDoubleList(buffer);
    boolean[] booleanArray = deserializeBooleanList(buffer);
    Binary[] stringArray = deserializeStringList(buffer);
    int[] indexes = new int[6];
    for (int i = 0; i < recordCount; ++i) {
      List<TSDataType> types = new ArrayList<>(recordSizeArray[i]);
      List<Object> values = new ArrayList<>(recordSizeArray[i]);
      for (int j = 0; j < recordSizeArray[i]; ++j) {
        byte b = buffer.get();
        types.add(TSDataType.deserialize(b));
        switch (types.get(j)) {
          case INT32:
            values.add(intArray[indexes[0]++]);
            break;
          case INT64:
            values.add(longArray[indexes[1]++]);
            break;
          case FLOAT:
            values.add(floatArray[indexes[2]++]);
            break;
          case DOUBLE:
            values.add(doubleArray[indexes[3]++]);
            break;
          case BOOLEAN:
            values.add(booleanArray[indexes[4]++]);
            break;
          case TEXT:
            values.add(stringArray[indexes[5]++]);
            break;
          default:
            throw new IOException("Unsupported data type " + types.get(j));
        }
      }
      typesList.add(types);
      valuesList.add(values);
    }
  }

  private static int[] deserializeIntList(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == 0) {
      return new int[0];
    }
    int[] intArray = new int[size];
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.GORILLA, TSDataType.INT32);
    for (int i = 0; i < size; ++i) {
      intArray[i] = decoder.readInt(buffer);
    }
    if (decoder.hasNext(buffer)) {
      System.out.println("ERROR");
    }
    return intArray;
  }

  private static long[] deserializeLongList(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == 0) {
      return new long[0];
    }
    long[] longArray = new long[size];
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.GORILLA, TSDataType.INT64);
    for (int i = 0; i < size; ++i) {
      longArray[i] = decoder.readLong(buffer);
    }
    return longArray;
  }

  private static float[] deserializeFloatList(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == 0) {
      return new float[0];
    }
    float[] floatArray = new float[size];
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.GORILLA, TSDataType.FLOAT);
    for (int i = 0; i < size; ++i) {
      floatArray[i] = decoder.readFloat(buffer);
    }
    return floatArray;
  }

  private static double[] deserializeDoubleList(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == 0) {
      return new double[0];
    }
    double[] doubleArray = new double[size];
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.GORILLA, TSDataType.DOUBLE);
    for (int i = 0; i < size; ++i) {
      doubleArray[i] = decoder.readDouble(buffer);
    }
    return doubleArray;
  }

  private static boolean[] deserializeBooleanList(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == 0) {
      return new boolean[0];
    }
    int byteSize = ReadWriteIOUtils.readInt(buffer);
    byte[] byteArray = new byte[byteSize];
    buffer.get(byteArray);
    boolean[] booleanArray = new boolean[size];
    for (int i = 0; i < size; i++) {
      int arrayIndex = i / 8;
      int bitPosition = i % 8;
      booleanArray[i] = (byteArray[arrayIndex] & (1 << bitPosition)) != 0;
    }
    return booleanArray;
  }

  private static Binary[] deserializeStringList(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == 0) {
      return new Binary[0];
    }
    Binary[] stringArray = new Binary[size];
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.DICTIONARY, TSDataType.TEXT);
    for (int i = 0; i < size; ++i) {
      stringArray[i] = decoder.readBinary(buffer);
    }
    return stringArray;
  }

  private static ByteBuffer serializeInfo(Map<String, Object> info) throws IOException {
    PublicBAOS baos = new PublicBAOS();
    ReadWriteIOUtils.write(info.size(), baos);
    for (Map.Entry<String, Object> entry : info.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), baos);
      Object value = entry.getValue();
      if (value instanceof Integer) {
        ReadWriteIOUtils.write(TSDataType.INT32.getType(), baos);
        ReadWriteIOUtils.write((Integer) value, baos);
      } else if (value instanceof Long) {
        ReadWriteIOUtils.write(TSDataType.INT64.getType(), baos);
        ReadWriteIOUtils.write((Long) value, baos);
      } else if (value instanceof Float) {
        ReadWriteIOUtils.write(TSDataType.FLOAT.getType(), baos);
        ReadWriteIOUtils.write((Float) value, baos);
      } else if (value instanceof Double) {
        ReadWriteIOUtils.write(TSDataType.DOUBLE.getType(), baos);
        ReadWriteIOUtils.write((Double) value, baos);
      } else if (value instanceof Boolean) {
        ReadWriteIOUtils.write(TSDataType.BOOLEAN.getType(), baos);
        ReadWriteIOUtils.write((Boolean) value, baos);
      } else if (value instanceof String) {
        ReadWriteIOUtils.write(TSDataType.TEXT.getType(), baos);
        ReadWriteIOUtils.write((String) value, baos);
      } else {
        throw new IOException("Unsupported data type " + value.getClass());
      }
    }
    ByteBuffer buffer = ByteBuffer.allocate(baos.size());
    buffer.put(baos.getBuf(), 0, baos.size());
    buffer.flip();
    return buffer;
  }

  private static Map<String, Object> deserializeInfo(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    Map<String, Object> info = new LinkedHashMap<>();
    for (int i = 0; i < size; ++i) {
      String key = ReadWriteIOUtils.readString(buffer);
      TSDataType type = TSDataType.deserialize(buffer.get());
      switch (type) {
        case INT32:
          info.put(key, ReadWriteIOUtils.readInt(buffer));
          break;
        case INT64:
          info.put(key, ReadWriteIOUtils.readLong(buffer));
          break;
        case FLOAT:
          info.put(key, ReadWriteIOUtils.readFloat(buffer));
          break;
        case DOUBLE:
          info.put(key, ReadWriteIOUtils.readDouble(buffer));
          break;
        case BOOLEAN:
          info.put(key, ReadWriteIOUtils.readBool(buffer));
          break;
        case TEXT:
          info.put(key, ReadWriteIOUtils.readString(buffer));
          break;
        default:
          throw new IOException("Unsupported data type " + type);
      }
    }
    return info;
  }
}
