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
package org.apache.iotdb.session.util;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.session.req.InsertRecordsRequest;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class SessionRPCUtils {
  public static ByteBuffer[] serializeInsertRecordsReq(
      List<String> deviceIds,
      List<List<String>> measurementIdsList,
      List<Long> timestamp,
      List<List<TSDataType>> tpyesList,
      List<List<Object>> valuesList)
      throws IOException {
    ByteBuffer schemaBuffer = serializeSchema(deviceIds, measurementIdsList);

    return null;
  }

  private static ByteBuffer serializeSchema(
      List<String> deviceIds, List<List<String>> measurementIdsList) throws IOException {
    // creating dictionary
    Map<String, Integer> dictionary = getDictionary(deviceIds, measurementIdsList);
    // serializing dictionary
    PublicBAOS schemaBufferOS = new PublicBAOS();
    serializeDictionary(dictionary, schemaBufferOS);
    dictionaryEncoding(dictionary, deviceIds, measurementIdsList, schemaBufferOS);
    ByteBuffer schemaBuffer = null;
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
      byte[] uncompressed = new byte[uncompressedLength];
      unCompressor.uncompress(schemaBuffer.array(), 5, schemaBuffer.limit() - 5, uncompressed, 0);
      buffer = ByteBuffer.wrap(uncompressed);
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
      byte[] uncompressed = new byte[uncompressedSize];
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(SessionConfig.rpcCompressionType);
      unCompressor.uncompress(buffer.array(), 5, buffer.limit() - 5, uncompressed, 0);
      dataBuffer = ByteBuffer.wrap(uncompressed);
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
    String[] stringArray = deserializeStringList(buffer);
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

  private static String[] deserializeStringList(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == 0) {
      return new String[0];
    }
    String[] stringArray = new String[size];
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.DICTIONARY, TSDataType.TEXT);
    for (int i = 0; i < size; ++i) {
      stringArray[i] = decoder.readBinary(buffer).toString();
    }
    return stringArray;
  }

  public static InsertRecordsRequest deserializeInsertRecordsReq(
      ByteBuffer schemaBuffer, ByteBuffer valueBuffer, ByteBuffer auxiliaryBuffer) {
    return null;
  }

  public static void main(String[] args) throws Exception {
    testValue();
  }

  private static void testValue() throws IOException, ClassNotFoundException {
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> measurements = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<Object> value = new ArrayList<>();
    int singleRowSize = 0;
    for (int j = 0; j < 100; ++j) {
      switch (((byte) j % 6)) {
        case 0:
          types.add(TSDataType.BOOLEAN);
          value.add(new Random().nextBoolean());
          singleRowSize += 2;
          break;
        case 1:
          types.add(TSDataType.INT32);
          value.add(new Random().nextInt());
          singleRowSize += 5;
          break;
        case 2:
          types.add(TSDataType.INT64);
          value.add(new Random().nextLong());
          singleRowSize += 9;
          break;
        case 3:
          types.add(TSDataType.FLOAT);
          value.add(new Random().nextFloat());
          singleRowSize += 5;
          break;
        case 4:
          types.add(TSDataType.DOUBLE);
          value.add(new Random().nextDouble());
          singleRowSize += 9;
          break;
        case 5:
          types.add(TSDataType.TEXT);
          value.add(String.valueOf(new Random().nextInt(100)));
          singleRowSize += 7;
          break;
      }
    }
    singleRowSize += 8;
    int totalSize = singleRowSize * 10000;
    for (int i = 0; i < 10000; ++i) {
      timestamps.add(System.currentTimeMillis());
      measurements.add(types);
      values.add(value);
    }
    long startTime = System.currentTimeMillis();
    ByteBuffer buffer = serializeValue(timestamps, measurements, values);
    System.out.println("Serialize time cost is " + (System.currentTimeMillis() - startTime));
    System.out.println("Original Size is " + totalSize);
    System.out.println("Serialized Size is " + buffer.limit());
    System.out.println("Compression rate is " + (double) totalSize / buffer.limit());
    List<Long> decodedTime = new ArrayList<>();
    List<List<TSDataType>> decodedTypes = new ArrayList<>();
    List<List<Object>> decodedValues = new ArrayList<>();
    startTime = System.currentTimeMillis();
    deserializeValueBuffer(buffer, decodedTime, decodedTypes, decodedValues);
    System.out.println("Time cost is " + (System.currentTimeMillis() - startTime));
    if (!decodedTime.equals(timestamps)
        || !decodedTypes.equals(measurements)
        || !decodedValues.equals(values)) {
      System.out.println("ERROR");
    }
  }

  private static void testSchema() throws IOException {
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementIdsList = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    int size = 0;
    int measurementSize = 0;
    for (int i = 0; i < 100; ++i) {
      String deviceId = "root.sg1.s10.d" + i;
      size += deviceId.getBytes().length;
      deviceIds.add("root.sg1.s10.d" + i);
      measurementSize += ("s" + i).getBytes().length;
      measurementIds.add("s" + i);
    }
    for (int i = 0; i < 100; ++i) {
      measurementIdsList.add(measurementIds);
    }
    size += measurementSize * 100;

    long startTime = System.currentTimeMillis();
    ByteBuffer buffer = serializeSchema(deviceIds, measurementIdsList);
    System.out.println("Time cost is " + (System.currentTimeMillis() - startTime));
    System.out.println("Original Size is " + size);
    System.out.println("Serialized Size is " + buffer.remaining());
    List<String> decodedDeviceIds = new ArrayList<>();
    List<List<String>> decodedMeasurementIdsList = new ArrayList<>();
    deserializeSchema(buffer, decodedDeviceIds, decodedMeasurementIdsList);
    if (deviceIds.equals(decodedDeviceIds)
        && measurementIdsList.equals(decodedMeasurementIdsList)) {
      System.out.println("Test passed");
    } else {
      System.out.println("Test failed");
    }
  }
}
