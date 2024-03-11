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
import org.apache.iotdb.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.PLAIN).getEncoder(TSDataType.INT32);
    for (int i = 0; i < recordCount; ++i) {
      List<TSDataType> types = typesList.get(i);
      List<Object> values = valuesList.get(i);
      int size = types.size();
      encoder.encode(size, buffer);
      for (int j = 0; j < size; ++j) {
        int offset = -1;
        switch (types.get(j)) {
          case INT32:
            offset = intList.size();
            intList.add((Integer) values.get(j));
            break;
          case INT64:
            offset = longList.size();
            longList.add((Long) values.get(j));
            break;
          case FLOAT:
            offset = floatList.size();
            floatList.add((Float) values.get(j));
            break;
          case DOUBLE:
            offset = doubleList.size();
            doubleList.add((Double) values.get(j));
            break;
          case BOOLEAN:
            offset = booleanList.size();
            booleanList.add((Boolean) values.get(j));
            break;
          case TEXT:
            offset = stringList.size();
            stringList.add((String) values.get(j));
            break;
          default:
            throw new IOException("Unsupported data type " + types.get(j));
        }
        encoder.encode((int) (types.get(j).getType()), buffer);
        encoder.encode(offset, buffer);
      }
    }
    encoder.flush(buffer);
    serializeIntList(intList, buffer);
    serializeLongList(longList, buffer);
    serializeFloatList(floatList, buffer);
    serializeDoubleList(doubleList, buffer);
    serializeBooleanList(booleanList, buffer);
    serializeStringList(stringList, buffer);
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
    ReadWriteIOUtils.write(intBuffer.getBuf().length, buffer);
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
    ReadWriteIOUtils.write(longBuffer.getBuf().length, buffer);
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
    ReadWriteIOUtils.write(floatBuffer.getBuf().length, buffer);
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
    ReadWriteIOUtils.write(doubleBuffer.getBuf().length, buffer);
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
    ReadWriteIOUtils.write(stringBuffer.getBuf().length, buffer);
    buffer.write(stringBuffer.getBuf(), 0, stringBuffer.size());
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
          value.add(true);
          singleRowSize += 2;
          break;
        case 1:
          types.add(TSDataType.INT32);
          value.add(1);
          singleRowSize += 5;
          break;
        case 2:
          types.add(TSDataType.INT64);
          value.add(1L);
          singleRowSize += 9;
          break;
        case 3:
          types.add(TSDataType.FLOAT);
          value.add(1.0f);
          singleRowSize += 5;
          break;
        case 4:
          types.add(TSDataType.DOUBLE);
          value.add(1.0);
          singleRowSize += 9;
          break;
        case 5:
          types.add(TSDataType.TEXT);
          value.add("123");
          singleRowSize += 7;
          break;
      }
    }
    singleRowSize += 8;
    int totalSize = singleRowSize * 1000;
    for (int i = 0; i < 1000; ++i) {
      timestamps.add(System.currentTimeMillis());
      measurements.add(types);
      values.add(value);
    }
    PublicBAOS baos = new PublicBAOS();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(values);
    //    oos.writeObject(measurements);
    //    oos.writeObject(timestamps);
    oos.flush();
    oos.close();
    ByteBuffer buffer = serializeValue(timestamps, measurements, values);
    System.out.println("Original Size is " + totalSize);
    System.out.println("Serialized Size is " + buffer.limit());
    System.out.println("Java serialize size is " + baos.size());
    System.out.println();
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
