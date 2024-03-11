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
import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
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

  public static InsertRecordsRequest deserializeInsertRecordsReq(
      ByteBuffer schemaBuffer, ByteBuffer valueBuffer, ByteBuffer auxiliaryBuffer) {
    return null;
  }

  public static void main(String[] args) throws Exception {
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
