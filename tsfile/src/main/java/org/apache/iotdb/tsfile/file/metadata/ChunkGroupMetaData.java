/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata of ChunkGroup.
 */
public class ChunkGroupMetaData {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkGroupMetaData.class);

  /**
   * Name of device, this field is not serialized.
   */
  private String deviceID;

  /**
   * Byte size of this metadata. this field is not serialized.
   */
  private int serializedSize;

  /**
   * All time series chunks in this chunk group.
   */
  private List<ChunkMetaData> chunkMetaDataList;

  private ChunkGroupMetaData() {
    chunkMetaDataList = new ArrayList<>();
  }

  /**
   * constructor of ChunkGroupMetaData.
   *
   * @param deviceID name of device
   * @param chunkMetaDataList all time series chunks in this chunk group. Can not be Null. notice:
   * after constructing a ChunkGroupMetadata instance. Donot use list.add() to modify
   * `chunkMetaDataList`. Instead, use addTimeSeriesChunkMetaData() to make sure getSerializedSize()
   * is correct.
   */
  public ChunkGroupMetaData(String deviceID, List<ChunkMetaData> chunkMetaDataList) {
    assert chunkMetaDataList != null;
    this.deviceID = deviceID;
    this.chunkMetaDataList = chunkMetaDataList;
    reCalculateSerializedSize();
  }

  /**
   * deserialize from InputStream.
   *
   * @param inputStream inputStream
   * @return ChunkGroupMetaData object
   * @throws IOException IOException
   */
  public static ChunkGroupMetaData deserializeFrom(InputStream inputStream) throws IOException {
    ChunkGroupMetaData chunkGroupMetaData = new ChunkGroupMetaData();

    chunkGroupMetaData.deviceID = ReadWriteIOUtils.readString(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    chunkGroupMetaData.serializedSize =
        Integer.BYTES + chunkGroupMetaData.deviceID.length() + Integer.BYTES;

    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      ChunkMetaData metaData = ChunkMetaData.deserializeFrom(inputStream);
      chunkMetaDataList.add(metaData);
      chunkGroupMetaData.serializedSize += metaData.getSerializedSize();
    }
    chunkGroupMetaData.chunkMetaDataList = chunkMetaDataList;

    return chunkGroupMetaData;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkGroupMetaData object
   * @throws IOException IOException
   */
  public static ChunkGroupMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
    ChunkGroupMetaData chunkGroupMetaData = new ChunkGroupMetaData();

    chunkGroupMetaData.deviceID = (ReadWriteIOUtils.readString(buffer));

    int size = ReadWriteIOUtils.readInt(buffer);

    chunkGroupMetaData.serializedSize =
        Integer.BYTES + chunkGroupMetaData.deviceID.length() + Integer.BYTES;

    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      ChunkMetaData metaData = ChunkMetaData.deserializeFrom(buffer);
      chunkMetaDataList.add(metaData);
      chunkGroupMetaData.serializedSize += metaData.getSerializedSize();
    }
    chunkGroupMetaData.chunkMetaDataList = chunkMetaDataList;

    return chunkGroupMetaData;
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  void reCalculateSerializedSize() {
    serializedSize = Integer.BYTES + deviceID.length() + Integer.BYTES; // size of chunkMetaDataList
    for (ChunkMetaData chunk : chunkMetaDataList) {
      serializedSize += chunk.getSerializedSize();
    }
  }

  /**
   * add time series chunk metadata to list. THREAD NOT SAFE
   *
   * @param metadata time series metadata to add
   */
  public void addTimeSeriesChunkMetaData(ChunkMetaData metadata) {
    if (chunkMetaDataList == null) {
      chunkMetaDataList = new ArrayList<>();
    }
    chunkMetaDataList.add(metadata);
    serializedSize += metadata.getSerializedSize();
  }

  public List<ChunkMetaData> getChunkMetaDataList() {
    return chunkMetaDataList == null ? null : Collections.unmodifiableList(chunkMetaDataList);
  }

  @Override
  public String toString() {
    return String.format("ChunkGroupMetaData{ time series chunk list: %s }", chunkMetaDataList);
  }

  public String getDeviceID() {
    return deviceID;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return byte length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(deviceID, outputStream);

    byteLen += ReadWriteIOUtils.write(chunkMetaDataList.size(), outputStream);
    for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
      byteLen += chunkMetaData.serializeTo(outputStream);
    }
    assert byteLen == getSerializedSize();
    return byteLen;
  }

  /**
   * serialize to ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return byte length
   * @throws IOException IOException
   */
  public int serializeTo(ByteBuffer buffer) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(deviceID, buffer);

    byteLen += ReadWriteIOUtils.write(chunkMetaDataList.size(), buffer);
    for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
      byteLen += chunkMetaData.serializeTo(buffer);
    }
    assert byteLen == getSerializedSize();

    return byteLen;
  }

}
