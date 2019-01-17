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

public class TsDeviceMetadata {

  /**
   * size of ChunkGroupMetadataBlock in byte.
   **/
  private int serializedSize =
      2 * Long.BYTES + Integer.BYTES;// this field does not need to be serialized.

  /**
   * start time for a device.
   **/
  private long startTime = Long.MAX_VALUE;

  /**
   * end time for a device.
   **/
  private long endTime = Long.MIN_VALUE;

  /**
   * Row groups in this file.
   */
  private List<ChunkGroupMetaData> chunkGroupMetadataList = new ArrayList<>();

  public TsDeviceMetadata() {
  }

  /**
   * deserialize from the inputstream.
   *
   * @param inputStream -input stream to deserialize
   * @return -device meta data
   */
  public static TsDeviceMetadata deserializeFrom(InputStream inputStream) throws IOException {
    TsDeviceMetadata deviceMetadata = new TsDeviceMetadata();

    deviceMetadata.startTime = ReadWriteIOUtils.readLong(inputStream);
    deviceMetadata.endTime = ReadWriteIOUtils.readLong(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    if (size > 0) {
      List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        chunkGroupMetaDataList.add(ChunkGroupMetaData.deserializeFrom(inputStream));
      }
      deviceMetadata.chunkGroupMetadataList = chunkGroupMetaDataList;
    }

    deviceMetadata.reCalculateSerializedSize();
    return deviceMetadata;
  }

  /**
   * deserialize from the given buffer.
   *
   * @param buffer -buffer to deserialize
   * @return -device meta data
   */
  public static TsDeviceMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
    TsDeviceMetadata deviceMetadata = new TsDeviceMetadata();

    deviceMetadata.startTime = ReadWriteIOUtils.readLong(buffer);
    deviceMetadata.endTime = ReadWriteIOUtils.readLong(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        chunkGroupMetaDataList.add(ChunkGroupMetaData.deserializeFrom(buffer));
      }
      deviceMetadata.chunkGroupMetadataList = chunkGroupMetaDataList;
    }

    deviceMetadata.reCalculateSerializedSize();
    return deviceMetadata;
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  private void reCalculateSerializedSize() {
    serializedSize = 2 * Long.BYTES + // startTime , endTime
        Integer.BYTES; // size of chunkGroupMetadataList

    for (ChunkGroupMetaData meta : chunkGroupMetadataList) {
      serializedSize += meta.getSerializedSize();
    }
  }

  /**
   * set the ChunkGroupMetadataList and recalculate serialized size.
   *
   * @param chunkGroupMetadataList -use to set the ChunkGroupMetadataList and recalculate serialized
   * size
   */
  public void setChunkGroupMetadataList(List<ChunkGroupMetaData> chunkGroupMetadataList) {
    this.chunkGroupMetadataList = chunkGroupMetadataList;
    reCalculateSerializedSize();
  }

  /**
   * add chunk group metadata to chunkGroups. THREAD NOT SAFE
   *
   * @param chunkGroup - chunk group metadata to add
   */
  public void addChunkGroupMetaData(ChunkGroupMetaData chunkGroup) {
    chunkGroupMetadataList.add(chunkGroup);
    serializedSize += chunkGroup.getSerializedSize();
    for (ChunkMetaData chunkMetaData : chunkGroup.getChunkMetaDataList()) {
      // update startTime and endTime
      startTime = Long.min(startTime, chunkMetaData.getStartTime());
      endTime = Long.max(endTime, chunkMetaData.getEndTime());
    }
  }

  public List<ChunkGroupMetaData> getChunkGroups() {
    return Collections.unmodifiableList(chunkGroupMetadataList);
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  /**
   * get the byte length of the outputStream.
   *
   * @param outputStream -outputStream to determine byte length
   * @return -byte length of the outputStream
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);

    if (chunkGroupMetadataList == null) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(chunkGroupMetadataList.size(), outputStream);
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetadataList) {
        byteLen += chunkGroupMetaData.serializeTo(outputStream);
      }
    }
    assert getSerializedSize() == byteLen;
    return byteLen;
  }

  /**
   * get the byte length of the given buffer.
   *
   * @param buffer -buffer to determine the byte length
   * @return -byte length
   */
  public int serializeTo(ByteBuffer buffer) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(startTime, buffer);
    byteLen += ReadWriteIOUtils.write(endTime, buffer);

    if (chunkGroupMetadataList == null) {
      byteLen += ReadWriteIOUtils.write(0, buffer);
    } else {
      byteLen += ReadWriteIOUtils.write(chunkGroupMetadataList.size(), buffer);
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetadataList) {
        byteLen += chunkGroupMetaData.serializeTo(buffer);
      }
    }

    return byteLen;
  }

  @Override
  public String toString() {
    return "TsDeviceMetadata{" + "serializedSize=" + serializedSize + ", startTime=" + startTime
        + ", endTime="
        + endTime + ", chunkGroupMetadataList=" + chunkGroupMetadataList + '}';
  }

}
