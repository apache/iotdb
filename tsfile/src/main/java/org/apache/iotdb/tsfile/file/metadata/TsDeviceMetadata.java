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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TsDeviceMetadata {

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
    // allowed to clair an empty TsDeviceMetadata whose fields will be assigned later.
  }


  /**
   * deserialize from the given buffer.
   *
   * @param buffer -buffer to deserialize
   * @return -device meta data
   */
  public static TsDeviceMetadata deserializeFrom(ByteBuffer buffer) {
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

    return deviceMetadata;
  }

  /**
   * add chunk group metadata to chunkGroups. THREAD NOT SAFE
   *
   * @param chunkGroup - chunk group metadata to add
   */
  public void addChunkGroupMetaData(ChunkGroupMetaData chunkGroup) {
    chunkGroupMetadataList.add(chunkGroup);
    for (ChunkMetaData chunkMetaData : chunkGroup.getChunkMetaDataList()) {
      // update startTime and endTime
      startTime = Long.min(startTime, chunkMetaData.getStartTime());
      endTime = Long.max(endTime, chunkMetaData.getEndTime());
    }
  }

  public List<ChunkGroupMetaData> getChunkGroupMetaDataList() {
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
    return byteLen;
  }


  @Override
  public String toString() {
    return "TsDeviceMetadata{" + " startTime=" + startTime
        + ", endTime="
        + endTime + ", chunkGroupMetadataList=" + chunkGroupMetadataList + '}';
  }

}
