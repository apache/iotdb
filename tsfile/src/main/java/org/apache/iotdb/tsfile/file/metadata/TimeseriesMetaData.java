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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class TimeseriesMetaData {

  private long startOffsetOfChunkMetaDataList;
  private int chunkMetaDataListDataSize;

  private String measurementId;
  private List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();

  public TimeseriesMetaData() {

  }

  public TimeseriesMetaData(String measurementId, List<ChunkMetaData> chunkMetaDataList) {
    this.measurementId = measurementId;
    this.chunkMetaDataList = chunkMetaDataList;
  }

  public static TimeseriesMetaData deserializeFrom(ByteBuffer buffer) {
    TimeseriesMetaData timeseriesMetaData = new TimeseriesMetaData();
    timeseriesMetaData.setMeasurementId(ReadWriteIOUtils.readString(buffer));
    timeseriesMetaData.setOffsetOfChunkMetaDataList(ReadWriteIOUtils.readLong(buffer));
    timeseriesMetaData.setDataSizeOfChunkMetaDataList(ReadWriteIOUtils.readInt(buffer));
    return timeseriesMetaData;
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
    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);
    byteLen += ReadWriteIOUtils.write(startOffsetOfChunkMetaDataList, outputStream);
    byteLen += ReadWriteIOUtils.write(chunkMetaDataListDataSize, outputStream);
    return byteLen;
  }

  public void addChunkMeteData(ChunkMetaData chunkMetaData) {
    chunkMetaDataList.add(chunkMetaData);
  }

  public List<ChunkMetaData> getChunkMetaDataList() {
    return chunkMetaDataList;
  }

  public void setChunkMetaDataList(List<ChunkMetaData> chunkMetaDataList) {
    this.chunkMetaDataList = chunkMetaDataList;
  }

  public long getOffsetOfChunkMetaDataList() {
    return startOffsetOfChunkMetaDataList;
  }

  public void setOffsetOfChunkMetaDataList(long position) {
    this.startOffsetOfChunkMetaDataList = position;
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  public int getDataSizeOfChunkMetaDataList() {
    return chunkMetaDataListDataSize;
  }

  public void setDataSizeOfChunkMetaDataList(int size) {
    this.chunkMetaDataListDataSize = size;
  }

}
