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
package org.apache.iotdb.db.engine.overflow.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Metadata of overflow series list.
 */
public class OFSeriesListMetadata {

  private String measurementId;
  private List<ChunkMetaData> timeSeriesList;

  private OFSeriesListMetadata() {
  }

  public OFSeriesListMetadata(String measurementId, List<ChunkMetaData> timeSeriesList) {
    this.measurementId = measurementId;
    this.timeSeriesList = timeSeriesList;
  }

  /**
   * function for deserializing data from input stream.
   */
  public static OFSeriesListMetadata deserializeFrom(InputStream inputStream) throws IOException {
    OFSeriesListMetadata ofSeriesListMetadata = new OFSeriesListMetadata();
    ofSeriesListMetadata.measurementId = ReadWriteIOUtils.readString(inputStream);
    int size = ReadWriteIOUtils.readInt(inputStream);
    List<ChunkMetaData> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      ChunkMetaData chunkMetaData = ChunkMetaData.deserializeFrom(inputStream);
      list.add(chunkMetaData);
    }
    ofSeriesListMetadata.timeSeriesList = list;
    return ofSeriesListMetadata;
  }

  public static OFSeriesListMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
    throw new NotImplementedException();
  }

  /**
   * add TimeSeriesChunkMetaData to timeSeriesList.
   */
  public void addSeriesMetaData(ChunkMetaData timeSeries) {
    if (timeSeriesList == null) {
      timeSeriesList = new ArrayList<ChunkMetaData>();
    }
    timeSeriesList.add(timeSeries);
  }

  public List<ChunkMetaData> getMetaDatas() {
    return timeSeriesList == null ? null : Collections.unmodifiableList(timeSeriesList);
  }

  @Override
  public String toString() {
    return String.format("OFSeriesListMetadata{ measurementId id: %s, series: %s }", measurementId,
        timeSeriesList.toString());
  }

  public String getMeasurementId() {
    return measurementId;
  }

  /**
   * function for serializing data to output stream.
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);
    byteLen += ReadWriteIOUtils.write(timeSeriesList.size(), outputStream);
    for (ChunkMetaData chunkMetaData : timeSeriesList) {
      byteLen += chunkMetaData.serializeTo(outputStream);
    }
    return byteLen;
  }

  public int serializeTo(ByteBuffer buffer) throws IOException {
    throw new NotImplementedException();
  }
}
