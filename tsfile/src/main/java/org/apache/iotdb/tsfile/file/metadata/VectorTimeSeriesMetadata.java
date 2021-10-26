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

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VectorTimeSeriesMetadata implements ITimeSeriesMetadata {

  // TimeSeriesMetadata for time column
  private final TimeseriesMetadata timeseriesMetadata;
  // TimeSeriesMetadata for all subSensors in the vector
  private final List<TimeseriesMetadata> valueTimeseriesMetadataList;

  public VectorTimeSeriesMetadata(
      TimeseriesMetadata timeseriesMetadata, List<TimeseriesMetadata> valueTimeseriesMetadataList) {
    this.timeseriesMetadata = timeseriesMetadata;
    this.valueTimeseriesMetadataList = valueTimeseriesMetadataList;
  }

  /**
   * If the vector contains only one sub sensor, just return the sub sensor's Statistics Otherwise,
   * return the Statistics of the time column
   */
  @Override
  public Statistics getStatistics() {
    return valueTimeseriesMetadataList.size() == 1
        ? valueTimeseriesMetadataList.get(0).getStatistics()
        : timeseriesMetadata.getStatistics();
  }

  public Statistics getStatistics(int index) {
    return valueTimeseriesMetadataList.get(index).getStatistics();
  }

  @Override
  public boolean isModified() {
    return timeseriesMetadata.isModified();
  }

  @Override
  public void setModified(boolean modified) {
    timeseriesMetadata.setModified(modified);
    for (TimeseriesMetadata subSensor : valueTimeseriesMetadataList) {
      subSensor.setModified(modified);
    }
  }

  @Override
  public boolean isSeq() {
    return timeseriesMetadata.isSeq();
  }

  @Override
  public void setSeq(boolean seq) {
    timeseriesMetadata.setSeq(seq);
    for (TimeseriesMetadata subSensor : valueTimeseriesMetadataList) {
      subSensor.setSeq(seq);
    }
  }

  /**
   * If the chunkMetadataLoader is MemChunkMetadataLoader, the VectorChunkMetadata is already
   * assembled while constructing the in-memory TsFileResource, so we just return the assembled
   * VectorChunkMetadata list.
   *
   * <p>Otherwise, we need to assemble the ChunkMetadata of time column and the ChunkMetadata of all
   * the subSensors to generate the VectorChunkMetadata
   */
  @Override
  public List<IChunkMetadata> loadChunkMetadataList() throws IOException {
    if (timeseriesMetadata.getChunkMetadataLoader().isMemChunkMetadataLoader()) {
      return timeseriesMetadata.loadChunkMetadataList();
    } else {
      List<IChunkMetadata> timeChunkMetadata = timeseriesMetadata.loadChunkMetadataList();
      List<List<IChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
      for (TimeseriesMetadata metadata : valueTimeseriesMetadataList) {
        valueChunkMetadataList.add(metadata.loadChunkMetadataList());
      }

      List<IChunkMetadata> res = new ArrayList<>();

      for (int i = 0; i < timeChunkMetadata.size(); i++) {
        List<IChunkMetadata> chunkMetadataList = new ArrayList<>();
        for (List<IChunkMetadata> chunkMetadata : valueChunkMetadataList) {
          chunkMetadataList.add(chunkMetadata.get(i));
        }
        res.add(new VectorChunkMetadata(timeChunkMetadata.get(i), chunkMetadataList));
      }
      return res;
    }
  }

  @Override
  public List<IChunkMetadata> getChunkMetadataList() {
    return null;
  }

  @Override
  public void setChunkMetadataLoader(IChunkMetadataLoader chunkMetadataLoader) {
    timeseriesMetadata.setChunkMetadataLoader(chunkMetadataLoader);
  }

  public List<TimeseriesMetadata> getValueTimeseriesMetadataList() {
    return valueTimeseriesMetadataList;
  }

  public TimeseriesMetadata getTimeseriesMetadata() {
    return timeseriesMetadata;
  }
}
