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

public class AlignedTimeSeriesMetadata implements ITimeSeriesMetadata {

  // TimeSeriesMetadata for time column
  private final TimeseriesMetadata timeseriesMetadata;
  // TimeSeriesMetadata for all subSensors in the vector
  private final List<TimeseriesMetadata> valueTimeseriesMetadataList;

  private IChunkMetadataLoader chunkMetadataLoader;

  public AlignedTimeSeriesMetadata(
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
    return valueTimeseriesMetadataList.size() == 1 && valueTimeseriesMetadataList.get(0) != null
        ? valueTimeseriesMetadataList.get(0).getStatistics()
        : timeseriesMetadata.getStatistics();
  }

  public Statistics getStatistics(int index) {
    TimeseriesMetadata v = valueTimeseriesMetadataList.get(index);
    return v == null ? null : v.getStatistics();
  }

  public List<Statistics> getValueStatisticsList() {
    List<Statistics> valueStatisticsList = new ArrayList<>();
    for (TimeseriesMetadata v : valueTimeseriesMetadataList) {
      valueStatisticsList.add(v == null ? null : v.getStatistics());
    }
    return valueStatisticsList;
  }

  public Statistics getTimeStatistics() {
    return timeseriesMetadata.getStatistics();
  }

  @Override
  public boolean isModified() {
    return timeseriesMetadata.isModified();
  }

  @Override
  public void setModified(boolean modified) {
    timeseriesMetadata.setModified(modified);
    for (TimeseriesMetadata subSensor : valueTimeseriesMetadataList) {
      if (subSensor != null) {
        subSensor.setModified(modified);
      }
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
      if (subSensor != null) {
        subSensor.setSeq(seq);
      }
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
    return chunkMetadataLoader.loadChunkMetadataList(this);
  }

  public List<AlignedChunkMetadata> getCopiedChunkMetadataList() {
    List<IChunkMetadata> timeChunkMetadata = timeseriesMetadata.getCopiedChunkMetadataList();
    List<List<IChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (TimeseriesMetadata metadata : valueTimeseriesMetadataList) {
      valueChunkMetadataList.add(metadata == null ? null : metadata.getCopiedChunkMetadataList());
    }

    return getAlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
  }

  public List<AlignedChunkMetadata> getChunkMetadataList() {
    List<IChunkMetadata> timeChunkMetadata = timeseriesMetadata.getChunkMetadataList();
    List<List<IChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (TimeseriesMetadata metadata : valueTimeseriesMetadataList) {
      valueChunkMetadataList.add(metadata == null ? null : metadata.getChunkMetadataList());
    }

    return getAlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
  }

  private List<AlignedChunkMetadata> getAlignedChunkMetadata(
      List<IChunkMetadata> timeChunkMetadata, List<List<IChunkMetadata>> valueChunkMetadataList) {
    List<AlignedChunkMetadata> res = new ArrayList<>();
    for (int i = 0; i < timeChunkMetadata.size(); i++) {
      List<IChunkMetadata> chunkMetadataList = new ArrayList<>();
      // only at least one sensor exits, we add the AlignedChunkMetadata to the list
      boolean exits = false;
      for (List<IChunkMetadata> chunkMetadata : valueChunkMetadataList) {
        IChunkMetadata v =
            chunkMetadata == null
                    || chunkMetadata.get(i).getStatistics().getCount() == 0 // empty chunk
                ? null
                : chunkMetadata.get(i);
        exits = (exits || v != null);
        chunkMetadataList.add(v);
      }
      if (exits) {
        res.add(new AlignedChunkMetadata(timeChunkMetadata.get(i), chunkMetadataList));
      }
    }
    return res;
  }

  @Override
  public void setChunkMetadataLoader(IChunkMetadataLoader chunkMetadataLoader) {
    this.chunkMetadataLoader = chunkMetadataLoader;
  }

  public List<TimeseriesMetadata> getValueTimeseriesMetadataList() {
    return valueTimeseriesMetadataList;
  }

  public TimeseriesMetadata getTimeseriesMetadata() {
    return timeseriesMetadata;
  }
}
