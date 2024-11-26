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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.file.metadata.statistics.Statistics;

import java.util.List;

public class AlignedTimeSeriesMetadata extends AbstractAlignedTimeSeriesMetadata {

  public AlignedTimeSeriesMetadata(
      TimeseriesMetadata timeseriesMetadata, List<TimeseriesMetadata> valueTimeseriesMetadataList) {
    super(timeseriesMetadata, valueTimeseriesMetadataList);
  }

  /**
   * If the vector contains only one sub sensor, just return the sub sensor's Statistics Otherwise,
   * return the Statistics of the time column.
   */
  @Override
  public Statistics getStatistics() {
    return valueTimeseriesMetadataList.size() == 1 && valueTimeseriesMetadataList.get(0) != null
        ? valueTimeseriesMetadataList.get(0).getStatistics()
        : timeseriesMetadata.getStatistics();
  }

  @Override
  public boolean timeAllSelected() {
    for (int index = 0; index < getMeasurementCount(); index++) {
      if (!hasNullValue(index)) {
        // When there is any value page point number that is the same as the time page,
        // it means that all timestamps in time page will be selected.
        return true;
      }
    }
    return false;
  }

  @Override
  void constructAlignedChunkMetadata(
      List<AlignedChunkMetadata> res,
      IChunkMetadata timeChunkMetadata,
      List<IChunkMetadata> chunkMetadataList,
      boolean exits) {
    if (exits) {
      res.add(new AlignedChunkMetadata(timeChunkMetadata, chunkMetadataList));
    }
  }
}
